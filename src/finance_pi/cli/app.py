from __future__ import annotations

import importlib.util
import platform
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import typer

from finance_pi.backtest import BacktestConfig, BacktestEngine
from finance_pi.calendar import TradingCalendar
from finance_pi.config import ProjectPaths, RuntimeSettings, diagnose_dotenv
from finance_pi.factors import ParquetFactorContext, factor_registry
from finance_pi.http import HttpJsonClient, SourceApiError
from finance_pi.ingest import IngestOrchestrator
from finance_pi.reports.data_quality import build_data_quality_report
from finance_pi.reports.fraud import build_fraud_report
from finance_pi.sources.kis import (
    KisAuthClient,
    KisDailyAdapter,
    KisDailyPriceClient,
    KisTokenCache,
    KisUniverseDailyAdapter,
)
from finance_pi.sources.krx import KrxDailyAdapter
from finance_pi.sources.naver import (
    NaverDailyBackfillAdapter,
    NaverDailyPriceClient,
    NaverFinanceClient,
    NaverSummaryAdapter,
)
from finance_pi.sources.opendart import (
    DartCompanyAdapter,
    DartFilingsAdapter,
    DartFinancialsAdapter,
    OpenDartClient,
)
from finance_pi.storage import CatalogBuilder, DataLakeLayout, dataset_registry
from finance_pi.storage.parquet import ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_daily_prices_adj,
    build_financials_silver,
    build_fundamentals_pit,
    build_security_master,
    build_silver_prices,
    build_universe_history,
)

app = typer.Typer(help="finance-pi command line tools")
catalog_app = typer.Typer(help="DuckDB catalog commands")
factors_app = typer.Typer(help="Factor library commands")
ingest_app = typer.Typer(help="Source ingest commands")
build_app = typer.Typer(help="Bronze to Silver/Gold build commands")
reports_app = typer.Typer(help="Report generation commands")
backtest_app = typer.Typer(help="Backtest commands")
app.add_typer(catalog_app, name="catalog")
app.add_typer(factors_app, name="factors")
app.add_typer(ingest_app, name="ingest")
app.add_typer(build_app, name="build")
app.add_typer(reports_app, name="reports")
app.add_typer(backtest_app, name="backtest")


@app.command("doctor")
def doctor(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    """Print a small server readiness report."""

    paths = ProjectPaths(root=root)
    typer.echo(f"Python: {sys.version.split()[0]}")
    typer.echo(f"Platform: {platform.platform()}")
    typer.echo(f"Machine: {platform.machine()}")
    typer.echo(f"Workspace: {paths.root.resolve()}")
    typer.echo(f"Data root: {paths.data_root.resolve()}")

    for module in ["polars", "duckdb", "pydantic", "typer", "httpx"]:
        status = "OK" if importlib.util.find_spec(module) else "MISSING"
        typer.echo(f"{module}: {status}")

    settings = RuntimeSettings.load(paths.root)
    typer.echo(f"KRX key: {'configured' if settings.has_krx else 'missing'}")
    typer.echo(f"OpenDART key: {'configured' if settings.has_opendart else 'missing'}")
    typer.echo(f"KIS key/token: {'configured' if settings.has_kis else 'missing'}")
    _print_dotenv_issues(paths.root)

    if platform.machine() in {"armv7l", "armv6l"}:
        typer.echo("Warning: 32-bit Raspberry Pi OS is not recommended for DuckDB/Polars.")


@app.command("check-krx")
def check_krx(
    check_date: str,
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    """Call KRX once per configured market and print authorization diagnostics."""

    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    if not settings.krx_openapi_key:
        raise typer.BadParameter("KRX_OPENAPI_KEY is required")

    client = _krx_client(settings)
    date_value = _parse_report_date(check_date)
    for market, path in _krx_market_paths(settings).items():
        try:
            payload = client.get_json(path, params={"basDd": date_value.strftime("%Y%m%d")})
            rows = payload.get("OutBlock_1", [])
            count = len(rows) if isinstance(rows, list) else 0
            typer.echo(f"{market}: OK rows={count} path={path}")
        except Exception as exc:  # noqa: BLE001
            typer.echo(f"{market}: FAIL path={path}")
            typer.echo(str(exc))
            typer.echo(
                "KRX 401 usually means the key is expired, whitespace-corrupted, or this "
                "specific stock API service was not approved in the KRX Open API portal."
            )
            raise typer.Exit(code=1) from exc


@app.command("check-kis")
def check_kis(
    ticker: str = typer.Argument("005930", help="Six-digit ticker"),
    check_date: str | None = typer.Argument(None, help="Date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    """Issue or reuse a KIS token and fetch one daily OHLCV sample."""

    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    _print_dotenv_issues(paths.root)
    date_value = _parse_report_date(check_date)
    try:
        rows = _kis_client(settings, paths).fetch_daily_prices(
            ticker.zfill(6),
            date_value,
            date_value,
        )
        typer.echo(
            f"KIS: OK ticker={ticker.zfill(6)} date={date_value.isoformat()} rows={len(rows)}"
        )
    except Exception as exc:  # noqa: BLE001
        typer.echo("KIS: FAIL")
        typer.echo(_kis_error_message(exc))
        raise typer.Exit(code=1) from exc


@app.command("init")
def init_workspace(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    paths = ProjectPaths(root=root)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    typer.echo(f"Initialized data lake at {paths.data_root}")


@catalog_app.command("build")
def build_catalog(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    paths = ProjectPaths(root=root)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    created = CatalogBuilder(paths.data_root, paths.catalog_path).build()
    typer.echo(f"Built {paths.catalog_path}")
    for name in created:
        typer.echo(f"  {name}")


@catalog_app.command("list")
def list_catalog_views() -> None:
    for name in sorted(dataset_registry):
        typer.echo(name)


@factors_app.command("list")
def list_factors() -> None:
    for name in factor_registry.names():
        cls = factor_registry.get(name)
        requires = ", ".join(cls.requires)
        typer.echo(f"{name}\t{cls.rebalance}\t{requires}")


@ingest_app.command("krx")
def ingest_krx(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    if not settings.krx_openapi_key:
        raise typer.BadParameter("KRX_OPENAPI_KEY is required")
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = KrxDailyAdapter(
        layout=layout,
        writer=ParquetDatasetWriter(),
        client=_krx_client(settings),
        daily_path=settings.krx_stock_daily_path,
        market_paths=_krx_market_paths(settings),
    )
    _print_results(
        IngestOrchestrator([adapter]).run(_parse_report_date(since), _parse_report_date(until))
    )


@ingest_app.command("dart-company")
def ingest_dart_company(
    snapshot_date: str | None = typer.Option(None, help="Snapshot date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    client = _opendart_client(settings)
    parsed = _parse_report_date(snapshot_date)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = DartCompanyAdapter(layout, ParquetDatasetWriter(), client, parsed)
    _print_results(IngestOrchestrator([adapter]).run(parsed, parsed))


@ingest_app.command("dart-filings")
def ingest_dart_filings(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    client = _opendart_client(settings)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = DartFilingsAdapter(layout, ParquetDatasetWriter(), client)
    _print_results(
        IngestOrchestrator([adapter]).run(_parse_report_date(since), _parse_report_date(until))
    )


@ingest_app.command("dart-financials")
def ingest_dart_financials(
    corp_code: str = typer.Option(..., help="DART corp_code"),
    year: int = typer.Option(..., help="Business year"),
    report_code: str = typer.Option("11011", help="11011 annual, 11012 half, 11013 Q1, 11014 Q3"),
    available_date: str | None = typer.Option(None, help="Available date as YYYY-MM-DD"),
    fs_div: str = typer.Option("CFS", help="CFS or OFS"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    client = _opendart_client(settings)
    parsed_available = _parse_report_date(available_date)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = DartFinancialsAdapter(
        layout,
        ParquetDatasetWriter(),
        client,
        corp_code,
        year,
        report_code,
        parsed_available,
        fs_div,
    )
    _print_results(IngestOrchestrator([adapter]).run(parsed_available, parsed_available))


@ingest_app.command("kis")
def ingest_kis(
    ticker: str = typer.Option(..., help="Six-digit ticker"),
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = KisDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        _kis_client(settings, paths),
        ticker,
    )
    _print_results(
        IngestOrchestrator([adapter]).run(_parse_report_date(since), _parse_report_date(until))
    )


@ingest_app.command("kis-universe")
def ingest_kis_universe(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    limit: int | None = typer.Option(None, help="Limit ticker count for a small smoke run"),
    chunk_days: int = typer.Option(90, help="Date range days per KIS round"),
    sleep_seconds: float = typer.Option(0.05, help="Sleep between ticker calls"),
    ticker_batch_size: int = typer.Option(50, help="Tickers per incremental KIS write"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    tickers = _latest_dart_tickers(paths, limit=limit)
    adapter = KisUniverseDailyAdapter(
        layout=DataLakeLayout(paths.data_root),
        writer=ParquetDatasetWriter(),
        client=_kis_client(settings, paths),
        tickers=tickers,
        chunk_days=chunk_days,
        sleep_seconds=sleep_seconds,
        ticker_batch_size=ticker_batch_size,
    )
    _run_and_print([adapter], _parse_report_date(since), _parse_report_date(until))


@ingest_app.command("naver-daily")
def ingest_naver_daily(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    limit: int | None = typer.Option(None, help="Limit ticker count for a small smoke run"),
    chunk_days: int = typer.Option(3650, help="Date range days per Naver request round"),
    ticker_batch_size: int = typer.Option(50, help="Tickers per incremental Naver write"),
    sleep_seconds: float = typer.Option(0.02, help="Sleep between ticker calls"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    tickers = _latest_dart_tickers(paths, limit=limit)
    adapter = NaverDailyBackfillAdapter(
        layout=DataLakeLayout(paths.data_root),
        writer=ParquetDatasetWriter(),
        client=_naver_daily_client(settings),
        tickers=tickers,
        chunk_days=chunk_days,
        ticker_batch_size=ticker_batch_size,
        sleep_seconds=sleep_seconds,
    )
    _run_and_print([adapter], _parse_report_date(since), _parse_report_date(until))


@ingest_app.command("naver-summary")
def ingest_naver_summary(
    snapshot_date: str | None = typer.Option(None, help="Snapshot date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    markets: str = typer.Option("KOSPI,KOSDAQ", help="Comma-separated KOSPI,KOSDAQ"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    parsed = _parse_report_date(snapshot_date)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = NaverSummaryAdapter(
        layout,
        ParquetDatasetWriter(),
        _naver_client(settings),
        parsed,
        _parse_markets(markets),
    )
    _print_results(IngestOrchestrator([adapter]).run(parsed, parsed))


@build_app.command("all")
def build_everything(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_all(ProjectPaths(root=root).data_root))


@app.command("bootstrap")
def bootstrap(
    since: str = typer.Option(..., help="Initial backfill start date as YYYY-MM-DD"),
    until: str | None = typer.Option(None, help="Initial backfill end date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    skip_kis: bool = typer.Option(False, help="Skip KIS price backfill"),
    skip_krx: bool = typer.Option(True, help="KRX is disabled by default"),
    skip_dart_company: bool = typer.Option(False, help="Skip DART corpCode snapshot"),
    naver_summary: bool = typer.Option(
        False,
        "--naver-summary/--no-naver-summary",
        help="Fetch current Naver summary for the bootstrap end date",
    ),
    price_source: str = typer.Option("naver", help="Price source: naver, kis, or both"),
    ticker_limit: int | None = typer.Option(None, help="Limit tickers for smoke bootstrap"),
    chunk_days: int = typer.Option(90, help="Date range days per KIS round"),
    naver_chunk_days: int = typer.Option(3650, help="Date range days per Naver round"),
    ticker_batch_size: int = typer.Option(50, help="Tickers per incremental price write"),
    sleep_seconds: float = typer.Option(0.05, help="Sleep between KIS ticker calls"),
    strict: bool = typer.Option(True, help="Fail if a configured live source fails"),
) -> None:
    """Run the first real bootstrap: source backfill, transforms, catalog, reports."""

    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    start = _parse_report_date(since)
    end = _parse_report_date(until) if until else _previous_weekday(date.today())
    source_choice = _parse_price_source(price_source)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    failures: list[str] = []

    if not skip_krx:
        if settings.has_krx:
            try:
                ingest_krx(start.isoformat(), end.isoformat(), paths.root)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"KRX backfill failed: {exc}")
        else:
            failures.append("KRX backfill skipped: KRX_OPENAPI_KEY missing")

    if not skip_dart_company:
        if settings.has_opendart:
            try:
                ingest_dart_company(end.isoformat(), paths.root)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"OpenDART company ingest failed: {exc}")
        else:
            failures.append("OpenDART company ingest skipped: OPENDART_API_KEY missing")

    if naver_summary:
        try:
            ingest_naver_summary(end.isoformat(), paths.root, "KOSPI,KOSDAQ")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Naver summary ingest failed: {exc}")

    if source_choice in {"naver", "both"}:
        try:
            ingest_naver_daily(
                start.isoformat(),
                end.isoformat(),
                paths.root,
                ticker_limit,
                naver_chunk_days,
                ticker_batch_size,
                0.02,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Naver daily price ingest failed: {exc}")

    if source_choice in {"kis", "both"} and not skip_kis:
        if settings.has_kis:
            try:
                ingest_kis_universe(
                    start.isoformat(),
                    end.isoformat(),
                    paths.root,
                    ticker_limit,
                    chunk_days,
                    sleep_seconds,
                    ticker_batch_size,
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"KIS universe price ingest failed: {exc}")
        else:
            failures.append("KIS price ingest skipped: KIS_APP_KEY/KIS_APP_SECRET missing")

    for message in failures:
        typer.echo(message)
    if strict and failures:
        raise typer.Exit(code=1)

    _print_summaries(build_all(paths.data_root))
    created = CatalogBuilder(paths.data_root, paths.catalog_path).build()
    typer.echo(f"Catalog: {paths.catalog_path}")
    typer.echo(f"Views: {len(created)}")


@build_app.command("silver-prices")
def build_cmd_silver_prices(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_silver_prices(ProjectPaths(root=root).data_root))


@build_app.command("identity")
def build_cmd_identity(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_security_master(ProjectPaths(root=root).data_root))


@build_app.command("universe")
def build_cmd_universe(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_universe_history(ProjectPaths(root=root).data_root))


@build_app.command("gold-prices")
def build_cmd_gold_prices(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_daily_prices_adj(ProjectPaths(root=root).data_root))


@build_app.command("financials")
def build_cmd_financials(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_financials_silver(ProjectPaths(root=root).data_root))


@build_app.command("fundamentals-pit")
def build_cmd_fundamentals_pit(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_fundamentals_pit(ProjectPaths(root=root).data_root))


@backtest_app.command("run")
def run_backtest(
    factor_name: str = typer.Option(..., "--factor", help="Registered factor name"),
    start: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    top_fraction: float = typer.Option(0.1, help="Selection fraction"),
) -> None:
    paths = ProjectPaths(root=root)
    prices_path = paths.data_root / "gold" / "daily_prices_adj" / "dt=*" / "part.parquet"
    price_files = sorted(paths.data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"))
    if not price_files:
        raise typer.BadParameter(f"No gold price data at {prices_path}")
    dates = (
        pl.read_parquet([path.as_posix() for path in price_files], hive_partitioning=True)
        .select("date")
        .unique()
        .to_series()
        .to_list()
    )
    calendar = TradingCalendar.from_dates(dates)
    factor = factor_registry.get(factor_name)()
    result = BacktestEngine(calendar).run(
        factor,
        ParquetFactorContext(paths.data_root),
        BacktestConfig(
            start=_parse_report_date(start),
            end=_parse_report_date(end),
            top_fraction=top_fraction,
        ),
    )
    output = paths.data_root / "backtests" / f"{factor_name}_{start}_{end}"
    output.mkdir(parents=True, exist_ok=True)
    result.nav.write_parquet(output / "nav.parquet")
    result.positions.write_parquet(output / "positions.parquet")
    result.ledger.write_parquet(output / "ledger.parquet")
    typer.echo(output)


@reports_app.command("dq")
def generate_dq_report(
    report_date: str | None = typer.Option(None, help="Report date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    parsed_date = _parse_report_date(report_date)
    paths = ProjectPaths(root=root)
    path = paths.data_root / "reports" / "data_quality" / f"{parsed_date.isoformat()}.html"
    build_data_quality_report(paths.data_root, parsed_date).write(path)
    typer.echo(path)


@reports_app.command("fraud")
def generate_fraud_report(
    report_date: str | None = typer.Option(None, help="Report date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    parsed_date = _parse_report_date(report_date)
    paths = ProjectPaths(root=root)
    path = paths.data_root / "reports" / "backtest_fraud" / f"{parsed_date.isoformat()}.html"
    build_fraud_report(paths.data_root, parsed_date).write(path)
    typer.echo(path)


@app.command("daily")
def run_daily(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    report_date: str | None = typer.Option(None, help="Report date as YYYY-MM-DD"),
    ingest: bool = typer.Option(True, help="Attempt live source ingest when keys are configured"),
    strict: bool = typer.Option(True, help="Fail if a configured live source fails"),
) -> None:
    """Run the daily local maintenance job.

    KIS is the default daily price source. Naver summary snapshots enrich KIS
    rows with market cap/share count. OpenDART filings are ingested when
    configured, then Silver/Gold datasets, the DuckDB catalog, and reports are
    rebuilt.
    """

    parsed_date = _parse_report_date(report_date)
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    if ingest:
        failures = _run_daily_ingest(paths, settings, parsed_date)
        for failure in failures:
            typer.echo(failure)
        if strict and failures:
            raise typer.Exit(code=1)
    summaries = build_all(paths.data_root)
    created = CatalogBuilder(paths.data_root, paths.catalog_path).build()

    dq_path = paths.data_root / "reports" / "data_quality" / f"{parsed_date.isoformat()}.html"
    fraud_path = (
        paths.data_root / "reports" / "backtest_fraud" / f"{parsed_date.isoformat()}.html"
    )
    build_data_quality_report(paths.data_root, parsed_date).write(dq_path)
    build_fraud_report(paths.data_root, parsed_date).write(fraud_path)

    for summary in summaries:
        typer.echo(f"Build: {summary.dataset} rows={summary.rows} files={summary.files}")
    typer.echo(f"Catalog: {paths.catalog_path}")
    typer.echo(f"Views: {len(created)}")
    typer.echo(f"Data quality report: {dq_path}")
    typer.echo(f"Fraud report: {fraud_path}")


def _parse_report_date(value: str | None) -> date:
    if value is None:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("expected YYYY-MM-DD") from exc


def _opendart_client(settings: RuntimeSettings) -> OpenDartClient:
    if not settings.opendart_api_key:
        raise typer.BadParameter("OPENDART_API_KEY is required")
    return OpenDartClient(
        settings.opendart_api_key,
        HttpJsonClient("opendart", settings.opendart_base_url),
    )


def _kis_client(settings: RuntimeSettings, paths: ProjectPaths) -> KisDailyPriceClient:
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise typer.BadParameter("KIS_APP_KEY and KIS_APP_SECRET are required")
    if settings.kis_access_token:
        token = settings.kis_access_token
    else:
        cache = KisTokenCache(paths.data_root / "_cache" / "kis" / "token.json")
        cached = cache.read(settings.kis_app_key)
        if cached is not None:
            token = cached.access_token
        else:
            try:
                issued = KisAuthClient(
                    settings.kis_base_url,
                    settings.kis_app_key,
                    settings.kis_app_secret,
                ).issue_token()
                cache.write(settings.kis_app_key, issued)
                token = issued.access_token
            except SourceApiError as exc:
                raise SourceApiError("kis", _kis_error_message(exc)) from exc
    return KisDailyPriceClient(
        HttpJsonClient("kis", settings.kis_base_url),
        settings.kis_app_key,
        settings.kis_app_secret,
        token,
    )


def _naver_client(settings: RuntimeSettings) -> NaverFinanceClient:
    return NaverFinanceClient(
        HttpJsonClient("naver", settings.naver_finance_base_url),
        settings.naver_finance_user_agent,
    )


def _naver_daily_client(settings: RuntimeSettings) -> NaverDailyPriceClient:
    return NaverDailyPriceClient(
        HttpJsonClient("naver", settings.naver_finance_api_base_url),
        settings.naver_finance_user_agent,
    )


def _krx_client(settings: RuntimeSettings) -> HttpJsonClient:
    if not settings.krx_openapi_key:
        raise typer.BadParameter("KRX_OPENAPI_KEY is required")
    return HttpJsonClient(
        "krx",
        settings.krx_base_url,
        default_headers={
            "AUTH_KEY": settings.krx_openapi_key.strip(),
            "Accept": "application/json",
        },
    )


def _krx_market_paths(settings: RuntimeSettings) -> dict[str, str]:
    all_paths = {
        "KOSPI": settings.krx_kospi_daily_path,
        "KOSDAQ": settings.krx_kosdaq_daily_path,
    }
    return {market: all_paths[market] for market in settings.krx_markets if market in all_paths}


def _run_daily_ingest(
    paths: ProjectPaths,
    settings: RuntimeSettings,
    report_date: date,
) -> list[str]:
    price_date = _previous_weekday(report_date)
    filing_start = _previous_weekday(report_date - timedelta(days=1))
    failures: list[str] = []
    if settings.has_opendart:
        try:
            ingest_dart_company(report_date.isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART company ingest failed: {exc}")
    else:
        typer.echo("OpenDART company/filings ingest skipped: OPENDART_API_KEY missing")

    try:
        ingest_naver_summary(price_date.isoformat(), paths.root, "KOSPI,KOSDAQ")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"Naver summary ingest failed: {exc}")

    if settings.has_kis:
        try:
            ingest_kis_universe(
                price_date.isoformat(),
                price_date.isoformat(),
                paths.root,
                None,
                1,
                0.05,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"KIS universe price ingest failed: {exc}")
    else:
        typer.echo("KIS price ingest skipped: KIS_APP_KEY/KIS_APP_SECRET missing")

    if settings.has_opendart:
        try:
            ingest_dart_filings(filing_start.isoformat(), report_date.isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART filings ingest failed: {exc}")
    return failures


def _latest_dart_tickers(paths: ProjectPaths, limit: int | None = None) -> tuple[str, ...]:
    files = sorted(paths.data_root.glob("bronze/dart_company/snapshot_dt=*/part.parquet"))
    if not files:
        raise typer.BadParameter("No DART company data. Run ingest dart-company first.")
    frame = pl.read_parquet([path.as_posix() for path in files], hive_partitioning=True)
    if frame.is_empty():
        raise typer.BadParameter("DART company dataset is empty.")
    latest = frame["snapshot_dt"].max()
    tickers = (
        frame.filter((pl.col("snapshot_dt") == latest) & pl.col("stock_code").is_not_null())
        .select(pl.col("stock_code").cast(pl.String).str.zfill(6).alias("ticker"))
        .unique()
        .sort("ticker")
        .to_series()
        .to_list()
    )
    if limit is not None:
        tickers = tickers[:limit]
    if not tickers:
        raise typer.BadParameter("No stock_code values found in latest DART company snapshot.")
    typer.echo(f"KIS universe tickers: {len(tickers)} from DART snapshot {latest}")
    return tuple(tickers)


def _parse_markets(value: str) -> tuple[str, ...]:
    markets = tuple(market.strip().upper() for market in value.split(",") if market.strip())
    invalid = [market for market in markets if market not in {"KOSPI", "KOSDAQ"}]
    if invalid:
        raise typer.BadParameter(f"unsupported markets: {', '.join(invalid)}")
    if not markets:
        raise typer.BadParameter("at least one market is required")
    return markets


def _parse_price_source(value: str) -> str:
    source = value.strip().lower()
    if source not in {"naver", "kis", "both"}:
        raise typer.BadParameter("price_source must be one of: naver, kis, both")
    return source


def _previous_weekday(value: date) -> date:
    current = value
    if current.weekday() >= 5:
        current -= timedelta(days=current.weekday() - 4)
    return current


def _print_results(results) -> None:
    for result in results:
        _print_result(result)


def _run_and_print(adapters, since: date, until: date) -> None:
    for result in IngestOrchestrator(adapters).run_iter(since, until):
        _print_result(result)


def _print_result(result) -> None:
    status = "skipped" if result.skipped else "wrote"
    suffix = f" ({result.reason})" if result.reason else ""
    typer.echo(f"{status}: {result.path} rows={result.rows}{suffix}")


def _print_summaries(summaries) -> None:
    for summary in summaries:
        typer.echo(f"{summary.dataset}\trows={summary.rows}\tfiles={summary.files}")


def _print_dotenv_issues(root: Path) -> None:
    for issue in diagnose_dotenv(root / ".env"):
        typer.echo(f".env warning line {issue.line_no}: {issue.message}")


def _kis_error_message(exc: Exception) -> str:
    message = str(exc)
    base = message.removeprefix("kis: ")
    lowered = base.lower()
    hints: list[str] = []
    if (
        ("egw00105" in lowered or "appsecret" in lowered)
        and "kis_app_secret was rejected" not in lowered
    ):
        hints.append(
            "hint: KIS_APP_SECRET was rejected. Copy the AppSecret exactly as a single "
            "line in .env; remove any wrapped secret fragments on following lines."
        )
    if (
        ("egw00103" in lowered or "appkey" in lowered)
        and "check kis_app_key" not in lowered
    ):
        hints.append("hint: check KIS_APP_KEY and whether the app is approved in KIS Open API.")
    if "egw00133" in lowered and "token issuance" not in lowered:
        hints.append(
            "hint: KIS limits token issuance to about once per minute. Wait 60 seconds "
            "once; successful future runs reuse data/_cache/kis/token.json."
        )
    if (
        ("invalid token" in lowered or "기간이 만료" in base)
        and "fresh token" not in lowered
    ):
        hints.append("hint: remove KIS_ACCESS_TOKEN and let finance-pi issue a fresh token.")
    return "\n".join([base, *hints])


if __name__ == "__main__":
    app()
