from __future__ import annotations

import importlib.util
import json
import platform
import re
import sys
from contextlib import suppress
from datetime import date, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

import polars as pl
import typer

from finance_pi.admin import run_admin as run_admin_server
from finance_pi.backtest import BacktestConfig, BacktestEngine
from finance_pi.calendar import TradingCalendar
from finance_pi.config import ProjectPaths, RuntimeSettings, diagnose_dotenv
from finance_pi.docs_site import build_docs_site
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
    DartFinancialsBulkAdapter,
    OpenDartClient,
)
from finance_pi.storage import CatalogBuilder, DataLakeLayout, dataset_registry
from finance_pi.storage.parquet import ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_all_iter,
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
docs_app = typer.Typer(help="Documentation publishing commands")
app.add_typer(catalog_app, name="catalog")
app.add_typer(factors_app, name="factors")
app.add_typer(ingest_app, name="ingest")
app.add_typer(build_app, name="build")
app.add_typer(reports_app, name="reports")
app.add_typer(backtest_app, name="backtest")
app.add_typer(docs_app, name="docs")


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


@app.command("admin")
def admin_server(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8400, help="Bind port"),
    token: str | None = typer.Option(
        None,
        help="Admin token. Defaults to FINANCE_PI_ADMIN_TOKEN or a generated token.",
    ),
) -> None:
    """Run the local web admin for the Raspberry Pi server."""

    run_admin_server(root, host, port, token)


@app.command("check-admin")
def check_admin(
    url: str = typer.Argument("http://127.0.0.1:8400", help="Admin base URL"),
    token: str | None = typer.Option(None, help="Optional token to also check /api/overview"),
    check_docs: bool = typer.Option(False, help="Also verify that /docs/ is published"),
) -> None:
    """Verify that a finance-pi admin server is reachable."""

    base_url = url.rstrip("/")
    health = _http_get_json(f"{base_url}/api/health")
    typer.echo(f"health: {health.get('status')} workspace={health.get('workspace')}")
    if token:
        overview = _http_get_json(f"{base_url}/api/overview", token=token)
        datasets = overview.get("datasets", [])
        ready = len([dataset for dataset in datasets if dataset.get("files", 0) > 0])
        typer.echo(f"overview: OK datasets={ready}/{len(datasets)}")
    if check_docs:
        docs_html = _http_get_text(f"{base_url}/docs/")
        if "finance-pi Documentation" not in docs_html:
            raise typer.BadParameter("admin /docs/ responded but did not look like docs HTML")
        typer.echo("docs: OK")


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


@docs_app.command("build")
def build_docs(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    output: Path | None = typer.Option(None, help="Output directory"),
) -> None:
    """Build and publish repository markdown docs as an HTML site."""

    paths = ProjectPaths(root=root)
    summary = build_docs_site(paths.root, output)
    typer.echo(f"Docs: {summary.index_path} pages={summary.pages}")


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
    chunk_days: int = typer.Option(7, help="Date range days per resumable DART filing chunk"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    client = _opendart_client(settings)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = DartFilingsAdapter(layout, ParquetDatasetWriter(), client, chunk_days)
    _run_and_print([adapter], _parse_report_date(since), _parse_report_date(until))


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


@ingest_app.command("dart-financials-bulk")
def ingest_dart_financials_bulk(
    since: str = typer.Option(..., help="Start filing date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End filing date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    report_codes: str = typer.Option(
        "11011",
        help="Comma-separated DART report codes; use 11013,11012,11014,11011 for quarterly",
    ),
    corp_limit: int | None = typer.Option(None, help="Limit unique corp_code count for smoke runs"),
    batch_size: int = typer.Option(25, help="Financial API calls per resumable chunk"),
    sleep_seconds: float = typer.Option(0.05, help="Sleep between OpenDART financial calls"),
    fs_div: str = typer.Option("CFS", help="CFS or OFS"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    start = _parse_report_date(since)
    end = _parse_report_date(until)
    try:
        requests = _dart_financial_requests(
            paths,
            start,
            end,
            _parse_report_codes(report_codes),
            corp_limit,
        )
    except typer.BadParameter as exc:
        typer.echo(f"OpenDART financial ingest skipped: {exc}")
        return
    typer.echo(f"OpenDART financial requests: {len(requests)} from filings {since}..{until}")
    adapter = DartFinancialsBulkAdapter(
        layout=DataLakeLayout(paths.data_root),
        writer=ParquetDatasetWriter(),
        client=_opendart_client(settings),
        requests=requests,
        fs_div=fs_div,
        batch_size=batch_size,
        sleep_seconds=sleep_seconds,
    )
    _run_and_print([adapter], start, end)


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
    for summary in build_all_iter(ProjectPaths(root=root).data_root):
        _print_summary(summary)


@app.command("bootstrap")
def bootstrap(
    since: str = typer.Option(..., help="Initial backfill start date as YYYY-MM-DD"),
    until: str | None = typer.Option(None, help="Initial backfill end date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    skip_kis: bool = typer.Option(False, help="Skip KIS price backfill"),
    skip_krx: bool = typer.Option(True, help="KRX is disabled by default"),
    skip_dart_company: bool = typer.Option(False, help="Skip DART corpCode snapshot"),
    skip_dart_filings: bool = typer.Option(False, help="Skip DART filing list backfill"),
    skip_dart_financials: bool = typer.Option(False, help="Skip DART financial statement backfill"),
    naver_summary: bool = typer.Option(
        False,
        "--naver-summary/--no-naver-summary",
        help="Fetch current Naver summary for the bootstrap end date",
    ),
    price_source: str = typer.Option("naver", help="Price source: naver, kis, or both"),
    financial_report_codes: str = typer.Option(
        "11011",
        help="DART financial reports to ingest; annual default, comma-separate for quarterly",
    ),
    financial_batch_size: int = typer.Option(25, help="DART financial API calls per chunk"),
    financial_sleep_seconds: float = typer.Option(0.05, help="Sleep between DART financial calls"),
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

    if not skip_dart_filings:
        if settings.has_opendart:
            try:
                ingest_dart_filings(start.isoformat(), end.isoformat(), paths.root, 7)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"OpenDART filings ingest failed: {exc}")
        else:
            failures.append("OpenDART filings ingest skipped: OPENDART_API_KEY missing")

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

    if not skip_dart_financials:
        if settings.has_opendart:
            try:
                ingest_dart_financials_bulk(
                    start.isoformat(),
                    end.isoformat(),
                    paths.root,
                    financial_report_codes,
                    ticker_limit,
                    financial_batch_size,
                    financial_sleep_seconds,
                    "CFS",
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"OpenDART financial ingest failed: {exc}")
        else:
            failures.append("OpenDART financial ingest skipped: OPENDART_API_KEY missing")

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


@reports_app.command("all")
def generate_all_reports(
    report_date: str | None = typer.Option(None, help="Report date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    generate_dq_report(report_date, root)
    generate_fraud_report(report_date, root)


@app.command("daily")
def run_daily(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    report_date: str | None = typer.Option(None, help="Report date as YYYY-MM-DD"),
    ingest: bool = typer.Option(True, help="Attempt live source ingest when keys are configured"),
    strict: bool = typer.Option(True, help="Fail if a configured live source fails"),
    include_fundamentals_pit: bool = typer.Option(
        False,
        "--include-fundamentals-pit/--skip-fundamentals-pit",
        help="Rebuild the large gold.fundamentals_pit cache during this daily run",
    ),
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
    summaries = _run_daily_builds(paths.data_root, include_fundamentals_pit)
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


@app.command("catchup")
def catchup_daily(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    since: str | None = typer.Option(
        None,
        help="First report date to run. Defaults to latest gold price date + 1 weekday.",
    ),
    until: str | None = typer.Option(None, help="Last report date to run. Defaults to today"),
    ingest: bool = typer.Option(True, help="Attempt live source ingest when keys are configured"),
    strict: bool = typer.Option(True, help="Fail if a configured live source fails"),
    include_fundamentals_pit: bool = typer.Option(
        False,
        "--include-fundamentals-pit/--skip-fundamentals-pit",
        help="Rebuild the large gold.fundamentals_pit cache during each daily run",
    ),
) -> None:
    """Run daily for each missing weekday in a date range."""

    paths = ProjectPaths(root=root)
    end = _parse_report_date(until)
    dates = _catchup_dates(paths.data_root, _parse_report_date(since) if since else None, end)
    if not dates:
        typer.echo("No catch-up dates to run.")
        return
    for report_day in dates:
        typer.echo(f"Catch-up daily: {report_day.isoformat()}")
        run_daily(paths.root, report_day.isoformat(), ingest, strict, include_fundamentals_pit)


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
                50,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"KIS universe price ingest failed: {exc}")
    else:
        typer.echo("KIS price ingest skipped: KIS_APP_KEY/KIS_APP_SECRET missing")

    if settings.has_opendart:
        try:
            ingest_dart_filings(
                filing_start.isoformat(),
                report_date.isoformat(),
                paths.root,
                7,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART filings ingest failed: {exc}")
        try:
            ingest_dart_financials_bulk(
                filing_start.isoformat(),
                report_date.isoformat(),
                paths.root,
                "11013,11012,11014,11011",
                None,
                25,
                0.05,
                "CFS",
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART financial ingest failed: {exc}")
    return failures


def _run_daily_builds(data_root: Path, include_fundamentals_pit: bool) -> list:
    builders = [
        build_silver_prices,
        build_security_master,
        build_universe_history,
        build_daily_prices_adj,
        build_financials_silver,
    ]
    if include_fundamentals_pit:
        builders.append(build_fundamentals_pit)

    summaries = []
    for builder in builders:
        summaries.extend(builder(data_root))
    return summaries


def _catchup_dates(data_root: Path, since: date | None, until: date) -> tuple[date, ...]:
    if since is None:
        latest = _latest_gold_price_date(data_root)
        if latest is None:
            raise typer.BadParameter("No gold price data found. Pass --since explicitly.")
        since = latest + timedelta(days=1)
    if until < since:
        return ()
    return TradingCalendar.weekdays(since, until).dates


def _latest_gold_price_date(data_root: Path) -> date | None:
    values: list[date] = []
    for path in data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"):
        for part in path.parts:
            if not part.startswith("dt="):
                continue
            with suppress(ValueError):
                values.append(date.fromisoformat(part.removeprefix("dt=")))
    return max(values) if values else None


def _dart_financial_requests(
    paths: ProjectPaths,
    since: date,
    until: date,
    report_codes: tuple[str, ...],
    corp_limit: int | None = None,
) -> tuple[dict[str, object], ...]:
    files = sorted(paths.data_root.glob("bronze/dart_filings/dt=*/part.parquet"))
    if not files:
        raise typer.BadParameter("No DART filings data. Run ingest dart-filings first.")

    filings = (
        _read_parquet_files_relaxed(files)
        .with_columns(
            pl.col("rcept_dt").cast(pl.Date, strict=False),
            pl.col("corp_code").cast(pl.String),
            pl.col("stock_code").cast(pl.String),
            pl.col("report_nm").cast(pl.String),
        )
        .filter(
            (pl.col("rcept_dt") >= since)
            & (pl.col("rcept_dt") <= until)
            & pl.col("stock_code").is_not_null()
        )
    )
    if filings.is_empty():
        raise typer.BadParameter(f"No listed DART filings found from {since} to {until}.")

    parsed_rows: list[dict[str, object]] = []
    for row in filings.iter_rows(named=True):
        parsed = _parse_dart_financial_report(str(row["report_nm"]), row["rcept_dt"])
        if parsed is None:
            continue
        bsns_year, report_code = parsed
        if report_code not in report_codes:
            continue
        parsed_rows.append(
            {
                "corp_code": row["corp_code"],
                "corp_name": row.get("corp_name"),
                "stock_code": str(row["stock_code"]).zfill(6),
                "bsns_year": bsns_year,
                "report_code": report_code,
                "available_date": row["rcept_dt"],
            }
        )

    if not parsed_rows:
        raise typer.BadParameter("No filings matched the requested financial report codes.")

    requests = pl.DataFrame(parsed_rows, infer_schema_length=None).sort("available_date")
    if corp_limit is not None:
        corps = (
            requests.select("corp_code")
            .unique()
            .sort("corp_code")
            .head(corp_limit)
            .to_series()
            .to_list()
        )
        requests = requests.filter(pl.col("corp_code").is_in(corps))

    return tuple(
        requests.group_by(["corp_code", "bsns_year", "report_code"], maintain_order=True)
        .agg(
            pl.col("corp_name").drop_nulls().last(),
            pl.col("stock_code").drop_nulls().last(),
            pl.col("available_date").max(),
        )
        .sort(["bsns_year", "report_code", "corp_code"])
        .to_dicts()
    )


def _read_parquet_files_relaxed(files: list[Path]) -> pl.DataFrame:
    try:
        return pl.read_parquet([path.as_posix() for path in files], hive_partitioning=True)
    except pl.exceptions.SchemaError:
        frames = [pl.read_parquet(path.as_posix(), hive_partitioning=True) for path in files]
        return pl.concat(frames, how="diagonal_relaxed")


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


def _parse_report_codes(value: str) -> tuple[str, ...]:
    codes = tuple(code.strip() for code in value.split(",") if code.strip())
    valid = {"11013", "11012", "11014", "11011"}
    invalid = [code for code in codes if code not in valid]
    if invalid:
        raise typer.BadParameter(f"unsupported DART report codes: {', '.join(invalid)}")
    if not codes:
        raise typer.BadParameter("at least one DART report code is required")
    return codes


def _parse_dart_financial_report(report_nm: str, rcept_dt: date) -> tuple[int, str] | None:
    match = re.search(r"\((\d{4})\.(\d{2})\)", report_nm)
    bsns_year = int(match.group(1)) if match else rcept_dt.year
    month = int(match.group(2)) if match else None

    if "\uc0ac\uc5c5\ubcf4\uace0\uc11c" in report_nm:
        return (bsns_year if match else rcept_dt.year - 1, "11011")
    if "\ubc18\uae30\ubcf4\uace0\uc11c" in report_nm:
        return (bsns_year, "11012")
    if "\ubd84\uae30\ubcf4\uace0\uc11c" in report_nm:
        if month is not None and month <= 3:
            return (bsns_year, "11013")
        if month is not None and month >= 9:
            return (bsns_year, "11014")
        if rcept_dt.month <= 5:
            return (bsns_year, "11013")
        return (bsns_year, "11014")
    return None


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
        _print_summary(summary)


def _print_summary(summary) -> None:
    typer.echo(f"{summary.dataset}\trows={summary.rows}\tfiles={summary.files}")


def _print_dotenv_issues(root: Path) -> None:
    for issue in diagnose_dotenv(root / ".env"):
        typer.echo(f".env warning line {issue.line_no}: {issue.message}")


def _http_get_json(url: str, token: str | None = None) -> dict[str, object]:
    payload = _http_get_text(url, token=token)
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise typer.BadParameter(f"admin returned non-object JSON at {url}")
    return data


def _http_get_text(url: str, token: str | None = None) -> str:
    headers = {"X-Admin-Token": token} if token else {}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=10) as response:
            return response.read().decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        raise typer.BadParameter(f"admin is not reachable at {url}: {exc}") from exc


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
