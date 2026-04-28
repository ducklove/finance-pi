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
from finance_pi.config import ProjectPaths, RuntimeSettings
from finance_pi.factors import ParquetFactorContext, factor_registry
from finance_pi.http import HttpJsonClient
from finance_pi.ingest import IngestOrchestrator
from finance_pi.reports.data_quality import build_data_quality_report
from finance_pi.reports.fraud import build_fraud_report
from finance_pi.sources.kis import KisAuthClient, KisDailyAdapter, KisDailyPriceClient
from finance_pi.sources.krx import KrxDailyAdapter
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

    if platform.machine() in {"armv7l", "armv6l"}:
        typer.echo("Warning: 32-bit Raspberry Pi OS is not recommended for DuckDB/Polars.")


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
        client=HttpJsonClient(
            "krx",
            settings.krx_base_url,
            default_headers={"AUTH_KEY": settings.krx_openapi_key},
        ),
        daily_path=settings.krx_stock_daily_path,
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
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise typer.BadParameter("KIS_APP_KEY and KIS_APP_SECRET are required")
    token = settings.kis_access_token or KisAuthClient(
        settings.kis_base_url,
        settings.kis_app_key,
        settings.kis_app_secret,
    ).issue_token().access_token
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = KisDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        KisDailyPriceClient(
            HttpJsonClient("kis", settings.kis_base_url),
            settings.kis_app_key,
            settings.kis_app_secret,
            token,
        ),
        ticker,
    )
    _print_results(
        IngestOrchestrator([adapter]).run(_parse_report_date(since), _parse_report_date(until))
    )


@build_app.command("all")
def build_everything(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_all(ProjectPaths(root=root).data_root))


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
) -> None:
    """Run the daily local maintenance job.

    Live source ingestion will plug into this command once the KRX/OpenDART/KIS
    adapters are configured. For now it guarantees the data lake layout,
    DuckDB catalog, and baseline reports exist.
    """

    parsed_date = _parse_report_date(report_date)
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    if ingest:
        _run_daily_ingest(paths, settings, parsed_date)
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


def _run_daily_ingest(paths: ProjectPaths, settings: RuntimeSettings, report_date: date) -> None:
    start = _previous_weekday(report_date)
    if settings.has_krx:
        try:
            ingest_krx(start.isoformat(), start.isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            typer.echo(f"KRX ingest failed: {exc}")
    else:
        typer.echo("KRX ingest skipped: KRX_OPENAPI_KEY missing")

    if settings.has_opendart:
        try:
            ingest_dart_filings(start.isoformat(), report_date.isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            typer.echo(f"OpenDART filings ingest failed: {exc}")
    else:
        typer.echo("OpenDART filings ingest skipped: OPENDART_API_KEY missing")


def _previous_weekday(value: date) -> date:
    current = value
    if current.weekday() >= 5:
        current -= timedelta(days=current.weekday() - 4)
    return current


def _print_results(results) -> None:
    for result in results:
        status = "skipped" if result.skipped else "wrote"
        suffix = f" ({result.reason})" if result.reason else ""
        typer.echo(f"{status}: {result.path} rows={result.rows}{suffix}")


def _print_summaries(summaries) -> None:
    for summary in summaries:
        typer.echo(f"{summary.dataset}\trows={summary.rows}\tfiles={summary.files}")


if __name__ == "__main__":
    app()
