from __future__ import annotations

import importlib.util
import csv
import io
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
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
from finance_pi.ingest import IngestOrchestrator, request_hash
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
from finance_pi.sources.schemas import PRICE_SCHEMA
from finance_pi.storage import CatalogBuilder, DataLakeLayout, dataset_registry
from finance_pi.storage.parquet import ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_all_iter,
    build_daily_market_caps,
    build_daily_prices_adj,
    build_financials_silver,
    build_fundamentals_pit,
    build_security_master,
    build_silver_market_caps,
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
backfill_app = typer.Typer(help="Historical backfill commands")
app.add_typer(catalog_app, name="catalog")
app.add_typer(factors_app, name="factors")
app.add_typer(ingest_app, name="ingest")
app.add_typer(build_app, name="build")
app.add_typer(reports_app, name="reports")
app.add_typer(backtest_app, name="backtest")
app.add_typer(docs_app, name="docs")
app.add_typer(backfill_app, name="backfill")

FRED_TIMEOUT_SECONDS = 10
YAHOO_TIMEOUT_SECONDS = 15
FRED_SERIES = {
    "cpi": [
        {
            "series_id": "US_CPI_ALL",
            "fred_id": "CPIAUCSL",
            "country": "US",
            "name": "US CPI All Urban Consumers",
            "frequency": "M",
            "index_base": "1982-84=100",
        },
        {
            "series_id": "KOR_CPI_ALL",
            "fred_id": "KORCPIALLMINMEI",
            "country": "KR",
            "name": "Korea CPI All Items",
            "frequency": "M",
            "index_base": "2015=100",
        },
    ],
    "rates": [
        {
            "series_id": "US_FED_FUNDS",
            "fred_id": "FEDFUNDS",
            "country": "US",
            "name": "Federal Funds Effective Rate",
            "frequency": "M",
            "tenor": "overnight",
            "unit": "percent",
        },
        {
            "series_id": "US_TREASURY_10Y",
            "fred_id": "DGS10",
            "country": "US",
            "name": "US Treasury 10-Year Constant Maturity",
            "frequency": "D",
            "tenor": "10Y",
            "unit": "percent",
        },
    ],
}
FRED_ECONOMIC_SERIES = [
    {
        "series_id": "CPILFESL",
        "fred_id": "CPILFESL",
        "country": "US",
        "name": "US Core CPI",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "PCEPI",
        "fred_id": "PCEPI",
        "country": "US",
        "name": "US PCE Price Index",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "PCEPILFE",
        "fred_id": "PCEPILFE",
        "country": "US",
        "name": "US Core PCE Price Index",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "PPIACO",
        "fred_id": "PPIACO",
        "country": "US",
        "name": "US Producer Price Index: All Commodities",
        "category": "inflation",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "UNRATE",
        "fred_id": "UNRATE",
        "country": "US",
        "name": "US Unemployment Rate",
        "category": "labor",
        "frequency": "M",
        "unit": "percent",
    },
    {
        "series_id": "PAYEMS",
        "fred_id": "PAYEMS",
        "country": "US",
        "name": "US Total Nonfarm Payrolls",
        "category": "labor",
        "frequency": "M",
        "unit": "thousands",
    },
    {
        "series_id": "GDP",
        "fred_id": "GDP",
        "country": "US",
        "name": "US Gross Domestic Product",
        "category": "growth",
        "frequency": "Q",
        "unit": "billions_usd_saar",
    },
    {
        "series_id": "GDPC1",
        "fred_id": "GDPC1",
        "country": "US",
        "name": "US Real Gross Domestic Product",
        "category": "growth",
        "frequency": "Q",
        "unit": "billions_chained_2017_usd_saar",
    },
    {
        "series_id": "INDPRO",
        "fred_id": "INDPRO",
        "country": "US",
        "name": "US Industrial Production Index",
        "category": "growth",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "RSAFS",
        "fred_id": "RSAFS",
        "country": "US",
        "name": "US Advance Retail Sales",
        "category": "consumption",
        "frequency": "M",
        "unit": "millions_usd",
    },
    {
        "series_id": "M2SL",
        "fred_id": "M2SL",
        "country": "US",
        "name": "US M2 Money Stock",
        "category": "money",
        "frequency": "M",
        "unit": "billions_usd",
    },
    {
        "series_id": "DGS2",
        "fred_id": "DGS2",
        "country": "US",
        "name": "US Treasury 2-Year Constant Maturity",
        "category": "rates",
        "frequency": "D",
        "unit": "percent",
    },
    {
        "series_id": "DGS5",
        "fred_id": "DGS5",
        "country": "US",
        "name": "US Treasury 5-Year Constant Maturity",
        "category": "rates",
        "frequency": "D",
        "unit": "percent",
    },
    {
        "series_id": "DGS30",
        "fred_id": "DGS30",
        "country": "US",
        "name": "US Treasury 30-Year Constant Maturity",
        "category": "rates",
        "frequency": "D",
        "unit": "percent",
    },
    {
        "series_id": "T10Y2Y",
        "fred_id": "T10Y2Y",
        "country": "US",
        "name": "US 10-Year Minus 2-Year Treasury Spread",
        "category": "rates",
        "frequency": "D",
        "unit": "percentage_points",
    },
    {
        "series_id": "T10Y3M",
        "fred_id": "T10Y3M",
        "country": "US",
        "name": "US 10-Year Minus 3-Month Treasury Spread",
        "category": "rates",
        "frequency": "D",
        "unit": "percentage_points",
    },
    {
        "series_id": "SOFR",
        "fred_id": "SOFR",
        "country": "US",
        "name": "Secured Overnight Financing Rate",
        "category": "rates",
        "frequency": "D",
        "unit": "percent",
    },
    {
        "series_id": "BAMLH0A0HYM2",
        "fred_id": "BAMLH0A0HYM2",
        "country": "US",
        "name": "US High Yield Option-Adjusted Spread",
        "category": "credit",
        "frequency": "D",
        "unit": "percent",
    },
    {
        "series_id": "MORTGAGE30US",
        "fred_id": "MORTGAGE30US",
        "country": "US",
        "name": "US 30-Year Fixed Mortgage Rate",
        "category": "housing",
        "frequency": "W",
        "unit": "percent",
    },
    {
        "series_id": "HOUST",
        "fred_id": "HOUST",
        "country": "US",
        "name": "US Housing Starts",
        "category": "housing",
        "frequency": "M",
        "unit": "thousands_saar",
    },
    {
        "series_id": "UMCSENT",
        "fred_id": "UMCSENT",
        "country": "US",
        "name": "University of Michigan Consumer Sentiment",
        "category": "sentiment",
        "frequency": "M",
        "unit": "index",
    },
    {
        "series_id": "DCOILWTICO",
        "fred_id": "DCOILWTICO",
        "country": "US",
        "name": "WTI Crude Oil Price",
        "category": "commodities",
        "frequency": "D",
        "unit": "usd_per_barrel",
    },
    {
        "series_id": "VIXCLS",
        "fred_id": "VIXCLS",
        "country": "US",
        "name": "CBOE Volatility Index: VIX",
        "category": "risk",
        "frequency": "D",
        "unit": "index",
    },
    {
        "series_id": "DTWEXBGS",
        "fred_id": "DTWEXBGS",
        "country": "US",
        "name": "Nominal Broad US Dollar Index",
        "category": "fx",
        "frequency": "D",
        "unit": "index",
    },
]
YAHOO_INDEX_SERIES = [
    {
        "series_id": "KOSPI",
        "symbol": "^KS11",
        "country": "KR",
        "name": "KOSPI Composite Index",
        "category": "equity_index",
        "currency": "KRW",
    },
    {
        "series_id": "SNP500",
        "symbol": "^GSPC",
        "country": "US",
        "name": "S&P 500 Index",
        "category": "equity_index",
        "currency": "USD",
    },
    {
        "series_id": "NASDAQ",
        "symbol": "^IXIC",
        "country": "US",
        "name": "NASDAQ Composite Index",
        "category": "equity_index",
        "currency": "USD",
    },
]
YAHOO_RATE_SERIES = [
    {
        "series_id": "US_TREASURY_10Y_YAHOO",
        "symbol": "^TNX",
        "country": "US",
        "name": "US Treasury 10-Year Yield",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
        "scale": "1",
    },
    {
        "series_id": "US_TREASURY_13W_YAHOO",
        "symbol": "^IRX",
        "country": "US",
        "name": "US Treasury 13-Week Yield",
        "frequency": "D",
        "tenor": "13W",
        "unit": "percent",
        "scale": "1",
    },
]
YAHOO_COMMODITY_SERIES = [
    {
        "series_id": "GOLD_USD_OZ",
        "symbol": "GC=F",
        "name": "Gold Futures",
        "commodity": "gold",
        "unit": "troy_oz",
        "currency": "USD",
    },
    {
        "series_id": "SILVER_USD_OZ",
        "symbol": "SI=F",
        "name": "Silver Futures",
        "commodity": "silver",
        "unit": "troy_oz",
        "currency": "USD",
    },
]
YAHOO_FX_SERIES = [
    {
        "series_id": "USD_KRW",
        "symbol": "KRW=X",
        "base_currency": "USD",
        "quote_currency": "KRW",
    },
    {
        "series_id": "USD_JPY",
        "symbol": "JPY=X",
        "base_currency": "USD",
        "quote_currency": "JPY",
    },
]


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


@app.command("admin-watchdog")
def admin_watchdog(
    url: str = typer.Option("http://127.0.0.1:8400", help="Admin base URL"),
    unit: str = typer.Option("finance-pi-admin.service", help="systemd user unit to restart"),
    timeout_seconds: float = typer.Option(5.0, min=0.5, help="Health check timeout"),
    dry_run: bool = typer.Option(False, help="Only report the restart action"),
) -> None:
    """Restart the admin user service when the HTTP health check stops responding."""

    base_url = url.rstrip("/")
    health_url = f"{base_url}/api/health"
    if _admin_health_ok(health_url, timeout_seconds):
        typer.echo(f"admin healthy: {health_url}")
        return

    typer.echo(f"admin unhealthy: {health_url}")
    command = ["systemctl", "--user", "restart", unit]
    if dry_run:
        typer.echo("would run: " + " ".join(command))
        return
    subprocess.run(command, check=True)
    typer.echo(f"restarted: {unit}")


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
    tickers = _latest_price_universe_tickers(paths, limit=limit)
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
    tickers = _latest_price_universe_tickers(paths, limit=limit)
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


@ingest_app.command("marcap")
def ingest_marcap(
    start_year: int = typer.Option(1995, help="First marcap year to ingest"),
    end_year: int = typer.Option(2025, help="Last marcap year to ingest"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    force: bool = typer.Option(False, help="Overwrite existing yearly marcap parquet files"),
) -> None:
    """Download FinanceData marcap yearly parquet files into Bronze."""

    if start_year < 1995:
        raise typer.BadParameter("marcap starts at 1995")
    if end_year < start_year:
        raise typer.BadParameter("end-year must be greater than or equal to start-year")

    paths = ProjectPaths(root=root)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    base_url = "https://raw.githubusercontent.com/FinanceData/marcap/master/data"
    for year in range(start_year, end_year + 1):
        output = paths.data_root / "bronze" / "marcap" / f"year={year}" / "part.parquet"
        if output.exists() and not force:
            typer.echo(f"marcap {year}: skip existing {output}")
            continue
        output.parent.mkdir(parents=True, exist_ok=True)
        url = f"{base_url}/marcap-{year}.parquet"
        request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
        typer.echo(f"marcap {year}: downloading {url}")
        with urlopen(request, timeout=120) as response, output.open("wb") as file:
            shutil.copyfileobj(response, file)
        typer.echo(f"marcap {year}: wrote {output}")


@ingest_app.command("pykrx-market-caps")
def ingest_pykrx_market_caps(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    markets: str = typer.Option("KOSPI,KOSDAQ", help="Comma-separated KOSPI,KOSDAQ"),
    local_price_dates: bool = typer.Option(
        True,
        "--local-price-dates/--calendar-days",
        help="Fetch only dates already present in silver.prices.",
    ),
    force: bool = typer.Option(False, help="Overwrite existing bronze KRX partitions"),
) -> None:
    """Fetch KRX market-cap rows through pykrx and write them as Bronze KRX source rows."""

    paths = ProjectPaths(root=root)
    RuntimeSettings.load(paths.root)
    if not (os.getenv("KRX_ID") and os.getenv("KRX_PW")):
        raise typer.BadParameter("KRX_ID and KRX_PW are required for pykrx KRX login")

    from pykrx import stock  # noqa: PLC0415

    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    start = _parse_report_date(since)
    end = _parse_report_date(until)
    selected_markets = _parse_markets(markets)
    dates = (
        _local_price_dates(paths.data_root, start, end)
        if local_price_dates
        else _calendar_dates(start, end)
    )
    for logical_date in dates:
        path = layout.partition_path("bronze.krx_daily_raw", logical_date)
        if path.exists() and not force:
            typer.echo(f"pykrx {logical_date}: skip existing {path}")
            continue
        rows: list[dict[str, object]] = []
        ymd = logical_date.strftime("%Y%m%d")
        for market in selected_markets:
            frame = stock.get_market_cap(ymd, market=market)
            if frame.empty:
                continue
            frame = frame.reset_index(names="ticker")
            for row in frame.to_dict("records"):
                rows.append(
                    {
                        "date": logical_date,
                        "ticker": str(row["ticker"]).zfill(6),
                        "isin": None,
                        "name": str(row["ticker"]).zfill(6),
                        "market": market,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": _number_or_none(row.get("종가")),
                        "volume": _number_or_none(row.get("거래량")),
                        "trading_value": _number_or_none(row.get("거래대금")),
                        "market_cap": _number_or_none(row.get("시가총액")),
                        "listed_shares": _number_or_none(row.get("상장주식수")),
                    }
                )
        if not rows:
            typer.echo(f"pykrx {logical_date}: no rows")
            continue
        output = pl.DataFrame(rows).select(
            [
                pl.col(column).cast(dtype, strict=False).alias(column)
                if column in rows[0]
                else pl.lit(None, dtype=dtype).alias(column)
                for column, dtype in PRICE_SCHEMA.items()
            ]
        )
        writer.write(
            output,
            path,
            mode="overwrite" if force else "fail",
            source="pykrx",
            request_hash=request_hash(
                "pykrx",
                "get_market_cap",
                {"date": logical_date.isoformat(), "markets": selected_markets},
            ),
            include_ingest_metadata=True,
        )
        typer.echo(f"pykrx {logical_date}: rows={output.height} wrote {path}")


@ingest_app.command("macro")
def ingest_macro(
    since: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    until: str | None = typer.Option(None, help="End date as YYYY-MM-DD. Defaults to today"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
) -> None:
    """Fetch CPI, rates, indices, commodities, and FX macro tables."""

    paths = ProjectPaths(root=root)
    start = _parse_report_date(since)
    end = _parse_report_date(until)
    settings = RuntimeSettings.load(paths.root)
    failures = _ingest_macro(paths, start, end, settings)
    for failure in failures:
        typer.echo(failure)
    if failures:
        raise typer.Exit(code=1)


@build_app.command("all")
def build_everything(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    for summary in build_all_iter(ProjectPaths(root=root).data_root):
        _print_summary(summary)


@build_app.command("market-caps")
def build_cmd_market_caps(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    data_root = ProjectPaths(root=root).data_root
    _print_summaries(build_silver_market_caps(data_root))
    _print_summaries(build_daily_market_caps(data_root))


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
        if not _has_naver_summary(paths):
            try:
                ingest_naver_summary(end.isoformat(), paths.root, "KOSPI,KOSDAQ")
            except Exception as exc:  # noqa: BLE001
                failures.append(f"Naver summary universe ingest failed: {exc}")
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


@backfill_app.command("yearly")
def backfill_yearly(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    start_year: int = typer.Option(2023, help="Newest year to backfill first"),
    end_year: int = typer.Option(2010, help="Oldest year to backfill, inclusive"),
    max_years: int | None = typer.Option(
        1,
        help="Maximum missing years to run this invocation; pass 0 for no limit",
    ),
    force: bool = typer.Option(False, help="Run even when a yearly completion marker exists"),
    dry_run: bool = typer.Option(False, help="Only print the planned yearly chunks"),
    strict: bool = typer.Option(True, help="Fail if a configured live source fails"),
    include_financials: bool = typer.Option(
        True,
        "--include-financials/--skip-financials",
        help="Ingest DART filings and financial statements for each year",
    ),
    include_fundamentals_pit: bool = typer.Option(
        False,
        "--include-fundamentals-pit/--skip-fundamentals-pit",
        help="Rebuild the large gold.fundamentals_pit cache after each year",
    ),
    financial_report_codes: str = typer.Option(
        "11011",
        help="DART financial reports to ingest; annual default, comma-separate for quarterly",
    ),
    ticker_limit: int | None = typer.Option(None, help="Limit tickers for a smoke run"),
    naver_chunk_days: int = typer.Option(370, help="Date range days per Naver request round"),
    ticker_batch_size: int = typer.Option(50, help="Tickers per incremental price write"),
    sleep_seconds: float = typer.Option(0.02, help="Sleep between Naver ticker calls"),
    financial_batch_size: int = typer.Option(25, help="DART financial API calls per chunk"),
    financial_sleep_seconds: float = typer.Option(0.05, help="Sleep between DART calls"),
) -> None:
    """Backfill historical data one calendar year at a time, newest to oldest."""

    paths = _validated_backfill_paths(root)
    settings = RuntimeSettings.load(paths.root)
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    years = _backfill_years(start_year, end_year)
    limit = None if max_years == 0 else max_years
    selected: list[int] = []
    for year in years:
        marker = _backfill_marker_path(paths.data_root, year)
        if marker.exists() and not force:
            typer.echo(f"skip year={year}: completion marker exists")
            continue
        selected.append(year)
        if limit is not None and len(selected) >= limit:
            break
    if not selected:
        typer.echo("No yearly backfill chunks to run.")
        return
    for year in selected:
        since, until = _year_bounds(year)
        typer.echo(f"Backfill year={year} range={since.isoformat()}..{until.isoformat()}")
        if dry_run:
            continue
        failures = _run_yearly_backfill(
            paths=paths,
            settings=settings,
            since=since,
            until=until,
            include_financials=include_financials,
            include_fundamentals_pit=include_fundamentals_pit,
            financial_report_codes=financial_report_codes,
            ticker_limit=ticker_limit,
            naver_chunk_days=naver_chunk_days,
            ticker_batch_size=ticker_batch_size,
            sleep_seconds=sleep_seconds,
            financial_batch_size=financial_batch_size,
            financial_sleep_seconds=financial_sleep_seconds,
        )
        for failure in failures:
            typer.echo(failure)
        if strict and failures:
            raise typer.Exit(code=1)
        _write_backfill_marker(
            paths.data_root,
            year,
            {
                "since": since.isoformat(),
                "until": until.isoformat(),
                "include_financials": include_financials,
                "include_fundamentals_pit": include_fundamentals_pit,
                "financial_report_codes": financial_report_codes,
                "failures": failures,
            },
        )
        typer.echo(f"completed year={year}")


@backfill_app.command("status")
def backfill_status(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    start_year: int = typer.Option(2023, help="Newest year to show"),
    end_year: int = typer.Option(2010, help="Oldest year to show, inclusive"),
) -> None:
    """Print yearly backfill markers and price coverage."""

    paths = _validated_backfill_paths(root)
    coverage = _dataset_coverage(paths.data_root, "gold/daily_prices_adj/dt=*/part.parquet")
    typer.echo(
        "gold.daily_prices_adj coverage: "
        f"{coverage.get('start') or '--'}..{coverage.get('end') or '--'} "
        f"rows={coverage.get('rows', 0)} files={coverage.get('files', 0)}"
    )
    typer.echo("year\tstatus\tprice_days\trows\tcoverage\tmarker")
    for item in _yearly_backfill_status(paths.data_root, start_year, end_year):
        typer.echo(
            f"{item['year']}\t{item['status']}\t{item['price_days']}\t{item['rows']}\t"
            f"{item['coverage']}\t{item['marker']}"
        )


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
    failures: list[str] = []
    if ingest:
        failures = _run_daily_ingest(paths, settings, parsed_date)
        for failure in failures:
            typer.echo(failure)
        if strict and failures:
            raise typer.Exit(code=1)
    summaries = _run_daily_builds(
        paths.data_root,
        include_fundamentals_pit,
        _previous_weekday(parsed_date),
    )
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
    price_date = _previous_weekday(parsed_date)
    if not failures or _gold_price_partition_exists(paths.data_root, price_date):
        _write_daily_marker(
            paths.data_root,
            parsed_date,
            {
                "report_date": parsed_date.isoformat(),
                "price_date": price_date.isoformat(),
                "failures": failures,
                "gold_price_partition": _gold_price_partition_exists(paths.data_root, price_date),
            },
        )


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
        if not _daily_complete(paths.data_root, report_day):
            typer.echo(f"Catch-up stopped: {report_day.isoformat()} did not complete")
            return


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
    macro_since = _macro_ingest_start(paths.data_root, report_date)
    for failure in _ingest_macro(paths, macro_since, report_date, settings):
        typer.echo(failure)
    return failures


def _ingest_macro(
    paths: ProjectPaths,
    since: date,
    until: date,
    settings: RuntimeSettings | None = None,
) -> list[str]:
    DataLakeLayout(paths.data_root).ensure_base_dirs()
    if settings is None:
        settings = RuntimeSettings.load(paths.root)
    failures: list[str] = []
    macro_frames: dict[str, list[pl.DataFrame]] = {
        "cpi": [],
        "rates": [],
        "commodities": [],
        "fx": [],
        "indices": [],
        "economic_indicators": [],
    }

    for table, series_list in FRED_SERIES.items():
        for series in series_list:
            try:
                rows = _fetch_fred_rows(series, since, until, table, settings.fred_api_key)
                if rows:
                    macro_frames[table].append(pl.DataFrame(rows))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"Macro {series['series_id']} ingest failed: {exc}")

    for series in FRED_ECONOMIC_SERIES:
        try:
            rows = _fetch_fred_indicator_rows(series, since, until, settings.fred_api_key)
            if rows:
                macro_frames["economic_indicators"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")

    for series in YAHOO_INDEX_SERIES:
        try:
            rows = _fetch_yahoo_index_rows(series, since, until)
            if rows:
                macro_frames["indices"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
    for series in YAHOO_RATE_SERIES:
        try:
            rows = _fetch_yahoo_rate_rows(series, since, until)
            if rows:
                macro_frames["rates"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
    for series in YAHOO_COMMODITY_SERIES:
        try:
            rows = _fetch_yahoo_commodity_rows(series, since, until)
            if rows:
                macro_frames["commodities"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
    for series in YAHOO_FX_SERIES:
        try:
            rows = _fetch_yahoo_fx_rows(series, since, until)
            if rows:
                macro_frames["fx"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")

    for table, frames in macro_frames.items():
        if not frames:
            continue
        frame = pl.concat(frames, how="diagonal_relaxed")
        _write_macro_table(paths.data_root, table, frame)
        typer.echo(f"Macro {table}: rows={frame.height}")
    return failures


def _fetch_fred_rows(
    series: dict[str, str],
    since: date,
    until: date,
    table: str,
    api_key: str | None = None,
) -> list[dict[str, object]]:
    values = _fetch_fred_values(series["fred_id"], since, until, api_key)
    if table == "cpi":
        return _cpi_rows(series, values)
    if table == "rates":
        return [
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": series["name"],
                "frequency": series["frequency"],
                "tenor": series["tenor"],
                "value": value,
                "unit": series["unit"],
                "source": "fred",
                "updated_at": datetime.now(UTC),
            }
            for logical_date, value in values
        ]
    if table == "commodities":
        return [
            {
                "date": logical_date,
                "series_id": series["series_id"],
                "name": series["name"],
                "commodity": series["commodity"],
                "value": value,
                "unit": series["unit"],
                "currency": series["currency"],
                "source": "fred",
                "updated_at": datetime.now(UTC),
            }
            for logical_date, value in values
        ]
    if table == "fx":
        return [
            {
                "date": logical_date,
                "series_id": series["series_id"],
                "base_currency": series["base_currency"],
                "quote_currency": series["quote_currency"],
                "value": value,
                "source": "fred",
                "updated_at": datetime.now(UTC),
            }
            for logical_date, value in values
        ]
    raise ValueError(f"unsupported FRED table: {table}")


def _fetch_fred_values(
    fred_id: str,
    since: date,
    until: date,
    api_key: str | None = None,
) -> list[tuple[date, float]]:
    if api_key:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={fred_id}"
            f"&api_key={api_key}"
            "&file_type=json"
            f"&observation_start={since.isoformat()}"
            f"&observation_end={until.isoformat()}"
        )
        request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
        try:
            with urlopen(request, timeout=FRED_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8"))
            observations = payload.get("observations")
            if not isinstance(observations, list):
                raise ValueError("FRED observations response is malformed")
            values = []
            for row in observations:
                raw_date = row.get("date")
                raw_value = row.get("value")
                if not raw_date or not raw_value or raw_value == ".":
                    continue
                values.append((date.fromisoformat(raw_date), float(raw_value)))
            return values
        except (HTTPError, URLError, TimeoutError):
            return _fetch_fred_csv_values(fred_id, since, until)

    return _fetch_fred_csv_values(fred_id, since, until)


def _fetch_fred_csv_values(fred_id: str, since: date, until: date) -> list[tuple[date, float]]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred_id}"
    request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
    with urlopen(request, timeout=FRED_TIMEOUT_SECONDS) as response:
        text = response.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    values = []
    for row in reader:
        raw_date = row.get("observation_date") or row.get("DATE")
        raw_value = row.get(fred_id)
        if not raw_date or not raw_value or raw_value == ".":
            continue
        logical_date = date.fromisoformat(raw_date)
        if logical_date < since or logical_date > until:
            continue
        values.append((logical_date, float(raw_value)))
    return values


def _fetch_fred_indicator_rows(
    series: dict[str, str],
    since: date,
    until: date,
    api_key: str | None = None,
) -> list[dict[str, object]]:
    return [
        {
            "date": logical_date,
            "country": series["country"],
            "series_id": series["series_id"],
            "name": series["name"],
            "category": series["category"],
            "frequency": series["frequency"],
            "value": value,
            "unit": series["unit"],
            "source": "fred",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, value in _fetch_fred_values(series["fred_id"], since, until, api_key)
    ]


def _cpi_rows(series: dict[str, str], values: list[tuple[date, float]]) -> list[dict[str, object]]:
    by_date = {logical_date: value for logical_date, value in values}
    rows: list[dict[str, object]] = []
    for logical_date, value in values:
        previous_month = _add_months(logical_date, -1)
        previous_year = _add_months(logical_date, -12)
        rows.append(
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": series["name"],
                "frequency": series["frequency"],
                "value": value,
                "index_base": series["index_base"],
                "yoy_pct": _pct_change(value, by_date.get(previous_year)),
                "mom_pct": _pct_change(value, by_date.get(previous_month)),
                "source": "fred",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def _fetch_yahoo_index_rows(
    series: dict[str, str],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    points = _fetch_yahoo_daily_closes(series["symbol"], since, until)
    rows = []
    previous_close: float | None = None
    for logical_date, close in points:
        rows.append(
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": series["name"],
                "frequency": "D",
                "category": series["category"],
                "value": close,
                "currency": series["currency"],
                "return_1d": _pct_change(close, previous_close),
                "return_1m": None,
                "source": "yahoo",
                "updated_at": datetime.now(UTC),
            }
        )
        previous_close = close
    return rows


def _fetch_yahoo_commodity_rows(
    series: dict[str, str],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    return [
        {
            "date": logical_date,
            "series_id": series["series_id"],
            "name": series["name"],
            "commodity": series["commodity"],
            "value": close,
            "unit": series["unit"],
            "currency": series["currency"],
            "source": "yahoo",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, close in _fetch_yahoo_daily_closes(series["symbol"], since, until)
    ]


def _fetch_yahoo_rate_rows(
    series: dict[str, str],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    scale = float(series.get("scale", "1"))
    return [
        {
            "date": logical_date,
            "country": series["country"],
            "series_id": series["series_id"],
            "name": series["name"],
            "frequency": series["frequency"],
            "tenor": series["tenor"],
            "value": close * scale,
            "unit": series["unit"],
            "source": "yahoo",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, close in _fetch_yahoo_daily_closes(series["symbol"], since, until)
    ]


def _fetch_yahoo_fx_rows(
    series: dict[str, str],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    return [
        {
            "date": logical_date,
            "series_id": series["series_id"],
            "base_currency": series["base_currency"],
            "quote_currency": series["quote_currency"],
            "value": close,
            "source": "yahoo",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, close in _fetch_yahoo_daily_closes(series["symbol"], since, until)
    ]


def _fetch_yahoo_daily_closes(
    symbol_value: str,
    since: date,
    until: date,
) -> list[tuple[date, float]]:
    period1 = int(datetime(since.year, since.month, since.day, tzinfo=UTC).timestamp())
    end_exclusive = until + timedelta(days=1)
    period2 = int(
        datetime(end_exclusive.year, end_exclusive.month, end_exclusive.day, tzinfo=UTC).timestamp()
    )
    symbol = quote(symbol_value, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?period1={period1}&period2={period2}&interval=1d"
    )
    request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
    with urlopen(request, timeout=YAHOO_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    closes = result["indicators"]["quote"][0].get("close") or []
    points = []
    for timestamp, close in zip(timestamps, closes, strict=False):
        if close is None:
            continue
        logical_date = datetime.fromtimestamp(timestamp, tz=UTC).date()
        points.append((logical_date, float(close)))
    return points


def _write_macro_table(data_root: Path, table: str, frame: pl.DataFrame) -> Path:
    layout = DataLakeLayout(data_root)
    path = layout.singleton_path(f"macro.{table}")
    existing = pl.read_parquet(path) if path.exists() else None
    if existing is not None and not existing.is_empty():
        incoming_ranges = (
            frame.group_by("series_id")
            .agg(pl.col("date").min().alias("min_date"), pl.col("date").max().alias("max_date"))
            .to_dicts()
        )
        for row in incoming_ranges:
            existing = existing.filter(
                ~(
                    (pl.col("series_id") == row["series_id"])
                    & (pl.col("date") >= row["min_date"])
                    & (pl.col("date") <= row["max_date"])
                )
            )
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    frame = frame.sort(["date", "series_id"]).unique(subset=["date", "series_id"], keep="last")
    return ParquetDatasetWriter().write(frame, path, mode="overwrite")


def _macro_ingest_start(data_root: Path, report_date: date) -> date:
    tables = ("cpi", "rates", "indices", "commodities", "fx", "economic_indicators")
    latest_values: list[date] = []
    for table in tables:
        path = data_root / "macro" / table / "part.parquet"
        if not path.exists():
            return date(1990, 1, 1)
        frame = pl.read_parquet(path, columns=["date"])
        if frame.is_empty():
            return date(1990, 1, 1)
        latest_values.append(frame.select(pl.col("date").max()).item())
    if not latest_values:
        return date(1990, 1, 1)
    # Re-read a trailing window so revised macro releases and CPI YoY/MoM remain correct.
    return min(latest_values) - timedelta(days=450)


def _pct_change(value: float, previous: float | None) -> float | None:
    if previous in (None, 0):
        return None
    return (value / previous - 1.0) * 100.0


def _add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(value.day, _last_day_of_month(year, month)))


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - timedelta(days=1)).day


def _run_daily_builds(data_root: Path, include_fundamentals_pit: bool, price_date: date) -> list:
    summaries = []
    price_dates = (price_date,)
    for builder in [
        build_silver_prices,
        build_security_master,
        build_universe_history,
        build_daily_prices_adj,
    ]:
        summaries.extend(builder(data_root, price_dates))
    summaries.extend(build_financials_silver(data_root))
    if include_fundamentals_pit:
        summaries.extend(build_fundamentals_pit(data_root))
    return summaries


def _run_full_builds(data_root: Path, include_fundamentals_pit: bool) -> list:
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


def _validated_backfill_paths(root: Path) -> ProjectPaths:
    resolved = root.resolve()
    if _looks_like_workspace_root(resolved):
        return ProjectPaths(root=root)

    suggestion = _nearest_workspace_root(resolved)
    message = (
        f"{resolved} does not look like the finance-pi workspace root. "
        "Run backfill commands from the repository root or pass --root explicitly."
    )
    if suggestion is not None:
        message += f" Try: --root {suggestion}"
    raise typer.BadParameter(message)


def _looks_like_workspace_root(path: Path) -> bool:
    return (path / "pyproject.toml").exists() and (path / "src" / "finance_pi").is_dir()


def _nearest_workspace_root(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if _looks_like_workspace_root(candidate):
            return candidate
    return None


def _run_yearly_backfill(
    *,
    paths: ProjectPaths,
    settings: RuntimeSettings,
    since: date,
    until: date,
    include_financials: bool,
    include_fundamentals_pit: bool,
    financial_report_codes: str,
    ticker_limit: int | None,
    naver_chunk_days: int,
    ticker_batch_size: int,
    sleep_seconds: float,
    financial_batch_size: int,
    financial_sleep_seconds: float,
) -> list[str]:
    failures: list[str] = []
    if settings.has_opendart and not any(
        paths.data_root.glob("bronze/dart_company/snapshot_dt=*/part.parquet")
    ):
        try:
            ingest_dart_company(date.today().isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART company ingest failed: {exc}")

    if not _has_naver_summary(paths):
        try:
            ingest_naver_summary(date.today().isoformat(), paths.root, "KOSPI,KOSDAQ")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Naver summary universe ingest failed: {exc}")

    try:
        ingest_naver_daily(
            since.isoformat(),
            until.isoformat(),
            paths.root,
            ticker_limit,
            naver_chunk_days,
            ticker_batch_size,
            sleep_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        failures.append(f"Naver daily price ingest failed: {exc}")

    if include_financials:
        if settings.has_opendart:
            try:
                ingest_dart_filings(since.isoformat(), until.isoformat(), paths.root, 7)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"OpenDART filings ingest failed: {exc}")
            try:
                ingest_dart_financials_bulk(
                    since.isoformat(),
                    until.isoformat(),
                    paths.root,
                    financial_report_codes,
                    None,
                    financial_batch_size,
                    financial_sleep_seconds,
                    "CFS",
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(f"OpenDART financial ingest failed: {exc}")
        else:
            failures.append("OpenDART financial backfill skipped: OPENDART_API_KEY missing")

    summaries = _run_full_builds(paths.data_root, include_fundamentals_pit)
    for summary in summaries:
        typer.echo(f"Build: {summary.dataset} rows={summary.rows} files={summary.files}")
    created = CatalogBuilder(paths.data_root, paths.catalog_path).build()
    typer.echo(f"Catalog: {paths.catalog_path}")
    typer.echo(f"Views: {len(created)}")
    return failures


def _backfill_years(start_year: int, end_year: int) -> tuple[int, ...]:
    if start_year < end_year:
        raise typer.BadParameter("start-year must be greater than or equal to end-year")
    return tuple(range(start_year, end_year - 1, -1))


def _year_bounds(year: int) -> tuple[date, date]:
    return date(year, 1, 1), date(year, 12, 31)


def _backfill_marker_path(data_root: Path, year: int) -> Path:
    return data_root / "_state" / "backfill" / "yearly" / f"{year}.json"


def _write_backfill_marker(data_root: Path, year: int, payload: dict[str, object]) -> Path:
    marker = _backfill_marker_path(data_root, year)
    marker.parent.mkdir(parents=True, exist_ok=True)
    failures = payload.get("failures")
    marker.write_text(
        json.dumps(
            {
                "year": year,
                "status": "complete_with_failures" if failures else "complete",
                "completed_at": datetime.now(UTC).isoformat(),
                **payload,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return marker


def _daily_marker_path(data_root: Path, logical_date: date) -> Path:
    return data_root / "_state" / "daily" / f"{logical_date.isoformat()}.json"


def _write_daily_marker(data_root: Path, logical_date: date, payload: dict[str, object]) -> Path:
    marker = _daily_marker_path(data_root, logical_date)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            {
                "status": "complete",
                "completed_at": datetime.now(UTC).isoformat(),
                **payload,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return marker


def _yearly_backfill_status(
    data_root: Path,
    start_year: int,
    end_year: int,
) -> tuple[dict[str, object], ...]:
    result: list[dict[str, object]] = []
    price_dates = _partition_dates(data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"))
    for year in _backfill_years(start_year, end_year):
        dates = tuple(value for value in price_dates if value.year == year)
        files = [
            path
            for path in data_root.glob(f"gold/daily_prices_adj/dt={year}-*/part.parquet")
            if path.is_file()
        ]
        marker = _backfill_marker_path(data_root, year)
        marker_status = _read_backfill_marker_status(marker)
        if marker_status:
            status = marker_status
        elif dates:
            status = "partial"
        else:
            status = "missing"
        coverage = "--" if not dates else f"{min(dates).isoformat()}..{max(dates).isoformat()}"
        result.append(
            {
                "year": year,
                "status": status,
                "price_days": len(dates),
                "rows": _count_parquet_rows(files),
                "coverage": coverage,
                "marker": marker.name if marker.exists() else "-",
            }
        )
    return tuple(result)


def _read_backfill_marker_status(marker: Path) -> str | None:
    if not marker.exists():
        return None
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "marker_invalid"
    status = data.get("status")
    return str(status) if status else "complete"


def _dataset_coverage(data_root: Path, pattern: str) -> dict[str, object]:
    files = [path for path in data_root.glob(pattern) if path.is_file()]
    dates = _partition_dates(files)
    return {
        "start": min(dates).isoformat() if dates else None,
        "end": max(dates).isoformat() if dates else None,
        "files": len(files),
        "rows": _count_parquet_rows(files),
    }


def _partition_dates(files) -> tuple[date, ...]:
    values: list[date] = []
    for path in files:
        for part in path.parts:
            if not part.startswith("dt="):
                continue
            with suppress(ValueError):
                values.append(date.fromisoformat(part.removeprefix("dt=")))
    return tuple(sorted(set(values)))


def _count_parquet_rows(files: list[Path]) -> int:
    if not files:
        return 0
    try:
        return int(
            pl.scan_parquet([path.as_posix() for path in files], hive_partitioning=True)
            .select(pl.len())
            .collect()
            .item()
        )
    except Exception:  # noqa: BLE001
        return 0


def _catchup_dates(data_root: Path, since: date | None, until: date) -> tuple[date, ...]:
    if since is None:
        latest = _latest_gold_price_date(data_root)
        latest_marker = _latest_daily_marker_date(data_root)
        latest_values = [value for value in (latest, latest_marker) if value is not None]
        latest = max(latest_values) if latest_values else None
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


def _latest_daily_marker_date(data_root: Path) -> date | None:
    values: list[date] = []
    for path in data_root.glob("_state/daily/*.json"):
        with suppress(ValueError):
            values.append(date.fromisoformat(path.stem))
    return max(values) if values else None


def _gold_price_partition_exists(data_root: Path, logical_date: date) -> bool:
    return (
        data_root / "gold" / "daily_prices_adj" / f"dt={logical_date.isoformat()}" / "part.parquet"
    ).exists()


def _daily_complete(data_root: Path, logical_date: date) -> bool:
    return _gold_price_partition_exists(data_root, logical_date) or _daily_marker_path(
        data_root, logical_date
    ).exists()


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


def _has_naver_summary(paths: ProjectPaths) -> bool:
    return any(paths.data_root.glob("bronze/naver_summary/dt=*/part.parquet"))


def _latest_price_universe_tickers(
    paths: ProjectPaths,
    limit: int | None = None,
) -> tuple[str, ...]:
    naver = _latest_naver_summary_tickers(paths)
    if naver:
        tickers = naver[:limit] if limit is not None else naver
        typer.echo(f"Price universe tickers: {len(tickers)} from Naver summary")
        return tuple(tickers)

    tickers = _latest_dart_tickers(paths)
    if limit is not None:
        tickers = tickers[:limit]
    typer.echo(f"Price universe tickers: {len(tickers)} from DART company snapshot")
    return tuple(tickers)


def _latest_naver_summary_tickers(paths: ProjectPaths) -> tuple[str, ...]:
    files = sorted(paths.data_root.glob("bronze/naver_summary/dt=*/part.parquet"))
    if not files:
        return ()
    frame = _read_parquet_files_relaxed(files)
    if frame.is_empty() or "ticker" not in frame.columns:
        return ()
    if "snapshot_dt" in frame.columns:
        frame = frame.with_columns(pl.col("snapshot_dt").cast(pl.Date, strict=False))
        latest = frame["snapshot_dt"].max()
        frame = frame.filter(pl.col("snapshot_dt") == latest)
    tickers = sorted(
        {
            ticker
            for value in frame["ticker"].to_list()
            if (ticker := _normalize_ticker_value(value)) is not None
        }
    )
    return tuple(tickers)


def _latest_dart_tickers(paths: ProjectPaths) -> tuple[str, ...]:
    files = sorted(paths.data_root.glob("bronze/dart_company/snapshot_dt=*/part.parquet"))
    if not files:
        raise typer.BadParameter("No DART company data. Run ingest dart-company first.")
    frame = _read_parquet_files_relaxed(files)
    if frame.is_empty():
        raise typer.BadParameter("DART company dataset is empty.")
    latest = frame["snapshot_dt"].max()
    values = (
        frame.filter((pl.col("snapshot_dt") == latest) & pl.col("stock_code").is_not_null())
        .select("stock_code")
        .unique()
        .sort("stock_code")
        .to_series()
        .to_list()
    )
    tickers = tuple(
        ticker for value in values if (ticker := _normalize_ticker_value(value)) is not None
    )
    if not tickers:
        raise typer.BadParameter("No stock_code values found in latest DART company snapshot.")
    return tickers


def _normalize_ticker_value(value: object) -> str | None:
    if value in (None, "", " "):
        return None
    text = str(value).strip().upper()
    if text.isdigit():
        text = text.zfill(6)
    if len(text) != 6 or not text.isalnum():
        return None
    return text


def _parse_markets(value: str) -> tuple[str, ...]:
    markets = tuple(market.strip().upper() for market in value.split(",") if market.strip())
    invalid = [market for market in markets if market not in {"KOSPI", "KOSDAQ"}]
    if invalid:
        raise typer.BadParameter(f"unsupported markets: {', '.join(invalid)}")
    if not markets:
        raise typer.BadParameter("at least one market is required")
    return markets


def _local_price_dates(data_root: Path, start: date, end: date) -> list[date]:
    dates: list[date] = []
    for path in data_root.glob("silver/prices/dt=*/part.parquet"):
        for part in path.parts:
            if part.startswith("dt="):
                with suppress(ValueError):
                    logical_date = date.fromisoformat(part.removeprefix("dt="))
                    if start <= logical_date <= end:
                        dates.append(logical_date)
    return sorted(set(dates))


def _calendar_dates(start: date, end: date) -> list[date]:
    dates: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def _number_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


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


def _admin_health_ok(url: str, timeout_seconds: float) -> bool:
    request = Request(url)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:  # noqa: BLE001
        return False
    return isinstance(payload, dict) and payload.get("status") == "ok"


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
