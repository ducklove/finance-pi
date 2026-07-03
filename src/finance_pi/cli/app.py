from __future__ import annotations

import csv
import importlib.util
import io
import json
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
from concurrent import futures
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta, timezone
from glob import glob
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import httpx
import polars as pl
import typer

from finance_pi.admin import run_admin as run_admin_server
from finance_pi.backtest import (
    BacktestConfig,
    BacktestEngine,
    CostModel,
    FixedBpsCostModel,
    KoreanTradingCostModel,
)
from finance_pi.calendar import TradingCalendar
from finance_pi.config import ProjectPaths, RuntimeSettings, diagnose_dotenv
from finance_pi.docs_site import build_docs_site
from finance_pi.factors import ParquetFactorContext, factor_registry
from finance_pi.http import HttpJsonClient, SourceApiError
from finance_pi.ingest import IngestOrchestrator, request_hash
from finance_pi.reports.data_quality import build_data_quality_report, build_dataset_scorecard
from finance_pi.reports.fraud import build_fraud_report
from finance_pi.sources.cnbc import (
    CNBC_COMMODITY_SERIES,
    CNBC_DERIVED_FX_SERIES,
    CNBC_FX_SERIES,
    CNBC_INDEX_SERIES,
    CNBC_RATE_SERIES,
)
from finance_pi.sources.cnbc import (
    cnbc_commodity_rows as _cnbc_commodity_rows,
)
from finance_pi.sources.cnbc import (
    cnbc_derived_fx_rows as _cnbc_derived_fx_rows,
)
from finance_pi.sources.cnbc import (
    cnbc_fx_rows as _cnbc_fx_rows,
)
from finance_pi.sources.cnbc import (
    cnbc_index_rows as _cnbc_index_rows,
)
from finance_pi.sources.cnbc import (
    cnbc_rate_rows as _cnbc_rate_rows,
)
from finance_pi.sources.cnbc import (
    fetch_cnbc_quotes as _fetch_cnbc_quotes,
)
from finance_pi.sources.kis import (
    KisAuthClient,
    KisDailyAdapter,
    KisDailyPriceClient,
    KisTokenCache,
    KisUniverseDailyAdapter,
)
from finance_pi.sources.krx import KrxDailyAdapter
from finance_pi.sources.macro_series import (
    ECOS_CPI_SERIES,
    ECOS_ECONOMIC_SERIES,
    ECOS_FX_SERIES,
    ECOS_RATE_SERIES,
    ECOS_TIMEOUT_SECONDS,
    FRED_ECONOMIC_SERIES,
    FRED_SERIES,
    FRED_TIMEOUT_SECONDS,
    KRX_BOND_RATE_SERIES,
    YAHOO_COMMODITY_SERIES,
    YAHOO_DERIVED_FX_SERIES,
    YAHOO_FX_SERIES,
    YAHOO_INDEX_SERIES,
    YAHOO_RATE_SERIES,
    YAHOO_TIMEOUT_SECONDS,
)
from finance_pi.sources.naver import (
    NaverDailyBackfillAdapter,
    NaverDailyPriceClient,
    NaverFinanceClient,
    NaverSummaryAdapter,
)
from finance_pi.sources.nps import NpsHoldingsAdapter, NpsHoldingsClient
from finance_pi.sources.opendart import (
    DartCompanyAdapter,
    DartFilingsAdapter,
    DartFinancialsAdapter,
    DartFinancialsBulkAdapter,
    OpenDartClient,
)
from finance_pi.sources.parsing import share_class_from_stock_kind
from finance_pi.sources.schemas import PRICE_SCHEMA
from finance_pi.storage import CatalogBuilder, DataLakeLayout, dataset_registry
from finance_pi.storage.parquet import ParquetDatasetWriter
from finance_pi.transforms import (
    BuildSummary,
    build_all,
    build_all_iter,
    build_corporate_actions,
    build_daily_market_caps,
    build_daily_prices_adj,
    build_financials_silver,
    build_fundamentals_pit,
    build_nps_holdings_delta,
    build_nps_holdings_silver,
    build_nps_universe,
    build_preferred_discount,
    build_security_master,
    build_security_relations,
    build_silver_market_caps,
    build_silver_prices,
    build_universe_history,
)
from finance_pi.util import backfill_marker_path as _backfill_marker_path
from finance_pi.util import parse_iso_date_strict as _coerce_date
from finance_pi.util import read_backfill_marker_status as _read_backfill_marker_status

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
    for row in factor_registry.describe():
        requires = ", ".join(row["requires"])
        typer.echo(f"{row['name']}\t{row['rebalance']}\tdirection={row['direction']}\t{requires}")


@app.command("mcp")
def run_mcp_server(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    """Run the MCP stdio server so LLM clients can query the data lake."""

    from finance_pi.mcp_server.server import McpDependencyError, run_stdio

    paths = ProjectPaths(root=root)
    try:
        run_stdio(paths.data_root)
    except McpDependencyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


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
    backfilled: bool = typer.Option(
        True,
        "--backfilled/--daily",
        help="Mark rows is_backfilled=True (historical bulk); the daily scheduler passes --daily",
    ),
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
        is_backfilled=backfilled,
    )
    _run_and_print([adapter], start, end)


@ingest_app.command("dart-dividends")
def ingest_dart_dividends(
    since: str = typer.Option(..., help="Start filing date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End filing date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    tickers: str | None = typer.Option(None, help="Optional comma-separated tickers"),
    corp_limit: int | None = typer.Option(None, help="Limit unique corp_code count for smoke runs"),
    sleep_seconds: float = typer.Option(0.05, help="Sleep between OpenDART dividend calls"),
) -> None:
    """Fetch per-share dividend rows from OpenDART alotMatter into silver.dividends."""

    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    start = _parse_report_date(since)
    end = _parse_report_date(until)
    selected_tickers = _parse_ticker_list(tickers) if tickers else None
    requests = _dart_dividend_requests(paths, start, end, selected_tickers, corp_limit)
    typer.echo(f"OpenDART dividend requests: {len(requests)} from filings {since}..{until}")
    rows = _fetch_dart_dividend_rows(paths, _opendart_client(settings), requests, sleep_seconds)
    summaries = _write_dividend_rows(paths.data_root, rows)
    _print_summaries(summaries)


@ingest_app.command("dart-share-counts")
def ingest_dart_share_counts(
    since: str = typer.Option(..., help="Start filing date as YYYY-MM-DD"),
    until: str = typer.Option(..., help="End filing date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    tickers: str | None = typer.Option(None, help="Optional comma-separated tickers"),
    report_codes: str = typer.Option(
        "11013,11012,11014,11011",
        help="Comma-separated DART report codes",
    ),
    corp_limit: int | None = typer.Option(None, help="Limit unique corp_code count for smoke runs"),
    sleep_seconds: float = typer.Option(0.05, help="Sleep between OpenDART stockTotqySttus calls"),
) -> None:
    """Fetch issued, treasury, and outstanding share counts from OpenDART."""

    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    start = _parse_report_date(since)
    end = _parse_report_date(until)
    selected_tickers = _parse_ticker_list(tickers) if tickers else None
    requests = _dart_share_count_requests(
        paths,
        start,
        end,
        selected_tickers,
        _parse_report_codes(report_codes),
        corp_limit,
    )
    typer.echo(f"OpenDART share-count requests: {len(requests)} from filings {since}..{until}")
    rows = _fetch_dart_share_count_rows(paths, _opendart_client(settings), requests, sleep_seconds)
    summaries = _write_share_count_rows(paths.data_root, rows)
    _print_summaries(summaries)


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


@ingest_app.command("nps-holdings")
def ingest_nps_holdings(
    since: str | None = typer.Option(None, help="Start snapshot date as YYYY-MM-DD"),
    until: str | None = typer.Option(None, help="End snapshot date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    from_sqlite: Path | None = typer.Option(
        None,
        help="Import legacy value-invest cache.db nps_holdings rows instead of scraping",
    ),
    discover_public_csv: bool = typer.Option(
        False,
        "--discover-public-csv/--fallback-public-csv",
        help="Discover the current data.go.kr CSV URL from the public metadata page",
    ),
    force: bool = typer.Option(False, help="Overwrite existing NPS bronze partitions"),
) -> None:
    paths = ProjectPaths(root=root)
    settings = RuntimeSettings.load(paths.root)
    start = _parse_report_date(since)
    end = _parse_report_date(until) if until else start
    if end < start:
        raise typer.BadParameter("until must be on or after since")
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    adapter = NpsHoldingsAdapter(
        layout=layout,
        writer=ParquetDatasetWriter(),
        client=NpsHoldingsClient(
            paths.data_root,
            user_agent=settings.naver_finance_user_agent,
            discover_public_csv=discover_public_csv,
        ),
        legacy_sqlite_path=from_sqlite,
        force=force,
    )
    _run_and_print([adapter], start, end)


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
    end = _parse_report_date(until) if until else _previous_weekday(_kst_today())
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
                    True,
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
def build_cmd_fundamentals_pit(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    dates: list[str] = typer.Option(
        [], "--date", help="Rebuild only these as-of dates (YYYY-MM-DD, repeatable)"
    ),
) -> None:
    parsed = [date.fromisoformat(value) for value in dates]
    data_root = ProjectPaths(root=root).data_root
    _print_summaries(build_fundamentals_pit(data_root, dates=parsed or None))


@build_app.command("relations")
def build_cmd_relations(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    _print_summaries(build_security_relations(ProjectPaths(root=root).data_root))


@build_app.command("preferred-discount")
def build_cmd_preferred_discount(
    root: Path = typer.Option(Path("."), help="Workspace root"),
    dates: list[str] = typer.Option(
        [], "--date", help="Rebuild only these partitions (YYYY-MM-DD, repeatable)"
    ),
    include_low_confidence: bool = typer.Option(
        False, help="Include low-confidence preferred/common pairs"
    ),
) -> None:
    parsed = [date.fromisoformat(value) for value in dates]
    _print_summaries(
        build_preferred_discount(
            ProjectPaths(root=root).data_root,
            dates=parsed or None,
            include_low_confidence=include_low_confidence,
        )
    )


@build_app.command("nps")
def build_cmd_nps(root: Path = typer.Option(Path("."), help="Workspace root")) -> None:
    data_root = ProjectPaths(root=root).data_root
    _print_summaries(build_nps_holdings_silver(data_root))
    _print_summaries(build_nps_universe(data_root))
    _print_summaries(build_nps_holdings_delta(data_root))


@app.command("nps-shadow")
def verify_nps_shadow(
    snapshot_date: str = typer.Option(..., "--date", help="Snapshot date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    sqlite_path: Path = typer.Option(
        Path("D:/Work/value-invest/cache.db"),
        help="value-invest cache.db path",
    ),
    top: int = typer.Option(100, help="Top N rows to compare"),
    tolerance: float = typer.Option(1.0, help="Allowed absolute market_value difference"),
) -> None:
    target = _parse_report_date(snapshot_date)
    paths = ProjectPaths(root=root)
    as_of, finance_rows = _nps_universe_shadow_rows(paths.data_root, target, top)
    legacy_rows = _legacy_nps_shadow_rows(sqlite_path, target, top)
    result = _compare_nps_shadow(finance_rows, legacy_rows, tolerance)
    typer.echo(
        "NPS shadow: "
        f"date={target.isoformat()} as_of={as_of.isoformat() if as_of else '--'} "
        f"finance={len(finance_rows)} legacy={len(legacy_rows)} status={result['status']}"
    )
    if result["missing"]:
        typer.echo(f"missing_in_finance: {', '.join(result['missing'][:20])}")
    if result["extra"]:
        typer.echo(f"extra_in_finance: {', '.join(result['extra'][:20])}")
    if result["order_mismatches"]:
        sample = ", ".join(result["order_mismatches"][:10])
        typer.echo(f"order_mismatches: {sample}")
    if result["value_mismatches"]:
        sample = ", ".join(result["value_mismatches"][:10])
        typer.echo(f"value_mismatches: {sample}")
    if result["status"] != "pass":
        raise typer.Exit(code=1)


def _backtest_cost_model(
    cost_bps: float,
    commission_bps: float | None,
    sell_tax_bps: float | None,
) -> CostModel:
    if commission_bps is None and sell_tax_bps is None:
        return FixedBpsCostModel(bps=cost_bps)
    return KoreanTradingCostModel(
        commission_bps=commission_bps if commission_bps is not None else 5.0,
        sell_tax_bps=sell_tax_bps if sell_tax_bps is not None else 15.0,
    )


@backtest_app.command("run")
def run_backtest(
    factor_name: str = typer.Option(..., "--factor", help="Registered factor name"),
    start: str = typer.Option(..., help="Start date as YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date as YYYY-MM-DD"),
    root: Path = typer.Option(Path("."), help="Workspace root"),
    top_fraction: float = typer.Option(0.1, help="Selection fraction"),
    cost_bps: float = typer.Option(
        10.0,
        "--cost-bps",
        help="Fixed round-trip cost in bps (ignored when commission/sell-tax given)",
    ),
    commission_bps: float | None = typer.Option(
        None,
        "--commission-bps",
        help="Commission bps per side; switches to KoreanTradingCostModel (default 5.0)",
    ),
    sell_tax_bps: float | None = typer.Option(
        None,
        "--sell-tax-bps",
        help="Sell-side transaction tax bps; switches to KoreanTradingCostModel (default 15.0)",
    ),
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
    cost_model = _backtest_cost_model(cost_bps, commission_bps, sell_tax_bps)
    result = BacktestEngine(calendar, cost_model=cost_model).run(
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
    price_date = _previous_weekday(parsed_date)
    failures: list[str] = []
    build_failures: list[str] = []
    if ingest:
        failures = _run_daily_ingest(paths, settings, parsed_date)
        for failure in failures:
            typer.echo(failure)
        if strict and failures:
            _record_daily_marker(paths.data_root, parsed_date, price_date, failures)
            _notify_daily_webhook(settings, parsed_date, failures, build_failures, None)
            raise typer.Exit(code=1)
    try:
        summaries = _run_daily_builds(
            paths.data_root,
            include_fundamentals_pit,
            price_date,
        )
    except Exception as exc:  # noqa: BLE001
        build_failures.append(f"Build step failed: {exc}")
        summaries = []
    quality_failures = _daily_price_quality_failures(paths.data_root, price_date)
    for failure in quality_failures:
        typer.echo(failure)
    build_failures.extend(quality_failures)
    created = CatalogBuilder(paths.data_root, paths.catalog_path).build()

    dq_path = paths.data_root / "reports" / "data_quality" / f"{parsed_date.isoformat()}.html"
    fraud_path = (
        paths.data_root / "reports" / "backtest_fraud" / f"{parsed_date.isoformat()}.html"
    )
    try:
        build_data_quality_report(paths.data_root, parsed_date).write(dq_path)
    except Exception as exc:  # noqa: BLE001
        build_failures.append(f"Data quality report failed: {exc}")
    try:
        build_fraud_report(paths.data_root, parsed_date).write(fraud_path)
    except Exception as exc:  # noqa: BLE001
        build_failures.append(f"Fraud report failed: {exc}")

    for summary in summaries:
        typer.echo(f"Build: {summary.dataset} rows={summary.rows} files={summary.files}")
    typer.echo(f"Catalog: {paths.catalog_path}")
    typer.echo(f"Views: {len(created)}")
    typer.echo(f"Data quality report: {dq_path}")
    typer.echo(f"Fraud report: {fraud_path}")
    _record_daily_marker(
        paths.data_root,
        parsed_date,
        price_date,
        [*failures, *build_failures],
    )

    scorecard = _safe_build_scorecard(paths.data_root, parsed_date)
    _notify_daily_webhook(settings, parsed_date, failures, build_failures, scorecard)
    if strict and quality_failures:
        raise typer.Exit(code=1)


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


def _safe_build_scorecard(data_root: Path, report_date: date) -> pl.DataFrame | None:
    try:
        return build_dataset_scorecard(data_root, report_date)
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Scorecard build failed: {exc}")
        return None


def _record_daily_marker(
    data_root: Path,
    report_date: date,
    price_date: date,
    failures: list[str],
) -> Path:
    return _write_daily_marker(
        data_root,
        report_date,
        {
            "report_date": report_date.isoformat(),
            "price_date": price_date.isoformat(),
            "failures": failures,
            "gold_price_partition": _gold_price_partition_exists(data_root, price_date),
        },
    )


def _notify_daily_webhook(
    settings: RuntimeSettings,
    report_date: date,
    failures: list[str],
    build_failures: list[str],
    scorecard: pl.DataFrame | None,
) -> None:
    """Best-effort daily/catchup summary POST. Never raises."""

    if not settings.has_webhook:
        return
    try:
        failed_steps = [*failures, *build_failures]
        scorecard_grades: dict[str, str] = {}
        if scorecard is not None and not scorecard.is_empty():
            scorecard_grades = {
                row["dataset"]: row["grade"] for row in scorecard.iter_rows(named=True)
            }
        worst_grade = _worst_scorecard_grade(scorecard_grades)
        ok = not failed_steps and worst_grade in (None, "A")

        should_send = (
            worst_grade in ("C", "F") or bool(failed_steps) or settings.webhook_always
        )
        if not should_send:
            return

        payload = _daily_webhook_payload(
            report_date, ok, failed_steps, scorecard_grades, worst_grade
        )
        httpx.post(settings.webhook_url, json=payload, timeout=5.0)
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Webhook notification failed (ignored): {exc}")


def _worst_scorecard_grade(scorecard_grades: dict[str, str]) -> str | None:
    grades = set(scorecard_grades.values())
    for grade in ("F", "C", "A"):
        if grade in grades:
            return grade
    return None


def _daily_webhook_payload(
    report_date: date,
    ok: bool,
    failed_steps: list[str],
    scorecard_grades: dict[str, str],
    worst_grade: str | None,
) -> dict[str, object]:
    status_text = "OK" if ok else "ISSUES"
    lines = [f"finance-pi daily {report_date.isoformat()}: {status_text}"]
    if failed_steps:
        lines.append(f"Failed steps ({len(failed_steps)}): " + "; ".join(failed_steps[:5]))
    if worst_grade is not None and worst_grade != "A":
        bad = [name for name, grade in scorecard_grades.items() if grade in ("C", "F")]
        lines.append(f"Worst dataset grade: {worst_grade} ({', '.join(sorted(bad))})")
    return {
        "content": "\n".join(lines),
        "date": report_date.isoformat(),
        "ok": ok,
        "failed_steps": failed_steps,
        "scorecard_grades": scorecard_grades,
        "worst_grade": worst_grade,
    }


def _kst_today() -> date:
    return datetime.now(timezone(timedelta(hours=9))).date()


def _parse_report_date(value: str | None) -> date:
    if value is None:
        return _kst_today()
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
        retry_attempts=settings.kis_daily_retry_attempts,
        retry_sleep_seconds=settings.kis_daily_retry_sleep_seconds,
        retry_backoff_multiplier=settings.kis_daily_retry_backoff_multiplier,
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
                settings.kis_daily_sleep_seconds,
                settings.kis_daily_ticker_batch_size,
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
                False,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART financial ingest failed: {exc}")
        try:
            ingest_dart_dividends(
                filing_start.isoformat(),
                report_date.isoformat(),
                paths.root,
                None,
                None,
                0.05,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_no_matching_dart_report_error(exc):
                typer.echo(f"OpenDART dividend ingest skipped: {exc}")
            else:
                failures.append(f"OpenDART dividend ingest failed: {exc}")
        try:
            ingest_dart_share_counts(
                filing_start.isoformat(),
                report_date.isoformat(),
                paths.root,
                None,
                "11013,11012,11014,11011",
                None,
                0.05,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_no_matching_dart_report_error(exc):
                typer.echo(f"OpenDART share-count ingest skipped: {exc}")
            else:
                failures.append(f"OpenDART share-count ingest failed: {exc}")
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

    if settings.ecos_api_key:
        for series in ECOS_CPI_SERIES:
            try:
                rows = _fetch_ecos_cpi_rows(series, since, until, settings.ecos_api_key)
                if rows:
                    macro_frames["cpi"].append(pl.DataFrame(rows))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
        for series in ECOS_RATE_SERIES:
            try:
                rows = _fetch_ecos_rate_rows(series, since, until, settings.ecos_api_key)
                if rows:
                    macro_frames["rates"].append(pl.DataFrame(rows))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
        for series in ECOS_FX_SERIES:
            try:
                rows = _fetch_ecos_fx_rows(series, since, until, settings.ecos_api_key)
                if rows:
                    macro_frames["fx"].append(pl.DataFrame(rows))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
        for series in ECOS_ECONOMIC_SERIES:
            try:
                rows = _fetch_ecos_indicator_rows(series, since, until, settings.ecos_api_key)
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
    for series in KRX_BOND_RATE_SERIES:
        try:
            rows = _fetch_krx_bond_rate_rows(series, since, until)
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
    for series in YAHOO_DERIVED_FX_SERIES:
        try:
            rows = _fetch_yahoo_derived_fx_rows(series, since, until)
            if rows:
                macro_frames["fx"].append(pl.DataFrame(rows))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"Macro {series['series_id']} ingest failed: {exc}")
    try:
        cnbc_quotes = _fetch_cnbc_quotes(
            [
                *[series["cnbc_symbol"] for series in CNBC_INDEX_SERIES],
                *[series["cnbc_symbol"] for series in CNBC_RATE_SERIES],
                *[series["cnbc_symbol"] for series in CNBC_COMMODITY_SERIES],
                *[series["cnbc_symbol"] for series in CNBC_FX_SERIES],
            ]
        )
    except Exception as exc:  # noqa: BLE001
        cnbc_quotes = {}
        failures.append(f"Macro CNBC quote ingest failed: {exc}")
    if cnbc_quotes:
        for table, rows in {
            "indices": _cnbc_index_rows(CNBC_INDEX_SERIES, cnbc_quotes, since, until),
            "rates": _cnbc_rate_rows(CNBC_RATE_SERIES, cnbc_quotes, since, until),
            "commodities": _cnbc_commodity_rows(CNBC_COMMODITY_SERIES, cnbc_quotes, since, until),
            "fx": [
                *_cnbc_fx_rows(CNBC_FX_SERIES, cnbc_quotes, since, until),
                *_cnbc_derived_fx_rows(CNBC_DERIVED_FX_SERIES, cnbc_quotes, since, until),
            ],
        }.items():
            if rows:
                macro_frames[table].append(pl.DataFrame(rows))

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


def _fetch_ecos_cpi_rows(
    series: dict[str, object],
    since: date,
    until: date,
    api_key: str,
) -> list[dict[str, object]]:
    values = _fetch_ecos_values(series, since, until, api_key)
    return _cpi_rows(
        {
            "country": str(series["country"]),
            "series_id": str(series["series_id"]),
            "name": str(series["name"]),
            "frequency": str(series["frequency"]),
            "index_base": str(series["index_base"]),
        },
        values,
        source="bok_ecos",
    )


def _fetch_ecos_rate_rows(
    series: dict[str, object],
    since: date,
    until: date,
    api_key: str,
) -> list[dict[str, object]]:
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
            "source": "bok_ecos",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, value in _fetch_ecos_values(series, since, until, api_key)
    ]


def _fetch_ecos_fx_rows(
    series: dict[str, object],
    since: date,
    until: date,
    api_key: str,
) -> list[dict[str, object]]:
    return [
        {
            "date": logical_date,
            "series_id": series["series_id"],
            "base_currency": series["base_currency"],
            "quote_currency": series["quote_currency"],
            "value": value,
            "source": "bok_ecos",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, value in _fetch_ecos_values(series, since, until, api_key)
    ]


def _fetch_ecos_indicator_rows(
    series: dict[str, object],
    since: date,
    until: date,
    api_key: str,
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
            "source": "bok_ecos",
            "updated_at": datetime.now(UTC),
        }
        for logical_date, value in _fetch_ecos_values(series, since, until, api_key)
    ]


def _fetch_ecos_values(
    series: dict[str, object],
    since: date,
    until: date,
    api_key: str,
) -> list[tuple[date, float]]:
    stat_code = str(series["stat_code"])
    cycle = str(series["cycle"])
    items = tuple(str(item) for item in series["items"])  # type: ignore[index]
    start_period = _ecos_period(since, cycle)
    end_period = _ecos_period(until, cycle)
    encoded_items = "/".join(quote(item, safe="") for item in items)
    base_url = "https://ecos.bok.or.kr/api/StatisticSearch"
    page_size = 1000
    start_index = 1
    rows: list[dict[str, object]] = []
    while True:
        end_index = start_index + page_size - 1
        url = (
            f"{base_url}/{api_key}/json/kr/{start_index}/{end_index}"
            f"/{stat_code}/{cycle}/{start_period}/{end_period}/{encoded_items}"
        )
        request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
        with urlopen(request, timeout=ECOS_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if "RESULT" in payload:
            result = payload["RESULT"]
            code = result.get("CODE")
            message = result.get("MESSAGE")
            if code == "INFO-200":
                break
            raise ValueError(f"ECOS {stat_code} {items}: {code} {message}")
        table = payload.get("StatisticSearch")
        if not isinstance(table, dict):
            raise ValueError(f"ECOS {stat_code} response is malformed")
        batch = table.get("row") or []
        if not isinstance(batch, list):
            raise ValueError(f"ECOS {stat_code} rows response is malformed")
        rows.extend(row for row in batch if isinstance(row, dict))
        total_count = int(table.get("list_total_count") or len(rows))
        if len(rows) >= total_count or len(batch) < page_size:
            break
        start_index += page_size

    values: list[tuple[date, float]] = []
    for row in rows:
        raw_time = row.get("TIME")
        raw_value = row.get("DATA_VALUE")
        if raw_time in (None, "") or raw_value in (None, ""):
            continue
        logical_date = _parse_ecos_period(str(raw_time), cycle)
        if logical_date < since or logical_date > until:
            continue
        values.append((logical_date, float(str(raw_value).replace(",", ""))))
    return sorted(values, key=lambda point: point[0])


def _ecos_period(value: date, cycle: str) -> str:
    if cycle == "D":
        return value.strftime("%Y%m%d")
    if cycle == "M":
        return value.strftime("%Y%m")
    if cycle == "Q":
        quarter = (value.month - 1) // 3 + 1
        return f"{value.year}Q{quarter}"
    if cycle == "A":
        return str(value.year)
    raise ValueError(f"unsupported ECOS cycle: {cycle}")


def _parse_ecos_period(value: str, cycle: str) -> date:
    if cycle == "D":
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    if cycle == "M":
        return date(int(value[:4]), int(value[4:6]), 1)
    if cycle == "Q":
        match = re.fullmatch(r"(\d{4})Q([1-4])", value)
        if match is None:
            raise ValueError(f"invalid ECOS quarter: {value}")
        year = int(match.group(1))
        month = (int(match.group(2)) - 1) * 3 + 1
        return date(year, month, 1)
    if cycle == "A":
        return date(int(value[:4]), 1, 1)
    raise ValueError(f"unsupported ECOS cycle: {cycle}")


def _cpi_rows(
    series: dict[str, str],
    values: list[tuple[date, float]],
    *,
    source: str = "fred",
) -> list[dict[str, object]]:
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
                "source": source,
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


def _fetch_krx_bond_rate_rows(
    series: dict[str, str],
    since: date,
    until: date,
    fetcher: object | None = None,
) -> list[dict[str, object]]:
    if fetcher is None:
        from pykrx import bond  # type: ignore[import-untyped]

        fetcher = bond.get_otc_treasury_yields
    frame = fetcher(since.strftime("%Y%m%d"), until.strftime("%Y%m%d"), series["krx_kind"])
    rows = []
    for row in frame.reset_index().to_dict("records"):
        raw_date = row.get("일자") or row.get("date") or row.get("Date")
        raw_value = row.get("수익률") or row.get("yield") or row.get("Yield")
        if raw_date is None or raw_value is None:
            continue
        logical_date = _parse_compact_or_iso_date(raw_date)
        if logical_date < since or logical_date > until:
            continue
        rows.append(
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": series["name"],
                "frequency": series["frequency"],
                "tenor": series["tenor"],
                "value": float(raw_value),
                "unit": series["unit"],
                "source": "pykrx",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


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


def _fetch_yahoo_derived_fx_rows(
    series: dict[str, str],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    numerator = dict(_fetch_yahoo_daily_closes(series["numerator_symbol"], since, until))
    denominator = dict(_fetch_yahoo_daily_closes(series["denominator_symbol"], since, until))
    rows = []
    for logical_date in sorted(set(numerator) & set(denominator)):
        denominator_value = denominator[logical_date]
        if denominator_value == 0:
            continue
        rows.append(
            {
                "date": logical_date,
                "series_id": series["series_id"],
                "base_currency": series["base_currency"],
                "quote_currency": series["quote_currency"],
                "value": numerator[logical_date] / denominator_value,
                "source": "yahoo_derived",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


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
    if table == "indices" and "return_1m" in frame.columns:
        frame = frame.with_columns(pl.col("return_1m").cast(pl.Float64, strict=False))
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


def _parse_compact_or_iso_date(value: object) -> date:
    if isinstance(value, date):
        return value
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text[:10])


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
        build_corporate_actions,
        build_daily_prices_adj,
    ]:
        summaries.extend(builder(data_root, price_dates))
    summaries.extend(build_security_relations(data_root))
    summaries.extend(build_preferred_discount(data_root, dates=price_dates))
    summaries.extend(build_financials_silver(data_root))
    if include_fundamentals_pit:
        summaries.extend(build_fundamentals_pit(data_root))
    return summaries


def _run_full_builds(data_root: Path, include_fundamentals_pit: bool) -> list:
    builders = [
        build_silver_prices,
        build_security_master,
        build_security_relations,
        build_universe_history,
        build_corporate_actions,
        build_daily_prices_adj,
        build_preferred_discount,
        build_nps_holdings_silver,
        build_nps_universe,
        build_nps_holdings_delta,
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
            ingest_dart_company(_kst_today().isoformat(), paths.root)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OpenDART company ingest failed: {exc}")

    if not _has_naver_summary(paths):
        try:
            ingest_naver_summary(_kst_today().isoformat(), paths.root, "KOSPI,KOSDAQ")
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
                    True,
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
    failures = payload.get("failures") or []
    has_gold = bool(payload.get("gold_price_partition"))
    if failures and not has_gold:
        status = "failed"
    elif failures:
        status = "complete_with_failures"
    else:
        status = "complete"
    marker.write_text(
        json.dumps(
            {
                "status": status,
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


def _read_daily_marker_status(marker: Path) -> str | None:
    if not marker.exists():
        return None
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "marker_invalid"
    status = data.get("status")
    if status:
        return str(status)
    failures = data.get("failures") or []
    return "complete_with_failures" if failures else "complete"


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


def _nps_universe_shadow_rows(
    data_root: Path,
    target: date,
    top: int,
) -> tuple[date | None, list[dict[str, object]]]:
    candidates: list[tuple[date, Path]] = []
    for path in data_root.glob("gold/nps_universe/dt=*/part.parquet"):
        for part in path.parts:
            if not part.startswith("dt="):
                continue
            with suppress(ValueError):
                logical_date = date.fromisoformat(part.removeprefix("dt="))
                if logical_date <= target:
                    candidates.append((logical_date, path))
            break
    if not candidates:
        return None, []
    as_of, path = max(candidates, key=lambda item: item[0])
    frame = (
        pl.read_parquet(path, hive_partitioning=True)
        .sort("rank")
        .head(max(1, top))
        .select("rank", "stock_code", "stock_name", "market_value")
    )
    return as_of, list(frame.iter_rows(named=True))


def _legacy_nps_shadow_rows(
    sqlite_path: Path,
    target: date,
    top: int,
) -> list[dict[str, object]]:
    if not sqlite_path.exists():
        raise typer.BadParameter(f"value-invest cache.db not found: {sqlite_path}")
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT stock_code, stock_name, market_value
            FROM nps_holdings
            WHERE date = ?
            ORDER BY market_value DESC, stock_code
            LIMIT ?
            """,
            (target.isoformat(), max(1, top)),
        ).fetchall()
    return [
        {
            "rank": index,
            "stock_code": str(row["stock_code"]),
            "stock_name": row["stock_name"],
            "market_value": row["market_value"],
        }
        for index, row in enumerate(rows, start=1)
    ]


def _compare_nps_shadow(
    finance_rows: list[dict[str, object]],
    legacy_rows: list[dict[str, object]],
    tolerance: float,
) -> dict[str, object]:
    finance_by_code = {str(row["stock_code"]): row for row in finance_rows}
    legacy_by_code = {str(row["stock_code"]): row for row in legacy_rows}
    finance_codes = list(finance_by_code)
    legacy_codes = list(legacy_by_code)
    missing = [code for code in legacy_codes if code not in finance_by_code]
    extra = [code for code in finance_codes if code not in legacy_by_code]
    order_mismatches = [
        f"{left}!={right}"
        for left, right in zip(finance_codes, legacy_codes, strict=False)
        if left != right
    ]
    value_mismatches: list[str] = []
    for code in sorted(set(finance_by_code) & set(legacy_by_code)):
        finance_value = _float_or_none(finance_by_code[code].get("market_value"))
        legacy_value = _float_or_none(legacy_by_code[code].get("market_value"))
        if finance_value is None or legacy_value is None:
            if finance_value != legacy_value:
                value_mismatches.append(f"{code}: {finance_value}!={legacy_value}")
            continue
        if abs(finance_value - legacy_value) > tolerance:
            value_mismatches.append(f"{code}: {finance_value:.0f}!={legacy_value:.0f}")
    status = (
        "pass"
        if not missing and not extra and not order_mismatches and not value_mismatches
        else "fail"
    )
    return {
        "status": status,
        "missing": missing,
        "extra": extra,
        "order_mismatches": order_mismatches,
        "value_mismatches": value_mismatches,
    }


def _float_or_none(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if value in (None, "", " ", "-"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


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
    incomplete_dates = _incomplete_daily_marker_dates(data_root, since, until)
    if since is None:
        latest = _latest_gold_price_date(data_root)
        latest_marker = _latest_complete_daily_marker_date(data_root)
        latest_values = [value for value in (latest, latest_marker) if value is not None]
        latest = max(latest_values) if latest_values else None
        if latest is None:
            if incomplete_dates:
                return incomplete_dates
            raise typer.BadParameter("No gold price data found. Pass --since explicitly.")
        since = latest + timedelta(days=1)
    if until < since:
        return incomplete_dates
    scheduled_dates = TradingCalendar.krx_trading_days(since, until).dates
    return tuple(sorted(set(incomplete_dates) | set(scheduled_dates)))


def _latest_gold_price_date(data_root: Path) -> date | None:
    values: list[date] = []
    for path in data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"):
        for part in path.parts:
            if not part.startswith("dt="):
                continue
            with suppress(ValueError):
                values.append(date.fromisoformat(part.removeprefix("dt=")))
    return max(values) if values else None


def _latest_complete_daily_marker_date(data_root: Path) -> date | None:
    values: list[date] = []
    for path in data_root.glob("_state/daily/*.json"):
        if _read_daily_marker_status(path) != "complete":
            continue
        with suppress(ValueError):
            values.append(date.fromisoformat(path.stem))
    return max(values) if values else None


def _incomplete_daily_marker_dates(
    data_root: Path,
    since: date | None,
    until: date,
) -> tuple[date, ...]:
    values: list[date] = []
    for path in data_root.glob("_state/daily/*.json"):
        status = _read_daily_marker_status(path)
        if status in (None, "complete"):
            continue
        with suppress(ValueError):
            value = date.fromisoformat(path.stem)
            if since is not None and value < since:
                continue
            if value <= until and TradingCalendar.is_krx_trading_day(value):
                values.append(value)
    return tuple(sorted(set(values)))


def _gold_price_partition_exists(data_root: Path, logical_date: date) -> bool:
    return (
        data_root / "gold" / "daily_prices_adj" / f"dt={logical_date.isoformat()}" / "part.parquet"
    ).exists()


def _daily_complete(data_root: Path, logical_date: date) -> bool:
    marker = _daily_marker_path(data_root, logical_date)
    if marker.exists():
        return _read_daily_marker_status(marker) in {"complete", "complete_with_failures"}
    return _gold_price_partition_exists(data_root, logical_date)


def _daily_price_quality_failures(data_root: Path, logical_date: date) -> list[str]:
    current_rows = _gold_price_row_count(data_root, logical_date)
    if current_rows <= 0:
        return [f"Gold price partition missing or empty for {logical_date.isoformat()}"]
    previous = _previous_gold_price_row_count(data_root, logical_date)
    if previous is None:
        return []
    previous_date, previous_rows = previous
    if previous_rows >= 100 and current_rows < int(previous_rows * 0.95):
        return [
            (
                f"Gold price row count low for {logical_date.isoformat()}: "
                f"{current_rows} rows vs {previous_rows} rows on {previous_date.isoformat()}"
            )
        ]
    return []


def _gold_price_row_count(data_root: Path, logical_date: date) -> int:
    path = (
        data_root / "gold" / "daily_prices_adj" / f"dt={logical_date.isoformat()}" / "part.parquet"
    )
    return _count_parquet_rows([path]) if path.exists() else 0


def _previous_gold_price_row_count(data_root: Path, logical_date: date) -> tuple[date, int] | None:
    previous_dates = [
        value
        for value in _partition_dates(data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"))
        if value < logical_date
    ]
    if not previous_dates:
        return None
    previous_date = max(previous_dates)
    rows = _gold_price_row_count(data_root, previous_date)
    return previous_date, rows


def _is_no_matching_dart_report_error(exc: Exception) -> bool:
    return "No filings matched the requested financial report codes" in str(exc)


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


def _dart_dividend_requests(
    paths: ProjectPaths,
    since: date,
    until: date,
    tickers: tuple[str, ...] | None = None,
    corp_limit: int | None = None,
) -> tuple[dict[str, object], ...]:
    requests = _dart_financial_requests(paths, since, until, ("11011",), None)
    if tickers is not None:
        ticker_set = set(tickers)
        requests = tuple(
            request
            for request in requests
            if str(request.get("stock_code", "")).zfill(6) in ticker_set
        )
    if corp_limit is not None:
        corps = sorted({str(request["corp_code"]) for request in requests})[:corp_limit]
        requests = tuple(request for request in requests if str(request["corp_code"]) in corps)
    return requests


def _dart_share_count_requests(
    paths: ProjectPaths,
    since: date,
    until: date,
    tickers: set[str] | None,
    report_codes: tuple[str, ...],
    corp_limit: int | None,
) -> tuple[dict[str, object], ...]:
    requests = _dart_financial_requests(paths, since, until, report_codes, None)
    if tickers:
        ticker_set = set(tickers)
        requests = tuple(
            request
            for request in requests
            if str(request.get("stock_code", "")).zfill(6) in ticker_set
        )
    if corp_limit is not None:
        corps = sorted({str(request["corp_code"]) for request in requests})[:corp_limit]
        requests = tuple(request for request in requests if str(request["corp_code"]) in corps)
    return requests


def _fetch_dart_dividend_rows(
    paths: ProjectPaths,
    client: OpenDartClient,
    requests: tuple[dict[str, object], ...],
    sleep_seconds: float,
) -> list[dict[str, object]]:
    mapping = _dividend_security_mapping(paths.data_root)
    rows: list[dict[str, object]] = []
    for index, request in enumerate(requests, start=1):
        corp_code = str(request["corp_code"])
        bsns_year = int(request["bsns_year"])
        report_code = str(request["report_code"])
        available_date = _coerce_date(request["available_date"])
        raw_rows = client.fetch_dividend_matters(corp_code, bsns_year, report_code)
        rows.extend(
            _normalize_dart_dividend_rows(
                raw_rows,
                bsns_year,
                report_code,
                available_date,
                mapping,
            )
        )
        if sleep_seconds > 0 and index < len(requests):
            sleep(sleep_seconds)
    return rows


def _fetch_dart_share_count_rows(
    paths: ProjectPaths,
    client: OpenDartClient,
    requests: tuple[dict[str, object], ...],
    sleep_seconds: float,
) -> list[dict[str, object]]:
    if sleep_seconds <= 0 and len(requests) > 1:
        return _fetch_dart_share_count_rows_parallel(client, requests)

    rows: list[dict[str, object]] = []
    for index, request in enumerate(requests, start=1):
        corp_code = str(request["corp_code"])
        bsns_year = int(request["bsns_year"])
        report_code = str(request["report_code"])
        available_date = _coerce_date(request["available_date"])
        raw_rows = client.fetch_stock_total_quantity(corp_code, bsns_year, report_code)
        rows.extend(
            _normalize_dart_share_count_rows(
                raw_rows,
                bsns_year,
                report_code,
                available_date,
            )
        )
        if sleep_seconds > 0 and index < len(requests):
            sleep(sleep_seconds)
    return rows


def _fetch_dart_share_count_rows_parallel(
    client: OpenDartClient,
    requests: tuple[dict[str, object], ...],
    max_workers: int = 4,
) -> list[dict[str, object]]:
    def fetch_one(request: dict[str, object]) -> list[dict[str, object]]:
        corp_code = str(request["corp_code"])
        bsns_year = int(request["bsns_year"])
        report_code = str(request["report_code"])
        available_date = _coerce_date(request["available_date"])
        raw_rows = client.fetch_stock_total_quantity(corp_code, bsns_year, report_code)
        return _normalize_dart_share_count_rows(raw_rows, bsns_year, report_code, available_date)

    rows: list[dict[str, object]] = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        submitted = {pool.submit(fetch_one, request): request for request in requests}
        failures = 0
        for future in futures.as_completed(submitted):
            request = submitted[future]
            try:
                rows.extend(future.result())
            except Exception as exc:  # noqa: BLE001
                failures += 1
                typer.echo(
                    "OpenDART share-count request failed: "
                    f"corp_code={request.get('corp_code')} "
                    f"bsns_year={request.get('bsns_year')} "
                    f"report_code={request.get('report_code')} "
                    f"error={exc}",
                    err=True,
                )
        if failures:
            typer.echo(f"OpenDART share-count failures: {failures}", err=True)
    return rows


def _receipt_date(rcept_no: object) -> date | None:
    """Derive the true DART receipt date from the first 8 digits of rcept_no.

    Mirrors ``finance_pi.sources.opendart.client._receipt_date``, which is not
    exported from that module's public surface. Kept in sync manually; see
    that function for the canonical implementation.
    """

    text = str(rcept_no or "")
    if len(text) < 8 or not text[:8].isdigit():
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _normalize_dart_dividend_rows(
    raw_rows: list[dict[str, object]],
    bsns_year: int,
    report_code: str,
    available_date: date,
    mapping: dict[tuple[str, str], dict[str, str]],
) -> list[dict[str, object]]:
    by_key: dict[tuple[str, str | None, int, str | None], dict[str, object]] = {}
    for row in raw_rows:
        metric = _dividend_metric(str(row.get("se") or ""))
        if metric is None:
            continue
        stock_kind = _clean_text(row.get("stock_knd"))
        share_class = share_class_from_stock_kind(stock_kind)
        if share_class is None:
            continue
        corp_code = str(row.get("corp_code") or "")
        corp_name = _clean_text(row.get("corp_name"))
        source_rcept_no = _clean_text(row.get("rcept_no"))
        for column, fiscal_year in [
            ("thstrm", bsns_year),
            ("frmtrm", bsns_year - 1),
            ("lwfr", bsns_year - 2),
        ]:
            value = _float_or_none(row.get(column))
            if value is None:
                continue
            key = (corp_code, stock_kind, fiscal_year, source_rcept_no)
            out = by_key.setdefault(
                key,
                {
                    "fiscal_year": fiscal_year,
                    "fiscal_period_end": date(fiscal_year, 12, 31),
                    "rcept_dt": _receipt_date(source_rcept_no) or available_date,
                    "available_date": available_date,
                    "corp_code": corp_code,
                    "corp_name": corp_name,
                    "security_id": None,
                    "ticker": None,
                    "share_class": share_class,
                    "stock_kind": stock_kind,
                    "cash_dividend_per_share": None,
                    "stock_dividend_per_share": None,
                    "cash_dividend_yield_pct": None,
                    "currency": "KRW",
                    "source_rcept_no": source_rcept_no,
                    "report_type": report_code,
                    "source": "opendart.alotMatter",
                    "is_estimated": False,
                },
            )
            out[metric] = value
            matched = mapping.get((corp_code, share_class))
            if matched is not None:
                out["security_id"] = matched["security_id"]
                out["ticker"] = matched["ticker"]
    return list(by_key.values())


def _normalize_dart_share_count_rows(
    raw_rows: list[dict[str, object]],
    bsns_year: int,
    report_code: str,
    available_date: date,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in raw_rows:
        stock_kind = _clean_text(row.get("se"))
        if stock_kind in (None, "합계", "비고"):
            continue
        fiscal_period_end = (
            _coerce_date(row.get("stlm_dt")) if row.get("stlm_dt") else date(bsns_year, 12, 31)
        )
        source_rcept_no = _clean_text(row.get("rcept_no"))
        rows.append(
            {
                "fiscal_year": bsns_year,
                "fiscal_period_end": fiscal_period_end,
                "rcept_dt": _receipt_date(source_rcept_no) or available_date,
                "available_date": available_date,
                "corp_code": str(row.get("corp_code") or ""),
                "corp_name": _clean_text(row.get("corp_name")),
                "share_class": share_class_from_stock_kind(stock_kind),
                "stock_kind": stock_kind,
                "authorized_shares": _int_or_none(row.get("isu_stock_totqy")),
                "cumulative_issued_shares": _int_or_none(row.get("now_to_isu_stock_totqy")),
                "cumulative_decreased_shares": _int_or_none(row.get("now_to_dcrs_stock_totqy")),
                "capital_reduction_shares": _int_or_none(row.get("redc")),
                "profit_retirement_shares": _int_or_none(row.get("profit_incnr")),
                "redemption_shares": _int_or_none(row.get("rdmstk_repy")),
                "other_decrease_shares": _int_or_none(row.get("etc")),
                "issued_shares": _int_or_none(row.get("istc_totqy")),
                "treasury_shares": _int_or_none(row.get("tesstk_co")),
                "outstanding_shares": _int_or_none(row.get("distb_stock_co")),
                "source_rcept_no": source_rcept_no,
                "report_type": report_code,
                "source": "opendart.stockTotqySttus",
                "is_estimated": False,
            }
        )
    return rows


def _write_dividend_rows(data_root: Path, rows: list[dict[str, object]]) -> list[BuildSummary]:
    if not rows:
        return [BuildSummary("silver.dividends", 0, 0)]
    frame = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col("fiscal_year").cast(pl.Int32, strict=False),
        pl.col("fiscal_period_end").cast(pl.Date, strict=False),
        pl.col("rcept_dt").cast(pl.Date, strict=False),
        pl.col("available_date").cast(pl.Date, strict=False),
        pl.col("cash_dividend_per_share").cast(pl.Float64, strict=False),
        pl.col("stock_dividend_per_share").cast(pl.Float64, strict=False),
        pl.col("cash_dividend_yield_pct").cast(pl.Float64, strict=False),
        pl.col("is_estimated").cast(pl.Boolean, strict=False),
    )
    existing = _read_optional_parquet(data_root / "silver/dividends/fiscal_year=*/part.parquet")
    if existing is not None and not existing.is_empty():
        incoming_years = frame["fiscal_year"].drop_nulls().unique().to_list()
        existing = existing.filter(~pl.col("fiscal_year").is_in(incoming_years))
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    frame = frame.sort(["fiscal_year", "corp_code", "share_class", "stock_kind", "available_date"])
    frame = frame.unique(
        subset=["fiscal_year", "corp_code", "stock_kind", "source_rcept_no"],
        keep="last",
    )
    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    files = 0
    rows = 0
    for fiscal_year in sorted(set(frame["fiscal_year"].drop_nulls().to_list())):
        partition = frame.filter(pl.col("fiscal_year") == fiscal_year).drop("fiscal_year")
        path = layout.partition_path("silver.dividends", date(int(fiscal_year), 12, 31))
        writer.write(partition, path, mode="overwrite")
        files += 1
        rows += partition.height
    return [BuildSummary("silver.dividends", rows, files)]


def _write_share_count_rows(data_root: Path, rows: list[dict[str, object]]) -> list[BuildSummary]:
    if not rows:
        return [BuildSummary("silver.share_counts", 0, 0)]
    frame = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col("fiscal_year").cast(pl.Int32, strict=False),
        pl.col("fiscal_period_end").cast(pl.Date, strict=False),
        pl.col("rcept_dt").cast(pl.Date, strict=False),
        pl.col("available_date").cast(pl.Date, strict=False),
        pl.col("authorized_shares").cast(pl.Float64, strict=False),
        pl.col("cumulative_issued_shares").cast(pl.Float64, strict=False),
        pl.col("cumulative_decreased_shares").cast(pl.Float64, strict=False),
        pl.col("capital_reduction_shares").cast(pl.Float64, strict=False),
        pl.col("profit_retirement_shares").cast(pl.Float64, strict=False),
        pl.col("redemption_shares").cast(pl.Float64, strict=False),
        pl.col("other_decrease_shares").cast(pl.Float64, strict=False),
        pl.col("issued_shares").cast(pl.Float64, strict=False),
        pl.col("treasury_shares").cast(pl.Float64, strict=False),
        pl.col("outstanding_shares").cast(pl.Float64, strict=False),
        pl.col("is_estimated").cast(pl.Boolean, strict=False),
    )
    existing = _read_optional_parquet(data_root / "silver/share_counts/fiscal_year=*/part.parquet")
    if existing is not None and not existing.is_empty():
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    frame = frame.unique(
        subset=["fiscal_year", "corp_code", "stock_kind", "source_rcept_no", "report_type"],
        keep="last",
    )
    frame = frame.sort(["fiscal_year", "corp_code", "stock_kind", "available_date"])
    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    files = 0
    rows_written = 0
    for fiscal_year in sorted(set(frame["fiscal_year"].drop_nulls().to_list())):
        partition = frame.filter(pl.col("fiscal_year") == fiscal_year).drop("fiscal_year")
        path = layout.partition_path("silver.share_counts", date(int(fiscal_year), 12, 31))
        writer.write(partition, path, mode="overwrite")
        files += 1
        rows_written += partition.height
    return [BuildSummary("silver.share_counts", rows_written, files)]


def _dividend_security_mapping(data_root: Path) -> dict[tuple[str, str], dict[str, str]]:
    master = _read_optional_parquet(data_root / "gold/security_master.parquet")
    if master is None or master.is_empty():
        return {}
    frame = master.select("corp_code", "name", "share_class", "security_id", "ticker")
    result: dict[tuple[str, str], dict[str, str]] = {}
    direct = frame.drop_nulls(["corp_code", "share_class", "security_id", "ticker"])
    counts = direct.group_by(["corp_code", "share_class"]).agg(pl.len().alias("n")).to_dicts()
    unique_keys = {(row["corp_code"], row["share_class"]) for row in counts if row["n"] == 1}
    for row in direct.to_dicts():
        key = (row["corp_code"], row["share_class"])
        if key in unique_keys:
            result[key] = {"security_id": row["security_id"], "ticker": row["ticker"]}
    preferred = frame.filter(
        pl.col("corp_code").is_null()
        & (pl.col("share_class") == "preferred")
        & pl.col("name").is_not_null()
        & pl.col("security_id").is_not_null()
        & pl.col("ticker").is_not_null()
    )
    for common in direct.filter(pl.col("share_class") == "common").to_dicts():
        corp_code = common["corp_code"]
        corp_name = str(common.get("name") or "")
        if not corp_code or not corp_name or (corp_code, "preferred") in result:
            continue
        candidates = preferred.filter(pl.col("name").str.starts_with(corp_name)).to_dicts()
        if len(candidates) == 1:
            row = candidates[0]
            result[(corp_code, "preferred")] = {
                "security_id": row["security_id"],
                "ticker": row["ticker"],
            }
    return result


def _read_optional_parquet(pattern: Path) -> pl.DataFrame | None:
    files = sorted(Path(path) for path in glob(pattern.as_posix()))
    if not files:
        return None
    return _read_parquet_files_relaxed(files)


def _dividend_metric(label: str) -> str | None:
    normalized = re.sub(r"\s+", "", label)
    if "주당현금배당금" in normalized:
        return "cash_dividend_per_share"
    if "주당주식배당" in normalized:
        return "stock_dividend_per_share"
    if "현금배당수익률" in normalized:
        return "cash_dividend_yield_pct"
    return None


def _clean_text(value: object) -> str | None:
    if value in (None, "", " ", "-"):
        return None
    return str(value).strip()


def _int_or_none(value: object) -> int | None:
    parsed = _float_or_none(value)
    return int(parsed) if parsed is not None else None


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


def _parse_ticker_list(value: str) -> tuple[str, ...]:
    tickers = []
    for item in value.split(","):
        ticker = _normalize_ticker_value(item)
        if ticker is not None:
            tickers.append(ticker)
    return tuple(dict.fromkeys(tickers))


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
    while not TradingCalendar.is_krx_trading_day(current):
        current -= timedelta(days=1)
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
