"""Pure tool logic for the finance-pi MCP server.

Every function here takes plain arguments and returns JSON-serializable
values. This module intentionally has no dependency on the ``mcp`` package so
the logic can be unit tested without the optional extra installed; the thin
protocol glue lives in :mod:`finance_pi.mcp_server.server`.

All tools are strictly read-only. ``query`` accepts a single SELECT/WITH
statement against the DuckDB catalog opened with ``read_only=True`` and
rejects write/DDL keywords after comment stripping. The installed duckdb
(1.5.x) has no ``statement_timeout`` setting, so query timeouts use a
``threading.Timer`` that calls ``connection.interrupt()`` instead.
"""

from __future__ import annotations

import re
import threading
from datetime import date, datetime
from decimal import Decimal
from glob import glob
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from finance_pi.backtest import (
    BacktestConfig,
    BacktestEngine,
    FixedBpsCostModel,
    KoreanTradingCostModel,
)
from finance_pi.calendar import TradingCalendar
from finance_pi.factors import ParquetFactorContext, factor_registry
from finance_pi.storage.datasets import dataset_registry

DEFAULT_MAX_ROWS = 200
HARD_MAX_ROWS = 1000
DEFAULT_MAX_RESULT_BYTES = 1_000_000
DEFAULT_QUERY_TIMEOUT_SECONDS = 30.0


class McpToolError(ValueError):
    """Raised when a tool receives invalid input or cannot serve the request."""


_FORBIDDEN_KEYWORDS = (
    "insert", "update", "delete", "merge", "create", "drop", "alter", "truncate",
    "attach", "detach", "copy", "export", "import", "pragma", "install", "load",
    "call", "set", "reset", "vacuum", "checkpoint", "grant", "revoke", "begin",
    "commit", "rollback",
)
_FORBIDDEN_SQL = re.compile(r"\b(" + "|".join(_FORBIDDEN_KEYWORDS) + r")\b", re.IGNORECASE)
_SELECT_START = re.compile(r"^[\s(]*(select|with)\b", re.IGNORECASE)
_VIEW_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*")
_TICKER = re.compile(r"[0-9A-Za-z]{1,12}")
_PARTITION_VALUE = re.compile(
    r"(?:dt|rcept_dt|snapshot_dt|request_dt|fiscal_year|year)=([0-9][0-9-]*)"
)

_DATASET_DESCRIPTIONS: dict[str, str] = {
    "bronze.krx_daily_raw": "Raw KRX OpenAPI daily OHLCV/market-cap rows.",
    "bronze.kis_daily_raw": "Raw KIS (Korea Investment & Securities) daily OHLCV rows.",
    "bronze.naver_daily_raw": "Raw Naver Finance daily price backfill rows.",
    "bronze.naver_summary_raw": "Raw Naver Finance per-ticker snapshots (market cap, PER, shares).",
    "bronze.nps_holdings_raw": "Raw National Pension Service (NPS) equity holdings disclosures.",
    "bronze.marcap_raw": "Raw FinanceData marcap yearly market-cap files (1995+).",
    "bronze.dart_filings_raw": "Raw OpenDART filing list rows.",
    "bronze.dart_financials_raw": "Raw OpenDART financial statement account rows.",
    "bronze.dart_company_raw": "Raw OpenDART corp-code snapshots.",
    "bronze.pre2010_raw": "Raw pre-2010 historical price rows from legacy sources.",
    "silver.prices": "Cleaned daily prices with security identity and trading flags.",
    "silver.market_caps": "Cleaned daily market caps mapped to security identity.",
    "silver.financials": "Normalized financial-statement facts with PIT available_date.",
    "silver.filings": "Normalized DART filing list.",
    "silver.corporate_actions": "Corporate action events with price adjustment factors.",
    "silver.dividends": "Per-share dividend declarations from DART (PIT available_date).",
    "silver.share_counts": "Issued/treasury/outstanding share counts from DART.",
    "silver.nps_holdings": "NPS holdings mapped to security identity.",
    "silver.security_identity": "Ticker-to-security identity mapping over time.",
    "silver.security_relations": "Common/preferred share relations with confidence.",
    "gold.daily_prices_adj": (
        "Adjusted daily OHLCV with return_1d - the primary backtest price table."
    ),
    "gold.daily_market_caps": "Daily market caps with ranks and estimation flags.",
    "gold.fundamentals_pit": (
        "Point-in-time fundamentals: account amounts visible as of each as_of_date. "
        "Filter as_of_date <= your evaluation date to stay look-ahead free."
    ),
    "gold.universe_history": "Daily investable universe membership with halt/designation flags.",
    "gold.nps_universe": "Point-in-time NPS holdings universe ranked by market value.",
    "gold.preferred_discount": "Preferred vs common share discount series with z-scores.",
    "gold.nps_holdings_delta": "NPS position changes between disclosure snapshots (PIT).",
    "gold.filing_events": "Classified DART filing events (buyback, rights issue, ...) with signs.",
    "gold.security_master": "Security master: ticker, name, market, listing/delisting dates.",
    "gold.identity_review": "Ticker reuse / identity gaps flagged for manual review.",
    "macro.cpi": "CPI series (KR/US) with YoY/MoM changes.",
    "macro.rates": "Interest rate series (policy rates, bond yields).",
    "macro.indices": "Equity/market index level series.",
    "macro.commodities": "Commodity price series.",
    "macro.fx": "FX rate series.",
    "macro.economic_indicators": "Miscellaneous economic indicator series.",
}

_EXTRA_VIEW_DESCRIPTIONS: dict[str, str] = {
    "analytics.daily_prices": (
        "Adjusted daily prices joined with ticker/name/market from the security master."
    ),
    "analytics.daily_market_caps": "Daily market caps with ticker and name.",
    "analytics.universe": "Daily universe membership joined with ticker/name.",
    "analytics.nps_universe": "Point-in-time NPS universe with ticker/name aliases.",
    "analytics.securities": "Alias of gold.security_master.",
    "metadata.datasets": "Catalog metadata: one row per registered dataset.",
}


def list_datasets(data_root: Path | str) -> list[dict[str, Any]]:
    """List data-lake datasets with layer, description, and freshness.

    Freshness comes from partition directory names on the filesystem (for
    example ``dt=2026-07-01``); no parquet file is opened, so this stays cheap
    on a Raspberry Pi.
    """

    root = Path(data_root)
    datasets: list[dict[str, Any]] = []
    for name in sorted(dataset_registry):
        spec = dataset_registry[name]
        matches = [Path(m) for m in glob(spec.glob_path(root).as_posix()) if Path(m).is_file()]
        datasets.append(
            {
                "name": name,
                "layer": spec.layer,
                "description": _DATASET_DESCRIPTIONS.get(name, ""),
                "partitioning": spec.relative_glob,
                "files": len(matches),
                "latest_partition": _latest_partition(matches),
            }
        )
    for name in sorted(_EXTRA_VIEW_DESCRIPTIONS):
        datasets.append(
            {
                "name": name,
                "layer": name.split(".", maxsplit=1)[0],
                "description": _EXTRA_VIEW_DESCRIPTIONS[name],
                "partitioning": None,
                "files": None,
                "latest_partition": None,
            }
        )
    return datasets


def describe_table(data_root: Path | str, view_name: str) -> dict[str, Any]:
    """Return column names/types of one catalog view plus its description."""

    catalog_path = _require_catalog(Path(data_root))
    if not _VIEW_NAME.fullmatch(view_name):
        raise McpToolError(
            f"invalid view name {view_name!r}; expected a schema-qualified name "
            "such as gold.daily_prices_adj"
        )
    conn = duckdb.connect(str(catalog_path), read_only=True)
    try:
        try:
            rows = conn.execute(f"DESCRIBE {view_name}").fetchall()
        except duckdb.Error as exc:
            raise McpToolError(f"cannot describe {view_name!r}: {exc}") from exc
    finally:
        conn.close()
    description = _DATASET_DESCRIPTIONS.get(view_name) or _EXTRA_VIEW_DESCRIPTIONS.get(
        view_name, ""
    )
    return {
        "view": view_name,
        "description": description,
        "columns": [{"name": row[0], "type": row[1]} for row in rows],
    }


def query(
    data_root: Path | str,
    sql: str,
    max_rows: int = DEFAULT_MAX_ROWS,
    *,
    timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
) -> dict[str, Any]:
    """Run one read-only SELECT/WITH statement against the DuckDB catalog.

    The statement is validated on a comment-stripped, string-blanked copy:
    it must start with SELECT/WITH, contain no write/DDL keywords, and be a
    single statement. Rows are capped by wrapping the query in
    ``SELECT * FROM (<sql>) LIMIT max_rows + 1`` and the connection is opened
    with ``read_only=True``. Timeouts use ``connection.interrupt()`` because
    duckdb 1.5.x has no ``statement_timeout`` setting.
    """

    catalog_path = _require_catalog(Path(data_root))
    _validate_sql(sql)
    max_rows = max(1, min(int(max_rows), HARD_MAX_ROWS))
    cleaned = sql.strip()
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    wrapped = f"SELECT * FROM (\n{cleaned}\n) AS finance_pi_query LIMIT {max_rows + 1}"

    conn = duckdb.connect(str(catalog_path), read_only=True)
    timer = threading.Timer(timeout_seconds, conn.interrupt)
    try:
        timer.start()
        try:
            cursor = conn.execute(wrapped)
            columns = [item[0] for item in cursor.description or []]
            raw_rows = cursor.fetchall()
        except duckdb.InterruptException as exc:
            raise McpToolError(f"query timed out after {timeout_seconds:g}s") from exc
        except duckdb.Error as exc:
            raise McpToolError(f"query failed: {exc}") from exc
    finally:
        timer.cancel()
        conn.close()

    truncated = len(raw_rows) > max_rows
    raw_rows = raw_rows[:max_rows]
    rows: list[list[Any]] = []
    budget = max_result_bytes
    for raw in raw_rows:
        row = [_json_value(value) for value in raw]
        budget -= len(str(row))
        if budget < 0 and rows:
            truncated = True
            break
        rows.append(row)
    return {
        "columns": columns,
        "rows": rows,
        "row_count_returned": len(rows),
        "truncated": truncated,
    }


def list_factors() -> list[dict[str, Any]]:
    """List registered factors with rebalance frequency, direction, inputs."""

    return factor_registry.describe()


def run_backtest(
    data_root: Path | str,
    factor: str,
    start: str,
    end: str,
    top_fraction: float = 0.1,
    cost_bps: float = 10.0,
    commission_bps: float | None = None,
    sell_tax_bps: float | None = None,
) -> dict[str, Any]:
    """Run a monthly-rebalanced backtest in memory and return a compact summary.

    Assembles calendar/context/engine the same way ``finance-pi backtest run``
    does. The cost-model selection mirrors the CLI's private
    ``_backtest_cost_model`` helper (replicated here because it is not public
    and the CLI module is heavy to import). No artifacts are written.
    """

    root = Path(data_root)
    start_date = _parse_date(start, "start")
    end_date = _parse_date(end, "end")
    if end_date < start_date:
        raise McpToolError("end must be on or after start")
    if not 0 < top_fraction <= 1:
        raise McpToolError("top_fraction must be in (0, 1]")

    price_glob = root / "gold" / "daily_prices_adj" / "dt=*" / "part.parquet"
    price_files = sorted(root.glob("gold/daily_prices_adj/dt=*/part.parquet"))
    if not price_files:
        raise McpToolError(
            f"No gold price data at {price_glob}; run the build pipeline "
            "(finance-pi build all) first"
        )
    dates = (
        pl.scan_parquet(price_glob.as_posix(), hive_partitioning=True)
        .select("date")
        .unique()
        .collect()
        .to_series()
        .to_list()
    )
    calendar = TradingCalendar.from_dates(dates)
    try:
        factor_cls = factor_registry.get(factor)
    except KeyError as exc:
        raise McpToolError(str(exc)) from exc

    if commission_bps is None and sell_tax_bps is None:
        cost_model: FixedBpsCostModel | KoreanTradingCostModel = FixedBpsCostModel(bps=cost_bps)
        cost_model_name = f"fixed_bps({cost_bps:g})"
    else:
        commission = commission_bps if commission_bps is not None else 5.0
        sell_tax = sell_tax_bps if sell_tax_bps is not None else 15.0
        cost_model = KoreanTradingCostModel(commission_bps=commission, sell_tax_bps=sell_tax)
        cost_model_name = f"korean_trading(commission={commission:g}, sell_tax={sell_tax:g})"

    config = BacktestConfig(start=start_date, end=end_date, top_fraction=top_fraction)
    result = BacktestEngine(calendar, cost_model=cost_model).run(
        factor_cls(), ParquetFactorContext(root), config
    )
    nav = result.nav.sort("date")
    if nav.is_empty():
        raise McpToolError("backtest produced no trading days in the requested window")

    nav_series = nav.get_column("nav")
    final_nav = float(nav_series[-1])
    total_return = final_nav / config.initial_nav - 1.0
    max_drawdown = float((nav_series / nav_series.cum_max() - 1.0).min())
    gross_growth = float(nav.select((1.0 + pl.col("gross_return")).product()).item())
    ledger = result.ledger
    return {
        "factor": factor,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "top_fraction": top_fraction,
        "cost_model": cost_model_name,
        "n_trading_days": nav.height,
        "n_rebalances": ledger.height,
        "final_nav": final_nav,
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "gross_return_total": gross_growth - 1.0,
        "cost_drag": (gross_growth - 1.0) - total_return,
        "total_cost": float(nav.get_column("cost").sum()),
        "avg_turnover": float(ledger.get_column("turnover").mean()) if ledger.height else 0.0,
        "artifacts": None,
        "note": (
            "In-memory run; artifacts were not written. Use 'finance-pi backtest run' "
            "to persist nav/positions/ledger parquet files under data/backtests/."
        ),
    }


def get_fundamentals(
    data_root: Path | str,
    ticker: str,
    as_of: str,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> dict[str, Any]:
    """Latest annual PIT fundamentals for a ticker known as of a given date.

    Thin convenience wrapper over :func:`query`: it joins
    ``gold.fundamentals_pit`` to ``gold.security_master`` and returns the
    account rows of the most recent annual (report_type 11011) snapshot whose
    ``as_of_date`` is on or before ``as_of``.
    """

    normalized = ticker.strip()
    if not _TICKER.fullmatch(normalized):
        raise McpToolError(f"invalid ticker {ticker!r}; expected an alphanumeric code")
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    as_of_date = _parse_date(as_of, "as_of")
    sql = f"""
    WITH sec AS (
        SELECT security_id, ticker, name
        FROM gold.security_master
        WHERE ticker = '{normalized}'
    ),
    latest AS (
        SELECT max(f.as_of_date) AS as_of_date
        FROM gold.fundamentals_pit AS f
        JOIN sec ON f.security_id = sec.security_id
        WHERE f.as_of_date <= DATE '{as_of_date.isoformat()}'
          AND f.report_type = '11011'
    )
    SELECT
        sec.ticker,
        sec.name,
        f.as_of_date,
        f.fiscal_period_end,
        f.rcept_dt,
        f.report_type,
        f.account_id,
        f.account_name,
        f.amount,
        f.is_consolidated
    FROM gold.fundamentals_pit AS f
    JOIN sec ON f.security_id = sec.security_id
    JOIN latest ON f.as_of_date = latest.as_of_date
    WHERE f.report_type = '11011'
    ORDER BY f.account_id
    """
    result = query(data_root, sql, max_rows=max_rows)
    return {"ticker": normalized, "as_of": as_of_date.isoformat(), **result}


def _require_catalog(data_root: Path) -> Path:
    catalog_path = data_root / "catalog" / "finance_pi.duckdb"
    if not catalog_path.exists():
        raise McpToolError(
            f"DuckDB catalog not found at {catalog_path}. Build it first with: "
            "python -m finance_pi.cli.app catalog build --root <workspace>"
        )
    return catalog_path


def _validate_sql(sql: str) -> None:
    skeleton = _sql_skeleton(sql).strip()
    body = skeleton
    while body.endswith(";"):
        body = body[:-1].rstrip()
    if not body:
        raise McpToolError("empty SQL statement")
    if ";" in body:
        raise McpToolError("multiple SQL statements are not allowed")
    if not _SELECT_START.match(body):
        raise McpToolError("only a single SELECT/WITH read query is allowed")
    match = _FORBIDDEN_SQL.search(body)
    if match:
        raise McpToolError(f"forbidden SQL keyword: {match.group(0).upper()}")


def _sql_skeleton(sql: str) -> str:
    """Validation copy of *sql*: comments removed, string contents blanked.

    Blanking string/identifier literals keeps keyword and semicolon checks
    from firing on data (``SELECT 'delete'`` stays legal) while comment
    removal keeps them from being hidden (``/* */ DROP ...`` is caught). The
    original SQL is what actually gets executed.
    """

    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            while i < n and sql[i] != "\n":
                i += 1
            out.append(" ")
        elif ch == "/" and nxt == "*":
            i += 2
            while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i = min(i + 2, n)
            out.append(" ")
        elif ch in ("'", '"'):
            quote = ch
            i += 1
            while i < n:
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:
                        i += 2
                        continue
                    break
                i += 1
            i += 1
            out.append(quote + quote)
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _latest_partition(matches: list[Path]) -> str | None:
    values: list[str] = []
    for path in matches:
        values.extend(_PARTITION_VALUE.findall(path.as_posix()))
    return max(values) if values else None


def _parse_date(value: object, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise McpToolError(f"{field} must be an ISO date (YYYY-MM-DD), got {value!r}") from exc


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes | bytearray):
        return value.hex()
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)
