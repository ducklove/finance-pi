from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from finance_pi.mcp_server import tools
from finance_pi.mcp_server.tools import McpToolError
from finance_pi.storage import CatalogBuilder

SIDS = (("S001", "000010", -0.01), ("S002", "000020", 0.01))


def _weekdays(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _write(root: Path, relative: str, frame: pl.DataFrame) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


@pytest.fixture(scope="module")
def data_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Tiny synthetic gold lake + DuckDB catalog: 2 securities, Q1 2024."""

    root = tmp_path_factory.mktemp("mcp_lake") / "data"
    days = _weekdays(date(2024, 1, 1), date(2024, 3, 29))
    price_rows = []
    universe_rows = []
    for index, day in enumerate(days):
        for sid, _ticker, drift in SIDS:
            close = 100.0 * (1.0 + drift) ** index
            price_rows.append(
                {
                    "date": day,
                    "security_id": sid,
                    "listing_id": f"L{sid[-1]}",
                    "open_adj": close,
                    "high_adj": close,
                    "low_adj": close,
                    "close_adj": close,
                    "return_1d": drift if index else 0.0,
                    "volume": 1000,
                    "trading_value": 1_000_000,
                    "market_cap": 1_000_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            )
            universe_rows.append(
                {
                    "date": day,
                    "security_id": sid,
                    "listing_id": f"L{sid[-1]}",
                    "market": "KOSPI",
                    "is_active": True,
                    "share_class": "common",
                    "security_type": "equity",
                    "is_spac_pre": False,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            )
    _write(root, "gold/daily_prices_adj/dt=2024-01-01/part.parquet", pl.DataFrame(price_rows))
    _write(root, "gold/universe_history/dt=2024-01-01/part.parquet", pl.DataFrame(universe_rows))
    _write(
        root,
        "gold/security_master.parquet",
        pl.DataFrame(
            [
                {
                    "security_id": sid,
                    "listing_id": f"L{sid[-1]}",
                    "ticker": ticker,
                    "name": f"Test Corp {sid}",
                    "market": "KOSPI",
                    "share_class": "common",
                    "security_type": "equity",
                }
                for sid, ticker, _drift in SIDS
            ]
        ),
    )
    fundamentals = pl.DataFrame(
        [
            {
                "security_id": "S001",
                "as_of_date": date(2024, 1, 31),
                "fiscal_period_end": date(2023, 12, 31),
                "rcept_dt": date(2024, 1, 30),
                "report_type": "11011",
                "account_id": account_id,
                "account_name": account_name,
                "amount": amount,
                "is_consolidated": True,
            }
            for account_id, account_name, amount in (
                ("ifrs-full_Assets", "자산총계", 5000.0),
                ("ifrs-full_Equity", "자본총계", 2500.0),
                ("ifrs-full_ProfitLoss", "당기순이익", 500.0),
            )
        ]
    )
    _write(root, "gold/fundamentals_pit/dt=2024-01-31/part.parquet", fundamentals)
    CatalogBuilder(root, root / "catalog" / "finance_pi.duckdb").build()
    return root


def test_list_datasets_reports_freshness_from_filesystem(data_root: Path) -> None:
    by_name = {row["name"]: row for row in tools.list_datasets(data_root)}

    prices = by_name["gold.daily_prices_adj"]
    assert prices["layer"] == "gold"
    assert prices["description"]
    assert prices["partitioning"] == "gold/daily_prices_adj/dt=*/part.parquet"
    assert prices["files"] == 1
    assert prices["latest_partition"] == "2024-01-01"
    assert by_name["gold.fundamentals_pit"]["latest_partition"] == "2024-01-31"

    empty = by_name["silver.prices"]
    assert empty["files"] == 0
    assert empty["latest_partition"] is None

    assert "analytics.daily_prices" in by_name
    assert by_name["metadata.datasets"]["description"]


def test_describe_table_returns_columns_and_description(data_root: Path) -> None:
    result = tools.describe_table(data_root, "gold.daily_prices_adj")
    names = [column["name"] for column in result["columns"]]
    assert "close_adj" in names
    assert "return_1d" in names
    assert result["description"]
    types = {column["name"]: column["type"] for column in result["columns"]}
    assert types["date"] == "DATE"


def test_describe_table_rejects_invalid_names(data_root: Path) -> None:
    with pytest.raises(McpToolError, match="invalid view name"):
        tools.describe_table(data_root, "gold.daily_prices_adj; DROP TABLE x")
    with pytest.raises(McpToolError, match="invalid view name"):
        tools.describe_table(data_root, "no_schema")
    with pytest.raises(McpToolError, match="cannot describe"):
        tools.describe_table(data_root, "gold.does_not_exist")


def test_query_happy_path(data_root: Path) -> None:
    result = tools.query(
        data_root,
        "SELECT security_id, count(*) AS n FROM gold.daily_prices_adj "
        "GROUP BY security_id ORDER BY security_id",
    )
    assert result["columns"] == ["security_id", "n"]
    assert result["row_count_returned"] == 2
    assert result["truncated"] is False
    assert result["rows"][0][0] == "S001"


def test_query_serializes_dates_and_allows_trailing_semicolon(data_root: Path) -> None:
    result = tools.query(data_root, "SELECT min(date) AS d FROM gold.daily_prices_adj;")
    assert result["rows"] == [["2024-01-01"]]


def test_query_limit_truncation(data_root: Path) -> None:
    result = tools.query(
        data_root, "SELECT date FROM gold.daily_prices_adj ORDER BY date", max_rows=5
    )
    assert result["row_count_returned"] == 5
    assert len(result["rows"]) == 5
    assert result["truncated"] is True


def test_query_byte_cap_truncates(data_root: Path) -> None:
    result = tools.query(
        data_root,
        "SELECT repeat('x', 10000) AS s FROM range(50)",
        max_rows=50,
        max_result_bytes=25_000,
    )
    assert result["truncated"] is True
    assert 1 <= result["row_count_returned"] < 50


def test_query_byte_cap_refuses_oversized_first_row(data_root: Path) -> None:
    result = tools.query(
        data_root,
        "SELECT repeat('x', 10000) AS s",
        max_result_bytes=1_000,
    )

    assert result["rows"] == []
    assert result["row_count_returned"] == 0
    assert result["truncated"] is True


def test_query_disables_external_file_access(data_root: Path) -> None:
    with pytest.raises(McpToolError, match="file system operations are disabled"):
        tools.query(data_root, "SELECT * FROM read_text('README.md')")


def test_query_timeout_interrupts(data_root: Path) -> None:
    heavy = "SELECT sum(a.range * b.range) FROM range(50000000) AS a, range(1000) AS b"
    with pytest.raises(McpToolError, match="timed out"):
        tools.query(data_root, heavy, timeout_seconds=0.2)


def test_query_allows_keywords_inside_string_literals(data_root: Path) -> None:
    result = tools.query(data_root, "SELECT 'insert delete create -- attach' AS s")
    assert result["rows"] == [["insert delete create -- attach"]]


REJECTED_SQL = [
    "INSERT INTO gold.daily_prices_adj VALUES (1)",
    "UPDATE gold.daily_prices_adj SET volume = 0",
    "DELETE FROM gold.daily_prices_adj",
    "CREATE TABLE t AS SELECT 1",
    "DROP VIEW gold.daily_prices_adj",
    "ALTER TABLE metadata.datasets ADD COLUMN x INT",
    "ATTACH 'other.duckdb' AS other",
    "COPY (SELECT 1) TO 'out.csv'",
    "EXPORT DATABASE 'somewhere'",
    "PRAGMA database_list",
    "INSTALL httpfs",
    "LOAD httpfs",
    "SET memory_limit='1GB'",
    "CALL pragma_version()",
    "VACUUM",
    "SHOW TABLES",
    # sneaky forms
    "WITH x AS (SELECT 1) DELETE FROM gold.daily_prices_adj",
    "-- harmless comment\nDROP VIEW gold.daily_prices_adj",
    "/* harmless */ DELETE FROM gold.daily_prices_adj",
    "SELECT 1; DROP VIEW gold.daily_prices_adj",
    "SELECT 1; SELECT 2",
    "   \n  ",
]


@pytest.mark.parametrize("sql", REJECTED_SQL)
def test_query_rejects_writes_ddl_and_multi_statements(data_root: Path, sql: str) -> None:
    with pytest.raises(McpToolError):
        tools.query(data_root, sql)


def test_query_requires_catalog(tmp_path: Path) -> None:
    with pytest.raises(McpToolError, match="catalog build"):
        tools.query(tmp_path / "no_lake", "SELECT 1")


def test_list_factors_describes_registry() -> None:
    rows = tools.list_factors()
    names = {row["name"] for row in rows}
    assert {"momentum_12_1", "reversal_1m", "value_earnings_yield"} <= names
    assert {"name", "rebalance", "direction", "requires"} <= set(rows[0])


def test_run_backtest_summary_keys_and_nav_sign(data_root: Path) -> None:
    summary = tools.run_backtest(
        data_root, "reversal_1m", "2024-01-01", "2024-03-29", top_fraction=0.5
    )
    expected_keys = {
        "factor",
        "start",
        "end",
        "n_trading_days",
        "n_rebalances",
        "final_nav",
        "total_return",
        "max_drawdown",
        "cost_drag",
        "total_cost",
        "avg_turnover",
    }
    assert expected_keys <= set(summary)
    assert summary["n_rebalances"] >= 2
    # reversal_1m (direction=-1) keeps selecting the steady -1%/day loser, so
    # the portfolio must end below its starting NAV with a negative drawdown.
    assert summary["final_nav"] < 1.0
    assert summary["total_return"] < 0.0
    assert summary["max_drawdown"] < 0.0
    assert summary["cost_drag"] > 0.0
    assert summary["artifacts"] is None


def test_run_backtest_rejects_bad_inputs(data_root: Path, tmp_path: Path) -> None:
    with pytest.raises(McpToolError, match="unknown factor"):
        tools.run_backtest(data_root, "nope", "2024-01-01", "2024-03-29")
    with pytest.raises(McpToolError, match="ISO date"):
        tools.run_backtest(data_root, "reversal_1m", "01/01/2024", "2024-03-29")
    with pytest.raises(McpToolError, match="on or after"):
        tools.run_backtest(data_root, "reversal_1m", "2024-03-29", "2024-01-01")
    with pytest.raises(McpToolError, match="No gold price data"):
        tools.run_backtest(tmp_path / "no_lake", "reversal_1m", "2024-01-01", "2024-03-29")


def test_get_fundamentals_latest_annual_snapshot(data_root: Path) -> None:
    result = tools.get_fundamentals(data_root, "10", "2024-06-30")
    assert result["ticker"] == "000010"
    assert result["row_count_returned"] == 3
    by_account = {
        row[result["columns"].index("account_id")]: row[result["columns"].index("amount")]
        for row in result["rows"]
    }
    assert by_account["ifrs-full_ProfitLoss"] == 500.0
    assert result["rows"][0][result["columns"].index("as_of_date")] == "2024-01-31"


def test_get_fundamentals_pit_visibility_and_validation(data_root: Path) -> None:
    # Nothing is visible before the first as_of partition.
    early = tools.get_fundamentals(data_root, "000010", "2023-12-31")
    assert early["row_count_returned"] == 0
    with pytest.raises(McpToolError, match="invalid ticker"):
        tools.get_fundamentals(data_root, "0059'30", "2024-06-30")
    with pytest.raises(McpToolError, match="ISO date"):
        tools.get_fundamentals(data_root, "000010", "not-a-date")
