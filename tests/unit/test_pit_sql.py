from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
from polars.testing import assert_frame_equal

from finance_pi.pit import build_fundamentals_pit_sql
from finance_pi.transforms.builders import build_fundamentals_pit


def _financials_row(
    security_id: str,
    account_id: str,
    amount: float,
    *,
    fiscal_period_end: date,
    available_date: date,
    rcept_dt: date | None = None,
    is_consolidated: bool = True,
) -> dict:
    rcept = rcept_dt or available_date
    return {
        "security_id": security_id,
        "corp_code": f"C{security_id[1:]}",
        "fiscal_period_end": fiscal_period_end,
        "event_date": fiscal_period_end,
        "rcept_dt": rcept,
        "available_date": available_date,
        "report_type": "11011",
        "statement_division": "BS",
        "account_id": account_id,
        "account_name": account_id,
        "amount": amount,
        "is_consolidated": is_consolidated,
        "accounting_basis": "K-IFRS",
        "is_backfilled": False,
        "fiscal_year": fiscal_period_end.year,
    }


def _synthetic_data() -> tuple[pl.DataFrame, pl.DataFrame]:
    financials = pl.DataFrame(
        [
            # Two fiscal periods for the same account: the newer period must win.
            _financials_row(
                "S001",
                "ifrs-full_Assets",
                1000.0,
                fiscal_period_end=date(2022, 12, 31),
                available_date=date(2023, 3, 15),
            ),
            _financials_row(
                "S001",
                "ifrs-full_Assets",
                2000.0,
                fiscal_period_end=date(2023, 12, 31),
                available_date=date(2024, 1, 2),
            ),
            # A correction: two available_dates for the same fiscal period.
            _financials_row(
                "S001",
                "ifrs-full_ProfitLoss",
                300.0,
                fiscal_period_end=date(2023, 12, 31),
                available_date=date(2024, 1, 2),
            ),
            _financials_row(
                "S001",
                "ifrs-full_ProfitLoss",
                310.0,
                fiscal_period_end=date(2023, 12, 31),
                available_date=date(2024, 1, 3),
            ),
            # Not yet available on either as-of date: must never appear.
            _financials_row(
                "S001",
                "ifrs-full_Equity",
                999.0,
                fiscal_period_end=date(2023, 12, 31),
                available_date=date(2024, 2, 1),
            ),
            _financials_row(
                "S002",
                "ifrs-full_Assets",
                500.0,
                fiscal_period_end=date(2022, 12, 31),
                available_date=date(2023, 4, 1),
            ),
            # Same dates for the same account: consolidated must beat separate.
            _financials_row(
                "S002",
                "ifrs-full_Equity",
                111.0,
                fiscal_period_end=date(2022, 12, 31),
                available_date=date(2023, 4, 1),
                is_consolidated=False,
            ),
            _financials_row(
                "S002",
                "ifrs-full_Equity",
                222.0,
                fiscal_period_end=date(2022, 12, 31),
                available_date=date(2023, 4, 1),
                is_consolidated=True,
            ),
        ]
    )
    universe = pl.DataFrame(
        [
            {"date": as_of, "security_id": security_id}
            for as_of in (date(2024, 1, 2), date(2024, 1, 3))
            for security_id in ("S001", "S002")
        ]
    )
    return financials, universe


def _run_polars_builder(
    tmp_path: Path, financials: pl.DataFrame, universe: pl.DataFrame
) -> pl.DataFrame:
    for (fiscal_year,), partition in financials.group_by("fiscal_year"):
        path = tmp_path / "silver" / "financials" / f"fiscal_year={fiscal_year}" / "part.parquet"
        path.parent.mkdir(parents=True)
        partition.drop("fiscal_year").write_parquet(path)
    for (as_of,), partition in universe.group_by("date"):
        path = tmp_path / "gold" / "universe_history" / f"dt={as_of}" / "part.parquet"
        path.parent.mkdir(parents=True)
        partition.write_parquet(path)

    build_fundamentals_pit(tmp_path)

    files = sorted((tmp_path / "gold" / "fundamentals_pit").glob("dt=*/part.parquet"))
    return pl.concat([pl.read_parquet(file, hive_partitioning=False) for file in files])


def _run_duckdb_sql(tmp_path: Path) -> pl.DataFrame:
    financials_glob = (
        tmp_path / "silver" / "financials" / "fiscal_year=*" / "part.parquet"
    ).as_posix()
    universe_glob = (tmp_path / "gold" / "universe_history" / "dt=*" / "part.parquet").as_posix()
    sql = build_fundamentals_pit_sql(calendar_view="universe", financials_view="financials")
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE VIEW financials AS SELECT * FROM "
            f"read_parquet('{financials_glob}', hive_partitioning = true)"
        )
        con.execute(
            "CREATE VIEW universe AS SELECT date, security_id FROM "
            f"read_parquet('{universe_glob}', hive_partitioning = true)"
        )
        cursor = con.execute(sql)
        columns = [name for name, *_ in cursor.description]
        rows = cursor.fetchall()
    finally:
        con.close()
    return pl.DataFrame(rows, schema=columns, orient="row")


def test_pit_sql_matches_polars_builder(tmp_path) -> None:
    financials, universe = _synthetic_data()

    polars_result = _run_polars_builder(tmp_path, financials, universe)
    sql_result = _run_duckdb_sql(tmp_path)

    key = ["as_of_date", "security_id", "account_id"]
    assert_frame_equal(polars_result.sort(key), sql_result.sort(key))

    amounts = {
        (row["as_of_date"], row["security_id"], row["account_id"]): row["amount"]
        for row in sql_result.iter_rows(named=True)
    }
    assert amounts == {
        # available_date == as_of_date is NOT visible (strict next-day policy),
        # so the FY2023 filing available on 2024-01-02 only shows up on 01-03.
        (date(2024, 1, 2), "S001", "ifrs-full_Assets"): 1000.0,
        (date(2024, 1, 3), "S001", "ifrs-full_Assets"): 2000.0,
        # The correction available on 01-03 is not visible until 01-04.
        (date(2024, 1, 3), "S001", "ifrs-full_ProfitLoss"): 300.0,
        (date(2024, 1, 2), "S002", "ifrs-full_Assets"): 500.0,
        (date(2024, 1, 3), "S002", "ifrs-full_Assets"): 500.0,
        # Consolidated beats separate when every date key ties.
        (date(2024, 1, 2), "S002", "ifrs-full_Equity"): 222.0,
        (date(2024, 1, 3), "S002", "ifrs-full_Equity"): 222.0,
    }
