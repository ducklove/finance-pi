from __future__ import annotations

import json
import logging
from datetime import date

import polars as pl
import pytest

from finance_pi.reports import build_data_quality_report, build_fraud_report
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_corporate_actions,
    build_daily_prices_adj,
    build_financials_silver,
    build_fundamentals_pit,
    build_security_master,
    build_silver_prices,
    build_universe_history,
)
from finance_pi.transforms import builders as builders_module


def _silver_price_row(
    logical_date: date,
    ticker: str,
    *,
    close: float,
    volume: int = 1000,
    listed_shares: int | None = None,
    market_cap: int | None = None,
    price_source: str = "krx",
) -> dict:
    return {
        "date": logical_date,
        "security_id": f"S{ticker}",
        "listing_id": f"L{ticker}",
        "ticker": ticker,
        "name": f"Name {ticker}",
        "market": "KOSPI",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "trading_value": None,
        "market_cap": market_cap,
        "listed_shares": listed_shares,
        "price_source": price_source,
        "price_basis": "adjusted" if price_source == "naver" else "raw",
        "is_halted": False,
        "is_designated": False,
        "is_liquidation_window": False,
    }


def _write_silver_prices(tmp_path, rows: list[dict]) -> None:
    layout = DataLakeLayout(tmp_path)
    writer = ParquetDatasetWriter()
    frame = pl.DataFrame(rows)
    for key, partition in frame.partition_by("date", as_dict=True).items():
        logical_date = key[0] if isinstance(key, tuple) else key
        writer.write(
            partition,
            layout.partition_path("silver.prices", logical_date),
            mode="overwrite",
        )


def test_build_all_promotes_bronze_to_gold(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "005930",
                    "isin": "KR7005930003",
                    "name": "삼성전자",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 90.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                }
            ]
        ),
        layout.partition_path("bronze.krx_daily_raw", date(2024, 1, 2)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 3),
                    "ticker": "005930",
                    "isin": "KR7005930003",
                    "name": "삼성전자",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 120.0,
                    "low": 95.0,
                    "close": 110.0,
                    "volume": 20,
                    "trading_value": 2200,
                    "market_cap": 1_100_000,
                    "listed_shares": 10_000,
                }
            ]
        ),
        layout.partition_path("bronze.krx_daily_raw", date(2024, 1, 3)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "snapshot_dt": date(2024, 1, 3),
                    "corp_code": "00126380",
                    "corp_name": "삼성전자",
                    "stock_code": "005930",
                    "modify_date": "20240103",
                }
            ]
        ),
        layout.partition_path("bronze.dart_company_raw", date(2024, 1, 3)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "security_id": None,
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2023, 12, 31),
                    "event_date": date(2023, 12, 31),
                    "rcept_dt": date(2024, 1, 2),
                    "available_date": date(2024, 1, 2),
                    "report_type": "11011",
                    "account_id": "ifrs-full_Assets",
                    "account_name": "Assets",
                    "amount": 1000.0,
                    "is_consolidated": True,
                    "accounting_basis": "K-IFRS",
                }
            ]
        ),
        layout.partition_path("bronze.dart_financials_raw", date(2024, 1, 2)),
    )

    summaries = build_all(tmp_path)

    assert {summary.dataset for summary in summaries} >= {
        "silver.prices",
        "gold.security_master",
        "gold.daily_prices_adj",
        "gold.fundamentals_pit",
    }
    master = pl.read_parquet(tmp_path / "gold" / "security_master.parquet")
    assert master.select("corp_code").item() == "00126380"
    prices = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-03" / "part.parquet"
    )
    assert prices.select("return_1d").item() == 0.1
    pit = pl.read_parquet(tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet")
    assert pit.select("amount").item() == 1000.0

    dq = build_data_quality_report(tmp_path, date(2024, 1, 3))
    fraud = build_fraud_report(tmp_path, date(2024, 1, 3))
    assert dq.checks
    assert fraud.checks


def test_daily_prices_adj_applies_corporate_action_factors(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "security_id": "S123456",
                    "listing_id": "L123456",
                    "ticker": "123456",
                    "name": "Test",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 120.0,
                    "low": 90.0,
                    "close": 100.0,
                    "volume": 1000,
                    "trading_value": 100_000,
                    "market_cap": None,
                    "listed_shares": None,
                    "price_source": "test",
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
                {
                    "date": date(2024, 1, 3),
                    "security_id": "S123456",
                    "listing_id": "L123456",
                    "ticker": "123456",
                    "name": "Test",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 110.0,
                    "volume": 1000,
                    "trading_value": 110_000,
                    "market_cap": None,
                    "listed_shares": None,
                    "price_source": "test",
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
            ]
        ),
        layout.partition_path("silver.prices", date(2024, 1, 2)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "effective_date": date(2024, 1, 3),
                    "security_id": "S123456",
                    "action_type": "reverse_split",
                    "adjustment_factor": 0.5,
                    "source_rcept_no": "manual",
                }
            ]
        ),
        layout.partition_path("silver.corporate_actions", date(2024, 1, 3)),
    )

    build_daily_prices_adj(tmp_path)

    first = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    second = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-03" / "part.parquet"
    )
    assert first.select("close_adj").item() == 50.0
    assert second.select("close_adj").item() == 110.0
    assert second.select("return_1d").item() == 1.2


def test_daily_prices_adj_nulls_returns_across_zero_volume_rows(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    rows = []
    for logical_date, close, volume in [
        (date(2024, 1, 2), 100.0, 10),
        (date(2024, 1, 3), 100.0, 0),
        (date(2024, 1, 4), 500.0, 10),
        (date(2024, 1, 5), 550.0, 10),
    ]:
        rows.append(
            {
                "date": logical_date,
                "security_id": "S123456",
                "listing_id": "L123456",
                "ticker": "123456",
                "name": "Test",
                "market": "KOSPI",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": volume,
                "trading_value": None,
                "market_cap": None,
                "listed_shares": None,
                "price_source": "test",
                "is_halted": False,
                "is_designated": False,
                "is_liquidation_window": False,
            }
        )
    writer.write(pl.DataFrame(rows), layout.partition_path("silver.prices", date(2024, 1, 2)))

    build_daily_prices_adj(tmp_path)

    returns = (
        pl.concat(
            [
                pl.read_parquet(path)
                for path in sorted(
                    (tmp_path / "gold" / "daily_prices_adj").glob("dt=*/part.parquet")
                )
            ],
            how="diagonal_relaxed",
        )
        .sort("date")
        .select("date", "return_1d")
    )
    assert returns.row(1, named=True)["return_1d"] is None
    assert returns.row(2, named=True)["return_1d"] is None
    assert returns.row(3, named=True)["return_1d"] == 0.1


def test_build_silver_prices_deduplicates_split_adjusted_candidates(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "123456",
                    "isin": None,
                    "name": "123456",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 100,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                },
                {
                    "date": date(2024, 1, 3),
                    "ticker": "123456",
                    "isin": None,
                    "name": "123456",
                    "market": "KOSPI",
                    "open": 20.0,
                    "high": 20.0,
                    "low": 20.0,
                    "close": 20.0,
                    "volume": 500,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                },
                {
                    "date": date(2024, 1, 3),
                    "ticker": "123456",
                    "isin": None,
                    "name": "123456",
                    "market": "KOSPI",
                    "open": 101.0,
                    "high": 101.0,
                    "low": 101.0,
                    "close": 101.0,
                    "volume": 99,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                },
                {
                    "date": date(2024, 1, 4),
                    "ticker": "123456",
                    "isin": None,
                    "name": "123456",
                    "market": "KOSPI",
                    "open": 105.0,
                    "high": 105.0,
                    "low": 105.0,
                    "close": 105.0,
                    "volume": 100,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                },
            ]
        ),
        tmp_path
        / "bronze"
        / "naver_daily"
        / "request_dt=2024-01-04"
        / "chunk=000001"
        / "part.parquet",
    )

    build_silver_prices(tmp_path)

    silver = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-03" / "part.parquet")
    assert silver.select("close").item() == 101.0
    assert silver.select("volume").item() == 99


def test_build_all_enriches_kis_prices_with_naver_summary(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "005930",
                    "isin": None,
                    "name": "005930",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 90.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]
        ),
        layout.partition_path("bronze.kis_daily_raw", date(2024, 1, 2)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "snapshot_dt": date(2024, 1, 2),
                    "ticker": "005930",
                    "name": "Samsung",
                    "market": "KOSPI",
                    "close": 100,
                    "change_abs": 0,
                    "change_rate_pct": 0.0,
                    "par_value": 100,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "foreign_ownership_pct": 49.0,
                    "volume": 10,
                    "per": 10.0,
                    "roe": 5.0,
                }
            ]
        ),
        layout.partition_path("bronze.naver_summary_raw", date(2024, 1, 2)),
    )

    build_all(tmp_path)

    silver = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-02" / "part.parquet")
    gold = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    assert silver.select("market_cap").item() == 1_000_000
    assert silver.select("listed_shares").item() == 10_000
    assert gold.select("market_cap").item() == 1_000_000
    assert gold.select("listed_shares").item() == 10_000


def test_build_all_enriches_prices_with_marcap_market_caps(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()

    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "005930",
                    "isin": None,
                    "name": "005930",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 90.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]
        ),
        layout.partition_path("bronze.kis_daily_raw", date(2024, 1, 2)),
    )
    marcap_path = tmp_path / "bronze" / "marcap" / "year=2024" / "part.parquet"
    marcap_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "Date": date(2024, 1, 2),
                "Rank": 1,
                "Code": "005930",
                "Name": "삼성전자",
                "Open": 100,
                "High": 110,
                "Low": 90,
                "Close": 100,
                "Volume": 10,
                "Amount": 1000,
                "Changes": 0,
                "ChangeCode": "0",
                "ChagesRatio": 0.0,
                "Marcap": 1_000_000,
                "Stocks": 10_000,
                "MarketId": "STK",
                "Market": "KOSPI",
                "Dept": "",
            }
        ]
    ).write_parquet(marcap_path)

    build_all(tmp_path)

    market_caps = pl.read_parquet(
        tmp_path / "gold" / "daily_market_caps" / "dt=2024-01-02" / "part.parquet"
    )
    silver = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-02" / "part.parquet")
    gold = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    assert market_caps.select("market_cap").item() == 1_000_000
    assert market_caps.select("listed_shares").item() == 10_000
    assert silver.select("market_cap").item() == 1_000_000
    assert silver.select("listed_shares").item() == 10_000
    assert gold.select("market_cap").item() == 1_000_000
    assert gold.select("listed_shares").item() == 10_000


def test_build_all_marks_named_alphanumeric_preferred_share(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    naver_daily_path = (
        tmp_path
        / "bronze"
        / "naver_daily"
        / "request_dt=2026-04-28"
        / "chunk=test"
        / "part.parquet"
    )
    naver_daily_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "date": date(2026, 4, 28),
                "ticker": "12345k",
                "isin": None,
                "name": "12345k",
                "market": "KRX",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10,
                "trading_value": None,
                "market_cap": None,
                "listed_shares": None,
            }
        ]
    ).write_parquet(naver_daily_path)
    writer.write(
        pl.DataFrame(
            [
                {
                    "snapshot_dt": date(2026, 4, 28),
                    "ticker": "12345K",
                    "name": "Test 2\uc6b0B",
                    "market": "KOSPI",
                    "close": 100,
                    "change_abs": 0,
                    "change_rate_pct": 0.0,
                    "par_value": 100,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "foreign_ownership_pct": 0.0,
                    "volume": 10,
                    "per": 10.0,
                    "roe": 5.0,
                }
            ]
        ),
        layout.partition_path("bronze.naver_summary_raw", date(2026, 4, 28)),
    )

    build_all(tmp_path)

    master = pl.read_parquet(tmp_path / "gold" / "security_master.parquet")
    row = master.row(0, named=True)
    assert row["ticker"] == "12345K"
    assert row["name"] == "Test 2\uc6b0B"
    assert row["market"] == "KOSPI"
    assert row["share_class"] == "preferred"


def test_build_financials_accepts_mixed_bronze_date_schemas(tmp_path) -> None:
    first = tmp_path / "bronze" / "dart_financials" / "rcept_dt=2026-03-15" / "part.parquet"
    second = tmp_path / "bronze" / "dart_financials" / "rcept_dt=2026-03-16" / "part.parquet"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "security_id": None,
                "corp_code": "00126380",
                "fiscal_period_end": date(2025, 12, 31),
                "event_date": date(2025, 12, 31),
                "rcept_dt": date(2026, 3, 15),
                "available_date": date(2026, 3, 15),
                "report_type": "11011",
                "account_id": "ifrs-full_Assets",
                "account_name": "Assets",
                "amount": 1000.0,
                "is_consolidated": True,
                "accounting_basis": "K-IFRS",
            }
        ]
    ).write_parquet(first)
    pl.DataFrame(
        [
            {
                "security_id": None,
                "corp_code": "00258801",
                "fiscal_period_end": "2025-12-31",
                "event_date": "2025-12-31",
                "rcept_dt": "2026-03-16",
                "available_date": "2026-03-16",
                "report_type": "11011",
                "account_id": "ifrs-full_Assets",
                "account_name": "Assets",
                "amount": 2000.0,
                "is_consolidated": True,
                "accounting_basis": "K-IFRS",
                "is_backfilled": True,
            }
        ]
    ).write_parquet(second)

    summary = build_financials_silver(tmp_path)[0]

    silver = pl.read_parquet(
        tmp_path / "silver" / "financials" / "fiscal_year=2025" / "part.parquet"
    )
    assert summary.rows == 2
    assert silver["rcept_dt"].dtype == pl.Date
    # Legacy bronze partitions without the column read as False.
    backfilled = {
        row["corp_code"]: row["is_backfilled"] for row in silver.iter_rows(named=True)
    }
    assert backfilled == {"00126380": False, "00258801": True}


def test_build_financials_normalizes_dart_web_korean_account_names(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    (tmp_path / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S009770",
                "ticker": "009770",
                "name": "Samjung Pulp",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
                "corp_code": "00128227",
            },
        ]
    ).write_parquet(tmp_path / "gold" / "security_master.parquet")
    rows = [
        ("자산총계", 322_255_669_541.0),
        ("부채총계", 35_210_679_822.0),
        ("자본총계", 287_044_989_719.0),
        ("기본주당이익(손실) (단위 : 원)", 12_556.0),
    ]
    bronze_path = layout.partition_path("bronze.dart_financials_raw", date(2026, 3, 19))
    bronze_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "security_id": None,
                "corp_code": "00128227",
                "fiscal_period_end": date(2025, 12, 31),
                "event_date": date(2025, 12, 31),
                "rcept_dt": date(2026, 3, 19),
                "available_date": date(2026, 3, 19),
                "report_type": "11011",
                "account_id": name,
                "account_name": name,
                "amount": amount,
                "is_consolidated": False,
                "accounting_basis": "제 52 기",
            }
            for name, amount in rows
        ]
    ).write_parquet(bronze_path)

    summary = build_financials_silver(tmp_path)[0]

    silver = pl.read_parquet(
        tmp_path / "silver" / "financials" / "fiscal_year=2025" / "part.parquet"
    )
    by_name = {row["account_name"]: row for row in silver.iter_rows(named=True)}
    assert summary.rows == 4
    assert by_name["자산총계"]["account_id"] == "ifrs-full_Assets"
    assert by_name["부채총계"]["account_id"] == "ifrs-full_Liabilities"
    assert by_name["자본총계"]["account_id"] == "ifrs-full_Equity"
    assert (
        by_name["기본주당이익(손실) (단위 : 원)"]["account_id"]
        == "ifrs-full_BasicEarningsLossPerShare"
    )
    assert by_name["자본총계"]["security_id"] == "S009770"


def test_fundamentals_pit_keeps_latest_account_per_day(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 3),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "market": "KOSPI",
                    "is_active": True,
                    "share_class": "common",
                    "security_type": "equity",
                    "is_spac_pre": False,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            ]
        ),
        layout.partition_path("gold.universe_history", date(2024, 1, 3)),
    )
    silver_path = tmp_path / "silver" / "financials" / "fiscal_year=2023" / "part.parquet"
    silver_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "corp_code": "00126380",
                "fiscal_period_end": date(2022, 12, 31),
                "event_date": date(2022, 12, 31),
                "rcept_dt": date(2023, 3, 15),
                "available_date": date(2023, 3, 15),
                "report_type": "11011",
                "account_id": "ifrs-full_Assets",
                "account_name": "Assets",
                "amount": 1000.0,
                "is_consolidated": True,
                "accounting_basis": "K-IFRS",
            },
            {
                "security_id": "S005930",
                "corp_code": "00126380",
                "fiscal_period_end": date(2023, 12, 31),
                "event_date": date(2023, 12, 31),
                "rcept_dt": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "report_type": "11011",
                "account_id": "ifrs-full_Assets",
                "account_name": "Assets",
                "amount": 2000.0,
                "is_consolidated": True,
                "accounting_basis": "K-IFRS",
            },
        ]
    ).write_parquet(silver_path)

    summary = build_fundamentals_pit(tmp_path)[0]

    pit = pl.read_parquet(tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet")
    assert summary.rows == 1
    assert pit.select("amount").item() == 2000.0


def test_fundamentals_pit_prefers_consolidated_and_next_day_availability(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    for as_of in (date(2024, 1, 2), date(2024, 1, 3)):
        writer.write(
            pl.DataFrame(
                [
                    {
                        "date": as_of,
                        "security_id": "S005930",
                        "listing_id": "L005930",
                        "market": "KOSPI",
                        "is_active": True,
                        "share_class": "common",
                        "security_type": "equity",
                        "is_spac_pre": False,
                        "is_halted": False,
                        "is_designated": False,
                        "is_liquidation_window": False,
                    }
                ]
            ),
            layout.partition_path("gold.universe_history", as_of),
        )
    silver_path = tmp_path / "silver" / "financials" / "fiscal_year=2023" / "part.parquet"
    silver_path.parent.mkdir(parents=True)
    base = {
        "security_id": "S005930",
        "corp_code": "00126380",
        "fiscal_period_end": date(2023, 12, 31),
        "event_date": date(2023, 12, 31),
        "report_type": "11011",
        "account_id": "ifrs-full_Assets",
        "account_name": "Assets",
        "accounting_basis": "K-IFRS",
    }
    pl.DataFrame(
        [
            {
                **base,
                "rcept_dt": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "amount": 111.0,
                "is_consolidated": False,
            },
            {
                **base,
                "rcept_dt": date(2024, 1, 2),
                "available_date": date(2024, 1, 2),
                "amount": 222.0,
                "is_consolidated": True,
            },
            # Correction available on the as-of day itself: strictly invisible.
            {
                **base,
                "rcept_dt": date(2024, 1, 3),
                "available_date": date(2024, 1, 3),
                "amount": 999.0,
                "is_consolidated": True,
            },
        ]
    ).write_parquet(silver_path)

    build_fundamentals_pit(tmp_path)

    assert not (tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-02" / "part.parquet").exists()
    pit = pl.read_parquet(tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet")
    assert pit.height == 1
    assert pit.select("amount").item() == 222.0


def test_build_corporate_actions_detects_split_and_adjusts_gold(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(
                date(2024, 1, 2),
                "111111",
                close=10_000.0,
                listed_shares=1_000_000,
                market_cap=10_000_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 3),
                "111111",
                close=10_100.0,
                listed_shares=1_000_000,
                market_cap=10_100_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 4),
                "111111",
                close=2_020.0,
                listed_shares=5_000_000,
                market_cap=10_100_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 5),
                "111111",
                close=2_040.0,
                listed_shares=5_000_000,
                market_cap=10_200_000_000,
            ),
        ],
    )

    summary = build_corporate_actions(tmp_path)[0]

    actions = pl.read_parquet(
        tmp_path / "silver" / "corporate_actions" / "dt=2024-01-04" / "part.parquet"
    )
    row = actions.row(0, named=True)
    assert summary.rows == 1
    assert row["security_id"] == "S111111"
    assert row["action_type"] == "split"
    assert row["adjustment_factor"] == pytest.approx(0.2)
    assert row["source"] == "shares_jump"
    assert row["confidence"] == "medium"

    build_daily_prices_adj(tmp_path)

    first = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    split_day = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-04" / "part.parquet"
    )
    assert first.select("close_adj").item() == pytest.approx(2_000.0)
    assert split_day.select("close_adj").item() == pytest.approx(2_020.0)
    assert split_day.select("return_1d").item() == pytest.approx(0.0)


def test_build_corporate_actions_corroborates_with_dart_filings(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(
                date(2024, 1, 2),
                "111111",
                close=10_000.0,
                listed_shares=1_000_000,
                market_cap=10_000_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 3),
                "111111",
                close=2_000.0,
                listed_shares=5_000_000,
                market_cap=10_000_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 2),
                "222222",
                close=505.0,
                listed_shares=1_000_000,
                market_cap=505_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 3),
                "222222",
                close=5_050.0,
                listed_shares=100_000,
                market_cap=505_000_000,
            ),
        ],
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "rcept_dt": date(2023, 12, 20),
                    "corp_code": "00000001",
                    "corp_name": "Split Corp",
                    "stock_code": "111111",
                    "rcept_no": "20231220000001",
                    "report_nm": "주식분할결정",
                    "rm": "",
                },
                {
                    "rcept_dt": date(2023, 12, 27),
                    "corp_code": "00000002",
                    "corp_name": "Reduction Corp",
                    "stock_code": "222222",
                    "rcept_no": "20231227000002",
                    "report_nm": "감자결정",
                    "rm": "",
                },
            ]
        ),
        layout.partition_path("bronze.dart_filings_raw", date(2023, 12, 20)),
    )

    build_corporate_actions(tmp_path)

    actions = pl.read_parquet(
        tmp_path / "silver" / "corporate_actions" / "dt=2024-01-03" / "part.parquet"
    ).sort("security_id")
    by_security = {row["security_id"]: row for row in actions.iter_rows(named=True)}
    assert by_security["S111111"]["action_type"] == "split"
    assert by_security["S111111"]["confidence"] == "high"
    assert by_security["S111111"]["source_rcept_no"] == "20231220000001"
    assert by_security["S222222"]["action_type"] == "capital_reduction"
    assert by_security["S222222"]["adjustment_factor"] == pytest.approx(10.0)
    assert by_security["S222222"]["source_rcept_no"] == "20231227000002"


def test_build_corporate_actions_ignores_non_action_changes(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    _write_silver_prices(
        tmp_path,
        [
            # Share issuance: shares double but the price does not halve.
            _silver_price_row(date(2024, 1, 2), "111111", close=100.0, listed_shares=1_000_000),
            _silver_price_row(date(2024, 1, 3), "111111", close=100.0, listed_shares=2_000_000),
            # Crash: price halves but shares stay flat.
            _silver_price_row(date(2024, 1, 2), "222222", close=100.0, listed_shares=1_000_000),
            _silver_price_row(date(2024, 1, 3), "222222", close=50.0, listed_shares=1_000_000),
        ],
    )

    summary = build_corporate_actions(tmp_path)[0]

    assert summary.rows == 0
    assert not list((tmp_path / "silver" / "corporate_actions").glob("dt=*/part.parquet"))


def test_build_corporate_actions_warns_when_long_history_yields_nothing(
    tmp_path, caplog, monkeypatch
) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    monkeypatch.setattr(builders_module, "CORPORATE_ACTIONS_WARN_MIN_DATES", 3)
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(date(2024, 1, day), "111111", close=100.0, listed_shares=1_000_000)
            for day in (2, 3, 4)
        ],
    )

    with caplog.at_level(logging.WARNING, logger="finance_pi.transforms.builders"):
        build_corporate_actions(tmp_path)

    assert any("corporate_actions is empty" in record.message for record in caplog.records)


def test_build_corporate_actions_preserves_manual_rows_and_removes_stale(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(
                date(2024, 1, 2),
                "111111",
                close=10_000.0,
                listed_shares=1_000_000,
                market_cap=10_000_000_000,
            ),
            _silver_price_row(
                date(2024, 1, 3),
                "111111",
                close=2_000.0,
                listed_shares=5_000_000,
                market_cap=10_000_000_000,
            ),
        ],
    )
    # Manually curated row for the same event (legacy schema without source).
    writer.write(
        pl.DataFrame(
            [
                {
                    "effective_date": date(2024, 1, 3),
                    "security_id": "S111111",
                    "action_type": "split",
                    "adjustment_factor": 0.25,
                    "source_rcept_no": "manual",
                }
            ]
        ),
        layout.partition_path("silver.corporate_actions", date(2024, 1, 3)),
    )
    # Stale detector-produced partition that no longer matches any detection.
    writer.write(
        pl.DataFrame(
            [
                {
                    "effective_date": date(2024, 2, 1),
                    "security_id": "S999999",
                    "action_type": "split",
                    "adjustment_factor": 0.5,
                    "source_rcept_no": None,
                    "source": "shares_jump",
                    "confidence": "medium",
                }
            ]
        ),
        layout.partition_path("silver.corporate_actions", date(2024, 2, 1)),
    )

    build_corporate_actions(tmp_path)

    kept = pl.read_parquet(
        tmp_path / "silver" / "corporate_actions" / "dt=2024-01-03" / "part.parquet"
    )
    assert kept.height == 1
    assert kept.select("adjustment_factor").item() == 0.25
    assert kept.select("source_rcept_no").item() == "manual"
    assert not (
        tmp_path / "silver" / "corporate_actions" / "dt=2024-02-01" / "part.parquet"
    ).exists()


def test_incremental_adj_rebuilds_history_when_new_action_appears(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(date(2024, 1, day), "111111", close=100.0, volume=10)
            for day in (2, 3, 4)
        ],
    )

    build_daily_prices_adj(tmp_path)

    state_path = tmp_path / "_state" / "corporate_actions_applied.json"
    assert json.loads(state_path.read_text(encoding="utf-8")) == {"events": []}

    _write_silver_prices(
        tmp_path,
        [_silver_price_row(date(2024, 1, 5), "111111", close=50.0, volume=10)],
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "effective_date": date(2024, 1, 5),
                    "security_id": "S111111",
                    "action_type": "split",
                    "adjustment_factor": 0.5,
                    "source_rcept_no": "manual",
                }
            ]
        ),
        layout.partition_path("silver.corporate_actions", date(2024, 1, 5)),
    )

    build_daily_prices_adj(tmp_path, dates=[date(2024, 1, 5)])

    first = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    latest = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-05" / "part.parquet"
    )
    assert first.select("close_adj").item() == pytest.approx(50.0)
    assert latest.select("close_adj").item() == pytest.approx(50.0)
    assert latest.select("return_1d").item() == pytest.approx(0.0)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["events"] == [
        {
            "security_id": "S111111",
            "effective_date": "2024-01-05",
            "adjustment_factor": 0.5,
        }
    ]

    # Without new events the incremental build must NOT rewrite old partitions.
    _write_silver_prices(
        tmp_path,
        [_silver_price_row(date(2024, 1, 2), "111111", close=999.0, volume=10)],
    )
    build_daily_prices_adj(tmp_path, dates=[date(2024, 1, 5)])
    first_again = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    )
    assert first_again.select("close_adj").item() == pytest.approx(50.0)


def test_adjustment_multiplies_only_raw_basis_rows(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(date(2024, 1, 2), "111111", close=100.0, price_source="krx"),
            _silver_price_row(date(2024, 1, 3), "111111", close=50.0, price_source="krx"),
            _silver_price_row(date(2024, 1, 2), "222222", close=100.0, price_source="naver"),
            _silver_price_row(date(2024, 1, 3), "222222", close=100.0, price_source="naver"),
        ],
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "effective_date": date(2024, 1, 3),
                    "security_id": security_id,
                    "action_type": "split",
                    "adjustment_factor": 0.5,
                    "source_rcept_no": "manual",
                }
                for security_id in ("S111111", "S222222")
            ]
        ),
        layout.partition_path("silver.corporate_actions", date(2024, 1, 3)),
    )

    build_daily_prices_adj(tmp_path)

    first = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet"
    ).sort("security_id")
    by_security = {row["security_id"]: row for row in first.iter_rows(named=True)}
    assert by_security["S111111"]["close_adj"] == pytest.approx(50.0)
    assert by_security["S222222"]["close_adj"] == pytest.approx(100.0)


def test_build_silver_prices_labels_price_basis(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "005930",
                    "isin": None,
                    "name": "Samsung",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": None,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]
        ),
        layout.partition_path("bronze.krx_daily_raw", date(2024, 1, 2)),
    )
    naver_path = (
        tmp_path
        / "bronze"
        / "naver_daily"
        / "request_dt=2024-01-04"
        / "chunk=000001"
        / "part.parquet"
    )
    naver_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "date": date(2024, 1, 3),
                "ticker": "005930",
                "isin": None,
                "name": "Samsung",
                "market": "KOSPI",
                "open": 101.0,
                "high": 101.0,
                "low": 101.0,
                "close": 101.0,
                "volume": 10,
                "trading_value": None,
                "market_cap": None,
                "listed_shares": None,
            }
        ]
    ).write_parquet(naver_path)

    build_silver_prices(tmp_path)

    krx_day = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-02" / "part.parquet")
    naver_day = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-03" / "part.parquet")
    assert krx_day.select("price_basis").item() == "raw"
    assert naver_day.select("price_basis").item() == "adjusted"


def test_return_1d_nulled_on_price_source_boundary(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(date(2024, 1, 2), "111111", close=100.0, price_source="krx"),
            _silver_price_row(date(2024, 1, 3), "111111", close=101.0, price_source="naver"),
            _silver_price_row(date(2024, 1, 4), "111111", close=102.0, price_source="naver"),
        ],
    )

    build_daily_prices_adj(tmp_path)

    boundary = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-03" / "part.parquet"
    )
    after = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-04" / "part.parquet"
    )
    assert boundary.select("return_1d").item() is None
    assert after.select("return_1d").item() == pytest.approx(102.0 / 101.0 - 1.0)


def test_security_master_sets_delisted_date_for_stale_securities(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    rows = []
    for day in range(1, 16):
        rows.append(_silver_price_row(date(2024, 1, day), "111111", close=100.0))
        if day <= 3:
            rows.append(_silver_price_row(date(2024, 1, day), "222222", close=50.0))
        if day <= 10:
            rows.append(_silver_price_row(date(2024, 1, day), "333333", close=75.0))
    _write_silver_prices(tmp_path, rows)

    build_security_master(tmp_path)

    master = pl.read_parquet(tmp_path / "gold" / "security_master.parquet")
    by_ticker = {row["ticker"]: row for row in master.iter_rows(named=True)}
    assert by_ticker["111111"]["delisted_date"] is None
    assert by_ticker["222222"]["delisted_date"] == date(2024, 1, 3)
    # Inside the N-trading-day buffer at the right edge: still considered active.
    assert by_ticker["333333"]["delisted_date"] is None
    assert "_last_seen_date" not in master.columns

    # A spurious later price row for the delisted security becomes inactive.
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(date(2024, 1, 16), "111111", close=100.0),
            _silver_price_row(date(2024, 1, 16), "222222", close=50.0),
        ],
    )
    build_universe_history(tmp_path, dates=[date(2024, 1, 16)])

    universe = pl.read_parquet(
        tmp_path / "gold" / "universe_history" / "dt=2024-01-16" / "part.parquet"
    )
    active = {
        row["security_id"]: row["is_active"] for row in universe.iter_rows(named=True)
    }
    assert active["S111111"] is True
    assert active["S222222"] is False


def test_identity_review_detects_ticker_reuse_gap(tmp_path, caplog, monkeypatch) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    monkeypatch.setattr(builders_module, "TICKER_REUSE_GAP_TRADING_DAYS", 5)
    rows = [_silver_price_row(date(2024, 1, day), "111111", close=100.0) for day in range(1, 13)]
    rows.extend(
        [
            # Ticker reused: trades stop, then the code reappears with a new name
            # after a long gap on the observed trading-day axis.
            {**_silver_price_row(date(2024, 1, 1), "222222", close=50.0), "name": "Old Corp"},
            {**_silver_price_row(date(2024, 1, 2), "222222", close=50.0), "name": "Old Corp"},
            {**_silver_price_row(date(2024, 1, 12), "222222", close=500.0), "name": "New Corp"},
        ]
    )
    _write_silver_prices(tmp_path, rows)

    with caplog.at_level(logging.WARNING, logger="finance_pi.transforms.builders"):
        summaries = build_security_master(tmp_path)

    review = pl.read_parquet(tmp_path / "gold" / "identity_review.parquet")
    assert review.height == 1
    row = review.row(0, named=True)
    assert row["ticker"] == "222222"
    assert row["gap_start"] == date(2024, 1, 2)
    assert row["gap_end"] == date(2024, 1, 12)
    assert row["name_before"] == "Old Corp"
    assert row["name_after"] == "New Corp"
    assert row["name_changed"] is True
    assert any("identity_review" in record.message for record in caplog.records)
    assert {summary.dataset for summary in summaries} == {
        "gold.security_master",
        "gold.identity_review",
    }


def test_identity_review_empty_for_contiguous_history(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    _write_silver_prices(
        tmp_path,
        [_silver_price_row(date(2024, 1, day), "111111", close=100.0) for day in range(1, 13)],
    )

    build_security_master(tmp_path)

    review = pl.read_parquet(tmp_path / "gold" / "identity_review.parquet")
    assert review.is_empty()


def test_incremental_master_merge_preserves_delisted_date(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    rows = []
    for day in range(1, 16):
        rows.append(_silver_price_row(date(2024, 1, day), "111111", close=100.0))
        if day <= 3:
            rows.append(_silver_price_row(date(2024, 1, day), "222222", close=50.0))
    _write_silver_prices(tmp_path, rows)
    build_security_master(tmp_path)

    # Backfill of an old date must not resurrect the delisted security.
    build_security_master(tmp_path, dates=[date(2024, 1, 2)])
    master = pl.read_parquet(tmp_path / "gold" / "security_master.parquet")
    by_ticker = {row["ticker"]: row for row in master.iter_rows(named=True)}
    assert by_ticker["222222"]["delisted_date"] == date(2024, 1, 3)

    # Trading after the recorded delisting clears it.
    _write_silver_prices(
        tmp_path,
        [_silver_price_row(date(2024, 1, 16), "222222", close=50.0)],
    )
    build_security_master(tmp_path, dates=[date(2024, 1, 16)])
    master = pl.read_parquet(tmp_path / "gold" / "security_master.parquet")
    by_ticker = {row["ticker"]: row for row in master.iter_rows(named=True)}
    assert by_ticker["222222"]["delisted_date"] is None


def test_incremental_silver_prices_picks_up_backfilled_naver_dates(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    naver_path = (
        tmp_path
        / "bronze"
        / "naver_daily"
        / "request_dt=2024-01-10"
        / "chunk=000001"
        / "part.parquet"
    )
    naver_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "date": date(2024, 1, 3),
                "ticker": "005930",
                "isin": None,
                "name": "Samsung",
                "market": "KOSPI",
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "volume": 10,
                "trading_value": None,
                "market_cap": None,
                "listed_shares": None,
            }
        ]
    ).write_parquet(naver_path)

    build_silver_prices(tmp_path, dates=[date(2024, 1, 3)])

    silver = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-03" / "part.parquet")
    assert silver.height == 1
    assert silver.select("close").item() == 100.0
    assert silver.select("price_source").item() == "naver"


def _pit_universe_row(as_of: date) -> dict:
    return {
        "date": as_of,
        "security_id": "S005930",
        "listing_id": "L005930",
        "market": "KOSPI",
        "is_active": True,
        "share_class": "common",
        "security_type": "equity",
        "is_spac_pre": False,
        "is_halted": False,
        "is_designated": False,
        "is_liquidation_window": False,
    }


def _pit_financials_row(*, account_id: str, amount: float, available_date: date) -> dict:
    return {
        "security_id": "S005930",
        "corp_code": "00126380",
        "fiscal_period_end": date(2023, 12, 31),
        "event_date": date(2023, 12, 31),
        "rcept_dt": available_date,
        "available_date": available_date,
        "report_type": "11011",
        "account_id": account_id,
        "account_name": account_id,
        "amount": amount,
        "is_consolidated": True,
        "accounting_basis": "K-IFRS",
    }


def _write_pit_fixture(
    tmp_path,
    as_of_dates: list[date],
    financials_rows: list[dict],
) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    for as_of in as_of_dates:
        writer.write(
            pl.DataFrame([_pit_universe_row(as_of)]),
            layout.partition_path("gold.universe_history", as_of),
            mode="overwrite",
        )
    silver_path = tmp_path / "silver" / "financials" / "fiscal_year=2023" / "part.parquet"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(financials_rows).write_parquet(silver_path)


def _pit_partition_mtimes(tmp_path) -> dict[str, int]:
    return {
        path.parent.name: path.stat().st_mtime_ns
        for path in sorted((tmp_path / "gold" / "fundamentals_pit").glob("dt=*/part.parquet"))
    }


def test_fundamentals_pit_second_run_skips_unchanged_dates(tmp_path) -> None:
    _write_pit_fixture(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 3)],
        [
            _pit_financials_row(
                account_id="ifrs-full_Assets", amount=1000.0, available_date=date(2024, 1, 1)
            )
        ],
    )

    first = build_fundamentals_pit(tmp_path)[0]
    assert first.files == 2
    mtimes = _pit_partition_mtimes(tmp_path)

    second = build_fundamentals_pit(tmp_path)[0]

    assert second.files == 0
    assert second.rows == 0
    assert _pit_partition_mtimes(tmp_path) == mtimes


def test_fundamentals_pit_rebuilds_only_dates_after_new_available_date(tmp_path) -> None:
    initial = [
        _pit_financials_row(
            account_id="ifrs-full_Assets", amount=1000.0, available_date=date(2024, 1, 1)
        )
    ]
    as_of_dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    _write_pit_fixture(tmp_path, as_of_dates, initial)
    build_fundamentals_pit(tmp_path)
    mtimes = _pit_partition_mtimes(tmp_path)
    assert set(mtimes) == {"dt=2024-01-02", "dt=2024-01-03", "dt=2024-01-04"}

    # A new filing available on 01-03 is visible strictly after that date, so
    # only as-of dates > 01-03 may change.
    _write_pit_fixture(
        tmp_path,
        as_of_dates,
        [
            *initial,
            _pit_financials_row(
                account_id="ifrs-full_Liabilities", amount=500.0, available_date=date(2024, 1, 3)
            ),
        ],
    )
    summary = build_fundamentals_pit(tmp_path)[0]

    assert summary.files == 1
    after = _pit_partition_mtimes(tmp_path)
    assert after["dt=2024-01-02"] == mtimes["dt=2024-01-02"]
    assert after["dt=2024-01-03"] == mtimes["dt=2024-01-03"]
    rebuilt = pl.read_parquet(
        tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-04" / "part.parquet"
    )
    assert rebuilt.height == 2
    assert set(rebuilt["account_id"].to_list()) == {"ifrs-full_Assets", "ifrs-full_Liabilities"}


def test_fundamentals_pit_explicit_dates_builds_only_those(tmp_path) -> None:
    _write_pit_fixture(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 3)],
        [
            _pit_financials_row(
                account_id="ifrs-full_Assets", amount=1000.0, available_date=date(2024, 1, 1)
            )
        ],
    )

    summary = build_fundamentals_pit(tmp_path, dates=[date(2024, 1, 3)])[0]

    assert summary.files == 1
    assert not (tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-02" / "part.parquet").exists()
    assert (tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet").exists()
    # Partial builds must not record the incremental state marker.
    assert not (tmp_path / "_state" / "fundamentals_pit.json").exists()
