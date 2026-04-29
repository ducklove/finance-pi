from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.reports import build_data_quality_report, build_fraud_report
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import build_all, build_financials_silver


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
    pit = pl.read_parquet(
        tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet"
    )
    assert pit.select("amount").item() == 1000.0

    dq = build_data_quality_report(tmp_path, date(2024, 1, 3))
    fraud = build_fraud_report(tmp_path, date(2024, 1, 3))
    assert dq.checks
    assert fraud.checks


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
            }
        ]
    ).write_parquet(second)

    summary = build_financials_silver(tmp_path)[0]

    silver = pl.read_parquet(
        tmp_path / "silver" / "financials" / "fiscal_year=2025" / "part.parquet"
    )
    assert summary.rows == 2
    assert silver["rcept_dt"].dtype == pl.Date


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

    from finance_pi.transforms import build_fundamentals_pit

    summary = build_fundamentals_pit(tmp_path)[0]

    pit = pl.read_parquet(
        tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet"
    )
    assert summary.rows == 1
    assert pit.select("amount").item() == 2000.0
