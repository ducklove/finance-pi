from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.reports import build_data_quality_report, build_fraud_report
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_daily_prices_adj,
    build_financials_silver,
    build_silver_prices,
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
            }
        ]
    ).write_parquet(second)

    summary = build_financials_silver(tmp_path)[0]

    silver = pl.read_parquet(
        tmp_path / "silver" / "financials" / "fiscal_year=2025" / "part.parquet"
    )
    assert summary.rows == 2
    assert silver["rcept_dt"].dtype == pl.Date


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

    from finance_pi.transforms import build_fundamentals_pit

    summary = build_fundamentals_pit(tmp_path)[0]

    pit = pl.read_parquet(tmp_path / "gold" / "fundamentals_pit" / "dt=2024-01-03" / "part.parquet")
    assert summary.rows == 1
    assert pit.select("amount").item() == 2000.0
