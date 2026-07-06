from __future__ import annotations

import json
import logging
import shutil
from datetime import date, timedelta

import polars as pl
import pytest

from finance_pi.reports import build_data_quality_report, build_fraud_report
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import (
    build_all,
    build_corporate_actions,
    build_daily_market_caps,
    build_daily_prices_adj,
    build_filings_silver,
    build_financials_silver,
    build_fundamentals_pit,
    build_nps_holdings_delta,
    build_preferred_discount,
    build_security_master,
    build_security_relations,
    build_silver_market_caps,
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


def _bronze_filing_row(
    rcept_dt: date,
    rcept_no: str,
    *,
    stock_code: str | None,
    corp_name: str = "테스트",
    report_nm: str = "주요사항보고서(자기주식취득결정)",
    rm: str | None = "",
) -> dict:
    return {
        "rcept_dt": rcept_dt,
        "corp_code": "00126380",
        "corp_name": corp_name,
        "stock_code": stock_code,
        "rcept_no": rcept_no,
        "report_nm": report_nm,
        "rm": rm,
    }


def test_build_filings_silver_normalizes_dedups_and_flags(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                _bronze_filing_row(
                    date(2024, 1, 5), "20240105000001", stock_code="005930"
                ),
                # Unlisted filer: no stock_code, so no security_id in silver.
                _bronze_filing_row(
                    date(2024, 1, 5),
                    "20240105000002",
                    stock_code=None,
                    corp_name="비상장회사",
                    report_nm="사업보고서 (2023.12)",
                ),
                # Correction remark from the DART list API rm field.
                _bronze_filing_row(
                    date(2024, 1, 5),
                    "20240105000003",
                    stock_code="000660",
                    report_nm="[기재정정]주요사항보고서(유상증자결정)",
                    rm="정",
                ),
            ]
        ),
        layout.partition_path("bronze.dart_filings_raw", date(2024, 1, 5)),
    )
    # Re-ingested duplicate of rcept_no ...001 under a newer rcept_dt: the
    # newest row must win the rcept_no dedup.
    writer.write(
        pl.DataFrame(
            [
                _bronze_filing_row(
                    date(2024, 1, 8),
                    "20240105000001",
                    stock_code="005930",
                    corp_name="삼성전자수정",
                ),
            ]
        ),
        layout.partition_path("bronze.dart_filings_raw", date(2024, 1, 8)),
    )

    summary = build_filings_silver(tmp_path)[0]

    assert summary.dataset == "silver.filings"
    assert summary.rows == 3
    first_day = pl.read_parquet(
        tmp_path / "silver" / "filings" / "dt=2024-01-05" / "part.parquet"
    ).sort("rcept_no")
    assert first_day["rcept_no"].to_list() == ["20240105000002", "20240105000003"]
    unlisted, correction = first_day.iter_rows(named=True)
    assert unlisted["stock_code"] is None
    assert unlisted["security_id"] is None
    assert unlisted["is_correction"] is False
    assert correction["security_id"] == "S000660"
    assert correction["is_correction"] is True
    assert correction["available_date"] == date(2024, 1, 5)
    deduped = pl.read_parquet(
        tmp_path / "silver" / "filings" / "dt=2024-01-08" / "part.parquet"
    )
    assert deduped.height == 1
    row = deduped.row(0, named=True)
    assert row["rcept_no"] == "20240105000001"
    assert row["corp_name"] == "삼성전자수정"
    assert row["security_id"] == "S005930"
    assert row["available_date"] == date(2024, 1, 8)


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


def _bronze_price_row(
    logical_date: date,
    ticker: str,
    *,
    close: float,
    volume: int,
) -> dict:
    return {
        "date": logical_date,
        "ticker": ticker,
        "isin": None,
        "name": ticker,
        "market": "KOSPI",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "trading_value": None,
        "market_cap": None,
        "listed_shares": None,
    }


def _read_all_silver_prices(tmp_path) -> pl.DataFrame:
    return pl.concat(
        [
            pl.read_parquet(path)
            for path in sorted((tmp_path / "silver" / "prices").glob("dt=*/part.parquet"))
        ],
        how="diagonal_relaxed",
    ).sort(["date", "ticker"])


def test_full_rebuild_chunked_matches_one_shot_at_chunk_boundaries(
    tmp_path, monkeypatch
) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    chunk_a = [date(2023, 12, 26), date(2023, 12, 27), date(2023, 12, 28)]
    chunk_b = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]

    rows = []
    # Ticker 111111: duplicate candidates on the FIRST date of chunk B; its
    # only continuity anchors (singleton rows) sit at the tail of chunk A, so
    # resolving the duplicate needs the backward context overlap. The bogus
    # split-adjusted candidate comes first in the file on purpose.
    rows.extend(_bronze_price_row(value, "111111", close=100.0, volume=100) for value in chunk_a)
    rows.append(_bronze_price_row(date(2024, 1, 2), "111111", close=20.0, volume=500))
    rows.append(_bronze_price_row(date(2024, 1, 2), "111111", close=101.0, volume=99))
    # Ticker 222222: duplicate candidates on the LAST date of chunk A; its only
    # anchors sit at the head of chunk B, exercising the forward context.
    rows.append(_bronze_price_row(date(2023, 12, 28), "222222", close=50.0, volume=500))
    rows.append(_bronze_price_row(date(2023, 12, 28), "222222", close=200.0, volume=99))
    rows.append(_bronze_price_row(date(2024, 1, 2), "222222", close=201.0, volume=100))
    rows.append(_bronze_price_row(date(2024, 1, 3), "222222", close=202.0, volume=100))
    # Ticker 333333: naver row that the higher-priority KRX source must beat.
    rows.append(_bronze_price_row(date(2024, 1, 2), "333333", close=999.0, volume=10))
    naver_path = (
        tmp_path
        / "bronze"
        / "naver_daily"
        / "request_dt=2024-01-05"
        / "chunk=000001"
        / "part.parquet"
    )
    naver_path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(naver_path)
    for value in [*chunk_a, *chunk_b]:
        writer.write(
            pl.DataFrame([_bronze_price_row(value, "333333", close=300.0, volume=10)]),
            layout.partition_path("bronze.krx_daily_raw", value),
        )

    # 2024-01-02 is 7 calendar days after 2023-12-26: two chunks, the boundary
    # splitting each duplicate from its anchors.
    monkeypatch.setattr(builders_module, "SILVER_PRICES_REBUILD_CHUNK_DAYS", 7)
    chunked_summary = build_silver_prices(tmp_path)[0]
    chunked = _read_all_silver_prices(tmp_path)
    shutil.rmtree(tmp_path / "silver" / "prices")

    monkeypatch.setattr(builders_module, "SILVER_PRICES_REBUILD_CHUNK_DAYS", 100_000)
    one_shot_summary = build_silver_prices(tmp_path)[0]
    one_shot = _read_all_silver_prices(tmp_path)

    assert chunked_summary == one_shot_summary
    assert (chunked_summary.rows, chunked_summary.files) == (13, 6)
    assert chunked.equals(one_shot)
    by_key = {(row["date"], row["ticker"]): row for row in chunked.iter_rows(named=True)}
    boundary_dup = by_key[(date(2024, 1, 2), "111111")]
    assert boundary_dup["close"] == 101.0
    assert boundary_dup["volume"] == 99
    assert by_key[(date(2023, 12, 28), "222222")]["close"] == 200.0
    assert by_key[(date(2024, 1, 2), "333333")]["close"] == 300.0
    assert by_key[(date(2024, 1, 2), "333333")]["price_source"] == "krx"


def test_write_by_partition_slices_unsorted_frame_with_null_keys(tmp_path) -> None:
    frame = pl.DataFrame(
        {
            "date": [
                date(2024, 1, 3),
                date(2024, 1, 2),
                None,
                date(2024, 1, 3),
                date(2024, 1, 5),
                date(2024, 1, 2),
            ],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    # 2024-01-05 exists in the frame but is not selected; nulls are skipped.
    summaries = builders_module._write_by_partition(
        tmp_path,
        "silver.prices",
        frame,
        "date",
        [date(2024, 1, 2), date(2024, 1, 3)],
    )

    assert summaries == [builders_module.BuildSummary("silver.prices", 4, 2)]
    assert not (tmp_path / "silver" / "prices" / "dt=2024-01-05" / "part.parquet").exists()
    second = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-02" / "part.parquet")
    third = pl.read_parquet(tmp_path / "silver" / "prices" / "dt=2024-01-03" / "part.parquet")
    # Original relative row order is preserved within each partition.
    assert second["value"].to_list() == [2, 6]
    assert third["value"].to_list() == [1, 4]


def _silver_market_cap_row(logical_date: date, ticker: str, market_cap: int) -> dict:
    return {
        "date": logical_date,
        "security_id": f"S{ticker}",
        "listing_id": f"L{ticker}",
        "ticker": ticker,
        "name": f"Name {ticker}",
        "market": "KOSPI",
        "rank": 1,
        "close": 100.0,
        "trading_value": 1000,
        "volume": 10,
        "market_cap": market_cap,
        "listed_shares": 10_000,
        "market_cap_source": "marcap",
        "is_estimated": False,
    }


def test_daily_market_caps_chunked_build_skips_existing_partitions(
    tmp_path, monkeypatch
) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    monkeypatch.setattr(builders_module, "DAILY_MARKET_CAPS_REBUILD_CHUNK_DAYS", 1)
    for day, cap in [(2, 100), (3, 200), (4, 300)]:
        writer.write(
            pl.DataFrame([_silver_market_cap_row(date(2024, 1, day), "111111", cap)]),
            layout.partition_path("silver.market_caps", date(2024, 1, day)),
        )
    _write_silver_prices(
        tmp_path,
        [
            _silver_price_row(
                date(2024, 1, 5),
                "111111",
                close=100.0,
                listed_shares=10_000,
                market_cap=1_000_000,
            )
        ],
    )
    # Pre-existing gold partition: append-only semantics must keep it intact.
    writer.write(
        pl.DataFrame([_silver_market_cap_row(date(2024, 1, 2), "111111", 999)]),
        layout.partition_path("gold.daily_market_caps", date(2024, 1, 2)),
    )

    summary = build_daily_market_caps(tmp_path)[0]

    assert summary.dataset == "gold.daily_market_caps"
    assert (summary.rows, summary.files) == (3, 3)
    kept = pl.read_parquet(tmp_path / "gold/daily_market_caps/dt=2024-01-02/part.parquet")
    assert kept.select("market_cap").item() == 999
    from_marcap = pl.read_parquet(tmp_path / "gold/daily_market_caps/dt=2024-01-03/part.parquet")
    assert from_marcap.select("market_cap").item() == 200
    from_prices = pl.read_parquet(tmp_path / "gold/daily_market_caps/dt=2024-01-05/part.parquet")
    assert from_prices.select("market_cap").item() == 1_000_000
    assert from_prices.select("market_cap_source").item() == "krx"


def test_build_silver_market_caps_processes_year_files_independently(tmp_path) -> None:
    for year, close in [(2023, 100), (2024, 200)]:
        path = tmp_path / "bronze" / "marcap" / f"year={year}" / "part.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame(
            [
                {
                    "Date": date(year, 1, 2),
                    "Rank": 1,
                    "Code": "005930",
                    "Name": "삼성전자",
                    "Open": 100,
                    "High": 110,
                    "Low": 90,
                    "Close": close,
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
        ).write_parquet(path)

    summary = build_silver_market_caps(tmp_path)[0]

    assert summary.dataset == "silver.market_caps"
    assert (summary.rows, summary.files) == (2, 2)
    first = pl.read_parquet(tmp_path / "silver/market_caps/dt=2023-01-02/part.parquet")
    second = pl.read_parquet(tmp_path / "silver/market_caps/dt=2024-01-02/part.parquet")
    assert first.select("close").item() == 100.0
    assert second.select("close").item() == 200.0


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


def _adj_price_row(logical_date: date, security_id: str, close_adj: float) -> dict:
    return {"date": logical_date, "security_id": security_id, "close_adj": close_adj}


def _write_adj_price_rows(tmp_path, rows: list[dict]) -> None:
    layout = DataLakeLayout(tmp_path)
    writer = ParquetDatasetWriter()
    frame = pl.DataFrame(rows)
    for key, partition in frame.partition_by("date", as_dict=True).items():
        logical_date = key[0] if isinstance(key, tuple) else key
        writer.write(
            partition,
            layout.partition_path("gold.daily_prices_adj", logical_date),
            mode="overwrite",
        )


def _relation_row(common_ticker: str, preferred_ticker: str, *, confidence: str = "high") -> dict:
    return {
        "common_security_id": f"S{common_ticker}",
        "preferred_security_id": f"S{preferred_ticker}",
        "common_ticker": common_ticker,
        "preferred_ticker": preferred_ticker,
        "preferred_name": f"Name {preferred_ticker}",
        "relation_type": "preferred_of",
        "confidence": confidence,
        "first_seen_date": None,
        "last_seen_date": None,
    }


def _read_preferred_discount(tmp_path) -> pl.DataFrame:
    return pl.concat(
        [
            pl.read_parquet(path)
            for path in sorted((tmp_path / "gold" / "preferred_discount").glob("dt=*/part.parquet"))
        ],
        how="diagonal_relaxed",
    ).sort(["date", "preferred_ticker"])


def test_build_security_relations_maps_preferred_pairs(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    rows = []
    for day in (2, 3, 4):
        logical_date = date(2024, 1, day)
        rows.append({**_silver_price_row(logical_date, "005930", close=100.0), "name": "삼성전자"})
        if day >= 3:
            rows.append(
                {**_silver_price_row(logical_date, "005935", close=50.0), "name": "삼성전자우"}
            )
        rows.append(
            {**_silver_price_row(logical_date, "111110", close=100.0), "name": "Common Corp"}
        )
        rows.append(
            {**_silver_price_row(logical_date, "111115", close=50.0), "name": "Unrelated 우"}
        )
        rows.append({**_silver_price_row(logical_date, "123450", close=100.0), "name": "Test"})
        rows.append(
            {**_silver_price_row(logical_date, "12345K", close=50.0), "name": "Test 2우B"}
        )
        rows.append(
            {**_silver_price_row(logical_date, "222220", close=10.0), "name": "Lonely Corp"}
        )
    _write_silver_prices(tmp_path, rows)
    build_security_master(tmp_path)

    summary = build_security_relations(tmp_path)[0]

    relations = pl.read_parquet(tmp_path / "silver" / "security_relations" / "part.parquet")
    by_preferred = {row["preferred_ticker"]: row for row in relations.iter_rows(named=True)}
    assert summary.dataset == "silver.security_relations"
    assert summary.rows == 3
    assert set(by_preferred) == {"005935", "111115", "12345K"}
    samsung = by_preferred["005935"]
    assert samsung["common_ticker"] == "005930"
    assert samsung["common_security_id"] == "S005930"
    assert samsung["preferred_security_id"] == "S005935"
    assert samsung["preferred_name"] == "삼성전자우"
    assert samsung["relation_type"] == "preferred_of"
    assert samsung["confidence"] == "high"
    # Overlap of both legs' observed trading ranges.
    assert samsung["first_seen_date"] == date(2024, 1, 3)
    assert samsung["last_seen_date"] == date(2024, 1, 4)
    # Ticker prefix matches but the name is unrelated: kept, flagged low.
    assert by_preferred["111115"]["confidence"] == "low"
    # Alphanumeric 신형우선주-style code with an agreeing name suffix.
    assert by_preferred["12345K"]["common_ticker"] == "123450"
    assert by_preferred["12345K"]["confidence"] == "high"


def test_build_preferred_discount_computes_discount_and_zscore(tmp_path, monkeypatch) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    monkeypatch.setattr(builders_module, "PREFERRED_DISCOUNT_WINDOW", 3)
    monkeypatch.setattr(builders_module, "PREFERRED_DISCOUNT_MIN_OBS", 2)
    days = [date(2024, 1, day) for day in (2, 3, 4, 5)]
    rows = []
    for logical_date, preferred_close in zip(days, [50.0, 60.0, 40.0, 50.0], strict=True):
        rows.append(_adj_price_row(logical_date, "S111110", 100.0))
        rows.append(_adj_price_row(logical_date, "S111115", preferred_close))
        rows.append(_adj_price_row(logical_date, "S222220", 100.0))
        rows.append(_adj_price_row(logical_date, "S222225", 90.0))
    _write_adj_price_rows(tmp_path, rows)
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                _relation_row("111110", "111115"),
                _relation_row("222220", "222225", confidence="low"),
            ]
        ),
        layout.singleton_path("silver.security_relations"),
        mode="overwrite",
    )

    summary = build_preferred_discount(tmp_path)[0]

    discount = _read_preferred_discount(tmp_path)
    assert summary.dataset == "gold.preferred_discount"
    # Low-confidence pairs are excluded by default.
    assert summary.rows == 4
    assert discount["preferred_security_id"].unique().to_list() == ["S111115"]
    first, second, third, fourth = discount.iter_rows(named=True)
    # discount = 1 - preferred/common: [0.5, 0.4, 0.6, 0.5]; window=3, min_obs=2.
    assert first["discount"] == pytest.approx(0.5)
    assert first["discount_mean_252"] is None
    assert first["discount_std_252"] is None
    assert first["discount_z"] is None
    assert second["discount"] == pytest.approx(0.4)
    assert second["discount_mean_252"] == pytest.approx(0.45)
    assert second["discount_std_252"] == pytest.approx(0.05 * 2**0.5)
    assert second["discount_z"] == pytest.approx(-(0.5**0.5))
    assert third["discount_mean_252"] == pytest.approx(0.5)
    assert third["discount_std_252"] == pytest.approx(0.1)
    assert third["discount_z"] == pytest.approx(1.0)
    assert fourth["discount_mean_252"] == pytest.approx(0.5)
    assert fourth["discount_z"] == pytest.approx(0.0)

    include_low = build_preferred_discount(tmp_path, include_low_confidence=True)[0]

    assert include_low.rows == 8
    latest = pl.read_parquet(
        tmp_path / "gold" / "preferred_discount" / "dt=2024-01-05" / "part.parquet"
    )
    by_preferred = {row["preferred_security_id"]: row for row in latest.iter_rows(named=True)}
    assert by_preferred["S222225"]["confidence"] == "low"
    assert by_preferred["S222225"]["discount"] == pytest.approx(0.1)


def test_build_preferred_discount_incremental_matches_full_build(tmp_path, monkeypatch) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    monkeypatch.setattr(builders_module, "PREFERRED_DISCOUNT_WINDOW", 3)
    monkeypatch.setattr(builders_module, "PREFERRED_DISCOUNT_MIN_OBS", 2)
    days = [date(2024, 1, day) for day in (2, 3, 4, 5)]
    rows = []
    for logical_date, preferred_close in zip(days, [50.0, 60.0, 40.0, 50.0], strict=True):
        rows.append(_adj_price_row(logical_date, "S111110", 100.0))
        rows.append(_adj_price_row(logical_date, "S111115", preferred_close))
    _write_adj_price_rows(tmp_path, rows)
    ParquetDatasetWriter().write(
        pl.DataFrame([_relation_row("111110", "111115")]),
        layout.singleton_path("silver.security_relations"),
        mode="overwrite",
    )
    build_preferred_discount(tmp_path)
    full_latest = pl.read_parquet(
        tmp_path / "gold" / "preferred_discount" / "dt=2024-01-05" / "part.parquet"
    )
    shutil.rmtree(tmp_path / "gold" / "preferred_discount")

    summary = build_preferred_discount(tmp_path, dates=[date(2024, 1, 5)])[0]

    partitions = sorted(
        path.parent.name
        for path in (tmp_path / "gold" / "preferred_discount").glob("dt=*/part.parquet")
    )
    assert summary.files == 1
    # Only the requested partition is written, but the trailing window is
    # loaded as lookback so the stats match the full-build values exactly.
    assert partitions == ["dt=2024-01-05"]
    incremental = pl.read_parquet(
        tmp_path / "gold" / "preferred_discount" / "dt=2024-01-05" / "part.parquet"
    )
    assert incremental.equals(full_latest)


def _nps_silver_row(snapshot: date, stock_code: str, ownership_pct: float) -> dict:
    return {
        "date": snapshot,
        "security_id": f"S{stock_code}",
        "listing_id": f"L{stock_code}",
        "stock_code": stock_code,
        "stock_name": f"Name {stock_code}",
        "shares": 100,
        "ownership_pct": ownership_pct,
        "price": 1000.0,
        "market_value": 100_000.0,
        "change_pct": None,
        "source_rank": None,
        "source": "fixture",
        "source_date": snapshot,
        "source_market_value": None,
        "source_weight_pct": None,
        "shares_source": "fixture",
        "price_date": snapshot,
        "is_exact_price": True,
    }


def _write_nps_silver(tmp_path, rows: list[dict]) -> None:
    layout = DataLakeLayout(tmp_path)
    writer = ParquetDatasetWriter()
    for key, partition in pl.DataFrame(rows).partition_by("date", as_dict=True).items():
        logical_date = key[0] if isinstance(key, tuple) else key
        writer.write(
            partition,
            layout.partition_path("silver.nps_holdings", logical_date),
            mode="overwrite",
        )


def test_build_nps_holdings_delta_tracks_change_enter_exit_and_pit(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    first, second = date(2025, 1, 10), date(2025, 2, 10)
    _write_nps_silver(
        tmp_path,
        [
            _nps_silver_row(first, "005930", 7.5),
            _nps_silver_row(first, "000660", 3.0),
            _nps_silver_row(second, "005930", 8.1),
            _nps_silver_row(second, "035420", 2.0),
        ],
    )

    summary = build_nps_holdings_delta(tmp_path)[0]

    assert summary.dataset == "gold.nps_holdings_delta"
    assert (summary.rows, summary.files) == (3, 1)
    partitions = sorted(
        path.parent.name
        for path in (tmp_path / "gold" / "nps_holdings_delta").glob("dt=*/part.parquet")
    )
    # The first snapshot has no predecessor, so only the later pair is emitted.
    assert partitions == ["dt=2025-02-10"]
    delta = pl.read_parquet(tmp_path / "gold/nps_holdings_delta/dt=2025-02-10/part.parquet")
    rows = {row["security_id"]: row for row in delta.iter_rows(named=True)}
    assert set(rows) == {"S005930", "S000660", "S035420"}
    # PIT: the delta is stamped with the LATER snapshot's publish date.
    expected_available = second + timedelta(days=builders_module.NPS_DISCLOSURE_LAG_DAYS)
    for row in rows.values():
        assert row["snapshot_date"] == second
        assert row["prev_snapshot_date"] == first
        assert row["available_date"] == expected_available
    changed = rows["S005930"]
    assert changed["ticker"] == "005930"
    assert changed["holding_ratio"] == pytest.approx(8.1)
    assert changed["prev_holding_ratio"] == pytest.approx(7.5)
    assert changed["delta_ratio"] == pytest.approx(0.6)
    assert (changed["entered"], changed["exited"]) == (False, False)
    entered = rows["S035420"]
    assert entered["ticker"] == "035420"
    assert entered["prev_holding_ratio"] == 0.0
    assert entered["delta_ratio"] == pytest.approx(2.0)
    assert (entered["entered"], entered["exited"]) == (True, False)
    exited = rows["S000660"]
    assert exited["ticker"] == "000660"
    assert exited["holding_ratio"] == 0.0
    assert exited["prev_holding_ratio"] == pytest.approx(3.0)
    assert exited["delta_ratio"] == pytest.approx(-3.0)
    assert (exited["entered"], exited["exited"]) == (False, True)


def test_build_nps_holdings_delta_chains_consecutive_snapshots(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    days = [date(2025, 1, 10), date(2025, 2, 10), date(2025, 3, 10)]
    _write_nps_silver(
        tmp_path,
        [
            _nps_silver_row(day, "005930", ratio)
            for day, ratio in zip(days, [7.5, 8.0, 7.0], strict=True)
        ],
    )

    # ``dates`` is accepted but always falls back to a full rebuild (documented).
    summary = build_nps_holdings_delta(tmp_path, dates=[date(2025, 3, 10)])[0]

    assert (summary.rows, summary.files) == (2, 2)
    march = pl.read_parquet(tmp_path / "gold/nps_holdings_delta/dt=2025-03-10/part.parquet")
    # Each delta pairs with its immediate predecessor snapshot, not the first.
    assert march["prev_snapshot_date"].item() == date(2025, 2, 10)
    assert march["delta_ratio"].item() == pytest.approx(-1.0)


def test_build_nps_holdings_delta_empty_and_single_snapshot(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()

    empty = build_nps_holdings_delta(tmp_path)[0]

    assert empty.dataset == "gold.nps_holdings_delta"
    assert (empty.rows, empty.files) == (0, 0)
    assert not list((tmp_path / "gold" / "nps_holdings_delta").glob("dt=*/part.parquet"))

    _write_nps_silver(tmp_path, [_nps_silver_row(date(2025, 1, 10), "005930", 7.5)])

    single = build_nps_holdings_delta(tmp_path)[0]

    # One snapshot has no predecessor: still an explicit empty summary, no crash.
    assert (single.rows, single.files) == (0, 0)
    assert not list((tmp_path / "gold" / "nps_holdings_delta").glob("dt=*/part.parquet"))


def test_preferred_discount_empty_when_no_preferred_shares(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    _write_silver_prices(
        tmp_path,
        [_silver_price_row(date(2024, 1, 2), "111110", close=100.0)],
    )
    build_security_master(tmp_path)

    relations_summary = build_security_relations(tmp_path)[0]

    relations = pl.read_parquet(tmp_path / "silver" / "security_relations" / "part.parquet")
    assert relations_summary.rows == 0
    assert relations.is_empty()
    assert relations.columns == [
        "common_security_id",
        "preferred_security_id",
        "common_ticker",
        "preferred_ticker",
        "preferred_name",
        "relation_type",
        "confidence",
        "first_seen_date",
        "last_seen_date",
    ]

    summary = build_preferred_discount(tmp_path)[0]

    assert summary.dataset == "gold.preferred_discount"
    assert (summary.rows, summary.files) == (0, 0)
    assert not list((tmp_path / "gold" / "preferred_discount").glob("dt=*/part.parquet"))
