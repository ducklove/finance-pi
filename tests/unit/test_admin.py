from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.admin.server import (
    AdminState,
    _admin_max_price_days,
    _admin_max_price_queries,
    _admin_max_price_tickers,
    _admin_max_request_threads,
    _api_docs_payload,
    _ensure_docs_built,
    _health_payload,
    _is_local_admin_client,
    _job_command,
)
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def test_admin_overview_reports_lightweight_dataset_metadata(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                {
                    "date": date(2026, 4, 28),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 100.0,
                    "high_adj": 101.0,
                    "low_adj": 99.0,
                    "close_adj": 100.0,
                    "return_1d": 0.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2026, 4, 28)),
    )

    overview = AdminState(tmp_path).overview()
    gold_prices = next(
        dataset for dataset in overview["datasets"] if dataset["name"] == "gold.daily_prices_adj"
    )

    assert overview["max_price_date"] == "2026-04-28"
    assert overview["price_coverage"] == {
        "dataset": "gold.daily_prices_adj",
        "start": "2026-04-28",
        "end": "2026-04-28",
    }
    assert gold_prices["files"] == 1
    assert gold_prices["rows"] is None
    assert gold_prices["coverage_start"] == "2026-04-28"
    assert gold_prices["coverage_end"] == "2026-04-28"
    assert gold_prices["status"] == "ready"


def test_admin_overview_reports_yearly_backfill_status(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                {
                    "date": date(2023, 1, 2),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 100.0,
                    "high_adj": 101.0,
                    "low_adj": 99.0,
                    "close_adj": 100.0,
                    "return_1d": 0.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2023, 1, 2)),
    )
    marker = data_root / "_state" / "backfill" / "yearly" / "2022.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"status":"complete"}', encoding="utf-8")

    overview = AdminState(tmp_path).overview()
    years = {item["year"]: item for item in overview["backfill"]["years"]}

    assert years[2023]["status"] == "partial"
    assert years[2023]["price_days"] == 1
    assert years[2023]["rows"] is None
    assert years[2023]["coverage"] == "2023-01-02..2023-01-02"
    assert years[2022]["status"] == "complete"
    assert years[2022]["marker"] == "2022.json"


def test_admin_close_prices_returns_ticker_range(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            }
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    for value, close in [(date(2026, 4, 28), 100.0), (date(2026, 4, 29), 102.0)]:
        writer.write(
            pl.DataFrame(
                [
                    {
                        "date": value,
                        "security_id": "S005930",
                        "listing_id": "L005930",
                        "open_adj": close,
                        "high_adj": close,
                        "low_adj": close,
                        "close_adj": close,
                        "return_1d": 0.0,
                        "volume": 10,
                        "trading_value": 1000,
                        "market_cap": 1_000_000,
                        "listed_shares": 10_000,
                        "is_halted": False,
                        "is_designated": False,
                        "is_liquidation_window": False,
                    }
                ]
            ),
            layout.partition_path("gold.daily_prices_adj", value),
        )

    payload = AdminState(tmp_path).close_prices(
        {"ticker": ["5930"], "since": ["2026-04-28"], "until": ["2026-04-29"]}
    )

    assert payload == {
        "ticker": "005930",
        "since": "2026-04-28",
        "until": "2026-04-29",
        "count": 2,
        "prices": [
            {"date": "2026-04-28", "close": 100.0},
            {"date": "2026-04-29", "close": 102.0},
        ],
    }


def test_admin_close_prices_accepts_batch_tickers(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            },
            {
                "security_id": "S000660",
                "ticker": "000660",
                "name": "SK Hynix",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            },
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2026, 4, 29),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 102.0,
                    "high_adj": 102.0,
                    "low_adj": 102.0,
                    "close_adj": 102.0,
                    "return_1d": 0.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
                {
                    "date": date(2026, 4, 29),
                    "security_id": "S000660",
                    "listing_id": "L000660",
                    "open_adj": 88.0,
                    "high_adj": 88.0,
                    "low_adj": 88.0,
                    "close_adj": 88.0,
                    "return_1d": 0.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2026, 4, 29)),
    )

    payload = AdminState(tmp_path).close_prices(
        {"tickers": ["5930,660"], "since": ["2026-04-29"], "until": ["2026-04-29"]}
    )

    assert payload == {
        "tickers": ["005930", "000660"],
        "since": "2026-04-29",
        "until": "2026-04-29",
        "count": 2,
        "prices": {
            "005930": [{"date": "2026-04-29", "close": 102.0}],
            "000660": [{"date": "2026-04-29", "close": 88.0}],
        },
    }


def test_admin_daily_prices_returns_ohlcv_batch(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            },
            {
                "security_id": "S000660",
                "ticker": "000660",
                "name": "SK Hynix",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            },
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2026, 4, 29),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 101.0,
                    "high_adj": 103.0,
                    "low_adj": 99.0,
                    "close_adj": 102.0,
                    "return_1d": 0.02,
                    "volume": 20,
                    "trading_value": 2040,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
                {
                    "date": date(2026, 4, 29),
                    "security_id": "S000660",
                    "listing_id": "L000660",
                    "open_adj": 86.0,
                    "high_adj": 90.0,
                    "low_adj": 85.0,
                    "close_adj": 88.0,
                    "return_1d": 0.01,
                    "volume": 30,
                    "trading_value": 2640,
                    "market_cap": 2_000_000,
                    "listed_shares": 20_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2026, 4, 29)),
    )

    payload = AdminState(tmp_path).daily_prices(
        {
            "tickers": ["5930,660"],
            "since": ["2026-04-29"],
            "until": ["2026-04-29"],
            "fields": ["close,volume,trading_value"],
        }
    )

    assert payload == {
        "tickers": ["005930", "000660"],
        "since": "2026-04-29",
        "until": "2026-04-29",
        "fields": ["close", "volume", "trading_value"],
        "count": 2,
        "prices": {
            "005930": [
                {
                    "date": "2026-04-29",
                    "close": 102.0,
                    "volume": 20,
                    "trading_value": 2040,
                }
            ],
            "000660": [
                {
                    "date": "2026-04-29",
                    "close": 88.0,
                    "volume": 30,
                    "trading_value": 2640,
                }
            ],
        },
    }


def test_admin_basic_fundamentals_returns_latest_available_metrics(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
                "corp_code": "00126380",
            },
            {
                "security_id": "S000660",
                "ticker": "000660",
                "name": "SK Hynix",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
                "corp_code": "00164779",
            },
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    writer.write(
        pl.DataFrame(
            [
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_Revenue",
                    "account_name": "매출액",
                    "amount": 333_000.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "dart_OperatingIncomeLoss",
                    "account_name": "영업이익",
                    "amount": 43_000.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
                    "account_name": "배당금의 지급",
                    "amount": 9_900.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "dart_AcquisitionOfTreasuryShares",
                    "account_name": "자기주식의 취득",
                    "amount": 8_200.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2024, 12, 31),
                    "event_date": date(2024, 12, 31),
                    "rcept_dt": date(2025, 3, 10),
                    "available_date": date(2025, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_Revenue",
                    "account_name": "매출액",
                    "amount": 300_000.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2024,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2024, 12, 31),
                    "event_date": date(2024, 12, 31),
                    "rcept_dt": date(2025, 3, 10),
                    "available_date": date(2025, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
                    "account_name": "배당금의 지급",
                    "amount": 8_800.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2024,
                },
                {
                    "security_id": "S000660",
                    "corp_code": "00164779",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 17),
                    "available_date": date(2026, 3, 17),
                    "report_type": "11011",
                    "account_id": "ifrs-full_Revenue",
                    "account_name": "매출액",
                    "amount": 97_000.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
            ]
        ),
        layout.partition_path("silver.financials", date(2025, 1, 1)),
    )

    payload = AdminState(tmp_path).basic_fundamentals(
        {"tickers": ["5930,660"], "as_of": ["2026-03-15"]}
    )

    assert payload["tickers"] == ["005930", "000660"]
    assert payload["count"] == 1
    assert payload["fundamentals"]["005930"]["fiscal_year"] == 2025
    assert payload["fundamentals"]["005930"]["metrics"]["revenue"]["amount"] == 333_000.0
    assert (
        payload["fundamentals"]["005930"]["metrics"]["operating_profit"]["amount"]
        == 43_000.0
    )
    assert (
        payload["fundamentals"]["005930"]["metrics"]["dividends_paid"]["amount"]
        == 9_900.0
    )
    assert (
        payload["fundamentals"]["005930"]["metrics"]["treasury_share_purchase"]["amount"]
        == 8_200.0
    )
    assert payload["fundamentals"]["000660"]["metrics"] == {}


def test_admin_capital_actions_returns_fiscal_year_series(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
                "corp_code": "00126380",
            }
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    writer.write(
        pl.DataFrame(
            [
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2024, 12, 31),
                    "event_date": date(2024, 12, 31),
                    "rcept_dt": date(2025, 3, 10),
                    "available_date": date(2025, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
                    "account_name": "배당금의 지급",
                    "amount": 8_800.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2024,
                }
            ]
        ),
        layout.partition_path("silver.financials", date(2024, 1, 1)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
                    "account_name": "배당금의 지급",
                    "amount": 9_900.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
                {
                    "security_id": "S005930",
                    "corp_code": "00126380",
                    "fiscal_period_end": date(2025, 12, 31),
                    "event_date": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "report_type": "11011",
                    "account_id": "dart_AcquisitionOfTreasuryShares",
                    "account_name": "자기주식의 취득",
                    "amount": 8_200.0,
                    "is_consolidated": True,
                    "accounting_basis": "연결",
                    "fiscal_year": 2025,
                },
            ]
        ),
        layout.partition_path("silver.financials", date(2025, 1, 1)),
    )

    payload = AdminState(tmp_path).capital_actions(
        {"ticker": ["5930"], "start_year": ["2024"], "end_year": ["2025"]}
    )

    assert payload["count"] == 2
    assert payload["capital_actions"]["005930"][0]["fiscal_year"] == 2024
    assert (
        payload["capital_actions"]["005930"][0]["metrics"]["dividends_paid"]["amount"]
        == 8_800.0
    )
    assert payload["capital_actions"]["005930"][1]["fiscal_year"] == 2025
    assert (
        payload["capital_actions"]["005930"][1]["metrics"]["treasury_share_purchase"]["amount"]
        == 8_200.0
    )


def test_admin_api_docs_describe_price_endpoints(tmp_path) -> None:
    payload = _api_docs_payload(AdminState(tmp_path))

    assert "/api/prices/close" == payload["endpoints"]["close_prices"]["path"]
    assert "/api/prices/daily" == payload["endpoints"]["daily_prices"]["path"]
    assert "/api/fundamentals/basic" == payload["endpoints"]["basic_fundamentals"]["path"]
    assert (
        "/api/fundamentals/capital-actions"
        == payload["endpoints"]["capital_actions"]["path"]
    )
    assert "/api/macro/cpi" == payload["endpoints"]["cpi"]["path"]
    assert "/api/macro/rates" == payload["endpoints"]["rates"]["path"]
    assert "/api/macro/indices" == payload["endpoints"]["indices"]["path"]
    assert "/api/macro/commodities" == payload["endpoints"]["commodities"]["path"]
    assert "/api/macro/fx" == payload["endpoints"]["fx"]["path"]
    assert (
        "/api/macro/economic-indicators"
        == payload["endpoints"]["economic_indicators"]["path"]
    )
    assert "volume" in payload["endpoints"]["daily_prices"]["fields"]["available"]
    assert "trading_value" in payload["endpoints"]["daily_prices"]["fields"]["default"]
    assert "revenue" in payload["endpoints"]["basic_fundamentals"]["metrics"]
    assert "dividends_paid" in payload["endpoints"]["basic_fundamentals"]["metrics"]
    assert "treasury_share_purchase" in payload["endpoints"]["capital_actions"]["metrics"]
    assert payload["limits"]["max_price_queries"] == 4


def test_admin_limits_use_safe_defaults(monkeypatch) -> None:
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_THREADS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", raising=False)
    assert _admin_max_request_threads() == 16
    assert _admin_max_price_queries() == 4
    assert _admin_max_price_tickers() == 500
    assert _admin_max_price_days() == 3700

    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_THREADS", "4")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", "2")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "10")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "30")
    assert _admin_max_request_threads() == 4
    assert _admin_max_price_queries() == 2
    assert _admin_max_price_tickers() == 10
    assert _admin_max_price_days() == 30

    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_THREADS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "not-a-number")
    assert _admin_max_request_threads() == 16
    assert _admin_max_price_queries() == 4
    assert _admin_max_price_tickers() == 500
    assert _admin_max_price_days() == 3700


def test_admin_daily_prices_rejects_large_requests(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "1")

    try:
        AdminState(tmp_path).daily_prices(
            {
                "tickers": ["005930,000660"],
                "since": ["2026-04-29"],
                "until": ["2026-04-30"],
            }
        )
    except ValueError as exc:
        assert "too many tickers" in str(exc)
    else:
        raise AssertionError("expected too many tickers error")

    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "10")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "1")
    try:
        AdminState(tmp_path).daily_prices(
            {
                "ticker": ["005930"],
                "since": ["2026-04-29"],
                "until": ["2026-04-30"],
            }
        )
    except ValueError as exc:
        assert "date range is too large" in str(exc)
    else:
        raise AssertionError("expected date range error")


def test_admin_macro_tables_return_filtered_rows(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 31),
                    "country": "KR",
                    "series_id": "KOR_CPI_ALL",
                    "name": "Korea CPI",
                    "frequency": "M",
                    "value": 112.3,
                    "index_base": "2020=100",
                    "yoy_pct": 2.8,
                    "mom_pct": 0.3,
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 2, 29),
                    "country": "US",
                    "series_id": "US_CPI_ALL",
                    "name": "US CPI",
                    "frequency": "M",
                    "value": 310.0,
                    "index_base": "1982-84=100",
                    "yoy_pct": 3.1,
                    "mom_pct": 0.4,
                    "source": "test",
                    "updated_at": None,
                },
            ]
        ),
        layout.singleton_path("macro.cpi"),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "series_id": "GOLD_USD_OZ",
                    "name": "Gold spot",
                    "commodity": "gold",
                    "value": 2050.0,
                    "unit": "troy_oz",
                    "currency": "USD",
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 2),
                    "series_id": "SILVER_USD_OZ",
                    "name": "Silver spot",
                    "commodity": "silver",
                    "value": 23.0,
                    "unit": "troy_oz",
                    "currency": "USD",
                    "source": "test",
                    "updated_at": None,
                },
            ]
        ),
        layout.singleton_path("macro.commodities"),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "series_id": "USD_KRW",
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "value": 1320.0,
                    "source": "test",
                    "updated_at": None,
                }
            ]
        ),
        layout.singleton_path("macro.fx"),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 1),
                    "country": "US",
                    "series_id": "UNRATE",
                    "name": "US Unemployment Rate",
                    "category": "labor",
                    "frequency": "M",
                    "value": 3.7,
                    "unit": "percent",
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 2),
                    "country": "US",
                    "series_id": "VIXCLS",
                    "name": "CBOE Volatility Index: VIX",
                    "category": "risk",
                    "frequency": "D",
                    "value": 14.2,
                    "unit": "index",
                    "source": "test",
                    "updated_at": None,
                },
            ]
        ),
        layout.singleton_path("macro.economic_indicators"),
    )

    state = AdminState(tmp_path)

    cpi = state.cpi({"country": ["KR"], "since": ["2024-01-01"]})
    gold = state.commodities({"commodity": ["gold"]})
    fx = state.fx({"base_currency": ["USD"], "quote_currency": ["KRW"]})
    labor = state.economic_indicators({"category": ["labor"]})

    assert cpi["count"] == 1
    assert cpi["cpi"][0]["series_id"] == "KOR_CPI_ALL"
    assert gold["count"] == 1
    assert gold["commodities"][0]["series_id"] == "GOLD_USD_OZ"
    assert fx["count"] == 1
    assert fx["fx"][0]["value"] == 1320.0
    assert labor["count"] == 1
    assert labor["economic_indicators"][0]["series_id"] == "UNRATE"


def test_admin_job_command_is_allowlisted(tmp_path) -> None:
    label, command = _job_command(
        "backtest",
        {
            "factor": "quality_roa",
            "start": "2024-01-01",
            "end": "2026-04-28",
            "top_fraction": "0.2",
        },
        tmp_path,
    )

    assert label == "Backtest quality_roa"
    assert "backtest" in command
    assert "--factor" in command
    assert "quality_roa" in command


def test_admin_docs_build_command_is_allowlisted(tmp_path) -> None:
    label, command = _job_command("docs_build", {}, tmp_path)

    assert label == "Build Docs"
    assert command[-4:] == ["docs", "build", "--root", str(tmp_path)]


def test_admin_backfill_job_command_is_allowlisted(tmp_path) -> None:
    label, command = _job_command(
        "backfill_yearly",
        {
            "start_year": "2023",
            "end_year": "2001",
            "max_years": "2",
            "include_financials": False,
            "include_fundamentals_pit": True,
            "no_strict": True,
            "force": True,
            "dry_run": True,
        },
        tmp_path,
    )

    assert label == "Backfill 2023..2001"
    assert command[:4] == [
        command[0],
        "-m",
        "finance_pi.cli.app",
        "backfill",
    ]
    assert command[4:6] == ["yearly", "--root"]
    assert "--start-year" in command
    assert "2023" in command
    assert "--end-year" in command
    assert "2001" in command
    assert "--max-years" in command
    assert "2" in command
    assert "--skip-financials" in command
    assert "--include-fundamentals-pit" in command
    assert "--no-strict" in command
    assert "--force" in command
    assert "--dry-run" in command


def test_admin_health_is_minimal_and_token_state_is_kept(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")
    health = _health_payload(state)

    assert state.token == "secret-token"
    assert health["status"] == "ok"
    assert health["workspace"] == str(tmp_path.resolve())
    assert health["auth"] == "local-or-token"


def test_admin_local_network_clients_bypass_token() -> None:
    assert _is_local_admin_client("127.0.0.1")
    assert _is_local_admin_client("192.168.0.10")
    assert _is_local_admin_client("10.0.0.5")
    assert _is_local_admin_client("172.16.3.8")
    assert _is_local_admin_client("169.254.1.2")
    assert _is_local_admin_client("::1")
    assert _is_local_admin_client("::ffff:192.168.0.10")
    assert not _is_local_admin_client("8.8.8.8")


def test_admin_ensure_docs_built_creates_site(tmp_path) -> None:
    (tmp_path / "README.md").write_text("# Project\n", encoding="utf-8")

    _ensure_docs_built(tmp_path)

    assert (tmp_path / "data" / "docs_site" / "index.html").exists()
    assert (tmp_path / "data" / "docs_site" / "manifest.json").exists()
