from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from http.client import HTTPMessage
from io import BytesIO

import polars as pl

from finance_pi.admin import server as admin_server
from finance_pi.admin.server import (
    MAX_JOBS_RETAINED,
    AdminJob,
    AdminServiceBusy,
    AdminState,
    _admin_max_jobs,
    _admin_max_price_days,
    _admin_max_price_queries,
    _admin_max_price_tickers,
    _admin_max_request_threads,
    _admin_price_query_wait_seconds,
    _api_docs_payload,
    _ensure_docs_built,
    _handler_for,
    _health_payload,
    _is_local_admin_client,
    _job_command,
)
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def _make_handler(state, *, client_ip="127.0.0.1", path="/api/jobs", method="POST",
                   headers=None, body=b"{}"):
    handler_cls = _handler_for(state)
    handler = handler_cls.__new__(handler_cls)
    handler.client_address = (client_ip, 55555)
    handler.headers = HTTPMessage()
    for key, value in (headers or {}).items():
        handler.headers[key] = value
    handler.headers["Content-Length"] = str(len(body))
    handler.path = path
    handler.command = method
    handler.request_version = "HTTP/1.1"
    handler.protocol_version = "HTTP/1.0"
    handler.requestline = f"{method} {path} HTTP/1.1"
    handler.rfile = BytesIO(body)
    handler.wfile = BytesIO()
    handler.close_connection = True
    handler._request_started_at = 0.0
    return handler


def _handler_response_status(handler) -> int:
    handler.wfile.seek(0)
    status_line = handler.wfile.readline()
    return int(status_line.split(b" ")[1])


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
                "security_id": "S005935",
                "ticker": "005935",
                "name": "Samsung Pref",
                "market": "KOSPI",
                "share_class": "preferred",
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
                    "account_id": "ifrs-full_ProfitLossAttributableToOwnersOfParent",
                    "account_name": "지배기업 소유주지분 순이익",
                    "amount": 30_000.0,
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
                    "account_id": "ifrs-full_EquityAttributableToOwnersOfParent",
                    "account_name": "지배기업 소유주지분",
                    "amount": 120_000.0,
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
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2025, 12, 31),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 100.0,
                    "high_adj": 100.0,
                    "low_adj": 100.0,
                    "close_adj": 100.0,
                    "return_1d": 0.0,
                    "volume": 1,
                    "trading_value": 100,
                    "market_cap": 1_000_000,
                    "listed_shares": 1_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
                {
                    "date": date(2025, 12, 31),
                    "security_id": "S005935",
                    "listing_id": "L005935",
                    "open_adj": 80.0,
                    "high_adj": 80.0,
                    "low_adj": 80.0,
                    "close_adj": 80.0,
                    "return_1d": 0.0,
                    "volume": 1,
                    "trading_value": 80,
                    "market_cap": 40_000,
                    "listed_shares": 500,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                },
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2025, 12, 31)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "fiscal_year": 2025,
                    "fiscal_period_end": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "share_class": "common",
                    "stock_kind": "보통주",
                    "authorized_shares": 20_000.0,
                    "cumulative_issued_shares": 1_200.0,
                    "cumulative_decreased_shares": 100.0,
                    "capital_reduction_shares": None,
                    "profit_retirement_shares": 100.0,
                    "redemption_shares": None,
                    "other_decrease_shares": None,
                    "issued_shares": 1_100.0,
                    "treasury_shares": 100.0,
                    "outstanding_shares": 1_000.0,
                    "source_rcept_no": "20260310000001",
                    "report_type": "11011",
                    "source": "opendart.stockTotqySttus",
                    "is_estimated": False,
                },
                {
                    "fiscal_year": 2025,
                    "fiscal_period_end": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 10),
                    "available_date": date(2026, 3, 10),
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "share_class": "preferred",
                    "stock_kind": "우선주",
                    "authorized_shares": 5_000.0,
                    "cumulative_issued_shares": 500.0,
                    "cumulative_decreased_shares": None,
                    "capital_reduction_shares": None,
                    "profit_retirement_shares": None,
                    "redemption_shares": None,
                    "other_decrease_shares": None,
                    "issued_shares": 500.0,
                    "treasury_shares": 100.0,
                    "outstanding_shares": 400.0,
                    "source_rcept_no": "20260310000001",
                    "report_type": "11011",
                    "source": "opendart.stockTotqySttus",
                    "is_estimated": False,
                },
            ]
        ),
        layout.partition_path("silver.share_counts", date(2025, 12, 31)),
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
    assert payload["fundamentals"]["005930"]["per_share"]["eps_annual"]["value"] == (
        30_000.0 / 1_400
    )
    assert payload["fundamentals"]["005930"]["per_share"]["eps_ttm"]["value"] == (
        30_000.0 / 1_400
    )
    assert payload["fundamentals"]["005930"]["per_share"]["bps"]["value"] == (120_000.0 / 1_400)
    assert payload["fundamentals"]["005930"]["per_share"]["eps_annual"]["shares"] == 1_400
    assert (
        payload["fundamentals"]["005930"]["per_share"]["eps_annual"]["share_basis"]
        == "dart_distributed_shares"
    )
    assert (
        payload["fundamentals"]["005930"]["per_share"]["eps_annual"]["includes_preferred"]
        is True
    )
    assert (
        payload["fundamentals"]["005930"]["per_share"]["eps_annual"][
            "treasury_shares_excluded"
        ]
        is True
    )
    assert (
        payload["fundamentals"]["005930"]["per_share"]["eps_annual"]["components"][0][
            "treasury_shares"
        ]
        == 100.0
    )
    assert payload["fundamentals"]["005930"]["per_share"]["eps_forward"]["value"] is None
    assert payload["fundamentals"]["000660"]["metrics"] == {}


def test_admin_basic_fundamentals_derives_equity_when_total_equity_is_suspicious(
    tmp_path,
) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S037350",
                "ticker": "037350",
                "name": "Sungdo",
                "market": "KOSDAQ",
                "share_class": "common",
                "security_type": "equity",
                "corp_code": "00216498",
            },
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    common = {
        "security_id": "S037350",
        "corp_code": "00216498",
        "fiscal_period_end": date(2025, 12, 31),
        "event_date": date(2025, 12, 31),
        "rcept_dt": date(2026, 3, 19),
        "available_date": date(2026, 3, 19),
        "report_type": "11011",
        "is_consolidated": True,
        "accounting_basis": "연결",
        "fiscal_year": 2025,
    }
    writer.write(
        pl.DataFrame(
            [
                {
                    **common,
                    "account_id": "ifrs-full_Assets",
                    "account_name": "자산총계",
                    "amount": 1_000.0,
                },
                {
                    **common,
                    "account_id": "ifrs-full_Liabilities",
                    "account_name": "부채총계",
                    "amount": 400.0,
                },
                {
                    **common,
                    "account_id": "ifrs-full_Equity",
                    "account_name": "자본총계",
                    "amount": 10.0,
                },
            ]
        ),
        layout.partition_path("silver.financials", date(2025, 1, 1)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "fiscal_year": 2025,
                    "fiscal_period_end": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 19),
                    "available_date": date(2026, 3, 19),
                    "corp_code": "00216498",
                    "corp_name": "Sungdo",
                    "share_class": "common",
                    "stock_kind": "보통주",
                    "authorized_shares": 100.0,
                    "cumulative_issued_shares": 12.0,
                    "cumulative_decreased_shares": None,
                    "capital_reduction_shares": None,
                    "profit_retirement_shares": None,
                    "redemption_shares": None,
                    "other_decrease_shares": None,
                    "issued_shares": 12.0,
                    "treasury_shares": 2.0,
                    "outstanding_shares": 10.0,
                    "source_rcept_no": "20260319000001",
                    "report_type": "11011",
                    "source": "opendart.stockTotqySttus",
                    "is_estimated": False,
                },
            ]
        ),
        layout.partition_path("silver.share_counts", date(2025, 12, 31)),
    )

    payload = AdminState(tmp_path).basic_fundamentals(
        {"ticker": ["037350"], "as_of": ["2026-05-10"]}
    )

    equity = payload["fundamentals"]["037350"]["metrics"]["equity"]
    assert equity["amount"] == 600.0
    assert equity["account_id"] == "derived_AssetsMinusLiabilities"
    assert equity["replaced_account_id"] == "ifrs-full_Equity"
    assert payload["fundamentals"]["037350"]["per_share"]["bps"]["value"] == 60.0
    assert (
        payload["fundamentals"]["037350"]["per_share"]["bps"]["treasury_shares_excluded"]
        is True
    )


def test_admin_basic_fundamentals_does_not_use_future_share_count_for_bps(
    tmp_path,
) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
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
    ).write_parquet(data_root / "gold" / "security_master.parquet")
    common = {
        "security_id": "S009770",
        "corp_code": "00128227",
        "fiscal_period_end": date(2018, 12, 31),
        "event_date": date(2018, 12, 31),
        "rcept_dt": date(2019, 4, 1),
        "available_date": date(2019, 4, 1),
        "report_type": "11011",
        "is_consolidated": True,
        "accounting_basis": "연결",
        "fiscal_year": 2018,
    }
    writer.write(
        pl.DataFrame(
            [
                {
                    **common,
                    "account_id": "ifrs_EquityAttributableToOwnersOfParent",
                    "account_name": "지배기업의 소유주에게 귀속되는 자본",
                    "amount": 180_345_954_779.0,
                },
            ]
        ),
        layout.partition_path("silver.financials", date(2018, 1, 1)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "fiscal_year": 2025,
                    "fiscal_period_end": date(2025, 12, 31),
                    "rcept_dt": date(2026, 3, 19),
                    "available_date": date(2026, 3, 19),
                    "corp_code": "00128227",
                    "corp_name": "Samjung Pulp",
                    "share_class": "common",
                    "stock_kind": "보통주",
                    "authorized_shares": None,
                    "cumulative_issued_shares": None,
                    "cumulative_decreased_shares": None,
                    "capital_reduction_shares": None,
                    "profit_retirement_shares": None,
                    "redemption_shares": None,
                    "other_decrease_shares": None,
                    "issued_shares": 2_499_971.0,
                    "treasury_shares": 0.0,
                    "outstanding_shares": 2_499_971.0,
                    "source_rcept_no": "20260319000474",
                    "report_type": "11011",
                    "source": "opendart.stockTotqySttus",
                    "is_estimated": False,
                },
            ]
        ),
        layout.partition_path("silver.share_counts", date(2025, 12, 31)),
    )

    payload = AdminState(tmp_path).basic_fundamentals(
        {"ticker": ["009770"], "as_of": ["2026-05-14"]}
    )

    fundamental = payload["fundamentals"]["009770"]
    assert fundamental["metrics"]["equity"]["amount"] == 180_345_954_779.0
    bps = fundamental["per_share"]["bps"]
    assert bps["available"] is False
    assert bps["value"] is None
    assert bps["numerator_amount"] == 180_345_954_779.0
    assert bps["reason"] == "share denominator is not available as of the equity fiscal period"


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


def test_admin_dividends_returns_security_level_dps(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "fiscal_period_end": date(2024, 12, 31),
                    "rcept_dt": date(2025, 3, 11),
                    "available_date": date(2025, 3, 11),
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "security_id": "S005930",
                    "ticker": "005930",
                    "share_class": "common",
                    "stock_kind": "보통주",
                    "cash_dividend_per_share": 1446.0,
                    "stock_dividend_per_share": None,
                    "cash_dividend_yield_pct": 2.7,
                    "currency": "KRW",
                    "source_rcept_no": "20250311001085",
                    "report_type": "11011",
                    "source": "opendart.alotMatter",
                    "is_estimated": False,
                },
                {
                    "fiscal_period_end": date(2024, 12, 31),
                    "rcept_dt": date(2025, 3, 11),
                    "available_date": date(2025, 3, 11),
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "security_id": "S005935",
                    "ticker": "005935",
                    "share_class": "preferred",
                    "stock_kind": "우선주",
                    "cash_dividend_per_share": 1447.0,
                    "stock_dividend_per_share": None,
                    "cash_dividend_yield_pct": 3.3,
                    "currency": "KRW",
                    "source_rcept_no": "20250311001085",
                    "report_type": "11011",
                    "source": "opendart.alotMatter",
                    "is_estimated": False,
                },
            ]
        ),
        layout.partition_path("silver.dividends", date(2024, 12, 31)),
    )

    payload = AdminState(tmp_path).dividends(
        {"tickers": ["005930,005935"], "start_year": ["2024"], "end_year": ["2024"]}
    )

    assert payload["count"] == 2
    assert payload["dividends"]["005930"][0]["cash_dividend_per_share"] == 1446.0
    assert payload["dividends"]["005935"][0]["cash_dividend_per_share"] == 1447.0


def test_admin_api_docs_describe_price_endpoints(tmp_path) -> None:
    payload = _api_docs_payload(AdminState(tmp_path))

    assert payload["workspace"] == tmp_path.resolve().name
    assert str(tmp_path.resolve()) not in json.dumps(payload)
    assert payload["endpoints"]["close_prices"]["path"] == "/api/prices/close"
    assert payload["endpoints"]["daily_prices"]["path"] == "/api/prices/daily"
    assert payload["endpoints"]["quotes"]["path"] == "/api/quotes"
    assert payload["endpoints"]["security_search"]["path"] == "/api/securities/search"
    assert payload["endpoints"]["basic_fundamentals"]["path"] == "/api/fundamentals/basic"
    assert (
        payload["endpoints"]["capital_actions"]["path"]
        == "/api/fundamentals/capital-actions"
    )
    assert payload["endpoints"]["dividends"]["path"] == "/api/fundamentals/dividends"
    assert payload["endpoints"]["cpi"]["path"] == "/api/macro/cpi"
    assert payload["endpoints"]["rates"]["path"] == "/api/macro/rates"
    assert payload["endpoints"]["indices"]["path"] == "/api/macro/indices"
    assert payload["endpoints"]["daily_indices"]["path"] == "/api/daily-indices"
    assert payload["endpoints"]["commodities"]["path"] == "/api/macro/commodities"
    assert payload["endpoints"]["fx"]["path"] == "/api/macro/fx"
    assert payload["endpoints"]["realtime_indicators"]["path"] == "/api/realtime/indicators"
    assert (
        payload["endpoints"]["economic_indicators"]["path"]
        == "/api/macro/economic-indicators"
    )
    assert "volume" in payload["endpoints"]["daily_prices"]["fields"]["available"]
    assert "trading_value" in payload["endpoints"]["daily_prices"]["fields"]["default"]
    assert "revenue" in payload["endpoints"]["basic_fundamentals"]["metrics"]
    assert "dividends_paid" in payload["endpoints"]["basic_fundamentals"]["metrics"]
    assert "treasury_share_purchase" in payload["endpoints"]["capital_actions"]["metrics"]
    assert payload["limits"]["max_admin_jobs"] == 1
    assert payload["limits"]["max_price_queries"] == 4
    assert payload["limits"]["price_query_wait_seconds"] == 15.0


def test_admin_limits_use_safe_defaults(monkeypatch) -> None:
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_JOBS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_THREADS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", raising=False)
    monkeypatch.delenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", raising=False)
    assert _admin_max_request_threads() == 16
    assert _admin_max_jobs() == 1
    assert _admin_max_price_queries() == 4
    assert _admin_price_query_wait_seconds() == 15.0
    assert _admin_max_price_tickers() == 500
    assert _admin_max_price_days() == 3700

    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_JOBS", "3")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_THREADS", "4")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", "2")
    monkeypatch.setenv("FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS", "0.25")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "10")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "30")
    assert _admin_max_request_threads() == 4
    assert _admin_max_jobs() == 3
    assert _admin_max_price_queries() == 2
    assert _admin_price_query_wait_seconds() == 0.25
    assert _admin_max_price_tickers() == 10
    assert _admin_max_price_days() == 30

    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_JOBS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_THREADS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "not-a-number")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "not-a-number")
    assert _admin_max_request_threads() == 16
    assert _admin_max_jobs() == 1
    assert _admin_max_price_queries() == 4
    assert _admin_price_query_wait_seconds() == 15.0
    assert _admin_max_price_tickers() == 500
    assert _admin_max_price_days() == 3700

    monkeypatch.setenv("FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS", "-10")
    assert _admin_price_query_wait_seconds() == 0.0


def test_admin_data_query_queue_timeout_raises_busy(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", "1")
    monkeypatch.setenv("FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS", "0")
    state = AdminState(tmp_path)
    assert state._price_query_slots.acquire(blocking=False)
    try:
        try:
            state.daily_prices(
                {
                    "ticker": ["005930"],
                    "since": ["2026-04-29"],
                    "until": ["2026-04-30"],
                }
            )
        except AdminServiceBusy as exc:
            assert "data query queue timeout" in str(exc)
        else:
            raise AssertionError("expected AdminServiceBusy")
    finally:
        state._price_query_slots.release()


def test_admin_start_job_rejects_when_job_slot_is_full(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_JOBS", "1")
    state = AdminState(tmp_path)
    assert state._job_slots.acquire(blocking=False)
    try:
        try:
            state.start_job({"action": "daily_no_ingest"})
        except AdminServiceBusy as exc:
            assert "admin job already running" in str(exc)
        else:
            raise AssertionError("expected AdminServiceBusy")
    finally:
        state._job_slots.release()


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


def test_admin_daily_prices_rejects_excessive_ticker_by_day_cells(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", "500")
    monkeypatch.setenv("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", "3700")

    tickers = ",".join(f"{100000 + i:06d}" for i in range(400))
    try:
        AdminState(tmp_path).daily_prices(
            {
                "tickers": [tickers],
                "since": ["2016-01-01"],
                "until": ["2025-01-01"],
            }
        )
    except ValueError as exc:
        assert "tickers x days is too large" in str(exc)
    else:
        raise AssertionError("expected tickers x days error")


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
                    "country": "US",
                    "series_id": "SP500",
                    "name": "S&P 500 Index",
                    "frequency": "D",
                    "category": "equity_index",
                    "value": 4750.0,
                    "currency": "USD",
                    "return_1d": 0.5,
                    "return_1m": None,
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 2),
                    "country": "JP",
                    "series_id": "NIKKEI_225",
                    "name": "Nikkei 225",
                    "frequency": "D",
                    "category": "equity_index",
                    "value": 33000.0,
                    "currency": "JPY",
                    "return_1d": 0.4,
                    "return_1m": None,
                    "source": "test",
                    "updated_at": None,
                },
            ]
        ),
        layout.singleton_path("macro.indices"),
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
    daily_indices = state.daily_indices({"series_id": ["SP500"]})
    gold = state.commodities({"commodity": ["gold"]})
    fx = state.fx({"base_currency": ["USD"], "quote_currency": ["KRW"]})
    labor = state.economic_indicators({"category": ["labor"]})

    assert cpi["count"] == 1
    assert cpi["cpi"][0]["series_id"] == "KOR_CPI_ALL"
    assert daily_indices["count"] == 1
    assert daily_indices["indices"][0]["series_id"] == "SP500"
    assert gold["count"] == 1
    assert gold["commodities"][0]["series_id"] == "GOLD_USD_OZ"
    assert fx["count"] == 1
    assert fx["fx"][0]["value"] == 1320.0
    assert labor["count"] == 1
    assert labor["economic_indicators"][0]["series_id"] == "UNRATE"


def test_admin_realtime_indicators_fetches_cnbc_snapshot(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_fetch(symbols: list[str]) -> dict[str, dict[str, str]]:
        now_text = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000%z")
        calls.append(tuple(symbols))
        return {
            ".SPX": {
                "symbol": ".SPX",
                "code": "0",
                "last": "7,599.96",
                "previous_day_closing": "7,580.06",
                "name": "S&P 500 Index",
                "last_time": now_text,
            }
        }

    monkeypatch.setattr(admin_server, "_fetch_cnbc_quotes", fake_fetch)

    state = AdminState(tmp_path)
    payload = state.realtime_indicators({"category": ["indices"], "series_id": ["SP500"]})
    cached = state.realtime_indicators({"category": ["indices"], "series_id": ["SP500"]})

    assert payload == cached
    assert len(calls) == 1
    assert ".SPX" in calls[0]
    assert payload["source"] == "cnbc"
    assert payload["count"] == 1
    assert payload["indicators"]["indices"][0]["series_id"] == "SP500"
    assert payload["indicators"]["indices"][0]["value"] == 7599.96


def test_admin_quotes_return_domestic_and_cnbc_snapshots(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                {
                    "snapshot_dt": date(2026, 6, 1),
                    "ticker": "005930",
                    "name": "Samsung Electronics",
                    "market": "KOSPI",
                    "close": 80000,
                    "change_abs": 1200,
                    "change_rate_pct": 1.52,
                    "par_value": 100,
                    "market_cap": 1_000_000,
                    "listed_shares": 1_000,
                    "foreign_ownership_pct": 50.0,
                    "volume": 123456,
                    "per": 10.0,
                    "roe": 9.0,
                }
            ]
        ),
        layout.partition_path("bronze.naver_summary_raw", date(2026, 6, 1)),
    )

    def fake_fetch(symbols: list[str]) -> dict[str, dict[str, str]]:
        assert symbols == ["AAPL"]
        return {
            "AAPL": {
                "symbol": "AAPL",
                "code": "0",
                "last": "200.50",
                "change": "1.25",
                "change_pct": "0.627",
                "volume": "123,000",
                "name": "Apple Inc",
                "exchange": "NASDAQ",
                "countryCode": "US",
                "assetType": "EQUITY",
                "currencyCode": "USD",
                "last_time": "2026-06-01T16:00:00.000-0400",
            }
        }

    monkeypatch.setattr(admin_server, "_fetch_cnbc_quotes", fake_fetch)

    payload = AdminState(tmp_path).quotes({"symbols": ["005930,AAPL"]})

    assert payload["count"] == 2
    assert payload["quotes"][0]["symbol"] == "005930"
    assert payload["quotes"][0]["price"] == 80000
    assert payload["quotes"][0]["change"] == 1200
    assert payload["quotes"][0]["volume"] == 123456
    assert payload["quotes"][1]["symbol"] == "AAPL"
    assert payload["quotes"][1]["price"] == 200.50
    assert payload["quotes"][1]["volume"] == 123000.0


def test_admin_security_search_batches_local_and_cnbc(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    (data_root / "gold").mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "security_id": "S005930",
                "ticker": "005930",
                "name": "Samsung Electronics",
                "market": "KOSPI",
                "share_class": "common",
                "security_type": "equity",
            }
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")

    def fake_fetch(symbols: list[str]) -> dict[str, dict[str, str]]:
        assert "AAPL" in symbols
        return {
            "AAPL": {
                "symbol": "AAPL",
                "code": "0",
                "last": "200.50",
                "name": "Apple Inc",
                "exchange": "NASDAQ",
                "countryCode": "US",
                "assetType": "EQUITY",
            }
        }

    monkeypatch.setattr(admin_server, "_fetch_cnbc_quotes", fake_fetch)

    payload = AdminState(tmp_path).security_search({"queries": ["Samsung,AAPL"], "limit": ["5"]})

    assert payload["count"] == 2
    assert payload["results"]["Samsung"][0]["symbol"] == "005930"
    assert payload["results"]["AAPL"][0]["symbol"] == "AAPL"
    assert payload["results"]["AAPL"][0]["name"] == "Apple Inc"


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
    assert health["workspace"] == tmp_path.resolve().name
    assert health["data_root_exists"] is False
    assert health["auth"] == "local-or-token"
    assert not any(
        isinstance(value, str) and str(tmp_path.resolve()) in value for value in health.values()
    )


def test_admin_local_network_clients_bypass_token() -> None:
    assert _is_local_admin_client("127.0.0.1")
    assert _is_local_admin_client("192.168.0.10")
    assert _is_local_admin_client("10.0.0.5")
    assert _is_local_admin_client("172.16.3.8")
    assert _is_local_admin_client("169.254.1.2")
    assert _is_local_admin_client("::1")
    assert _is_local_admin_client("::ffff:192.168.0.10")
    assert not _is_local_admin_client("8.8.8.8")


def test_admin_post_jobs_from_private_lan_peer_requires_token(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")
    body = b'{"action": "docs_build"}'

    handler = _make_handler(state, client_ip="192.168.1.50", body=body)
    handler.do_POST()
    assert _handler_response_status(handler) == HTTPStatus.UNAUTHORIZED

    handler = _make_handler(
        state,
        client_ip="192.168.1.50",
        body=body,
        headers={"X-Admin-Token": "secret-token"},
    )
    handler.do_POST()
    assert _handler_response_status(handler) == HTTPStatus.CREATED


def test_admin_post_jobs_from_loopback_bypasses_token(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")

    handler = _make_handler(state, client_ip="127.0.0.1", body=b'{"action": "docs_build"}')
    handler.do_POST()

    assert _handler_response_status(handler) == HTTPStatus.CREATED


def test_admin_post_jobs_rejects_mismatched_origin(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")

    handler = _make_handler(
        state,
        client_ip="127.0.0.1",
        body=b'{"action": "docs_build"}',
        headers={"Host": "127.0.0.1:8400", "Origin": "http://evil.example.com"},
    )
    handler.do_POST()

    assert _handler_response_status(handler) == HTTPStatus.FORBIDDEN


def test_admin_post_jobs_rejects_cross_site_sec_fetch_site(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")

    handler = _make_handler(
        state,
        client_ip="127.0.0.1",
        body=b'{"action": "docs_build"}',
        headers={"Host": "127.0.0.1:8400", "Sec-Fetch-Site": "cross-site"},
    )
    handler.do_POST()

    assert _handler_response_status(handler) == HTTPStatus.FORBIDDEN


def test_admin_post_jobs_allows_matching_origin(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")

    handler = _make_handler(
        state,
        client_ip="127.0.0.1",
        body=b'{"action": "docs_build"}',
        headers={"Host": "127.0.0.1:8400", "Origin": "http://127.0.0.1:8400"},
    )
    handler.do_POST()

    assert _handler_response_status(handler) == HTTPStatus.CREATED


def test_admin_health_endpoint_has_no_absolute_paths(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")

    handler = _make_handler(state, client_ip="8.8.8.8", path="/api/health", method="GET")
    handler.do_GET()

    handler.wfile.seek(0)
    raw = handler.wfile.read()
    body = raw.split(b"\r\n\r\n", 1)[1]
    assert str(tmp_path.resolve()).encode("utf-8") not in raw
    assert b":\\\\" not in body  # no windows-style absolute path fragments leak either


def test_admin_job_eviction_keeps_only_recent_completed_jobs(tmp_path) -> None:
    state = AdminState(tmp_path)
    log_dir = state.paths.data_root / "admin" / "jobs"
    log_dir.mkdir(parents=True, exist_ok=True)

    base_time = datetime.now(UTC)
    for i in range(55):
        job_id = f"job{i:04d}"
        log_path = log_dir / f"{job_id}.log"
        log_path.write_text("done", encoding="utf-8")
        job = AdminJob(
            id=job_id,
            action="docs_build",
            label="Build Docs",
            command=["true"],
            log_path=log_path,
            status="done",
            returncode=0,
            started_at=base_time + timedelta(seconds=i),
            ended_at=base_time + timedelta(seconds=i + 1),
        )
        with state.lock:
            state.jobs[job.id] = job
            state._evict_old_jobs()

    assert len(state.jobs) == MAX_JOBS_RETAINED
    remaining_ids = set(state.jobs)
    # the oldest jobs should have been evicted, newest retained
    assert "job0000" not in remaining_ids
    assert "job0054" in remaining_ids
    for i in range(55 - MAX_JOBS_RETAINED):
        assert not (log_dir / f"job{i:04d}.log").exists()
    for i in range(55 - MAX_JOBS_RETAINED, 55):
        assert (log_dir / f"job{i:04d}.log").exists()


def test_admin_job_eviction_never_drops_running_jobs(tmp_path) -> None:
    state = AdminState(tmp_path)
    log_dir = state.paths.data_root / "admin" / "jobs"
    log_dir.mkdir(parents=True, exist_ok=True)

    base_time = datetime.now(UTC)
    running_job = AdminJob(
        id="running-job",
        action="docs_build",
        label="Build Docs",
        command=["true"],
        log_path=log_dir / "running-job.log",
        status="running",
        started_at=base_time,
    )
    with state.lock:
        state.jobs[running_job.id] = running_job

    for i in range(60):
        job_id = f"done{i:04d}"
        log_path = log_dir / f"{job_id}.log"
        log_path.write_text("done", encoding="utf-8")
        job = AdminJob(
            id=job_id,
            action="docs_build",
            label="Build Docs",
            command=["true"],
            log_path=log_path,
            status="done",
            returncode=0,
            started_at=base_time + timedelta(seconds=i + 1),
            ended_at=base_time + timedelta(seconds=i + 2),
        )
        with state.lock:
            state.jobs[job.id] = job
            state._evict_old_jobs()

    assert "running-job" in state.jobs
    assert state.jobs["running-job"].status == "running"


def test_admin_ensure_docs_built_creates_site(tmp_path) -> None:
    (tmp_path / "README.md").write_text("# Project\n", encoding="utf-8")

    _ensure_docs_built(tmp_path)

    assert (tmp_path / "data" / "docs_site" / "index.html").exists()
    assert (tmp_path / "data" / "docs_site" / "manifest.json").exists()
