from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.cli.app import _dart_financial_requests
from finance_pi.config import ProjectPaths
from finance_pi.ingest import ResponseCache, request_hash
from finance_pi.ingest.models import IngestUnit, RawBatch
from finance_pi.sources.kis.adapter import KisUniverseDailyAdapter
from finance_pi.sources.naver.adapter import NaverDailyBackfillAdapter
from finance_pi.sources.opendart.adapter import DartFilingsAdapter, DartFinancialsBulkAdapter
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def test_request_hash_is_stable_for_param_order() -> None:
    left = request_hash("krx", "daily", {"b": 2, "a": 1})
    right = request_hash("krx", "daily", {"a": 1, "b": 2})
    assert left == right


def test_response_cache_roundtrip(tmp_path) -> None:
    cache = ResponseCache(tmp_path)
    cache.write_json("krx", "abc", {"rows": [{"ticker": "005930"}]})
    assert cache.exists("krx", "abc")
    assert cache.read_json("krx", "abc") == {"rows": [{"ticker": "005930"}]}


def test_raw_batch_to_frame_accepts_null_then_string() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"stock_code": None},
            {"stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"stock_code": pl.String})

    assert frame["stock_code"].to_list() == [None, "036720"]


def test_raw_batch_to_frame_casts_json_date_strings() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"snapshot_dt": "2026-04-28", "stock_code": None},
            {"snapshot_dt": "2026-04-28", "stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"snapshot_dt": pl.Date, "stock_code": pl.String})

    assert frame["snapshot_dt"].dtype == pl.Date
    assert frame["snapshot_dt"].to_list() == [date(2026, 4, 28), date(2026, 4, 28)]
    assert frame["stock_code"].to_list() == [None, "036720"]


def test_dart_filings_adapter_chunks_and_marks_completed_ranges(tmp_path) -> None:
    class FakeOpenDartClient:
        def fetch_filings(self, since: date, until: date):
            return [
                {
                    "rcept_dt": since,
                    "corp_code": "00126380",
                    "corp_name": "Samsung",
                    "stock_code": "005930",
                    "rcept_no": f"{since:%Y%m%d}000001",
                    "report_nm": "report",
                    "rm": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = DartFilingsAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeOpenDartClient(),
        chunk_days=2,
    )

    units = list(adapter.list_pending(date(2026, 4, 1), date(2026, 4, 5)))
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert len(units) == 3
    assert result.rows == 1
    assert len(list(adapter.list_pending(date(2026, 4, 1), date(2026, 4, 5)))) == 2


def test_dart_financial_requests_accepts_mixed_filing_date_schemas(tmp_path) -> None:
    data_root = tmp_path / "data"
    first = data_root / "bronze" / "dart_filings" / "dt=2026-03-15" / "part.parquet"
    second = data_root / "bronze" / "dart_filings" / "dt=2026-03-16" / "part.parquet"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "rcept_dt": date(2026, 3, 15),
                "corp_code": "00126380",
                "corp_name": "Samsung",
                "stock_code": "005930",
                "rcept_no": "20260315000001",
                "report_nm": "\uc0ac\uc5c5\ubcf4\uace0\uc11c (2025.12)",
                "rm": None,
            }
        ]
    ).write_parquet(first)
    pl.DataFrame(
        [
            {
                "rcept_dt": "2026-03-16",
                "corp_code": "00258801",
                "corp_name": "Kakao",
                "stock_code": "035720",
                "rcept_no": "20260316000001",
                "report_nm": "\uc0ac\uc5c5\ubcf4\uace0\uc11c (2025.12)",
                "rm": None,
            }
        ]
    ).write_parquet(second)

    requests = _dart_financial_requests(
        ProjectPaths(root=tmp_path),
        date(2026, 3, 1),
        date(2026, 3, 31),
        ("11011",),
    )

    assert [request["corp_code"] for request in requests] == ["00126380", "00258801"]
    assert {request["bsns_year"] for request in requests} == {2025}


def test_kis_universe_adapter_writes_combined_date_partitions(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720"),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    result = adapter.write_bronze(adapter.fetch(unit))

    frame = pl.read_parquet(result.path)
    assert result.rows == 2
    assert sorted(frame["ticker"].to_list()) == ["005930", "035720"]


def test_kis_universe_adapter_merges_existing_partial_partition(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    first = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    second = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720"),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(first.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    first.write_bronze(first.fetch(unit))

    unit = next(iter(second.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    result = second.write_bronze(second.fetch(unit))

    frame = pl.read_parquet(result.path)
    assert result.rows == 1
    assert result.reason == "merged existing KIS partitions: 1"
    assert sorted(frame["ticker"].to_list()) == ["005930", "035720"]


def test_kis_universe_adapter_batches_and_skips_existing_tickers(tmp_path) -> None:
    class FakeKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
                    "market": "KRX",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": None,
                    "listed_shares": None,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    first = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    unit = next(iter(first.list_pending(date(2026, 4, 28), date(2026, 4, 28))))
    first.write_bronze(first.fetch(unit))

    resumed = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeKisClient(),
        ("005930", "035720", "000660"),
        chunk_days=1,
        ticker_batch_size=1,
        sleep_seconds=0,
    )
    units = list(resumed.list_pending(date(2026, 4, 28), date(2026, 4, 28)))

    assert [unit.params["tickers"] for unit in units] == [("035720",), ("000660",)]


def test_naver_daily_backfill_adapter_writes_request_chunks(tmp_path) -> None:
    class FakeNaverClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [
                {
                    "date": since,
                    "ticker": ticker,
                    "isin": None,
                    "name": ticker,
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

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = NaverDailyBackfillAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeNaverClient(),
        ("005930", "035720"),
        chunk_days=3650,
        ticker_batch_size=1,
        sleep_seconds=0,
    )

    units = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    result = adapter.write_bronze(adapter.fetch(units[0]))

    assert len(units) == 2
    assert result.rows == 1
    assert "bronze" in result.path.parts
    assert "naver_daily" in result.path.parts
    remaining = list(adapter.list_pending(date(2026, 4, 28), date(2026, 4, 28)))
    assert len(remaining) == 1
    assert remaining[0].params["tickers"] == ("035720",)


def test_dart_financials_bulk_merges_same_rcept_date_partitions(tmp_path) -> None:
    class FakeOpenDartClient:
        def fetch_financials(
            self,
            corp_code: str,
            bsns_year: int,
            reprt_code: str,
            *,
            available_date: date,
            fs_div: str = "CFS",
        ):
            return [
                {
                    "security_id": None,
                    "corp_code": corp_code,
                    "fiscal_period_end": date(bsns_year, 12, 31),
                    "event_date": date(bsns_year, 12, 31),
                    "rcept_dt": available_date,
                    "available_date": available_date,
                    "report_type": reprt_code,
                    "account_id": "ifrs-full_Assets",
                    "account_name": "Assets",
                    "amount": 1000.0,
                    "is_consolidated": fs_div == "CFS",
                    "accounting_basis": "K-IFRS",
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = DartFinancialsBulkAdapter(
        layout,
        ParquetDatasetWriter(),
        FakeOpenDartClient(),
        (
            {
                "corp_code": "00126380",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
            {
                "corp_code": "00258801",
                "bsns_year": 2025,
                "report_code": "11011",
                "available_date": date(2026, 3, 15),
            },
        ),
        batch_size=1,
        sleep_seconds=0,
    )

    units = list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31)))
    first = adapter.write_bronze(adapter.fetch(units[0]))
    second = adapter.write_bronze(adapter.fetch(units[1]))

    frame = pl.read_parquet(layout.partition_path("bronze.dart_financials_raw", date(2026, 3, 15)))
    assert first.rows == 1
    assert second.rows == 1
    assert sorted(frame["corp_code"].to_list()) == ["00126380", "00258801"]
    assert list(adapter.list_pending(date(2026, 3, 1), date(2026, 3, 31))) == []
