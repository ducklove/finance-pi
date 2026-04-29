from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.ingest import ResponseCache, request_hash
from finance_pi.ingest.models import IngestUnit, RawBatch
from finance_pi.sources.kis.adapter import KisUniverseDailyAdapter
from finance_pi.sources.naver.adapter import NaverDailyBackfillAdapter
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
