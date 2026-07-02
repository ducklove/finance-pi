from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.ingest.models import IngestUnit
from finance_pi.sources.kis.adapter import KisUniverseDailyAdapter
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def _price_row(ticker: str, logical_date: date, *, close: float) -> dict[str, object]:
    return {
        "date": logical_date,
        "ticker": ticker,
        "isin": None,
        "name": ticker,
        "market": "KRX",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": close,
        "volume": 10,
        "trading_value": 1000,
        "market_cap": None,
        "listed_shares": None,
    }


def test_kis_universe_adapter_merge_applies_corrected_values_deterministically(tmp_path) -> None:
    logical_date = date(2026, 4, 28)

    class FirstKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [_price_row(ticker, since, close=100.0)]

    class CorrectedKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [_price_row(ticker, since, close=101.0)]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()

    first = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        FirstKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    # Build the IngestUnit directly (bypassing list_pending's already-fetched-ticker
    # skip) so the same (date, ticker) key can be re-fetched to exercise corrections.
    unit = IngestUnit(
        first.name,
        logical_date,
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        {
            "ticker_count": 1,
            "total_ticker_count": 1,
            "ticker_start": 0,
            "tickers": ("005930",),
            "since": logical_date.isoformat(),
            "until": logical_date.isoformat(),
        },
    )
    first_result = first.write_bronze(first.fetch(unit))

    frame = pl.read_parquet(first_result.path)
    assert frame["close"].to_list() == [100.0]
    mtime_after_first_write = first_result.path.stat().st_mtime_ns

    corrected = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        CorrectedKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    result = corrected.write_bronze(corrected.fetch(unit))

    frame = pl.read_parquet(result.path)
    assert frame["ticker"].to_list() == ["005930"]
    assert frame["close"].to_list() == [101.0]
    assert not result.skipped
    assert result.path.stat().st_mtime_ns != mtime_after_first_write

    # Running the identical correction again should be a stable no-op merge: the
    # surviving row is deterministic and repeated writes converge to the same file.
    rerun = KisUniverseDailyAdapter(
        layout,
        ParquetDatasetWriter(),
        CorrectedKisClient(),
        ("005930",),
        chunk_days=1,
        sleep_seconds=0,
    )
    rerun_result = rerun.write_bronze(rerun.fetch(unit))

    frame = pl.read_parquet(rerun_result.path)
    assert frame["close"].to_list() == [101.0]
    assert rerun_result.skipped


def test_kis_universe_adapter_merge_is_deterministic_across_many_tickers(tmp_path) -> None:
    logical_date = date(2026, 4, 28)
    tickers = tuple(f"{i:06d}" for i in range(20))

    class FirstKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [_price_row(ticker, since, close=100.0)]

    class CorrectedKisClient:
        def fetch_daily_prices(self, ticker: str, since: date, until: date):
            return [_price_row(ticker, since, close=101.0)]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    unit = IngestUnit(
        "kis_universe_daily",
        logical_date,
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        {
            "ticker_count": len(tickers),
            "total_ticker_count": len(tickers),
            "ticker_start": 0,
            "tickers": tickers,
            "since": logical_date.isoformat(),
            "until": logical_date.isoformat(),
        },
    )

    first = KisUniverseDailyAdapter(
        layout, ParquetDatasetWriter(), FirstKisClient(), tickers, chunk_days=1, sleep_seconds=0
    )
    first.write_bronze(first.fetch(unit))

    for _ in range(5):
        corrected = KisUniverseDailyAdapter(
            layout,
            ParquetDatasetWriter(),
            CorrectedKisClient(),
            tickers,
            chunk_days=1,
            sleep_seconds=0,
        )
        result = corrected.write_bronze(corrected.fetch(unit))
        frame = pl.read_parquet(result.path).sort("ticker")
        assert frame["close"].to_list() == [101.0] * len(tickers)
        assert frame["ticker"].to_list() == sorted(tickers)
