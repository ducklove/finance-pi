from datetime import UTC, date, datetime

import pytest

from finance_pi.cli import app as cli


def test_naver_summary_json_paginates_and_keeps_units():
    from finance_pi.sources.naver.client import NaverFinanceClient

    class JsonClient:
        def get_json(self, path, *, params, headers):
            stock = {"itemCode": "005930", "stockName": "삼성전자", "closePrice": "248,500",
                     "compareToPreviousClosePrice": "-11,000", "fluctuationsRatio": "-4.24",
                     "marketValueRaw": "1452800234088000", "accumulatedTradingVolume": "100"}
            return {"stocks": [stock] * (100 if params["page"] == 1 else 1), "totalCount": 101}

    rows = NaverFinanceClient(None, summary_http=JsonClient()).fetch_market_summary(
        date(2026, 9, 14), markets=("KOSPI",)
    )
    assert len(rows) == 101
    assert rows[0]["market_cap"] == 1452800234088000
    assert rows[0]["close"] == 248500
    assert rows[0]["change_abs"] == -11000
    assert rows[0]["listed_shares"] is None


def test_final_price_collection_refetches_previously_seen_tickers(tmp_path, monkeypatch):
    from finance_pi.sources.kis import adapter as kis
    from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter

    monkeypatch.setattr(kis, "_existing_tickers_for_date", lambda *args: {"005930"})
    day = date(2026, 9, 14)
    adapter = kis.KisUniverseDailyAdapter(
        DataLakeLayout(tmp_path), ParquetDatasetWriter(), None, ("005930", "000950"),
        refresh_existing=True,
    )
    pending = list(adapter.list_pending(day, day))
    assert pending[0].params["tickers"] == ("005930", "000950")


@pytest.mark.parametrize(
    "day,hour,minute,expected",
    [(11, 16, 0, date(2026, 9, 11)), (14, 17, 30, date(2026, 9, 11)),
     (14, 20, 9, date(2026, 9, 11)), (14, 20, 10, date(2026, 9, 14)),
     (14, 20, 30, date(2026, 9, 14)), (15, 0, 0, date(2026, 9, 14))],
)
def test_daily_cutoff_follows_krx_aftermarket(monkeypatch, day, hour, minute, expected):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=UTC):
            return cls(2026, 9, day, hour, minute, tzinfo=tz)

    monkeypatch.setattr(cli, "datetime", Clock)
    assert cli._latest_closed_price_date() == expected


def test_early_complete_marker_is_retried_after_close(tmp_path):
    import json

    day = date(2026, 9, 14)
    marker = cli._daily_marker_path(tmp_path, day)
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({
        "status": "complete", "price_date": day.isoformat(),
        "completed_at": "2026-09-14T09:00:00+00:00",
    }), encoding="utf-8")
    assert cli._read_daily_marker_status(marker) == "provisional"
    assert cli._catchup_dates(tmp_path, None, day) == (day,)
    marker.write_text(json.dumps({
        "status": "complete", "price_date": day.isoformat(),
        "completed_at": "2026-09-14T12:00:00+00:00",
    }), encoding="utf-8")
    assert cli._read_daily_marker_status(marker) == "complete"
    assert cli._catchup_dates(tmp_path, None, day) == ()


def test_naver_fallback_refreshes_existing_chunk_and_preserves_failed_rows(tmp_path):
    from dataclasses import replace

    import polars as pl

    from finance_pi.ingest.models import RawBatch
    from finance_pi.sources.naver.adapter import NaverDailyBackfillAdapter
    from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter

    day = date(2026, 9, 14)
    adapter = NaverDailyBackfillAdapter(
        DataLakeLayout(tmp_path), ParquetDatasetWriter(), None, ("530060", "530061"),
    )
    unit = next(iter(adapter.list_pending(day, day)))
    original = [{"date": day, "ticker": code, "close": value}
                for code, value in (("530060", 6825), ("530061", 100))]
    adapter.write_bronze(RawBatch(unit, original))
    assert list(adapter.list_pending(day, day)) == []
    refreshed = replace(adapter, refresh_existing=True)
    assert list(refreshed.list_pending(day, day)) == [unit]
    result = refreshed.write_bronze(RawBatch(unit, [
        {"date": day, "ticker": "530060", "close": 6805},
        {"_failures": ["530061:timeout"]},
    ]))
    rows = pl.read_parquet(result.path).sort("ticker")
    assert rows["close"].to_list() == [6805, 100]
    assert rows.height == 2
    assert result.reason == "partial Naver failures: 1"
    assert refreshed._sidecar_path(result.path).exists()


def test_daily_fallback_requests_refresh_after_market_close(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from finance_pi.config import ProjectPaths

    day = date(2026, 9, 14)
    monkeypatch.setattr(cli, "_latest_closed_price_date", lambda: day)
    ingest = Mock()
    monkeypatch.setattr(cli, "ingest_naver_daily", ingest)
    result = cli._ingest_daily_prices(ProjectPaths(tmp_path), SimpleNamespace(has_kis=False), day)
    assert result == ([], [])
    assert ingest.call_args.kwargs == {"refresh_existing": True}
