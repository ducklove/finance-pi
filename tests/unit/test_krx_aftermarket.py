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
