from datetime import UTC, datetime

import pytest
from test_admin import _handler_response_status, _make_handler

from finance_pi.admin.server import AdminState
from finance_pi.research import gold
from finance_pi.sources.gold.prices import parse_daily_averages
from finance_pi.sources.gold.trends import imf_points


def test_volume_conversion_and_missing_values():
    result = imf_points(
        {"series_name": "Gold, Millions", "period": ["2000", "2001", "2002"], "value": [1, "NA", 0]}
    )
    assert result == [{"date": "2000-12", "value": 31.103477}, {"date": "2002-12", "value": 0}]


def test_daily_monthly_mean_and_incomplete_month():
    dates = [datetime(2020 + i // 12, i % 12 + 1, 1, tzinfo=UTC) for i in range(38)]
    result = {
        "meta": {"exchangeTimezoneName": "UTC"},
        "timestamp": [int(d.timestamp()) for d in dates],
        "indicators": {"quote": [{"close": [10.0] * 38}]},
    }
    result["timestamp"].insert(1, int(datetime(2020, 1, 2, tzinfo=UTC).timestamp()))
    result["indicators"]["quote"][0]["close"].insert(1, 30.0)
    points = parse_daily_averages(result, datetime(2023, 2, 15, tzinfo=UTC))
    assert points[0]["value"] == 20
    assert points[-1]["date"] == "2023-01"


def test_failed_refresh_keeps_previous_release(tmp_path, monkeypatch):
    gold.publish(tmp_path, {"old": True}, {}, {})
    before = (tmp_path / "research/gold/current.json").read_bytes()
    monkeypatch.setattr(gold, "collect_history", lambda: {"new": True})

    def fail():
        raise ValueError("source unavailable")

    monkeypatch.setattr(gold, "collect_trends", fail)
    with pytest.raises(ValueError):
        gold.refresh(tmp_path)
    assert (tmp_path / "research/gold/current.json").read_bytes() == before
    assert len(list((tmp_path / "research/gold/releases").glob("*.json"))) == 1


def test_api_read_and_auth(tmp_path):
    gold.publish(tmp_path / "data", {"assets": []}, {}, {})
    state = AdminState(tmp_path, token="test-token")
    handler = _make_handler(state, path="/api/research/gold", method="GET")
    handler.do_GET()
    assert _handler_response_status(handler) == 200
    assert b"finance-pi" in handler.wfile.getvalue()
    denied = _make_handler(state, path="/api/research/gold", method="GET", client_ip="8.8.8.8")
    denied.do_GET()
    assert _handler_response_status(denied) == 401


def test_missing_snapshot_is_explicit(tmp_path):
    handler = _make_handler(AdminState(tmp_path), path="/api/research/gold", method="GET")
    handler.do_GET()
    assert _handler_response_status(handler) == 404


def _releases(root):
    return sorted((root / "research/gold/releases").glob("*.json"))


def test_unchanged_content_adds_no_release_but_refreshes_current(tmp_path):
    history = {"schemaVersion": 2, "generatedAt": "2026-09-29T00:00:00+00:00", "assets": [1]}
    trends = {"schemaVersion": 1, "generatedAt": "2026-09-29T00:00:00+00:00", "mining": []}
    first = gold.publish(tmp_path, history, trends, {"items": []})
    [release] = _releases(tmp_path)
    release_bytes = release.read_bytes()

    # Next day: same data, only run timestamps differ.
    history2 = {**history, "generatedAt": "2026-09-30T00:00:00+00:00"}
    trends2 = {**trends, "generatedAt": "2026-09-30T00:00:00+00:00"}
    second = gold.publish(tmp_path, history2, trends2, {"items": []})

    assert _releases(tmp_path) == [release]
    assert release.read_bytes() == release_bytes
    assert gold.content_fingerprint(first) == gold.content_fingerprint(second)
    current = gold.read_snapshot(tmp_path)
    assert current["publishedAt"] == second["publishedAt"]
    assert current["history"]["generatedAt"] == "2026-09-30T00:00:00+00:00"


def test_changed_content_writes_a_new_release(tmp_path):
    gold.publish(tmp_path, {"generatedAt": "a", "assets": [1]}, {}, {})
    gold.publish(tmp_path, {"generatedAt": "b", "assets": [1, 2]}, {}, {})
    assert len(_releases(tmp_path)) == 2
    # Research metadata dates (e.g. a paper's publishedAt) are content, not run stamps.
    gold.publish(tmp_path, {"generatedAt": "c", "assets": [1, 2]}, {}, {"publishedAt": "x"})
    assert len(_releases(tmp_path)) == 3


def test_corrupt_latest_release_is_superseded(tmp_path):
    gold.publish(tmp_path, {"assets": [1]}, {}, {})
    [release] = _releases(tmp_path)
    release.write_bytes(b"{broken")
    gold.publish(tmp_path, {"assets": [1]}, {}, {})
    assert len(_releases(tmp_path)) == 2
