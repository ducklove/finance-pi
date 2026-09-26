from datetime import UTC, datetime

import pytest

from finance_pi.research import gold
from finance_pi.sources.gold.market_size import (
    TROY_OZ_PER_TONNE,
    derive,
    reconstruct_stock,
    year_end_debt,
)


def test_backcast_uses_subsequent_production_not_anchor_year_twice():
    points = [{"date": "2023-12", "value": 3}, {"date": "2024-12", "value": 4}]
    assert reconstruct_stock(points, {"year": 2024, "tonnes": 100}) == [
        {"date": "2023-12", "value": 96},
        {"date": "2024-12", "value": 100},
    ]
    with pytest.raises(ValueError, match="missing mining year"):
        reconstruct_stock(points + [{"date": "2026-12", "value": 3}], {"year": 2026, "tonnes": 110})


def test_treasury_uses_calendar_december_last_business_day_and_usd():
    payload = {
        "meta": {"total-count": 4},
        "data": [
            {"record_date": "2024-12-30", "tot_pub_debt_out_amt": "123.45"},
            {"record_date": "2024-12-31", "tot_pub_debt_out_amt": "234.56"},
            {"record_date": "2024-09-30", "tot_pub_debt_out_amt": "999"},
            {"record_date": "2025-12-31", "tot_pub_debt_out_amt": "888"},
        ],
    }
    assert year_end_debt(payload, datetime(2025, 12, 1, tzinfo=UTC)) == [
        {"date": "2024-12", "value": 234.56, "observedAt": "2024-12-31"}
    ]
    payload["meta"]["total-count"] = 5
    with pytest.raises(ValueError, match="pagination"):
        year_end_debt(payload, datetime(2026, 1, 1, tzinfo=UTC))


def test_valuation_units_intersection_and_end_year_denominator():
    points = [{"date": f"{year}-12", "value": 1} for year in range(2000, 2025)]
    prices = [{**p, "value": 10} for p in points]
    history = {"assets": [{"id": "gold", "points": prices, "source": "wb"}]}
    trends = {"mining": [{"id": "world", "points": points, "source": "usgs"}]}
    inputs = {"stockAnchor": {"year": 2024, "tonnes": 100}, "miningExtensions": []}
    debt = [{**p, "value": 1000} for p in points[1:]]
    result = derive(history, trends, inputs, debt, datetime(2025, 1, 1, tzinfo=UTC))
    assert pytest.approx(32150.7465686) == TROY_OZ_PER_TONNE
    assert result["marketCap"][-1]["value"] == pytest.approx(100 * TROY_OZ_PER_TONNE * 10)
    assert result["goldDebtRatioPct"][0]["date"] == "2001-12"
    assert result["goldDebtRatioPct"][-1]["value"] == pytest.approx(TROY_OZ_PER_TONNE * 100)
    assert result["miningStockRatioPct"][-1]["value"] == 1
    assert result["stockToFlowYears"][-1]["value"] == 100
    assert result["stock"][0]["value"] == 76


def test_failed_debt_collection_retains_previous_complete_snapshot(tmp_path, monkeypatch):
    gold.publish(tmp_path, {}, {}, {}, {"old": True})
    before = (tmp_path / "research/gold/current.json").read_bytes()
    monkeypatch.setattr(gold, "collect_history", lambda: {})
    monkeypatch.setattr(gold, "collect_trends", lambda: {})

    def fail(*_args):
        raise ValueError("Treasury unavailable")

    monkeypatch.setattr(gold, "collect_market_size", fail)
    with pytest.raises(ValueError, match="Treasury unavailable"):
        gold.refresh(tmp_path)
    assert (tmp_path / "research/gold/current.json").read_bytes() == before
