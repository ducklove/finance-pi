from datetime import date, timedelta

import polars as pl
import pytest

from finance_pi.admin.server import AdminServiceBusy, AdminState, _job_command
from finance_pi.research.pairs import PairConfig, analyze, digest, load_snapshot, signals, simulate


def fixture_bars(count=85):
    return [
        {
            "date": str(date(2024, 1, 1) + timedelta(days=i)),
            "common": {"price": 100.0 + i, "trading_value": 1e9, "tradable": True},
            "preferred": {
                "price": (100 + i) * (0.7 + (i % 7) * 0.01),
                "trading_value": 1e9,
                "tradable": True,
            },
        }
        for i in range(count)
    ]


def config(**kwargs):
    return PairConfig(
        **{
            "common": "005930",
            "preferred": "005935",
            "start": date(2024, 1, 25),
            "end": date(2024, 3, 25),
            "window": 20,
            **kwargs,
        }
    )


def test_next_day_execution_and_no_future_leak():
    bars = fixture_bars()
    c = config(entry_z=0.5, exit_z=0)
    sig = signals(bars, c)
    result = simulate(bars, sig, c, "switch")
    assert result["trades"]
    assert all(t["date"] > t["signal_date"] for t in result["trades"])
    changed = fixture_bars()
    changed[-1]["preferred"]["price"] *= 0.1
    assert signals(changed, c)[:-1] == sig[:-1]


def test_quantities_drift_without_daily_rebalancing_and_cash_nonnegative():
    bars = fixture_bars()
    c = config(commission_bps=0, sell_tax_bps=0, slippage_bps=0)
    result = simulate(bars, signals(bars, c), c, "common")
    assert len(result["trades"]) == 1
    first, last = result["nav"][0], result["nav"][-1]
    assert first["common_quantity"] == last["common_quantity"]
    assert last["nav"] == pytest.approx(last["cash"] + last["common_quantity"] * 184)
    assert all(row["cash"] >= -1e-6 for row in result["nav"])


def test_low_liquidity_partial_fills_and_halts():
    bars = fixture_bars()
    for bar in bars:
        bar["common"]["trading_value"] = bar["common"]["price"] * 100
    bars[30]["common"]["tradable"] = False
    c = config()
    result = simulate(bars, signals(bars, c), c, "common")
    assert all(t["quantity"] == 1 for t in result["trades"])
    assert bars[30]["date"] not in [t["date"] for t in result["trades"]]


def test_reproducible_snapshot_and_cost_stress():
    bars = fixture_bars()
    snap = {"bars": bars, "snapshot_id": digest(bars)}
    result = analyze(snap, config())
    assert result == analyze(snap, config())
    assert result["live_eligible"] is False
    assert len(result["scenarios"]) == 4
    assert result["stress"]["cost"] >= result["scenarios"][0]["cost"]


@pytest.mark.parametrize(
    "kwargs",
    [{"window": 0}, {"capital": float("nan")}, {"exit_z": 3}, {"strategy": "unknown"}, {"foo": 1}],
)
def test_config_rejects_unsafe_or_unknown_values(kwargs):
    with pytest.raises(ValueError):
        config(**kwargs)


def test_constant_spread_does_not_fabricate_signal():
    bars = fixture_bars()
    for bar in bars:
        bar["preferred"]["price"] = bar["common"]["price"] * 0.7
    with pytest.raises(ValueError, match="이력"):
        analyze({"bars": bars}, config())


def test_snapshot_rejects_missing_leg_and_unknown_pair(tmp_path):
    rel = tmp_path / "silver/security_relations/part.parquet"
    rel.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "common_security_id": "S005930",
                "preferred_security_id": "S005935",
                "confidence": "high",
            }
        ]
    ).write_parquet(rel)
    p = tmp_path / "gold/daily_prices_adj/dt=2024-01-25/part.parquet"
    p.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "date": date(2024, 1, 25),
                "security_id": "S005930",
                "close_adj": 100.0,
                "trading_value": 1e9,
                "is_halted": False,
                "is_designated": False,
                "is_liquidation_window": False,
            }
        ]
    ).write_parquet(p)
    with pytest.raises(ValueError, match="완전"):
        load_snapshot(tmp_path, config())


def test_research_slot_and_config_rejection(tmp_path):
    state = AdminState(tmp_path)
    state._research_slot.acquire()
    params = {k: [str(v)] for k, v in config().model_dump(mode="json").items()}
    with pytest.raises(AdminServiceBusy):
        state.pair_research(params)
    with pytest.raises(ValueError):
        state.pair_research({**params, "strategy": ["unregistered"]})


def test_job_rejects_unknown_factor_and_preserves_preferred(tmp_path):
    state = AdminState(tmp_path)
    with pytest.raises(ValueError):
        _job_command("backtest", {"factor": "invalid"}, state.paths.root)
    _, command = _job_command("backtest", {"factor": "preferred_discount_z"}, state.paths.root)
    assert "preferred_discount_z" in command
