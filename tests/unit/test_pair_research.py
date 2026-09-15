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


def test_delayed_mixed_fills_reserve_cash_for_both_assets():
    bars = fixture_bars()
    for i, bar in enumerate(bars):
        for leg in ("common", "preferred"):
            bar[leg]["trading_value"] = 0 if i < 60 else 1e10
            if i >= 60:
                bar[leg]["price"] *= 10
    c = config(commission_bps=0, sell_tax_bps=0, slippage_bps=0)
    r = simulate(bars, signals(bars, c), c, "mixed")
    assert {t["leg"] for t in r["trades"]} == {"common", "preferred"}
    first = next(b for b in r["nav"] if b["common_quantity"] > 0)
    assert first["preferred_quantity"] > 0
    assert first["cash"] >= 0


def test_missing_turnover_is_labelled_proxy_and_missing_volume_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "finance_pi.research.pairs.pair_list",
        lambda root: [{"common": "005930", "preferred": "005935"}],
    )
    for day in (date(2024, 1, 25), date(2024, 1, 26)):
        p = tmp_path / f"gold/daily_prices_adj/dt={day}/part.parquet"
        p.parent.mkdir(parents=True)
        pl.DataFrame(
            [
                {
                    "date": day,
                    "security_id": code,
                    "close_adj": 100.0,
                    "trading_value": None,
                    "volume": 100,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
                for code in ("S005930", "S005935")
            ]
        ).write_parquet(p)
    snap = load_snapshot(tmp_path, config())
    assert snap["bars"][0]["common"]["liquidity_basis"] == "adjusted_close_times_volume_proxy"
    assert snap["bars"][0]["common"]["trading_value"] == 10000
    pl.read_parquet(p).with_columns(pl.lit(None).alias("volume")).write_parquet(p)
    with pytest.raises(ValueError, match="모두 누락"):
        load_snapshot(tmp_path, config())


def test_reproducible_snapshot_and_cost_stress():
    bars = fixture_bars()
    snap = {"bars": bars, "snapshot_id": digest(bars)}
    result = analyze(snap, config())
    assert result == analyze(snap, config())
    assert result["live_eligible"] is False
    assert len(result["scenarios"]) == 4
    assert result["stress"]["cost"] >= result["scenarios"][0]["cost"]


def test_etf_log_ratio_is_invariant_to_unit_price_scale():
    c = config(strategy="etf_switch", common="069500", preferred="102110", sell_tax_bps=0)
    a = signals(fixture_bars(), c)
    scaled = fixture_bars()
    for bar in scaled:
        bar["common"]["price"] *= 7
    b = signals(scaled, c)
    assert [s["target"] for s in a] == [s["target"] for s in b]
    for x, y in zip(a[c.window :], b[c.window :], strict=True):
        assert x["z"] == pytest.approx(y["z"])
        assert x["relative_deviation_bps"] == pytest.approx(y["relative_deviation_bps"])


def test_etf_cost_gate_blocks_small_excursion_and_preserves_next_day_rule():
    bars = fixture_bars()
    for i, bar in enumerate(bars):
        bar["preferred"]["price"] = bar["common"]["price"] * (1 + (i % 7) * 0.00001)
    c = config(strategy="etf_switch", common="069500", preferred="102110", entry_z=0.5, exit_z=0)
    sig = signals(bars, c)
    assert all(s["target"] == "common" for s in sig)
    assert any("비용 이하" in s["reason"] for s in sig)
    assert all(t["date"] > t["signal_date"] for t in simulate(bars, sig, c, "switch")["trades"])


def test_unreviewed_etf_pair_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="연구 대상"):
        load_snapshot(tmp_path, config(strategy="etf_switch"))


def test_period_validation_has_no_future_leak_and_does_not_claim_oos():
    bars = fixture_bars(300)
    c = config(end=date(2024, 10, 26))
    a = analyze({"bars": bars}, c)["validation"]
    assert a["status"] == "available" and a["out_of_sample_claim"] is False
    assert len(a["periods"]) == 3
    assert sum(p["observations"] for p in a["periods"]) == 276
    assert all(p["start"] < p["end"] for p in a["periods"])
    bars[-1]["preferred"]["price"] *= 0.1
    b = analyze({"bars": bars}, c)["validation"]
    assert a["periods"][:2] == b["periods"][:2]
    assert (
        analyze({"bars": fixture_bars()}, config())["validation"]["status"] == "insufficient_data"
    )


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
    pl.read_parquet(p).with_columns(pl.lit("S000660").alias("security_id")).write_parquet(p)
    with pytest.raises(ValueError, match="모두 누락"):
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
