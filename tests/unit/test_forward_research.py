from datetime import date

import pytest
from test_pair_research import config, fixture_bars

from finance_pi.research.forward import forward_analysis
from finance_pi.research.pairs import analyze, digest


def test_forward_never_borrows_a_signal_from_before_registration():
    bars = fixture_bars()
    r = forward_analysis({"bars": bars}, config(), date(2024, 2, 1))
    assert r["status"] == "available"
    for s in r["scenarios"]:
        assert s["nav"][0]["cash"] == config().capital
        assert s["trades"][0]["date"] == "2024-02-02"
        assert all("2024-02-01" <= t["signal_date"] < t["date"] for t in s["trades"])
    assert r["ledger_hash"] == digest({k: v for k, v in r.items() if k != "ledger_hash"})


def test_forward_future_input_does_not_change_past_ledger():
    bars = fixture_bars()
    before = forward_analysis({"bars": bars[:60]}, config(), date(2024, 2, 1))
    bars[-1]["common"]["price"] *= 10
    after = forward_analysis({"bars": bars}, config(), date(2024, 2, 1))
    for a, b in zip(before["scenarios"], after["scenarios"], strict=True):
        for field in ("nav", "trades"):
            assert b[field][: len(a[field])] == a[field]


def test_forward_future_start_has_no_fabricated_return():
    r = forward_analysis({"bars": fixture_bars()}, config(), date(2024, 4, 1))
    assert r["status"] == "waiting"
    assert r["scenarios"] == []
    with pytest.raises(ValueError):
        forward_analysis({"bars": fixture_bars()}, config(), config().start)


def test_liquidity_stress_keeps_same_assumptions_for_benchmark():
    from finance_pi.research.pairs import signals, simulate

    bars = fixture_bars()
    for b in bars:
        for leg in ("common", "preferred"):
            b[leg]["trading_value"] = b[leg]["price"] * 1000
    c = config()
    r = analyze({"bars": bars}, c)
    for s in r["liquidity_stress"]["scenarios"]:
        constrained = c.model_copy(
            update={"participation": c.participation * s["participation_multiplier"]}
        )
        bench = simulate(bars, signals(bars, c), constrained, "mixed", 2)
        assert s["benchmark_return_pct"] == bench["return_pct"]
        assert s["excess_return_pct"] == pytest.approx(s["return_pct"] - bench["return_pct"])
