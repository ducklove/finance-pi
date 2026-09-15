from datetime import date, datetime, timedelta

import pytest

from finance_pi.research.basis import KST, BasisConfig, Quote, analyze_basis, plan, replay
from finance_pi.research.pairs import digest


def config(**kw):
    return BasisConfig(
        **{
            "expiry": date(2026, 9, 18),
            "capital": 10000,
            "spot_fee_bps": 0,
            "futures_fee_bps": 0,
            "sell_tax_bps": 0,
            "slippage_bps": 0,
            "funding_pct": 0,
            "borrow_pct": 0,
            "leg_risk_bps": 0,
            "max_contracts": 1,
            "participation": 1,
            "take_profit_fraction": 2,
            **kw,
        }
    )


def quote(day, spot=100, future=110, **kw):
    stamp = (
        datetime(2026, 9, day, 15, 40, tzinfo=KST)
        if kw.get("settlement") is not None
        else datetime(2026, 9, day, 14, 59, tzinfo=KST)
    )
    return Quote(
        **{
            "timestamp": stamp,
            "spot_at": stamp,
            "future_at": stamp,
            "spot_bid": spot,
            "spot_ask": spot,
            "future_bid": future,
            "future_ask": future,
            "underlying": spot,
            "spot_depth": 1000,
            "future_depth": 1000,
            **kw,
        }
    )


def test_cash_carry_payoff_equals_locked_spread_and_next_quote_execution():
    qs = [quote(16), quote(17), quote(18, future=100, settlement=100)]
    r = replay(qs, config())
    assert r["pnl"] == pytest.approx(100)
    assert r["events"][0]["at"] > r["events"][0]["signal_at"]
    assert r["ledger"][-1]["future_pnl"] == 100
    assert r["return_on_capital_pct"] == 1
    assert r["orders_sent"] == 0


def test_reverse_requires_locate_and_pays_dividend_manufacture():
    qs = [quote(16, future=90), quote(17, future=90), quote(18, future=100, dividend_per_share=1)]
    assert replay(qs, config(direction="reverse_carry"))["status"] == "no_trade"
    r = replay(qs, config(direction="reverse_carry", borrow_confirmed=True, borrow_shares=10))
    assert r["pnl"] == 90
    assert r["ledger"][-1]["dividends"] == -10


def test_fee_tax_financing_are_deducted_once():
    qs = [quote(16), quote(17), quote(18, future=100)]
    c = config(spot_fee_bps=10, futures_fee_bps=10, sell_tax_bps=20, funding_pct=3.65)
    r = replay(qs, c)
    # 현물 왕복 2 + 매도세 2 + 선물 1.1+1 + 하루 금융비용 0.122
    assert r["pnl"] == pytest.approx(100 - 6.1 - 0.122)
    assert r["ledger"][-1]["costs"] == pytest.approx(6.222)


def test_hedged_profit_can_still_have_margin_liquidation():
    qs = [
        quote(16, spot=1000, future=1100),
        quote(17, spot=1000, future=1100),
        quote(18, spot=2000, future=2100),
    ]
    r = replay(qs, config(capital=15000, reserve_pct=0))
    assert r["status"] == "margin_liquidation"
    assert r["pnl"] == 0
    assert r["additional_capital"] > 0


def test_no_lookahead_stale_quote_and_depth_gates():
    qs = [quote(16), quote(17, future_depth=0), quote(18, future=100)]
    assert replay(qs, config())["contracts"] == 0
    q = quote(16)
    for changed in (
        q.model_copy(update={"future_at": q.timestamp - timedelta(seconds=6)}),
        q.model_copy(update={"future_at": q.timestamp - timedelta(seconds=3)}),
    ):
        assert plan(changed, config())["blocked"]
    a = replay([quote(16), quote(17), quote(18, future=100)], config())
    b = replay([quote(16), quote(17), quote(18, future=105)], config())
    assert a["events"][0] == b["events"][0]


def test_exit_liquidity_and_corporate_action_never_fabricate_realized_profit():
    for end in (quote(18, future=100, spot_depth=0), quote(18, corporate_action=True)):
        r = replay([quote(16), quote(17), end], config())
        assert r["status"] == "unresolved"
        assert r["pnl"] is None and not r["realized"]


def test_borrow_recall_exits_and_disallows_new_entry():
    c = config(direction="reverse_carry", borrow_confirmed=True, borrow_shares=10)
    r = replay(
        [quote(16, future=90), quote(17, future=90), quote(18, future=95, borrow_available=False)],
        c,
    )
    assert r["status"] == "borrow_recall"
    assert r["pnl"] == 50


@pytest.mark.parametrize(
    "bad",
    [
        lambda: Quote(**{**quote(16).model_dump(), "timestamp": datetime(2026, 9, 16)}),
        lambda: config(multiplier=100),
        lambda: config(maintenance_margin_pct=90),
        lambda: replay([quote(17), quote(16), quote(18)], config()),
        lambda: replay([quote(16, settlement=100), quote(17), quote(18)], config()),
        lambda: replay([quote(16, underlying=1), quote(17), quote(18)], config()),
    ],
)
def test_invalid_contracts_times_and_future_data_rejected(bad):
    with pytest.raises(ValueError):
        bad()


def test_scenario_replay_roundtrip_and_hash():
    payload = {"config": {}, "scenario": {}}
    r = analyze_basis(payload)
    assert r["input_hash"] == digest(payload)
    assert r["result_hash"] == digest({k: v for k, v in r.items() if k != "result_hash"})
    replayed = analyze_basis(r["replay_input"])
    assert replayed["scenarios"][0]["ledger"] == r["scenarios"][0]["ledger"]
    assert r["source_verified"] is False and r["live_eligible"] is False


def test_index_etf_requires_whole_contract_capital_and_exposes_tracking_risk():
    payload = {
        "config": {
            "kind": "index_etf",
            "multiplier": 250000,
            "capital": 300000000,
            "sell_tax_bps": 0,
            "take_profit_fraction": 2,
        },
        "scenario": {"spot": 40000, "future": 408, "underlying": 400},
    }
    r = analyze_basis(payload)
    assert r["scenarios"][0]["contracts"] >= 1
    assert r["scenarios"][-1]["pnl"] < r["scenarios"][0]["pnl"]
    payload["config"]["capital"] = 30000000
    assert analyze_basis(payload)["scenarios"][0]["status"] == "no_trade"


def test_basis_http_contract_auth_origin_size_and_slot(tmp_path):
    import json

    from test_admin import _handler_response_status, _make_handler

    from finance_pi.admin.server import AdminState

    state = AdminState(tmp_path, "research-token")
    body = json.dumps({"config": {}, "scenario": {}}).encode()

    def handler(**kw):
        return _make_handler(state, path="/api/research/basis-analysis", body=body, **kw)

    h = handler()
    h.do_POST()
    assert _handler_response_status(h) == 200
    for h in (
        handler(client_ip="8.8.8.8"),
        handler(headers={"Origin": "https://evil.example", "Host": "localhost:8400"}),
    ):
        h.do_POST()
        assert _handler_response_status(h) in {401, 403}
    h = handler()
    h.headers.replace_header("Content-Length", "500001")
    h.do_POST()
    assert _handler_response_status(h) == 400
    state._research_slot.acquire()
    try:
        h = handler()
        h.do_POST()
        assert _handler_response_status(h) == 400
    finally:
        state._research_slot.release()


def test_final_settlement_is_never_known_before_close():
    q = quote(18, settlement=100)
    early = q.model_copy(update={"timestamp": datetime(2026, 9, 18, 14, 59, tzinfo=KST)})
    with pytest.raises(ValueError, match="마감 전"):
        replay([quote(16), quote(17), early], config())
