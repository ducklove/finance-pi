from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from finance_pi.factors import InMemoryFactorContext, factor_registry

AS_OF = date(2024, 6, 28)


def _fundamentals_row(
    security_id: str,
    account_id: str,
    amount: float,
    *,
    report_type: str = "11011",
) -> dict:
    return {
        "as_of_date": AS_OF,
        "security_id": security_id,
        "account_id": account_id,
        "amount": amount,
        "report_type": report_type,
    }


def _price_row(security_id: str, market_cap: float) -> dict:
    return {"date": AS_OF, "security_id": security_id, "market_cap": market_cap}


def test_builtin_factors_are_registered() -> None:
    assert {
        "momentum_12_1",
        "quality_roa",
        "value_earnings_yield",
        "value_book_to_price",
        "value_dividend_yield",
        "quality_gpa",
        "quality_accruals",
        "lowvol_252",
        "reversal_1m",
        "momentum_52w_high",
        "nps_flow",
        "event_buyback",
        "composite_value_quality",
    } <= set(factor_registry.names())


def test_registry_describe_exposes_direction() -> None:
    rows = {row["name"]: row for row in factor_registry.describe()}
    assert rows["quality_accruals"]["direction"] == -1
    assert rows["lowvol_252"]["direction"] == -1
    assert rows["reversal_1m"]["direction"] == -1
    assert rows["momentum_12_1"]["direction"] == 1
    assert rows["quality_roa"]["requires"] == ["gold.fundamentals_pit"]
    assert rows["momentum_12_1"]["rebalance"] == "monthly"
    assert rows["nps_flow"]["direction"] == 1
    assert rows["nps_flow"]["requires"] == ["gold.nps_holdings_delta", "gold.daily_prices_adj"]
    assert rows["event_buyback"]["direction"] == 1
    assert rows["event_buyback"]["requires"] == [
        "gold.filing_events",
        "gold.daily_prices_adj",
    ]


def test_momentum_factor_returns_lazy_scores() -> None:
    start = date(2024, 1, 1)
    rows = [
        {"date": start + timedelta(days=i), "security_id": "S001", "close_adj": float(100 + i)}
        for i in range(260)
    ]
    ctx = InMemoryFactorContext({"gold.daily_prices_adj": pl.DataFrame(rows)})
    factor = factor_registry.get("momentum_12_1")()

    result = factor.compute(ctx).collect()

    assert result.select("score").drop_nulls().height > 0


def test_value_earnings_yield_scores_known_inputs() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Both accounts reported: owners-of-parent (90) must win, not 190.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLossAttributableToOwnersOfParent", 90.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    _fundamentals_row("S002", "ifrs-full_ProfitLoss", 50.0),
                    _fundamentals_row("S003", "net_income", 30.0),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    _price_row("S001", 1000.0),
                    _price_row("S002", 500.0),
                    _price_row("S003", 200.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("value_earnings_yield")()

    result = factor.compute(ctx).collect()

    scores = {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}
    assert scores["S001"] == pytest.approx(90.0 / 1000.0)
    assert scores["S002"] == pytest.approx(50.0 / 500.0)
    assert scores["S003"] == pytest.approx(30.0 / 200.0)


def test_value_earnings_yield_ignores_non_annual_reports() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    _fundamentals_row(
                        "S002", "ifrs-full_ProfitLoss", 400.0, report_type="11013"
                    ),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [_price_row("S001", 1000.0), _price_row("S002", 1000.0)]
            ),
        }
    )
    factor = factor_registry.get("value_earnings_yield")()

    result = factor.compute(ctx).collect()

    assert result["security_id"].to_list() == ["S001"]
    assert result["score"].item() == pytest.approx(0.1)


def test_quality_roa_scores_known_inputs() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Owners-of-parent (90) wins over ProfitLoss (100); no summing.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLossAttributableToOwnersOfParent", 90.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    # ifrs-full_Assets (1000) wins over the ad-hoc alias (1234).
                    _fundamentals_row("S001", "ifrs-full_Assets", 1000.0),
                    _fundamentals_row("S001", "assets", 1234.0),
                    # Non-annual rows must be ignored entirely.
                    _fundamentals_row(
                        "S001", "ifrs-full_ProfitLoss", 999.0, report_type="11013"
                    ),
                    _fundamentals_row("S002", "profit_loss", 20.0),
                    _fundamentals_row("S002", "total_assets", 200.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("quality_roa")()

    result = factor.compute(ctx).collect()

    scores = {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}
    assert scores["S001"] == pytest.approx(90.0 / 1000.0)
    assert scores["S002"] == pytest.approx(20.0 / 200.0)


def _scores_by_security(result: pl.DataFrame) -> dict[str, float | None]:
    return {row["security_id"]: row["score"] for row in result.iter_rows(named=True)}


def test_value_book_to_price_prefers_owners_equity() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Owners-of-parent equity (800) wins over total equity (900).
                    _fundamentals_row(
                        "S001", "ifrs-full_EquityAttributableToOwnersOfParent", 800.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_Equity", 900.0),
                    _fundamentals_row("S002", "equity", 400.0),
                    # Non-annual rows must be ignored entirely.
                    _fundamentals_row("S003", "ifrs-full_Equity", 999.0, report_type="11013"),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    _price_row("S001", 1000.0),
                    _price_row("S002", 1000.0),
                    _price_row("S003", 1000.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("value_book_to_price")()

    result = factor.compute(ctx).collect()

    scores = _scores_by_security(result)
    assert scores["S001"] == pytest.approx(0.8)
    assert scores["S002"] == pytest.approx(0.4)
    assert "S003" not in scores


def _dividend_row(
    security_id: str | None,
    fiscal_year: int,
    available_date: date,
    dps: float,
    *,
    report_type: str = "11011",
) -> dict:
    return {
        "security_id": security_id,
        "fiscal_period_end": date(fiscal_year, 12, 31),
        "available_date": available_date,
        "cash_dividend_per_share": dps,
        "report_type": report_type,
    }


def _yield_price_row(
    day: date, security_id: str, market_cap: float, listed_shares: int
) -> dict:
    return {
        "date": day,
        "security_id": security_id,
        "market_cap": market_cap,
        "listed_shares": listed_shares,
    }


def test_value_dividend_yield_pit_and_hand_computed() -> None:
    ctx = InMemoryFactorContext(
        {
            "silver.dividends": pl.DataFrame(
                [
                    _dividend_row("S001", 2023, date(2024, 3, 15), 500.0),
                    # Correction filed later for the same fiscal year overrides.
                    _dividend_row("S001", 2023, date(2024, 4, 1), 600.0),
                    # Interim report rows must be ignored.
                    _dividend_row("S002", 2023, date(2024, 3, 15), 900.0, report_type="11012"),
                    # Same available_date, two fiscal years: latest fiscal year wins.
                    _dividend_row("S004", 2022, date(2024, 3, 10), 300.0),
                    _dividend_row("S004", 2023, date(2024, 3, 10), 700.0),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    # Filing day itself: not knowable yet (available_date < date).
                    _yield_price_row(date(2024, 3, 15), "S001", 1_000_000.0, 100),
                    _yield_price_row(date(2024, 3, 16), "S001", 1_000_000.0, 100),
                    _yield_price_row(date(2024, 4, 2), "S001", 1_000_000.0, 100),
                    # 439 days after the last effective date: stale, no score.
                    _yield_price_row(date(2025, 6, 15), "S001", 1_000_000.0, 100),
                    _yield_price_row(date(2024, 3, 16), "S002", 1_000_000.0, 100),
                    _yield_price_row(date(2024, 3, 11), "S004", 2_000_000.0, 100),
                ]
            ),
        }
    )
    factor = factor_registry.get("value_dividend_yield")()

    result = factor.compute(ctx).collect()

    scores = {
        (row["security_id"], row["date"]): row["score"] for row in result.iter_rows(named=True)
    }
    assert scores[("S001", date(2024, 3, 15))] is None
    assert scores[("S001", date(2024, 3, 16))] == pytest.approx(500.0 * 100 / 1_000_000.0)
    assert scores[("S001", date(2024, 4, 2))] == pytest.approx(600.0 * 100 / 1_000_000.0)
    assert scores[("S001", date(2025, 6, 15))] is None
    assert scores[("S002", date(2024, 3, 16))] is None
    assert scores[("S004", date(2024, 3, 11))] == pytest.approx(700.0 * 100 / 2_000_000.0)


def _nps_delta_row(
    security_id: str,
    snapshot_date: date,
    available_date: date,
    delta_ratio: float,
) -> dict:
    return {
        "security_id": security_id,
        "ticker": security_id.removeprefix("S"),
        "snapshot_date": snapshot_date,
        "prev_snapshot_date": snapshot_date - timedelta(days=31),
        "available_date": available_date,
        "holding_ratio": max(delta_ratio, 0.0),
        "prev_holding_ratio": max(-delta_ratio, 0.0),
        "delta_ratio": delta_ratio,
        "entered": False,
        "exited": delta_ratio < 0,
    }


def _flow_price_row(day: date, security_id: str) -> dict:
    return {"date": day, "security_id": security_id}


def test_nps_flow_pit_broadcast_and_staleness() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.nps_holdings_delta": pl.DataFrame(
                [
                    _nps_delta_row("S001", date(2024, 3, 10), date(2024, 3, 15), 0.5),
                    _nps_delta_row("S001", date(2024, 4, 10), date(2024, 4, 15), -0.2),
                    # Exit row: NPS selling out scores as a negative flow.
                    _nps_delta_row("S002", date(2024, 3, 10), date(2024, 3, 15), -3.0),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    # Strictly before and ON available_date: delta not knowable yet.
                    _flow_price_row(date(2024, 3, 11), "S001"),
                    _flow_price_row(date(2024, 3, 15), "S001"),
                    _flow_price_row(date(2024, 3, 16), "S001"),
                    # Broadcast forward until the next delta becomes visible.
                    _flow_price_row(date(2024, 4, 15), "S001"),
                    _flow_price_row(date(2024, 4, 16), "S001"),
                    # 370d staleness cap around the last effective date (2024-04-16).
                    _flow_price_row(date(2025, 4, 21), "S001"),
                    _flow_price_row(date(2025, 4, 22), "S001"),
                    _flow_price_row(date(2024, 3, 16), "S002"),
                    # No delta disclosed at all for S003.
                    _flow_price_row(date(2024, 3, 16), "S003"),
                ]
            ),
        }
    )
    cls = factor_registry.get("nps_flow")
    assert cls.direction == 1

    result = cls().compute(ctx).collect()

    scores = {
        (row["security_id"], row["date"]): row["score"] for row in result.iter_rows(named=True)
    }
    assert scores[("S001", date(2024, 3, 11))] is None
    assert scores[("S001", date(2024, 3, 15))] is None
    assert scores[("S001", date(2024, 3, 16))] == pytest.approx(0.5)
    # The second delta (available 2024-04-15) must not leak into 2024-04-15 itself.
    assert scores[("S001", date(2024, 4, 15))] == pytest.approx(0.5)
    assert scores[("S001", date(2024, 4, 16))] == pytest.approx(-0.2)
    assert scores[("S001", date(2025, 4, 21))] == pytest.approx(-0.2)
    assert scores[("S001", date(2025, 4, 22))] is None
    assert scores[("S002", date(2024, 3, 16))] == pytest.approx(-3.0)
    assert scores[("S003", date(2024, 3, 16))] is None


def _filing_event_row(
    security_id: str,
    available_date: date,
    *,
    event_type: str = "buyback",
) -> dict:
    return {
        "event_date": available_date,
        "available_date": available_date,
        "security_id": security_id,
        "ticker": security_id.removeprefix("S"),
        "event_type": event_type,
        "expected_sign": 1,
        "rcept_no": f"R{security_id}{available_date.isoformat()}",
        "report_nm": "주요사항보고서(자기주식취득결정)",
        "is_correction": False,
    }


def test_event_buyback_pit_decay_and_staleness() -> None:
    from math import exp

    available = date(2024, 3, 15)  # effective (first scoring day) = 2024-03-16
    ctx = InMemoryFactorContext(
        {
            "gold.filing_events": pl.DataFrame(
                [
                    _filing_event_row("S001", available),
                    # A non-buyback event type must never score.
                    _filing_event_row("S002", available, event_type="rights_issue"),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    # Announcement day itself: not knowable yet (strict PIT).
                    _flow_price_row(date(2024, 3, 15), "S001"),
                    _flow_price_row(date(2024, 3, 16), "S001"),
                    _flow_price_row(date(2024, 4, 5), "S001"),
                    # 91 calendar days after the effective date: last scoring day.
                    _flow_price_row(date(2024, 6, 15), "S001"),
                    # 92 days after (> ~63 trading days): stale, no score.
                    _flow_price_row(date(2024, 6, 16), "S001"),
                    _flow_price_row(date(2024, 3, 16), "S002"),
                    # No event at all for S003.
                    _flow_price_row(date(2024, 3, 16), "S003"),
                ]
            ),
        }
    )
    cls = factor_registry.get("event_buyback")
    assert cls.direction == 1

    result = cls().compute(ctx).collect()

    scores = {
        (row["security_id"], row["date"]): row["score"] for row in result.iter_rows(named=True)
    }
    assert scores[("S001", date(2024, 3, 15))] is None
    assert scores[("S001", date(2024, 3, 16))] == pytest.approx(exp(-1 / 21))
    assert scores[("S001", date(2024, 4, 5))] == pytest.approx(exp(-21 / 21))
    assert scores[("S001", date(2024, 6, 15))] == pytest.approx(exp(-92 / 21))
    assert scores[("S001", date(2024, 6, 16))] is None
    assert scores[("S002", date(2024, 3, 16))] is None
    assert scores[("S003", date(2024, 3, 16))] is None


def test_event_buyback_most_recent_event_wins() -> None:
    from math import exp

    ctx = InMemoryFactorContext(
        {
            "gold.filing_events": pl.DataFrame(
                [
                    _filing_event_row("S001", date(2024, 3, 15)),
                    _filing_event_row("S001", date(2024, 4, 1)),
                ]
            ),
            "gold.daily_prices_adj": pl.DataFrame(
                [
                    _flow_price_row(date(2024, 3, 20), "S001"),
                    # Second event not yet visible ON its available_date.
                    _flow_price_row(date(2024, 4, 1), "S001"),
                    _flow_price_row(date(2024, 4, 10), "S001"),
                ]
            ),
        }
    )
    factor = factor_registry.get("event_buyback")()

    result = factor.compute(ctx).collect()

    scores = {
        (row["security_id"], row["date"]): row["score"] for row in result.iter_rows(named=True)
    }
    assert scores[("S001", date(2024, 3, 20))] == pytest.approx(exp(-5 / 21))
    assert scores[("S001", date(2024, 4, 1))] == pytest.approx(exp(-17 / 21))
    assert scores[("S001", date(2024, 4, 10))] == pytest.approx(exp(-9 / 21))


def test_quality_gpa_prefers_gross_profit_then_derives_it() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    # Reported gross profit (300) wins over revenue - cost of sales (150).
                    _fundamentals_row("S001", "ifrs-full_GrossProfit", 300.0),
                    _fundamentals_row("S001", "ifrs-full_Revenue", 250.0),
                    _fundamentals_row("S001", "ifrs-full_CostOfSales", 100.0),
                    _fundamentals_row("S001", "ifrs-full_Assets", 1000.0),
                    # No GrossProfit account: falls back to revenue - cost of sales.
                    _fundamentals_row("S002", "ifrs-full_Revenue", 500.0),
                    _fundamentals_row("S002", "ifrs-full_CostOfSales", 200.0),
                    _fundamentals_row("S002", "ifrs-full_Assets", 1000.0),
                    # Revenue alone cannot produce a gross profit: null score.
                    _fundamentals_row("S003", "ifrs-full_Revenue", 500.0),
                    _fundamentals_row("S003", "ifrs-full_Assets", 1000.0),
                ]
            ),
        }
    )
    factor = factor_registry.get("quality_gpa")()

    result = factor.compute(ctx).collect()

    scores = _scores_by_security(result)
    assert scores["S001"] == pytest.approx(0.3)
    assert scores["S002"] == pytest.approx(0.3)
    assert scores["S003"] is None


def test_quality_accruals_hand_computed_and_defensive() -> None:
    ctx = InMemoryFactorContext(
        {
            "gold.fundamentals_pit": pl.DataFrame(
                [
                    _fundamentals_row("S001", "ifrs-full_ProfitLoss", 100.0),
                    _fundamentals_row(
                        "S001", "ifrs-full_CashFlowsFromUsedInOperatingActivities", 150.0
                    ),
                    _fundamentals_row("S001", "ifrs-full_Assets", 1000.0),
                    # Cash-flow account missing: score must be null, not invented.
                    _fundamentals_row("S002", "ifrs-full_ProfitLoss", 80.0),
                    _fundamentals_row("S002", "ifrs-full_Assets", 400.0),
                ]
            ),
        }
    )
    cls = factor_registry.get("quality_accruals")
    assert cls.direction == -1

    result = cls().compute(ctx).collect()

    scores = _scores_by_security(result)
    assert scores["S001"] == pytest.approx((100.0 - 150.0) / 1000.0)
    assert scores["S002"] is None


def test_lowvol_252_matches_sample_std() -> None:
    from statistics import stdev

    start = date(2023, 1, 1)
    returns = [0.01 if i % 2 == 0 else -0.01 for i in range(250)]
    rows = [
        {"date": start + timedelta(days=i), "security_id": "S001", "return_1d": returns[i]}
        for i in range(250)
    ]
    ctx = InMemoryFactorContext({"gold.daily_prices_adj": pl.DataFrame(rows)})
    cls = factor_registry.get("lowvol_252")
    assert cls.direction == -1

    result = cls().compute(ctx).collect().sort("date")

    scores = result.get_column("score").to_list()
    assert scores[198] is None  # below min_samples=200
    assert scores[199] == pytest.approx(stdev(returns[:200]))
    assert scores[-1] == pytest.approx(stdev(returns))


def test_reversal_1m_is_21d_return_lower_better() -> None:
    start = date(2024, 1, 1)
    rows = [
        {"date": start + timedelta(days=i), "security_id": "S001", "close_adj": float(100 + i)}
        for i in range(30)
    ]
    ctx = InMemoryFactorContext({"gold.daily_prices_adj": pl.DataFrame(rows)})
    cls = factor_registry.get("reversal_1m")
    assert cls.direction == -1

    result = cls().compute(ctx).collect().sort("date")

    scores = result.get_column("score").to_list()
    assert scores[20] is None
    assert scores[21] == pytest.approx(121.0 / 100.0 - 1)
    assert scores[29] == pytest.approx(129.0 / 108.0 - 1)


def test_momentum_52w_high_hand_computed() -> None:
    start = date(2023, 1, 1)
    rows = [
        {"date": start + timedelta(days=i), "security_id": "S001", "close_adj": float(100 + i)}
        for i in range(260)
    ] + [
        {"date": start + timedelta(days=i), "security_id": "S002", "close_adj": float(400 - i)}
        for i in range(260)
    ]
    ctx = InMemoryFactorContext({"gold.daily_prices_adj": pl.DataFrame(rows)})
    factor = factor_registry.get("momentum_52w_high")()

    result = factor.compute(ctx).collect()

    by_key = {
        (row["security_id"], row["date"]): row["score"] for row in result.iter_rows(named=True)
    }
    last = start + timedelta(days=259)
    assert by_key[("S001", start + timedelta(days=198))] is None  # below min_samples=200
    assert by_key[("S001", last)] == pytest.approx(1.0)  # rising series sits at its high
    # Falling series: 252d window covers days 8..259, high = 400 - 8 = 392.
    assert by_key[("S002", last)] == pytest.approx((400.0 - 259.0) / 392.0)
