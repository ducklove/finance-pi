from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.backtest import (
    BacktestConfig,
    BacktestEngine,
    FixedBpsCostModel,
    KoreanTradingCostModel,
)
from finance_pi.calendar import TradingCalendar
from finance_pi.factors import InMemoryFactorContext
from finance_pi.factors.base import Factor


class StaticScoreFactor(Factor):
    requires = ["scores"]

    def compute(self, ctx):
        return ctx.scan("scores")


# Sparse calendar spanning three month-ends: rebalances land on Jan 31,
# Feb 29 and Mar 5 (last date), with default lags entry_date == rebalance.
SPARSE_DATES = (
    date(2024, 1, 30),
    date(2024, 1, 31),
    date(2024, 2, 5),
    date(2024, 2, 6),
    date(2024, 2, 7),
    date(2024, 2, 29),
    date(2024, 3, 4),
    date(2024, 3, 5),
)
SIGNAL_DATES = (date(2024, 1, 30), date(2024, 2, 7), date(2024, 3, 4))


def _universe_frame(
    dates,
    sids,
    *,
    include_flag_columns: bool = True,
    flag_overrides: dict[tuple[date, str], dict[str, bool]] | None = None,
) -> pl.DataFrame:
    flag_overrides = flag_overrides or {}
    rows = []
    for d in dates:
        for sid in sids:
            row = {
                "date": d,
                "security_id": sid,
                "listing_id": f"L{sid[-1]}",
                "market": "KOSPI",
                "is_active": True,
                "share_class": "common",
                "security_type": "equity",
                "is_spac_pre": False,
            }
            if include_flag_columns:
                row["is_halted"] = False
                row["is_designated"] = False
                row["is_liquidation_window"] = False
                row.update(flag_overrides.get((d, sid), {}))
            rows.append(row)
    return pl.DataFrame(rows)


def _prices_frame(returns: dict[tuple[date, str], float]) -> pl.DataFrame:
    rows = [
        {
            "date": d,
            "security_id": sid,
            "listing_id": f"L{sid[-1]}",
            "close_adj": 100.0,
            "return_1d": value,
            "market_cap": 1_000_000,
        }
        for (d, sid), value in returns.items()
    ]
    return pl.DataFrame(rows)


def _scores_frame(scores: dict[tuple[date, str], float]) -> pl.DataFrame:
    rows = [{"date": d, "security_id": sid, "score": value} for (d, sid), value in scores.items()]
    return pl.DataFrame(rows)


def _ctx(scores: pl.DataFrame, universe: pl.DataFrame, prices: pl.DataFrame):
    return InMemoryFactorContext(
        {
            "scores": scores,
            "gold.universe_history": universe,
            "gold.daily_prices_adj": prices,
        }
    )


def _rotating_scores() -> pl.DataFrame:
    # Jan 30 signal picks S001+S002; Feb 7 and Mar 4 pick S002+S003.
    scores: dict[tuple[date, str], float] = {
        (date(2024, 1, 30), "S001"): 3.0,
        (date(2024, 1, 30), "S002"): 2.0,
        (date(2024, 1, 30), "S003"): 1.0,
    }
    for signal_date in SIGNAL_DATES[1:]:
        scores[(signal_date, "S002")] = 3.0
        scores[(signal_date, "S003")] = 2.0
        scores[(signal_date, "S001")] = 1.0
    return _scores_frame(scores)


def test_backtest_separates_signal_and_entry_dates() -> None:
    calendar = TradingCalendar.weekdays(date(2024, 1, 1), date(2024, 2, 29))
    dates = list(calendar.dates)
    scores = pl.DataFrame(
        [{"date": d, "security_id": "S001", "score": 1.0} for d in dates]
        + [{"date": d, "security_id": "S002", "score": 0.5} for d in dates]
    )
    universe = _universe_frame(dates, ["S001", "S002"])
    prices = _prices_frame({(d, sid): 0.001 for d in dates for sid in ["S001", "S002"]})
    ctx = _ctx(scores, universe, prices)

    result = BacktestEngine(calendar).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(start=date(2024, 1, 1), end=date(2024, 2, 29), top_fraction=0.5),
    )

    assert not result.ledger.is_empty()
    first = result.ledger.row(0, named=True)
    assert first["entry_date"] > first["signal_date"]
    assert result.nav.select("nav").drop_nulls().height > 0


def test_entry_day_return_attributed_to_previous_portfolio() -> None:
    calendar = TradingCalendar.from_dates(SPARSE_DATES)
    sids = ["S001", "S002", "S003"]
    returns = {(d, sid): 0.0 for d in SPARSE_DATES for sid in sids}
    # First entry day: nothing is held yet, so these must not appear.
    returns[(date(2024, 1, 31), "S001")] = 0.5
    returns[(date(2024, 1, 31), "S002")] = 0.5
    # Second entry day: S003 is bought at this close, so its spike is not
    # earned; S002 is held by the outgoing portfolio and is earned.
    returns[(date(2024, 2, 29), "S003")] = 1.0
    returns[(date(2024, 2, 29), "S002")] = 0.04
    ctx = _ctx(_rotating_scores(), _universe_frame(SPARSE_DATES, sids), _prices_frame(returns))

    result = BacktestEngine(calendar).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(
            start=date(2024, 1, 30),
            end=date(2024, 3, 5),
            top_fraction=0.5,
            max_position_weight=0.5,
        ),
    )

    by_date = {row["date"]: row for row in result.nav.to_dicts()}
    assert by_date[date(2024, 1, 31)]["gross_return"] == 0.0
    assert abs(by_date[date(2024, 2, 29)]["gross_return"] - 0.5 * 0.04) < 1e-15


def test_every_trading_day_attributed_to_exactly_one_portfolio() -> None:
    calendar = TradingCalendar.weekdays(date(2024, 1, 1), date(2024, 4, 30))
    dates = list(calendar.dates)
    sids = ["S001", "S002"]
    scores = pl.DataFrame(
        [{"date": d, "security_id": "S001", "score": 1.0} for d in dates]
        + [{"date": d, "security_id": "S002", "score": 0.5} for d in dates]
    )
    prices = _prices_frame({(d, sid): 0.001 for d in dates for sid in sids})
    ctx = _ctx(scores, _universe_frame(dates, sids), prices)
    config = BacktestConfig(start=date(2024, 1, 1), end=date(2024, 4, 30), top_fraction=0.5)

    result = BacktestEngine(calendar).run(StaticScoreFactor(), ctx, config)

    assert result.ledger.height >= 3
    per_day = result.positions.group_by("date").agg(
        pl.col("entry_date").n_unique().alias("portfolios")
    )
    assert per_day.select(pl.col("portfolios").max()).item() == 1
    first_entry = result.ledger.select(pl.col("entry_date").min()).item()
    expected = [d for d in calendar.between(config.start, config.end) if d > first_entry]
    covered = sorted(result.positions.select("date").unique().to_series().to_list())
    assert covered == expected


def test_missing_price_applies_missing_return_then_drops() -> None:
    dates = SPARSE_DATES[:6]  # Jan 30 .. Feb 29
    calendar = TradingCalendar.from_dates(dates)
    sids = ["S001", "S002"]
    scores = _scores_frame(
        {(d, sid): 2.0 if sid == "S001" else 1.0 for d in SIGNAL_DATES[:2] for sid in sids}
    )
    returns: dict[tuple[date, str], float] = {(d, "S001"): 0.0 for d in dates}
    for d in (date(2024, 1, 30), date(2024, 1, 31), date(2024, 2, 5)):
        returns[(d, "S002")] = 0.0  # S002 prices disappear after Feb 5
    ctx = _ctx(scores, _universe_frame(dates, sids), _prices_frame(returns))
    config = BacktestConfig(
        start=date(2024, 1, 30),
        end=date(2024, 2, 29),
        top_fraction=1.0,
        max_position_weight=0.5,
        missing_return=-0.3,
    )
    engine = BacktestEngine(calendar, FixedBpsCostModel(bps=0.0))

    result = engine.run(StaticScoreFactor(), ctx, config)

    by_date = {row["date"]: row for row in result.nav.to_dicts()}
    assert abs(by_date[date(2024, 2, 6)]["gross_return"] - 0.5 * -0.3) < 1e-15
    assert by_date[date(2024, 2, 7)]["gross_return"] == 0.0
    assert by_date[date(2024, 2, 29)]["gross_return"] == 0.0
    assert result.ledger.get_column("missing_positions").to_list() == [1, 0]

    kept = engine.run(
        StaticScoreFactor(), ctx, config.model_copy(update={"drop_after_missing": False})
    )
    kept_by_date = {row["date"]: row for row in kept.nav.to_dicts()}
    assert abs(kept_by_date[date(2024, 2, 7)]["gross_return"] - 0.5 * -0.3) < 1e-15
    assert abs(kept_by_date[date(2024, 2, 29)]["gross_return"] - 0.5 * -0.3) < 1e-15


def test_non_finite_scores_are_never_selected() -> None:
    dates = SPARSE_DATES[:6]
    calendar = TradingCalendar.from_dates(dates)
    sids = ["S001", "S002", "S003"]
    scores = _scores_frame(
        {
            **{(d, "S001"): float("nan") for d in SIGNAL_DATES[:2]},
            **{(d, "S002"): 1.0 for d in SIGNAL_DATES[:2]},
            **{(d, "S003"): float("inf") for d in SIGNAL_DATES[:2]},
        }
    )
    returns = {(d, sid): 0.0 for d in dates for sid in sids}
    ctx = _ctx(scores, _universe_frame(dates, sids), _prices_frame(returns))

    result = BacktestEngine(calendar).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(start=date(2024, 1, 30), end=date(2024, 2, 29), top_fraction=0.5),
    )

    assert set(result.positions.get_column("security_id").to_list()) == {"S002"}


def test_flagged_securities_excluded_when_columns_present() -> None:
    dates = SPARSE_DATES[:6]
    calendar = TradingCalendar.from_dates(dates)
    sids = ["S001", "S002"]
    scores = _scores_frame(
        {(d, sid): 2.0 if sid == "S001" else 1.0 for d in SIGNAL_DATES[:2] for sid in sids}
    )
    returns = {(d, sid): 0.0 for d in dates for sid in sids}
    universe = _universe_frame(
        dates, sids, flag_overrides={(d, "S001"): {"is_halted": True} for d in dates}
    )
    ctx = _ctx(scores, universe, _prices_frame(returns))
    config = BacktestConfig(start=date(2024, 1, 30), end=date(2024, 2, 29), top_fraction=0.5)

    result = BacktestEngine(calendar).run(StaticScoreFactor(), ctx, config)
    assert set(result.positions.get_column("security_id").to_list()) == {"S002"}

    unfiltered = BacktestEngine(calendar).run(
        StaticScoreFactor(), ctx, config.model_copy(update={"exclude_flagged": False})
    )
    assert set(unfiltered.positions.get_column("security_id").to_list()) == {"S001"}


def test_universe_without_flag_columns_treated_as_unflagged() -> None:
    dates = SPARSE_DATES[:6]
    calendar = TradingCalendar.from_dates(dates)
    sids = ["S001", "S002"]
    scores = _scores_frame(
        {(d, sid): 2.0 if sid == "S001" else 1.0 for d in SIGNAL_DATES[:2] for sid in sids}
    )
    returns = {(d, sid): 0.0 for d in dates for sid in sids}
    universe = _universe_frame(dates, sids, include_flag_columns=False)
    ctx = _ctx(scores, universe, _prices_frame(returns))

    result = BacktestEngine(calendar).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(start=date(2024, 1, 30), end=date(2024, 2, 29), top_fraction=0.5),
    )

    assert set(result.positions.get_column("security_id").to_list()) == {"S001"}


def test_korean_cost_model_hand_computed() -> None:
    model = KoreanTradingCostModel(commission_bps=5.0, sell_tax_bps=15.0)
    # commission: (0.5 + 0.5) * 5bps = 0.0005; sell tax: 0.5 * 15bps = 0.00075
    assert abs(model.cost_for_trades(0.5, 0.5) - 0.00125) < 1e-15
    assert abs(model.cost_for_turnover(1.0) - 0.00125) < 1e-15
    assert model.cost_for_trades(1.0, 0.0) == 5.0 / 10_000
    assert abs(model.cost_for_trades(0.0, 1.0) - 20.0 / 10_000) < 1e-15


def test_fixed_bps_cost_model_backward_compatible() -> None:
    assert FixedBpsCostModel(bps=10.0).cost_for_turnover(0.5) == 0.5 * 10.0 / 10_000


def test_final_liquidation_charge() -> None:
    calendar = TradingCalendar.from_dates(SPARSE_DATES)
    sids = ["S001", "S002", "S003"]
    returns = {(d, sid): 0.0 for d in SPARSE_DATES for sid in sids}
    ctx = _ctx(_rotating_scores(), _universe_frame(SPARSE_DATES, sids), _prices_frame(returns))
    model = KoreanTradingCostModel(commission_bps=5.0, sell_tax_bps=15.0)
    engine = BacktestEngine(calendar, model)
    config = BacktestConfig(
        start=date(2024, 1, 30),
        end=date(2024, 3, 5),
        top_fraction=0.5,
        max_position_weight=0.5,
    )

    base = engine.run(StaticScoreFactor(), ctx, config)
    charged = engine.run(
        StaticScoreFactor(), ctx, config.model_copy(update={"charge_final_liquidation": True})
    )

    last = date(2024, 3, 5)
    cost_off = base.nav.filter(pl.col("date") == last).get_column("cost").item()
    cost_on = charged.nav.filter(pl.col("date") == last).get_column("cost").item()
    # Final portfolio holds two names at 0.5 each -> sells 1.0 on liquidation.
    assert abs((cost_on - cost_off) - model.cost_for_trades(0.0, 1.0)) < 1e-15


def test_engine_arithmetic_matches_hand_computed_nav() -> None:
    calendar = TradingCalendar.from_dates(SPARSE_DATES)
    sids = ["S001", "S002", "S003"]
    returns = {
        (date(2024, 1, 30), "S001"): 0.9,
        (date(2024, 1, 30), "S002"): 0.9,
        (date(2024, 1, 30), "S003"): 0.9,
        (date(2024, 1, 31), "S001"): 0.8,
        (date(2024, 1, 31), "S002"): 0.8,
        (date(2024, 1, 31), "S003"): 0.8,
        (date(2024, 2, 5), "S001"): 0.01,
        (date(2024, 2, 5), "S002"): 0.02,
        (date(2024, 2, 5), "S003"): 0.03,
        (date(2024, 2, 6), "S001"): 0.01,
        (date(2024, 2, 6), "S002"): -0.01,
        (date(2024, 2, 6), "S003"): 0.05,
        (date(2024, 2, 7), "S001"): 0.0,
        (date(2024, 2, 7), "S002"): 0.03,
        (date(2024, 2, 7), "S003"): 0.02,
        (date(2024, 2, 29), "S001"): 0.10,
        (date(2024, 2, 29), "S002"): 0.01,
        (date(2024, 2, 29), "S003"): -0.02,
        (date(2024, 3, 4), "S001"): 0.50,
        (date(2024, 3, 4), "S002"): 0.02,
        (date(2024, 3, 4), "S003"): 0.01,
        (date(2024, 3, 5), "S001"): 0.50,
        (date(2024, 3, 5), "S002"): 0.01,
        (date(2024, 3, 5), "S003"): 0.03,
    }
    ctx = _ctx(_rotating_scores(), _universe_frame(SPARSE_DATES, sids), _prices_frame(returns))
    model = KoreanTradingCostModel(commission_bps=5.0, sell_tax_bps=15.0)

    result = BacktestEngine(calendar, model).run(
        StaticScoreFactor(),
        ctx,
        BacktestConfig(
            start=date(2024, 1, 30),
            end=date(2024, 3, 5),
            top_fraction=0.5,
            max_position_weight=0.5,
        ),
    )

    # Portfolio A (S001+S002) enters Jan 31, earns Feb 5..Feb 29.
    # Portfolio B (S002+S003) enters Feb 29, earns Mar 4..Mar 5.
    # Portfolio C (S002+S003) enters Mar 5 (backtest end), earns nothing.
    cost_a = model.cost_for_trades(1.0, 0.0)
    cost_b = model.cost_for_trades(0.5, 0.5)
    daily = [
        (0.0, 0.0),  # Jan 30
        (0.0, cost_a),  # Jan 31: buy A at close, no return earned yet
        (0.5 * 0.01 + 0.5 * 0.02, 0.0),  # Feb 5
        (0.5 * 0.01 + 0.5 * -0.01, 0.0),  # Feb 6
        (0.5 * 0.0 + 0.5 * 0.03, 0.0),  # Feb 7
        (0.5 * 0.10 + 0.5 * 0.01, cost_b),  # Feb 29: A earns B's entry day
        (0.5 * 0.02 + 0.5 * 0.01, 0.0),  # Mar 4: B
        (0.5 * 0.01 + 0.5 * 0.03, 0.0),  # Mar 5: B earns C's entry day
    ]
    expected_nav = 1.0
    for gross, cost in daily:
        expected_nav *= 1.0 + (gross - cost)

    nav = result.nav.sort("date")
    assert nav.height == len(SPARSE_DATES)
    for row, (gross, cost) in zip(nav.to_dicts(), daily, strict=True):
        assert abs(row["gross_return"] - gross) < 1e-15
        assert abs(row["cost"] - cost) < 1e-15
    assert result.ledger.get_column("turnover").to_list() == [1.0, 1.0, 0.0]
    assert abs(nav.get_column("nav").to_list()[-1] - expected_nav) < 1e-12
