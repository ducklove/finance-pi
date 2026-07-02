from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import ceil

import polars as pl

from finance_pi.backtest.costs import CostModel, FixedBpsCostModel
from finance_pi.backtest.models import BacktestConfig, BacktestResult
from finance_pi.calendar import TradingCalendar
from finance_pi.factors import Factor, FactorContext

_UNIVERSE_FLAG_COLUMNS = ("is_halted", "is_designated", "is_liquidation_window")


@dataclass(frozen=True)
class BacktestEngine:
    """Monthly-rebalanced portfolio backtest.

    Return attribution convention: a portfolio entered at ``entry_date`` is
    bought at that day's close, so it earns ``return_1d`` on the trading days
    in ``(entry_date, exit_date]``, where ``exit_date`` is the next
    portfolio's ``entry_date`` (the portfolio is sold at that close) or
    ``config.end`` for the final portfolio. Every trading day after the first
    entry is therefore attributed to exactly one portfolio; an entry date
    itself always belongs to the outgoing portfolio.
    """

    calendar: TradingCalendar
    cost_model: CostModel = FixedBpsCostModel()

    def run(self, factor: Factor, ctx: FactorContext, config: BacktestConfig) -> BacktestResult:
        scores = factor.compute(ctx).collect()
        direction = getattr(factor, "direction", 1)
        universe = ctx.scan("gold.universe_history").collect()
        prices = ctx.scan("gold.daily_prices_adj").collect()

        rebalance_dates = self.calendar.month_end_dates(config.start, config.end)
        schedules = self._build_schedules(rebalance_dates, config)
        position_rows: list[dict[str, object]] = []
        ledger_rows: list[dict[str, object]] = []
        cost_rows: list[dict[str, object]] = []
        previous_weights: dict[str, float] = {}

        for index, schedule in enumerate(schedules):
            signal_date = schedule["signal_date"]
            entry_date = schedule["entry_date"]
            if index + 1 < len(schedules):
                exit_date = schedules[index + 1]["entry_date"]
            else:
                exit_date = config.end
            active = _active_universe(universe, signal_date, config)
            # Rank by score * direction so direction=-1 factors select the lowest raw scores.
            candidates = (
                scores.filter((pl.col("date") == signal_date) & pl.col("score").is_finite())
                .join(active, on="security_id", how="inner")
                .with_columns((pl.col("score") * direction).alias("_ranking_score"))
                .sort("_ranking_score", descending=True)
                .drop("_ranking_score")
            )
            count = candidates.height
            selected_count = min(count, max(1, ceil(count * config.top_fraction))) if count else 0
            selected = candidates.head(selected_count)
            target_weight = min(
                config.max_position_weight,
                1.0 / selected_count if selected_count else 0.0,
            )
            current_weights = {
                row["security_id"]: target_weight
                for row in selected.select("security_id").to_dicts()
            }
            buys, sells = _trade_flows(previous_weights, current_weights)
            entry_cost = self._trade_cost(buys, sells)
            ledger_rows.append(
                {
                    "signal_date": signal_date,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "selected_count": selected_count,
                    "turnover": buys + sells,
                    "entry_cost": entry_cost,
                }
            )
            cost_rows.append({"date": entry_date, "cost": entry_cost})
            # Bought at entry close: returns accrue on (entry_date, exit_date].
            holding_dates = [
                trading_date
                for trading_date in self.calendar.between(entry_date, exit_date)
                if trading_date > entry_date
            ]
            for security_id, weight in current_weights.items():
                for trading_date in holding_dates:
                    position_rows.append(
                        {
                            "date": trading_date,
                            "security_id": security_id,
                            "weight": weight,
                            "signal_date": signal_date,
                            "entry_date": entry_date,
                        }
                    )
            previous_weights = current_weights

        if config.charge_final_liquidation and previous_weights:
            window = self.calendar.between(config.start, config.end)
            if window:
                final_cost = self._trade_cost(0.0, sum(previous_weights.values()))
                cost_rows.append({"date": window[-1], "cost": final_cost})

        positions = pl.DataFrame(position_rows) if position_rows else _empty_positions()
        ledger = pl.DataFrame(ledger_rows) if ledger_rows else _empty_ledger()
        costs = pl.DataFrame(cost_rows, schema={"date": pl.Date, "cost": pl.Float64})
        attributed = _attribute_returns(positions, prices, config)
        ledger = _with_missing_positions(ledger, attributed)
        nav = self._compute_nav(attributed, costs, config)
        return BacktestResult(nav=nav, positions=positions, ledger=ledger)

    def _trade_cost(self, buys: float, sells: float) -> float:
        cost_for_trades = getattr(self.cost_model, "cost_for_trades", None)
        if cost_for_trades is not None:
            return cost_for_trades(buys, sells)
        return self.cost_model.cost_for_turnover(buys + sells)

    def _build_schedules(
        self,
        rebalance_dates: tuple[date, ...],
        config: BacktestConfig,
    ) -> list[dict[str, date]]:
        schedules: list[dict[str, date]] = []
        for rebalance_date in rebalance_dates:
            signal_date = self.calendar.previous(rebalance_date, offset=config.signal_lag_days)
            entry_date = self.calendar.next(signal_date, offset=config.entry_lag_days)
            if config.start <= entry_date <= config.end:
                schedules.append(
                    {
                        "rebalance_date": rebalance_date,
                        "signal_date": signal_date,
                        "entry_date": entry_date,
                    }
                )
        return schedules

    def _compute_nav(
        self,
        attributed: pl.DataFrame,
        costs: pl.DataFrame,
        config: BacktestConfig,
    ) -> pl.DataFrame:
        """NAV over every trading day in the config window.

        Days on or before the first entry carry zero gross return; entry
        costs land on their entry date, so the first portfolio's cost is
        charged even though no return accrues that day.
        """
        base = pl.DataFrame(
            {"date": list(self.calendar.between(config.start, config.end))},
            schema={"date": pl.Date},
        )
        daily_returns = attributed.group_by("date").agg(
            pl.col("contribution").sum().alias("gross_return")
        )
        daily_costs = costs.group_by("date").agg(pl.col("cost").sum())
        return (
            base.join(daily_returns, on="date", how="left")
            .join(daily_costs, on="date", how="left")
            .with_columns(
                pl.col("gross_return").fill_null(0.0),
                pl.col("cost").fill_null(0.0),
            )
            .with_columns((pl.col("gross_return") - pl.col("cost")).alias("return"))
            .sort("date")
            .with_columns(((1 + pl.col("return")).cum_prod() * config.initial_nav).alias("nav"))
        )


def _active_universe(
    universe: pl.DataFrame, signal_date: date, config: BacktestConfig
) -> pl.DataFrame:
    predicate = (
        (pl.col("date") == signal_date)
        & pl.col("is_active")
        & pl.col("share_class").is_in(list(config.share_classes))
        & (pl.col("is_spac_pre").fill_null(False).not_())
    )
    if config.exclude_flagged:
        # Older gold data may lack these columns; missing means never flagged.
        for flag in _UNIVERSE_FLAG_COLUMNS:
            if flag in universe.columns:
                predicate = predicate & pl.col(flag).fill_null(False).not_()
    return universe.filter(predicate).select("security_id")


def _attribute_returns(
    positions: pl.DataFrame, prices: pl.DataFrame, config: BacktestConfig
) -> pl.DataFrame:
    """Per position-day return contributions with missing-price handling.

    A held security with null ``return_1d`` earns ``config.missing_return``
    on its first missing day within the holding period; when
    ``drop_after_missing`` is set it stops contributing for the rest of that
    period (implicit cash, like weights summing below 1).
    """
    joined = (
        positions.join(
            prices.select(["date", "security_id", "return_1d"]),
            on=["date", "security_id"],
            how="left",
        )
        .sort(["entry_date", "security_id", "date"])
        .with_columns(pl.col("return_1d").is_null().alias("is_missing"))
        .with_columns(
            (pl.col("is_missing").cum_sum() - pl.col("is_missing").cast(pl.UInt32))
            .over(["entry_date", "security_id"])
            .alias("prior_missing")
        )
    )
    applied = (
        pl.when(pl.col("is_missing"))
        .then(pl.lit(config.missing_return))
        .otherwise(pl.col("return_1d"))
    )
    if config.drop_after_missing:
        applied = pl.when(pl.col("prior_missing") > 0).then(pl.lit(0.0)).otherwise(applied)
    return joined.with_columns((pl.col("weight") * applied).alias("contribution"))


def _with_missing_positions(ledger: pl.DataFrame, attributed: pl.DataFrame) -> pl.DataFrame:
    counts = (
        attributed.filter(pl.col("is_missing"))
        .group_by("entry_date")
        .agg(pl.col("security_id").n_unique().cast(pl.Int64).alias("missing_positions"))
    )
    return ledger.join(counts, on="entry_date", how="left").with_columns(
        pl.col("missing_positions").fill_null(0)
    )


def _trade_flows(previous: dict[str, float], current: dict[str, float]) -> tuple[float, float]:
    keys = set(previous) | set(current)
    buys = sum(max(current.get(key, 0.0) - previous.get(key, 0.0), 0.0) for key in keys)
    sells = sum(max(previous.get(key, 0.0) - current.get(key, 0.0), 0.0) for key in keys)
    return buys, sells


def _empty_positions() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [],
            "security_id": [],
            "weight": [],
            "signal_date": [],
            "entry_date": [],
        },
        schema={
            "date": pl.Date,
            "security_id": pl.String,
            "weight": pl.Float64,
            "signal_date": pl.Date,
            "entry_date": pl.Date,
        },
    )


def _empty_ledger() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "signal_date": [],
            "entry_date": [],
            "exit_date": [],
            "selected_count": [],
            "turnover": [],
            "entry_cost": [],
        },
        schema={
            "signal_date": pl.Date,
            "entry_date": pl.Date,
            "exit_date": pl.Date,
            "selected_count": pl.Int64,
            "turnover": pl.Float64,
            "entry_cost": pl.Float64,
        },
    )
