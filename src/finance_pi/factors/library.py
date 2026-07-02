from __future__ import annotations

import polars as pl

from finance_pi.factors.base import Factor, factor_registry

# DART annual report code (사업보고서). Interim reports are excluded until TTM lands.
ANNUAL_REPORT_TYPE = "11011"

# Priority order: owners-of-parent first to avoid double counting with ProfitLoss.
NET_INCOME_ACCOUNTS = (
    "ifrs-full_ProfitLossAttributableToOwnersOfParent",
    "ifrs-full_ProfitLoss",
    "profit_loss",
    "net_income",
)
ASSET_ACCOUNTS = (
    "ifrs-full_Assets",
    "assets",
    "total_assets",
)


def _amount_by_priority(accounts: tuple[str, ...]) -> pl.Expr:
    """Aggregation expression: first non-null amount in account priority order."""
    return pl.coalesce(
        pl.col("amount").filter(pl.col("account_id") == account).first() for account in accounts
    )


@factor_registry.register("momentum_12_1")
class Momentum12_1(Factor):
    """12-month momentum excluding the most recent 1-month return."""

    requires = ["gold.daily_prices_adj"]
    rebalance = "monthly"

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        return (
            prices.sort(["security_id", "date"])
            .with_columns(
                pl.col("close_adj").pct_change(252).over("security_id").alias("ret_12m"),
                pl.col("close_adj").pct_change(21).over("security_id").alias("ret_1m"),
            )
            .with_columns(((1 + pl.col("ret_12m")) / (1 + pl.col("ret_1m")) - 1).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("value_earnings_yield")
class ValueEarningsYield(Factor):
    """Net income divided by market capitalization, PIT by construction."""

    requires = ["gold.fundamentals_pit", "gold.daily_prices_adj"]
    rebalance = "monthly"
    net_income_accounts = NET_INCOME_ACCOUNTS

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = (
            ctx.scan("gold.fundamentals_pit")
            .filter(
                (pl.col("report_type") == ANNUAL_REPORT_TYPE)
                & pl.col("account_id").is_in(self.net_income_accounts)
            )
            .group_by(["as_of_date", "security_id"])
            .agg(_amount_by_priority(self.net_income_accounts).alias("net_income"))
            .rename({"as_of_date": "date"})
        )
        prices = ctx.scan("gold.daily_prices_adj").select(["date", "security_id", "market_cap"])
        return (
            fundamentals.join(prices, on=["date", "security_id"], how="inner")
            .with_columns((pl.col("net_income") / pl.col("market_cap")).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("quality_roa")
class QualityRoa(Factor):
    """Return on assets from PIT fundamentals."""

    requires = ["gold.fundamentals_pit"]
    rebalance = "monthly"
    net_income_accounts = NET_INCOME_ACCOUNTS
    asset_accounts = ASSET_ACCOUNTS

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = ctx.scan("gold.fundamentals_pit").filter(
            pl.col("report_type") == ANNUAL_REPORT_TYPE
        )
        return (
            fundamentals.group_by(["as_of_date", "security_id"])
            .agg(
                _amount_by_priority(self.net_income_accounts).alias("net_income"),
                _amount_by_priority(self.asset_accounts).alias("assets"),
            )
            .with_columns((pl.col("net_income") / pl.col("assets")).alias("score"))
            .rename({"as_of_date": "date"})
            .select(["date", "security_id", "score"])
        )
