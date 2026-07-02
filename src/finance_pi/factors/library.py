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
EQUITY_ACCOUNTS = (
    "ifrs-full_EquityAttributableToOwnersOfParent",
    "ifrs-full_Equity",
    "equity",
    "total_equity",
)
GROSS_PROFIT_ACCOUNTS = (
    "ifrs-full_GrossProfit",
    "gross_profit",
)
REVENUE_ACCOUNTS = (
    "ifrs-full_Revenue",
    "revenue",
)
COST_OF_SALES_ACCOUNTS = (
    "ifrs-full_CostOfSales",
    "cost_of_sales",
)
OPERATING_CASH_FLOW_ACCOUNTS = (
    "ifrs-full_CashFlowsFromUsedInOperatingActivities",
    "operating_cash_flow",
)


def _amount_by_priority(accounts: tuple[str, ...]) -> pl.Expr:
    """Aggregation expression: first non-null amount in account priority order."""
    return pl.coalesce(
        pl.col("amount").filter(pl.col("account_id") == account).first() for account in accounts
    )


def _annual_fundamentals(ctx, metrics: dict[str, tuple[str, ...]]) -> pl.LazyFrame:
    """One row per (date, security_id) with a column per metric, annual PIT rows only."""
    all_accounts = [account for accounts in metrics.values() for account in accounts]
    return (
        ctx.scan("gold.fundamentals_pit")
        .filter(
            (pl.col("report_type") == ANNUAL_REPORT_TYPE) & pl.col("account_id").is_in(all_accounts)
        )
        .group_by(["as_of_date", "security_id"])
        .agg(_amount_by_priority(accounts).alias(name) for name, accounts in metrics.items())
        .rename({"as_of_date": "date"})
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


@factor_registry.register("value_book_to_price")
class ValueBookToPrice(Factor):
    """Book equity over market cap: high book-to-price earns the value premium (Fama-French)."""

    requires = ["gold.fundamentals_pit", "gold.daily_prices_adj"]
    rebalance = "monthly"
    equity_accounts = EQUITY_ACCOUNTS

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = _annual_fundamentals(ctx, {"equity": self.equity_accounts})
        prices = ctx.scan("gold.daily_prices_adj").select(["date", "security_id", "market_cap"])
        return (
            fundamentals.join(prices, on=["date", "security_id"], how="inner")
            .with_columns((pl.col("equity") / pl.col("market_cap")).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("value_dividend_yield")
class ValueDividendYield(Factor):
    """Trailing annual cash dividend per share over price: yield proxies the value premium.

    PIT policy mirrors gold.fundamentals_pit: a dividend filing becomes visible
    strictly after its ``available_date``. Per-share yield is computed as
    ``dps * listed_shares / market_cap`` (== dps / close) so only gold price
    columns are needed. A declaration older than ``max_staleness`` no longer
    scores — unlike financials, a firm that stops paying simply files no new
    per-share row, so carrying the stale amount forward would fake a yield.
    """

    requires = ["silver.dividends", "gold.daily_prices_adj"]
    rebalance = "monthly"
    max_staleness = "430d"

    def compute(self, ctx) -> pl.LazyFrame:
        dividends = (
            ctx.scan("silver.dividends")
            .filter(
                (pl.col("report_type") == ANNUAL_REPORT_TYPE)
                & pl.col("security_id").is_not_null()
                & pl.col("available_date").is_not_null()
                & pl.col("cash_dividend_per_share").is_not_null()
            )
            .with_columns(pl.col("available_date").dt.offset_by("1d").alias("effective_date"))
            .group_by(["security_id", "effective_date"])
            .agg(
                pl.col("cash_dividend_per_share")
                .sort_by("fiscal_period_end")
                .last()
                .alias("dividend_per_share")
            )
            .sort(["security_id", "effective_date"])
        )
        prices = (
            ctx.scan("gold.daily_prices_adj")
            .select(["date", "security_id", "market_cap", "listed_shares"])
            .sort(["security_id", "date"])
        )
        return (
            prices.join_asof(
                dividends,
                left_on="date",
                right_on="effective_date",
                by="security_id",
                strategy="backward",
                tolerance=self.max_staleness,
                # Both sides are sorted just above; skip the per-group check warning.
                check_sortedness=False,
            )
            .with_columns(
                (
                    pl.col("dividend_per_share") * pl.col("listed_shares") / pl.col("market_cap")
                ).alias("score")
            )
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("quality_gpa")
class QualityGpa(Factor):
    """Gross profitability (gross profit / assets): Novy-Marx's quality premium."""

    requires = ["gold.fundamentals_pit"]
    rebalance = "monthly"
    gross_profit_accounts = GROSS_PROFIT_ACCOUNTS
    revenue_accounts = REVENUE_ACCOUNTS
    cost_of_sales_accounts = COST_OF_SALES_ACCOUNTS
    asset_accounts = ASSET_ACCOUNTS

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = _annual_fundamentals(
            ctx,
            {
                "gross_profit": self.gross_profit_accounts,
                "revenue": self.revenue_accounts,
                "cost_of_sales": self.cost_of_sales_accounts,
                "assets": self.asset_accounts,
            },
        )
        gross_profit = pl.coalesce(
            pl.col("gross_profit"), pl.col("revenue") - pl.col("cost_of_sales")
        )
        return fundamentals.with_columns((gross_profit / pl.col("assets")).alias("score")).select(
            ["date", "security_id", "score"]
        )


@factor_registry.register("quality_accruals")
class QualityAccruals(Factor):
    """Accruals ((net income - operating cash flow) / assets): high accruals predict low returns.

    Sloan (1996); lower is better, hence ``direction = -1``. Cash-flow accounts
    come from fnlttSinglAcntAll ingestion; when a filing lacks them the score
    is null and the security is simply not ranked.
    """

    requires = ["gold.fundamentals_pit"]
    rebalance = "monthly"
    direction = -1
    net_income_accounts = NET_INCOME_ACCOUNTS
    operating_cash_flow_accounts = OPERATING_CASH_FLOW_ACCOUNTS
    asset_accounts = ASSET_ACCOUNTS

    def compute(self, ctx) -> pl.LazyFrame:
        fundamentals = _annual_fundamentals(
            ctx,
            {
                "net_income": self.net_income_accounts,
                "operating_cash_flow": self.operating_cash_flow_accounts,
                "assets": self.asset_accounts,
            },
        )
        accruals = (pl.col("net_income") - pl.col("operating_cash_flow")) / pl.col("assets")
        return fundamentals.with_columns(accruals.alias("score")).select(
            ["date", "security_id", "score"]
        )


@factor_registry.register("lowvol_252")
class LowVol252(Factor):
    """252-day daily-return volatility: low-volatility stocks earn higher risk-adjusted returns."""

    requires = ["gold.daily_prices_adj"]
    rebalance = "monthly"
    direction = -1
    window = 252
    min_samples = 200

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        return (
            prices.sort(["security_id", "date"])
            .with_columns(
                pl.col("return_1d")
                .rolling_std(window_size=self.window, min_samples=self.min_samples)
                .over("security_id")
                .alias("score")
            )
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("reversal_1m")
class Reversal1M(Factor):
    """21-day return: short-term losers outperform winners (Jegadeesh short-term reversal)."""

    requires = ["gold.daily_prices_adj"]
    rebalance = "monthly"
    direction = -1

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        return (
            prices.sort(["security_id", "date"])
            .with_columns(pl.col("close_adj").pct_change(21).over("security_id").alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("momentum_52w_high")
class Momentum52WeekHigh(Factor):
    """Close over rolling 252-day high: nearness to the 52-week high predicts continuation.

    George & Hwang (2004); anchoring keeps prices near the high underreacting.
    """

    requires = ["gold.daily_prices_adj"]
    rebalance = "monthly"
    window = 252
    min_samples = 200

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        rolling_high = (
            pl.col("close_adj")
            .rolling_max(window_size=self.window, min_samples=self.min_samples)
            .over("security_id")
        )
        return (
            prices.sort(["security_id", "date"])
            .with_columns((pl.col("close_adj") / rolling_high).alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("nps_flow")
class NpsFlow(Factor):
    """NPS holdings-change flow: the pension fund's accumulation is Korean smart money.

    Score is the latest ``delta_ratio`` (ownership_pct change between
    consecutive disclosure snapshots, including enter/exit rows) from
    gold.nps_holdings_delta, broadcast forward per security until the next
    delta so monthly rebalances see the latest known flow. PIT mirrors
    gold.fundamentals_pit: a delta becomes usable strictly AFTER its
    ``available_date`` (effective from the next day). A delta older than
    ``max_staleness`` no longer scores — an old disclosure change carries no
    flow information.
    """

    requires = ["gold.nps_holdings_delta", "gold.daily_prices_adj"]
    rebalance = "monthly"
    direction = 1
    max_staleness = "370d"

    def compute(self, ctx) -> pl.LazyFrame:
        deltas = (
            ctx.scan("gold.nps_holdings_delta")
            .filter(
                pl.col("security_id").is_not_null()
                & pl.col("available_date").is_not_null()
                & pl.col("delta_ratio").is_not_null()
            )
            .with_columns(pl.col("available_date").dt.offset_by("1d").alias("effective_date"))
            .group_by(["security_id", "effective_date"])
            .agg(pl.col("delta_ratio").sort_by("snapshot_date").last().alias("delta_ratio"))
            .sort(["security_id", "effective_date"])
        )
        prices = (
            ctx.scan("gold.daily_prices_adj")
            .select(["date", "security_id"])
            .sort(["security_id", "date"])
        )
        return (
            prices.join_asof(
                deltas,
                left_on="date",
                right_on="effective_date",
                by="security_id",
                strategy="backward",
                tolerance=self.max_staleness,
                # Both sides are sorted just above; skip the per-group check warning.
                check_sortedness=False,
            )
            .with_columns(pl.col("delta_ratio").alias("score"))
            .select(["date", "security_id", "score"])
        )


@factor_registry.register("preferred_discount_z")
class PreferredDiscountZ(Factor):
    """Preferred-share discount z-score; higher = preferred unusually cheap vs common.

    Scores map to the preferred security_id, so backtests need
    ``BacktestConfig(share_classes=("preferred",))`` (or common+preferred).
    """

    requires = ["gold.preferred_discount"]
    rebalance = "monthly"

    def compute(self, ctx) -> pl.LazyFrame:
        return (
            ctx.scan("gold.preferred_discount")
            .filter(pl.col("discount_z").is_not_null())
            .select(
                "date",
                pl.col("preferred_security_id").alias("security_id"),
                pl.col("discount_z").alias("score"),
            )
        )
