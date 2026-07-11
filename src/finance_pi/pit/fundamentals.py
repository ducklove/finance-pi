from __future__ import annotations


def build_fundamentals_pit_sql(
    *,
    calendar_view: str = "gold.universe_history",
    financials_view: str = "silver.financials",
) -> str:
    """Return DuckDB SQL for point-in-time fundamentals.

    Mirrors ``transforms.builders.build_fundamentals_pit``: keeps only rows whose
    available date is strictly before the as-of date (next-trading-day
    availability, so intraday filings never inform same-day decisions), then
    keeps annual and interim report types independently, selecting the newest
    fiscal period within each report/statement/account grain and then breaking
    ties by availability, receipt date, and consolidation scope.
    """

    return f"""
    SELECT
        u.date AS as_of_date,
        f.*
    FROM {calendar_view} AS u
    JOIN {financials_view} AS f
      ON f.security_id = u.security_id
     AND f.available_date < u.date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            u.date,
            u.security_id,
            f.report_type,
            f.statement_division,
            f.account_id
        ORDER BY
            f.fiscal_period_end DESC NULLS LAST,
            f.available_date DESC NULLS LAST,
            f.rcept_dt DESC NULLS LAST,
            f.is_consolidated DESC NULLS LAST
    ) = 1
    """
