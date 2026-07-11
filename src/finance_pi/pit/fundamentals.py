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
    keeps the newest annual and newest interim report independently, selecting
    the newest fiscal period within each period-scope/account grain and breaking
    ties by availability, receipt date, and consolidation scope.
    """

    return f"""
    SELECT
        u.date AS as_of_date,
        f.*,
        CASE
            WHEN f.report_type IN ('11011', 'annual') THEN 'annual'
            ELSE 'interim'
        END AS period_scope
    FROM {calendar_view} AS u
    JOIN {financials_view} AS f
      ON f.security_id = u.security_id
     AND f.available_date < u.date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            u.date,
            u.security_id,
            CASE
                WHEN f.report_type IN ('11011', 'annual') THEN 'annual'
                ELSE 'interim'
            END,
            f.account_id
        ORDER BY
            f.fiscal_period_end DESC NULLS LAST,
            f.available_date DESC NULLS LAST,
            f.rcept_dt DESC NULLS LAST,
            f.is_consolidated DESC NULLS LAST
    ) = 1
    """
