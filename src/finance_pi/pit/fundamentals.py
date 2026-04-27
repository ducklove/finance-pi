from __future__ import annotations


def build_fundamentals_pit_sql(
    *,
    calendar_view: str = "gold.universe_history",
    financials_view: str = "silver.financials",
) -> str:
    """Return DuckDB SQL for point-in-time fundamentals.

    The query keeps only rows whose available date is no later than the as-of
    date, then chooses the latest correction known at that point.
    """

    return f"""
    SELECT
        u.date AS as_of_date,
        u.security_id,
        f.fiscal_period_end,
        f.event_date,
        f.rcept_dt,
        f.available_date,
        f.report_type,
        f.account_id,
        f.account_name,
        f.amount,
        f.is_consolidated,
        f.accounting_basis
    FROM {calendar_view} AS u
    JOIN {financials_view} AS f
      ON f.security_id = u.security_id
     AND f.available_date <= u.date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY u.date, u.security_id, f.account_id, f.fiscal_period_end
        ORDER BY f.available_date DESC, f.rcept_dt DESC
    ) = 1
    """
