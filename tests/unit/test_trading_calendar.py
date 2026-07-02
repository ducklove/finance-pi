from __future__ import annotations

from datetime import date

import pytest

from finance_pi.calendar.trading_calendar import (
    TradingCalendar,
    _uncovered_years_warned,
)


def test_is_krx_trading_day_holiday_2026() -> None:
    assert not TradingCalendar.is_krx_trading_day(date(2026, 1, 1))


def test_is_krx_trading_day_weekday_2026() -> None:
    # 2026-01-02 is a Friday and not a KRX holiday.
    assert TradingCalendar.is_krx_trading_day(date(2026, 1, 2))


def test_is_krx_trading_day_weekend_2026() -> None:
    # 2026-01-03 is a Saturday.
    assert not TradingCalendar.is_krx_trading_day(date(2026, 1, 3))


def test_uncovered_year_falls_back_to_weekday_rule_and_warns_once() -> None:
    import warnings

    _uncovered_years_warned.discard(2099)
    try:
        with pytest.warns(UserWarning, match="2099"):
            # Wednesday, not a holiday under the weekday-only fallback.
            assert TradingCalendar.is_krx_trading_day(date(2099, 1, 7))

        # Second call for the same uncovered year must not warn again.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert TradingCalendar.is_krx_trading_day(date(2099, 1, 8))
        assert not caught
    finally:
        _uncovered_years_warned.discard(2099)


def test_krx_trading_days_excludes_2026_holidays() -> None:
    calendar = TradingCalendar.krx_trading_days(date(2026, 1, 1), date(2026, 1, 2))
    assert calendar.dates == (date(2026, 1, 2),)


def test_month_end_dates() -> None:
    calendar = TradingCalendar.weekdays(date(2024, 1, 29), date(2024, 3, 1))
    ends = calendar.month_end_dates(date(2024, 1, 29), date(2024, 3, 1))
    assert ends == (date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 1))


def test_previous_and_next_with_offset() -> None:
    calendar = TradingCalendar.from_dates(
        [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
    )
    assert calendar.previous(date(2024, 1, 5), offset=1) == date(2024, 1, 4)
    assert calendar.previous(date(2024, 1, 5), offset=0) == date(2024, 1, 5)
    assert calendar.next(date(2024, 1, 2), offset=1) == date(2024, 1, 3)
    assert calendar.next(date(2024, 1, 2), offset=0) == date(2024, 1, 2)

    with pytest.raises(IndexError):
        calendar.previous(date(2024, 1, 2), offset=5)
    with pytest.raises(IndexError):
        calendar.next(date(2024, 1, 5), offset=5)


def test_from_dates_deduplicates_and_sorts() -> None:
    calendar = TradingCalendar.from_dates(
        [date(2024, 1, 3), date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 2)]
    )
    assert calendar.dates == (date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3))


def test_from_dates_rejects_empty() -> None:
    with pytest.raises(ValueError, match="at least one"):
        TradingCalendar.from_dates([])
