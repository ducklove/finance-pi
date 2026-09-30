from __future__ import annotations

from datetime import date, timedelta

import pytest

from finance_pi.calendar.trading_calendar import (
    KRX_HOLIDAYS_BY_YEAR,
    TradingCalendar,
    _uncovered_years_warned,
)

# value-invest domain/market_calendar.py HOLIDAYS[2027] (krx-2026-2027-reviewed-20260916).
HUB_REVIEWED_2027 = [
    "01-01", "02-06", "02-07", "02-08", "02-09", "03-01", "05-01", "05-03", "05-05",
    "05-13", "06-06", "07-17", "07-19", "08-15", "08-16", "09-14", "09-15", "09-16",
    "10-03", "10-04", "10-09", "10-11", "12-25", "12-27", "12-31",
]


def test_is_krx_trading_day_holiday_2026() -> None:
    assert not TradingCalendar.is_krx_trading_day(date(2026, 1, 1))


def test_constitution_day_2026_is_closed() -> None:
    assert not TradingCalendar.is_krx_trading_day(date(2026, 7, 17))
    assert TradingCalendar.krx_trading_days(date(2026, 7, 16), date(2026, 7, 20)).dates == (
        date(2026, 7, 16),
        date(2026, 7, 20),
    )


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


def test_holiday_calendar_covers_next_60_days() -> None:
    # Early warning: fails ~2 months before the calendar runs out, so the next
    # year's reviewed KRX holidays get added before the daily job treats them as
    # trading days (the 2026-09 stall).
    horizon = date.today() + timedelta(days=60)
    missing = [
        year
        for year in range(date.today().year, horizon.year + 1)
        if year not in KRX_HOLIDAYS_BY_YEAR
    ]
    assert not missing, (
        f"KRX holiday data missing for {missing}; add the reviewed KRX holidays to "
        "finance_pi/calendar/trading_calendar.py (and value-invest domain/market_calendar.py)"
    )


def test_2027_holidays_match_hub_reviewed_list() -> None:
    assert sorted(d.strftime("%m-%d") for d in KRX_HOLIDAYS_BY_YEAR[2027]) == sorted(
        HUB_REVIEWED_2027
    )


def test_2027_holidays_are_closed() -> None:
    # Seollal (Feb 8-9), substitute Children's Day (May 3), Chuseok (Sep 14-16),
    # substitute Hangul Day (Oct 11) and the year-end close.
    for closed in (
        date(2027, 2, 8),
        date(2027, 2, 9),
        date(2027, 5, 3),
        date(2027, 9, 14),
        date(2027, 9, 15),
        date(2027, 9, 16),
        date(2027, 10, 11),
        date(2027, 12, 31),
    ):
        assert not TradingCalendar.is_krx_trading_day(closed), closed
    assert TradingCalendar.krx_trading_days(date(2027, 9, 13), date(2027, 9, 17)).dates == (
        date(2027, 9, 13),
        date(2027, 9, 17),
    )
