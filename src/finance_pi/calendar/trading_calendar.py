from __future__ import annotations

import warnings
from bisect import bisect_left, bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta

KRX_HOLIDAYS_BY_YEAR: dict[int, frozenset[date]] = {
    2025: frozenset(
        {
            date(2025, 1, 1),
            date(2025, 1, 27),
            date(2025, 1, 28),
            date(2025, 1, 29),
            date(2025, 1, 30),
            date(2025, 3, 3),
            date(2025, 3, 10),
            date(2025, 5, 1),
            date(2025, 5, 5),
            date(2025, 5, 6),
            date(2025, 6, 3),
            date(2025, 6, 6),
            date(2025, 8, 15),
            date(2025, 10, 3),
            date(2025, 10, 6),
            date(2025, 10, 7),
            date(2025, 10, 8),
            date(2025, 10, 9),
            date(2025, 12, 25),
            date(2025, 12, 31),
        }
    ),
    2026: frozenset(
        {
            date(2026, 1, 1),
            date(2026, 2, 16),
            date(2026, 2, 17),
            date(2026, 2, 18),
            date(2026, 3, 2),
            date(2026, 5, 1),
            date(2026, 5, 5),
            date(2026, 5, 25),
            date(2026, 6, 3),
            # 2026년부터 제헌절 휴장. KRX 공지: 20260520000197/32154.htm
            date(2026, 7, 17),
            date(2026, 8, 17),
            date(2026, 9, 24),
            date(2026, 9, 25),
            date(2026, 10, 5),
            date(2026, 10, 9),
            date(2026, 12, 25),
            date(2026, 12, 31),
        }
    ),
    # value-invest domain/market_calendar.py (krx-2026-2027-reviewed-20260916)와 동일.
    # 근거: 우주항공청 2026-06-29 월력요항 + KRX 휴장 규칙(근로자의 날·연말 휴장).
    # 주말에 걸친 공휴일도 원본 목록 그대로 둔다(주말은 어차피 비거래일).
    # 임시 휴장·수능일 개장시간 변경은 KRX 공지 확인 후 반영한다.
    2027: frozenset(
        {
            date(2027, 1, 1),
            date(2027, 2, 6),
            date(2027, 2, 7),
            date(2027, 2, 8),
            date(2027, 2, 9),
            date(2027, 3, 1),
            date(2027, 5, 1),
            date(2027, 5, 3),
            date(2027, 5, 5),
            date(2027, 5, 13),
            date(2027, 6, 6),
            date(2027, 7, 17),
            date(2027, 7, 19),
            date(2027, 8, 15),
            date(2027, 8, 16),
            date(2027, 9, 14),
            date(2027, 9, 15),
            date(2027, 9, 16),
            date(2027, 10, 3),
            date(2027, 10, 4),
            date(2027, 10, 9),
            date(2027, 10, 11),
            date(2027, 12, 25),
            date(2027, 12, 27),
            date(2027, 12, 31),
        }
    ),
}

# Backward-compatible flat alias (2026 only) for existing importers.
KRX_HOLIDAYS = KRX_HOLIDAYS_BY_YEAR[2026]

_uncovered_years_warned: set[int] = set()


def _holidays_for_year(year: int) -> frozenset[date]:
    holidays = KRX_HOLIDAYS_BY_YEAR.get(year)
    if holidays is None:
        if year not in _uncovered_years_warned:
            _uncovered_years_warned.add(year)
            warnings.warn(
                f"No KRX holiday calendar for {year}; falling back to weekday-only rule.",
                stacklevel=3,
            )
        return frozenset()
    return holidays


@dataclass(frozen=True)
class TradingCalendar:
    """Trading-day helper backed by an explicit date list."""

    dates: tuple[date, ...]

    @classmethod
    def from_dates(cls, dates: Iterable[date]) -> TradingCalendar:
        unique = tuple(sorted(set(dates)))
        if not unique:
            raise ValueError("calendar requires at least one trading date")
        return cls(unique)

    @classmethod
    def weekdays(cls, since: date, until: date) -> TradingCalendar:
        if until < since:
            raise ValueError("until must be on or after since")
        dates: list[date] = []
        current = since
        while current <= until:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        return cls.from_dates(dates)

    @classmethod
    def krx_trading_days(cls, since: date, until: date) -> TradingCalendar:
        if until < since:
            raise ValueError("until must be on or after since")
        dates: list[date] = []
        current = since
        while current <= until:
            if current.weekday() < 5 and current not in _holidays_for_year(current.year):
                dates.append(current)
            current += timedelta(days=1)
        return cls(tuple(dates))

    @classmethod
    def is_krx_trading_day(cls, value: date) -> bool:
        return value.weekday() < 5 and value not in _holidays_for_year(value.year)

    def contains(self, value: date) -> bool:
        index = bisect_left(self.dates, value)
        return index < len(self.dates) and self.dates[index] == value

    def previous(self, value: date, *, offset: int = 1) -> date:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        index = bisect_left(self.dates, value) - offset
        if index < 0:
            raise IndexError("calendar does not go back far enough")
        return self.dates[index]

    def next(self, value: date, *, offset: int = 1) -> date:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        index = bisect_right(self.dates, value) + offset - 1
        if index >= len(self.dates):
            raise IndexError("calendar does not go forward far enough")
        return self.dates[index]

    def between(self, since: date, until: date) -> tuple[date, ...]:
        start = bisect_left(self.dates, since)
        end = bisect_right(self.dates, until)
        return self.dates[start:end]

    def month_end_dates(self, since: date, until: date) -> tuple[date, ...]:
        dates = self.between(since, until)
        result: list[date] = []
        for index, current in enumerate(dates):
            is_last = index == len(dates) - 1
            if is_last or dates[index + 1].month != current.month:
                result.append(current)
        return tuple(result)
