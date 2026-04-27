from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta


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
