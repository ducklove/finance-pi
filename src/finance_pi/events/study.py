"""Market-adjusted event study over gold.filing_events and gold.daily_prices_adj.

Event-day convention (PIT): event day 0 is the FIRST trading day strictly
after ``available_date`` — the same strict ``available_date < as_of``
visibility rule used by gold.fundamentals_pit. A filing received (and made
public) during day T has unknown intraday timing, so the first day the whole
market could trade on the news close-to-close is the first trading day D with
``D > available_date``. Relative days count along the trading-day axis of
gold.daily_prices_adj (rel_day -1 is one trading day before day 0).

Abnormal return: ``AR = return_1d - market_return`` where ``market_return``
is the equal-weighted mean ``return_1d`` across every security with a
non-null return that day (the active gold.daily_prices_adj universe). This is
self-contained — no index dependency, no beta estimation — at the cost of a
small-cap tilt: the equal weighting behaves like a broad small-cap benchmark,
so ARs of large caps embed some size effect. Good enough for ranking event
types; refine with an index/beta model if precision matters later.

Missing data: a window observation is dropped (not zero-filled) when either
the security return or the market mean is unavailable that day; per-event CAR
is the cumulative sum over the surviving observations. An event is dropped
entirely when fewer than ``MIN_WINDOW_COVERAGE`` of its window days survive.
"""

from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from glob import glob
from html import escape
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Minimum share of the (window_pre + 1 + window_post) days that must have a
# usable abnormal return; events below this (including events cut short by
# the edge of price history) are dropped rather than biasing the means.
MIN_WINDOW_COVERAGE = 0.7


@dataclass(frozen=True)
class EventStudyResult:
    event_type: str
    start: date
    end: date
    window_pre: int
    window_post: int
    # One row per relative day: rel_day, mean_ar, mean_car, median_car, n.
    by_rel_day: pl.DataFrame
    # n_events, car_pre [-pre..-1], ar_day0, car_post [+1..+post], car_full,
    # hit_rate = share of events with positive CAR over [+1..+post].
    summary: dict[str, float | int]

    def to_html(self) -> str:
        summary_rows = "\n".join(
            f"<tr><td>{escape(name)}</td><td>{value}</td></tr>"
            for name, value in self._formatted_summary()
        )
        day_rows = "\n".join(
            "<tr>"
            f"<td>{row['rel_day']:+d}</td>"
            f"<td>{row['mean_ar']:+.4%}</td>"
            f"<td>{row['mean_car']:+.4%}</td>"
            f"<td>{row['median_car']:+.4%}</td>"
            f"<td>{row['n']}</td>"
            "</tr>"
            for row in self.by_rel_day.iter_rows(named=True)
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>finance-pi event study {escape(self.event_type)}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #1f2937; }}
    table {{ border-collapse: collapse; margin-bottom: 24px; }}
    th, td {{ border-bottom: 1px solid #d1d5db; padding: 8px 10px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
  </style>
</head>
<body>
  <h1>Event Study: {escape(self.event_type)}</h1>
  <p>
    Events {self.start.isoformat()}..{self.end.isoformat()},
    window [-{self.window_pre}..+{self.window_post}] trading days around day 0
    (first trading day strictly after available_date).
    AR = return_1d minus the equal-weighted market mean return.
  </p>
  <h2>Summary</h2>
  <table>
    <thead><tr><th>Metric</th><th>Value</th></tr></thead>
    <tbody>{summary_rows}</tbody>
  </table>
  <h2>By relative day</h2>
  <table>
    <thead>
      <tr><th>rel_day</th><th>mean AR</th><th>mean CAR</th><th>median CAR</th><th>n</th></tr>
    </thead>
    <tbody>{day_rows}</tbody>
  </table>
</body>
</html>
"""

    def _formatted_summary(self) -> list[tuple[str, str]]:
        formatted: list[tuple[str, str]] = [("n_events", str(self.summary["n_events"]))]
        for name in ("car_pre", "ar_day0", "car_post", "car_full"):
            formatted.append((name, f"{self.summary[name]:+.4%}"))
        formatted.append(("hit_rate", f"{self.summary['hit_rate']:.1%}"))
        return formatted

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_html(), encoding="utf-8")
        return path


def run_event_study(
    data_root: Path,
    event_type: str,
    window_pre: int = 5,
    window_post: int = 20,
    start: date | None = None,
    end: date | None = None,
    min_events: int = 10,
) -> EventStudyResult:
    """Run a market-adjusted event study for one gold.filing_events type.

    ``start``/``end`` bound the EVENT dates (rcept_dt), not the price window;
    price data outside the bounds is still read for window edges. Raises
    ``ValueError`` when fewer than ``min_events`` usable events survive the
    day-0 alignment and window-coverage filters.
    """
    events = _read_events(data_root, event_type, start, end)
    if events.is_empty():
        raise ValueError(
            f"no {event_type!r} events in gold.filing_events for the requested period"
        )
    axis = _trading_day_axis(data_root)
    if not axis:
        raise ValueError("gold.daily_prices_adj has no partitions; build prices first")

    windows = _event_windows(events, axis, window_pre, window_post)
    observed = _observed_abnormal_returns(data_root, windows)
    window_length = window_pre + window_post + 1
    kept = (
        observed.group_by("event_id")
        .agg(pl.len().alias("_observed_days"))
        .filter(pl.col("_observed_days") >= MIN_WINDOW_COVERAGE * window_length)
        .select("event_id")
    )
    n_events = kept.height
    if n_events < min_events:
        raise ValueError(
            f"only {n_events} usable {event_type!r} events "
            f"(>= {MIN_WINDOW_COVERAGE:.0%} window coverage); need at least {min_events}"
        )
    observed = (
        observed.join(kept, on="event_id", how="semi")
        .sort(["event_id", "rel_day"])
        .with_columns(pl.col("abnormal_return").cum_sum().over("event_id").alias("car"))
    )
    by_rel_day = (
        observed.group_by("rel_day")
        .agg(
            pl.col("abnormal_return").mean().alias("mean_ar"),
            pl.col("car").mean().alias("mean_car"),
            pl.col("car").median().alias("median_car"),
            pl.len().cast(pl.Int64).alias("n"),
        )
        .sort("rel_day")
    )
    summary = _summarize(observed, n_events)
    event_dates = events["event_date"]
    return EventStudyResult(
        event_type=event_type,
        start=start or event_dates.min(),
        end=end or event_dates.max(),
        window_pre=window_pre,
        window_post=window_post,
        by_rel_day=by_rel_day,
        summary=summary,
    )


def _summarize(observed: pl.DataFrame, n_events: int) -> dict[str, float | int]:
    """Per-event CAR decomposition: car_full = car_pre + ar_day0 + car_post.

    ``car_pre`` averages CAR over [-window_pre..-1] (pre-announcement run-up /
    leakage), ``ar_day0`` is the mean day-0 reaction, and ``car_post`` averages
    CAR over [+1..+window_post] — the post-reaction drift a next-day trader
    could actually capture, matching the hit_rate definition. Ranges with no
    surviving observations contribute 0 to that event's component.
    """
    per_event = observed.group_by("event_id").agg(
        pl.col("abnormal_return").filter(pl.col("rel_day") < 0).sum().alias("car_pre"),
        pl.col("abnormal_return").filter(pl.col("rel_day") == 0).sum().alias("ar_day0"),
        pl.col("abnormal_return").filter(pl.col("rel_day") > 0).sum().alias("car_post"),
        pl.col("abnormal_return").sum().alias("car_full"),
    )
    return {
        "n_events": n_events,
        "car_pre": float(per_event["car_pre"].mean()),
        "ar_day0": float(per_event["ar_day0"].mean()),
        "car_post": float(per_event["car_post"].mean()),
        "car_full": float(per_event["car_full"].mean()),
        "hit_rate": float((per_event["car_post"] > 0).mean()),
    }


def _read_events(
    data_root: Path,
    event_type: str,
    start: date | None,
    end: date | None,
) -> pl.DataFrame:
    files = _partition_files(
        data_root / "gold/filing_events/dt=*/part.parquet", start=start, end=end
    )
    if not files:
        return pl.DataFrame(
            schema={"event_date": pl.Date, "available_date": pl.Date, "security_id": pl.String}
        )
    events = (
        pl.scan_parquet(files, hive_partitioning=False)
        .select("event_date", "available_date", "security_id", "event_type")
        .filter((pl.col("event_type") == event_type) & pl.col("security_id").is_not_null())
        .collect()
        .drop("event_type")
    )
    if start is not None:
        events = events.filter(pl.col("event_date") >= start)
    if end is not None:
        events = events.filter(pl.col("event_date") <= end)
    # Same-day repeats (e.g. two tranches filed together) share one market
    # reaction; collapse them so the episode is not double-weighted.
    return events.sort(["security_id", "available_date", "event_date"]).unique(
        subset=["security_id", "available_date"], keep="first"
    )


def _trading_day_axis(data_root: Path) -> list[date]:
    """Trading days from gold.daily_prices_adj partition names (no data read)."""
    days: set[date] = set()
    for file in glob((data_root / "gold/daily_prices_adj/dt=*/part.parquet").as_posix()):
        value = _partition_date(Path(file))
        if value is not None:
            days.add(value)
    return sorted(days)


def _event_windows(
    events: pl.DataFrame,
    axis: list[date],
    window_pre: int,
    window_post: int,
) -> pl.DataFrame:
    """One row per (event, rel_day) with the trading date of that rel_day.

    Day 0 = first axis date strictly after available_date (``bisect_right``).
    Events whose day 0 falls past the end of the axis are dropped (nothing to
    measure yet); window days beyond either axis edge are simply absent and
    count against the event's window coverage.
    """
    anchors: list[tuple[int, str, int]] = []
    for event_id, row in enumerate(events.iter_rows(named=True)):
        day0_index = bisect_right(axis, row["available_date"])
        if day0_index >= len(axis):
            continue
        anchors.append((event_id, row["security_id"], day0_index))
    if not anchors:
        return pl.DataFrame(
            schema={
                "event_id": pl.Int64,
                "security_id": pl.String,
                "rel_day": pl.Int64,
                "date": pl.Date,
            }
        )
    axis_frame = pl.DataFrame(
        {"_axis_index": list(range(len(axis))), "date": axis},
        schema={"_axis_index": pl.Int64, "date": pl.Date},
    )
    return (
        pl.DataFrame(
            {
                "event_id": [anchor[0] for anchor in anchors],
                "security_id": [anchor[1] for anchor in anchors],
                "_day0_index": [anchor[2] for anchor in anchors],
            },
            schema={"event_id": pl.Int64, "security_id": pl.String, "_day0_index": pl.Int64},
        )
        .join(
            pl.DataFrame(
                {"rel_day": list(range(-window_pre, window_post + 1))},
                schema={"rel_day": pl.Int64},
            ),
            how="cross",
        )
        .with_columns((pl.col("_day0_index") + pl.col("rel_day")).alias("_axis_index"))
        .filter((pl.col("_axis_index") >= 0) & (pl.col("_axis_index") < len(axis)))
        .join(axis_frame, on="_axis_index", how="inner")
        .select("event_id", "security_id", "rel_day", "date")
    )


def _observed_abnormal_returns(data_root: Path, windows: pl.DataFrame) -> pl.DataFrame:
    """Join window days with security and market returns; drop unusable days.

    Memory: only price partitions inside the study span are scanned, and only
    (date, security_id, return_1d) columns are decoded — the market mean needs
    every security per day, but security-level rows are pruned to the event
    securities before collection.
    """
    empty = pl.DataFrame(
        schema={
            "event_id": pl.Int64,
            "security_id": pl.String,
            "rel_day": pl.Int64,
            "date": pl.Date,
            "abnormal_return": pl.Float64,
        }
    )
    if windows.is_empty():
        return empty
    files = _partition_files(
        data_root / "gold/daily_prices_adj/dt=*/part.parquet",
        start=windows["date"].min(),
        end=windows["date"].max(),
    )
    if not files:
        return empty
    scan = pl.scan_parquet(files, hive_partitioning=False)
    market = (
        scan.select("date", "return_1d")
        .drop_nulls()
        .group_by("date")
        .agg(pl.col("return_1d").mean().alias("market_return"))
        .collect()
    )
    security_ids = windows["security_id"].unique().to_list()
    returns = (
        scan.select("date", "security_id", "return_1d")
        .filter(pl.col("security_id").is_in(security_ids))
        .collect()
    )
    return (
        windows.join(returns, on=["date", "security_id"], how="left")
        .join(market, on="date", how="left")
        .with_columns((pl.col("return_1d") - pl.col("market_return")).alias("abnormal_return"))
        .filter(pl.col("abnormal_return").is_not_null())
        .select("event_id", "security_id", "rel_day", "date", "abnormal_return")
    )


def _partition_files(
    pattern: Path,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """dt=-partitioned files under ``pattern`` whose date is within [start, end]."""
    selected: list[str] = []
    for file in sorted(glob(pattern.as_posix())):
        value = _partition_date(Path(file))
        if value is None:
            continue
        if start is not None and value < start:
            continue
        if end is not None and value > end:
            continue
        selected.append(file)
    return selected


def _partition_date(path: Path) -> date | None:
    for part in path.parts:
        if part.startswith("dt="):
            try:
                return date.fromisoformat(part.removeprefix("dt="))
            except ValueError:
                return None
    return None
