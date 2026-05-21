from __future__ import annotations

import argparse
from datetime import date
from glob import glob
from pathlib import Path

import polars as pl

from finance_pi.transforms import build_daily_prices_adj


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild gold.daily_prices_adj for repaired silver price dates."
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--dates-csv",
        type=Path,
        default=Path("data/reports/price_adjustment_audit/repaired_silver_dates.csv"),
    )
    args = parser.parse_args()

    target_dates = target_rebuild_dates(args.data_root, args.dates_csv)
    rows = 0
    files = 0
    for logical_date in target_dates:
        summary = build_daily_prices_adj(args.data_root, dates={logical_date})[0]
        rows += summary.rows
        files += summary.files
    print(f"rebuild_dates={len(target_dates)}")
    print(f"rows={rows}")
    print(f"files={files}")


def target_rebuild_dates(data_root: Path, dates_csv: Path) -> list[date]:
    if not dates_csv.exists():
        raise FileNotFoundError(dates_csv)
    changed_dates = {
        value
        for value in pl.read_csv(dates_csv, try_parse_dates=True).get_column("date").to_list()
        if isinstance(value, date)
    }
    silver_dates = _partition_dates(data_root / "silver/prices/dt=*/part.parquet")
    if not changed_dates or not silver_dates:
        return []
    silver_dates_sorted = sorted(silver_dates)
    targets = set(changed_dates)
    for changed in changed_dates:
        for logical_date in silver_dates_sorted:
            if logical_date > changed:
                targets.add(logical_date)
                break
    return sorted(targets)


def _partition_dates(pattern: Path) -> set[date]:
    values: set[date] = set()
    for name in glob(pattern.as_posix()):
        path = Path(name)
        for part in path.parts:
            if part.startswith("dt="):
                values.add(date.fromisoformat(part.removeprefix("dt=")))
                break
    return values


if __name__ == "__main__":
    main()
