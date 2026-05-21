from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from finance_pi.transforms.builders import (
    _deduplicate_price_candidates,
    _normalize_price_frame,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair silver.prices rows polluted by duplicate Naver adjusted/raw candidates."
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--audit-csv",
        type=Path,
        default=Path("data/reports/price_adjustment_audit/suspicious_price_jumps.csv"),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    summary = repair_naver_duplicate_prices(args.data_root, args.audit_csv, dry_run=args.dry_run)
    print(f"target_tickers={summary['target_tickers']}")
    print(f"duplicate_keys={summary['duplicate_keys']}")
    print(f"silver_rows_changed={summary['silver_rows_changed']}")
    print(f"silver_partitions_written={summary['silver_partitions_written']}")


def repair_naver_duplicate_prices(
    data_root: Path,
    audit_csv: Path,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    if not audit_csv.exists():
        raise FileNotFoundError(audit_csv)

    audit = pl.read_csv(audit_csv).with_columns(pl.col("ticker").cast(pl.String).str.zfill(6))
    target_tickers = audit.select("ticker").unique().to_series().to_list()
    if not target_tickers:
        return {
            "target_tickers": 0,
            "duplicate_keys": 0,
            "silver_rows_changed": 0,
            "silver_partitions_written": 0,
        }

    naver_glob = data_root / "bronze/naver_daily/request_dt=*/chunk=*/part.parquet"
    naver = (
        pl.scan_parquet(naver_glob.as_posix(), hive_partitioning=True)
        .with_columns(pl.col("ticker").cast(pl.String).str.zfill(6).alias("ticker"))
        .filter(pl.col("ticker").is_in(target_tickers))
        .collect()
    )
    if naver.is_empty():
        return {
            "target_tickers": len(target_tickers),
            "duplicate_keys": 0,
            "silver_rows_changed": 0,
            "silver_partitions_written": 0,
        }

    normalized = _normalize_price_frame(naver, "naver")
    duplicate_keys = (
        normalized.group_by(["date", "ticker"])
        .agg(pl.len().alias("candidate_count"))
        .filter(pl.col("candidate_count") > 1)
        .select("date", "ticker")
    )
    if duplicate_keys.is_empty():
        return {
            "target_tickers": len(target_tickers),
            "duplicate_keys": 0,
            "silver_rows_changed": 0,
            "silver_partitions_written": 0,
        }

    duplicate_markers = duplicate_keys.with_columns(pl.lit(True).alias("_naver_duplicate_key"))
    selected = (
        _deduplicate_price_candidates(normalized)
        .join(duplicate_markers, on=["date", "ticker"], how="left")
        .with_columns(pl.col("_naver_duplicate_key").fill_null(False))
        .select(
            "date",
            "security_id",
            "listing_id",
            "ticker",
            "name",
            "market",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trading_value",
            "market_cap",
            "listed_shares",
            "price_source",
            "is_halted",
            "is_designated",
            "is_liquidation_window",
            "_naver_duplicate_key",
        )
    )

    by_date: dict[date, pl.DataFrame] = {}
    for logical_date, rows in selected.partition_by("date", as_dict=True).items():
        key = logical_date[0] if isinstance(logical_date, tuple) else logical_date
        by_date[key] = rows

    changed_rows = 0
    written = 0
    changed_dates: set[date] = set()
    for logical_date, replacements in sorted(by_date.items()):
        path = data_root / "silver" / "prices" / f"dt={logical_date.isoformat()}" / "part.parquet"
        if not path.exists():
            continue
        current = pl.read_parquet(path, hive_partitioning=True)
        if "dt" in current.columns:
            current = current.drop("dt")
        current_targets = current.join(
            replacements.select("date", "ticker"),
            on=["date", "ticker"],
            how="semi",
        )
        if current_targets.is_empty():
            continue
        changed = current_targets.join(
            replacements.select(
                "date",
                "ticker",
                pl.col("close").alias("_new_close"),
                "_naver_duplicate_key",
            ),
            on=["date", "ticker"],
            how="left",
        ).filter(
            (pl.col("close") != pl.col("_new_close"))
            & (
                pl.col("_naver_duplicate_key")
                | (
                    (pl.col("price_source") == "kis")
                    & (pl.col("volume").fill_null(0) == 0)
                )
            )
        )
        if changed.is_empty():
            continue

        effective_replacements = replacements.join(
            changed.select("date", "ticker"),
            on=["date", "ticker"],
            how="semi",
        ).drop("_naver_duplicate_key")
        updated = pl.concat(
            [
                current.join(
                    effective_replacements.select("date", "ticker"),
                    on=["date", "ticker"],
                    how="anti",
                ),
                effective_replacements.select(current.columns),
            ],
            how="diagonal_relaxed",
        ).sort(["date", "ticker", "price_source"])
        changed_rows += changed.height
        changed_dates.add(logical_date)
        if not dry_run:
            updated.write_parquet(path)
        written += 1

    if changed_dates:
        _write_changed_dates(data_root, changed_dates, dry_run=dry_run)

    return {
        "target_tickers": len(target_tickers),
        "duplicate_keys": duplicate_keys.height,
        "silver_rows_changed": changed_rows,
        "silver_partitions_written": written,
    }


def _write_changed_dates(data_root: Path, changed_dates: set[date], *, dry_run: bool) -> None:
    path = data_root / "reports" / "price_adjustment_audit" / "repaired_silver_dates.csv"
    frame = pl.DataFrame({"date": sorted(changed_dates)})
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)


if __name__ == "__main__":
    main()
