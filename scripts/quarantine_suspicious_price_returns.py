from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set return_1d to null for audited suspicious adjusted-price rows."
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--audit-csv",
        type=Path,
        default=Path("data/reports/price_adjustment_audit/suspicious_price_jumps.csv"),
    )
    args = parser.parse_args()
    summary = quarantine_suspicious_price_returns(args.data_root, args.audit_csv)
    print(f"quarantined_rows={summary['quarantined_rows']}")
    print(f"partitions_written={summary['partitions_written']}")
    print(summary["quarantine_csv"])


def quarantine_suspicious_price_returns(data_root: Path, audit_csv: Path) -> dict[str, object]:
    if not audit_csv.exists():
        raise FileNotFoundError(audit_csv)
    audit = (
        pl.read_csv(audit_csv, try_parse_dates=True)
        .select("date", "security_id", "ticker", "name", "return_1d", "audit_flag")
        .with_columns(pl.col("date").cast(pl.Date))
    )
    if audit.is_empty():
        return {
            "quarantined_rows": 0,
            "partitions_written": 0,
            "quarantine_csv": "",
        }

    quarantined_frames: list[pl.DataFrame] = []
    written = 0
    for logical_date, rows in audit.partition_by("date", as_dict=True).items():
        key = logical_date[0] if isinstance(logical_date, tuple) else logical_date
        if not isinstance(key, date):
            continue
        path = data_root / "gold" / "daily_prices_adj" / f"dt={key.isoformat()}" / "part.parquet"
        if not path.exists():
            continue
        current = pl.read_parquet(path, hive_partitioning=True)
        if "dt" in current.columns:
            current = current.drop("dt")
        keys = rows.select("date", "security_id")
        matches = current.join(keys, on=["date", "security_id"], how="semi")
        if matches.is_empty():
            continue
        updated = (
            current.join(
                keys.with_columns(pl.lit(True).alias("_quarantine")),
                on=["date", "security_id"],
                how="left",
            )
            .with_columns(
                pl.when(pl.col("_quarantine").fill_null(False))
                .then(None)
                .otherwise(pl.col("return_1d"))
                .alias("return_1d")
            )
            .drop("_quarantine")
        )
        updated.write_parquet(path)
        quarantined_frames.append(rows)
        written += 1

    quarantine_csv = data_root / "reports" / "price_adjustment_audit" / "quarantined_returns.csv"
    if quarantined_frames:
        (
            pl.concat(quarantined_frames, how="diagonal_relaxed")
            .sort(["date", "security_id"])
            .write_csv(quarantine_csv)
        )
    return {
        "quarantined_rows": sum(frame.height for frame in quarantined_frames),
        "partitions_written": written,
        "quarantine_csv": quarantine_csv.as_posix(),
    }


if __name__ == "__main__":
    main()
