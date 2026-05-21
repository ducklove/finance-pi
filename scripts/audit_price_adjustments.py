from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit adjusted-price continuity across all securities."
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--threshold", type=float, default=1.0)
    parser.add_argument("--cluster-threshold", type=int, default=10)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/reports/price_adjustment_audit"),
    )
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    events, date_clusters, security_summary = audit_price_adjustments(
        args.data_root,
        threshold=args.threshold,
        cluster_threshold=args.cluster_threshold,
    )

    events_path = args.out_dir / "suspicious_price_jumps.csv"
    clusters_path = args.out_dir / "suspicious_price_jump_dates.csv"
    securities_path = args.out_dir / "suspicious_price_jump_securities.csv"
    events.write_csv(events_path)
    date_clusters.write_csv(clusters_path)
    security_summary.write_csv(securities_path)

    print(f"suspicious_events={events.height} threshold={args.threshold}")
    print(f"clustered_dates={date_clusters.height} cluster_threshold={args.cluster_threshold}")
    print(f"affected_securities={security_summary.height}")
    print(events_path)
    print(clusters_path)
    print(securities_path)


def audit_price_adjustments(
    data_root: Path,
    *,
    threshold: float,
    cluster_threshold: int,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    price_glob = data_root / "gold/daily_prices_adj/dt=*/part.parquet"
    master_path = data_root / "gold/security_master.parquet"
    silver_glob = data_root / "silver/prices/dt=*/part.parquet"

    prices = (
        pl.scan_parquet(price_glob.as_posix(), hive_partitioning=True)
        .select(
            "date",
            "security_id",
            "close_adj",
            "return_1d",
            "volume",
            "is_halted",
        )
        .filter(pl.col("return_1d").is_not_null() & (pl.col("return_1d").abs() >= threshold))
    )
    master = pl.scan_parquet(master_path.as_posix()).select(
        "security_id",
        "ticker",
        "name",
        "market",
    )
    silver = (
        pl.scan_parquet(silver_glob.as_posix(), hive_partitioning=True)
        .select("date", "security_id", "price_source")
        .unique(subset=["date", "security_id"], keep="first")
    )

    events = (
        prices.join(master, on="security_id", how="left")
        .join(silver, on=["date", "security_id"], how="left")
        .with_columns(
            pl.col("return_1d").abs().alias("abs_return_1d"),
            pl.when(pl.col("volume").fill_null(0) == 0)
            .then(pl.lit("zero_volume"))
            .when(pl.col("is_halted").fill_null(False))
            .then(pl.lit("halted"))
            .otherwise(pl.lit("large_return"))
            .alias("initial_flag"),
        )
        .select(
            "date",
            "ticker",
            "name",
            "market",
            "security_id",
            "price_source",
            "close_adj",
            "return_1d",
            "abs_return_1d",
            "volume",
            "is_halted",
            "initial_flag",
        )
        .sort(["date", "ticker"])
        .collect()
    )

    date_clusters = (
        events.group_by("date")
        .agg(
            pl.len().alias("event_count"),
            pl.col("ticker").n_unique().alias("ticker_count"),
            pl.col("abs_return_1d").max().alias("max_abs_return"),
            pl.col("price_source").drop_nulls().unique().str.join("|").alias("price_sources"),
        )
        .filter(pl.col("event_count") >= cluster_threshold)
        .sort(["event_count", "date"], descending=[True, False])
    )
    clustered_events = date_clusters.select("date").with_columns(
        pl.lit("clustered_date").alias("cluster_flag")
    )
    events = (
        events.join(clustered_events, on="date", how="left")
        .with_columns(
            pl.coalesce(["cluster_flag", "initial_flag"]).alias("audit_flag"),
        )
        .drop("cluster_flag")
    )

    security_summary = (
        events.group_by("ticker", "name", "market", "security_id")
        .agg(
            pl.len().alias("event_count"),
            pl.col("date").min().alias("first_event_date"),
            pl.col("date").max().alias("last_event_date"),
            pl.col("abs_return_1d").max().alias("max_abs_return"),
            pl.col("audit_flag").unique().str.join("|").alias("flags"),
        )
        .sort(["event_count", "max_abs_return"], descending=True)
    )
    return events, date_clusters, security_summary


if __name__ == "__main__":
    main()
