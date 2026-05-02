from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from glob import glob
from pathlib import Path

import polars as pl

from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class BuildSummary:
    dataset: str
    rows: int
    files: int


def build_all(data_root: Path) -> list[BuildSummary]:
    return list(build_all_iter(data_root))


def build_all_iter(data_root: Path) -> Iterable[BuildSummary]:
    for builder in [
        build_silver_prices,
        build_security_master,
        build_universe_history,
        build_daily_prices_adj,
        build_financials_silver,
        build_fundamentals_pit,
    ]:
        yield from builder(data_root)


def build_silver_prices(data_root: Path) -> list[BuildSummary]:
    frames: list[pl.DataFrame] = []
    naver_summary = _naver_summary_frame(data_root)
    for source, pattern in [
        ("krx", "bronze/krx_daily/dt=*/part.parquet"),
        ("kis", "bronze/kis_daily/dt=*/part.parquet"),
        ("naver", "bronze/naver_daily/request_dt=*/chunk=*/part.parquet"),
        ("pre2010", "bronze/pre2010/source=*/dt=*/part.parquet"),
    ]:
        frame = _read_optional(data_root / pattern)
        if frame is not None and not frame.is_empty():
            prices = _normalize_price_frame(frame, source)
            if naver_summary is not None:
                prices = _enrich_prices_with_naver(prices, naver_summary)
            frames.append(prices)
    if not frames:
        return [BuildSummary("silver.prices", 0, 0)]

    prices = pl.concat(frames, how="diagonal_relaxed").sort(["date", "ticker", "price_source"])
    prices = prices.unique(subset=["date", "ticker"], keep="first")
    return _write_by_date(data_root, "silver.prices", prices, "date")


def build_security_master(data_root: Path) -> list[BuildSummary]:
    prices = _read_optional(data_root / "silver/prices/dt=*/part.parquet")
    if prices is None or prices.is_empty():
        return [BuildSummary("gold.security_master", 0, 0)]

    companies = _latest_company_frame(data_root)
    grouped = (
        prices.group_by("ticker")
        .agg(
            pl.col("name").drop_nulls().last().alias("name"),
            pl.col("market").drop_nulls().last().alias("market"),
            pl.col("date").min().alias("listed_date"),
            pl.col("date").max().alias("last_seen_date"),
        )
        .with_columns(pl.col("ticker").cast(pl.String))
    )
    if companies is not None and not companies.is_empty():
        grouped = grouped.join(
            companies.select(
                pl.col("stock_code").alias("ticker"),
                "corp_code",
                pl.col("corp_name").alias("corp_name"),
            ),
            on="ticker",
            how="left",
        )
    else:
        grouped = grouped.with_columns(
            pl.lit(None, dtype=pl.String).alias("corp_code"),
            pl.lit(None, dtype=pl.String).alias("corp_name"),
        )

    master = grouped.with_columns(
        pl.concat_str([pl.lit("I"), pl.coalesce(["corp_code", "ticker"])]).alias("issuer_id"),
        pl.concat_str([pl.lit("S"), pl.col("ticker")]).alias("security_id"),
        pl.concat_str([pl.lit("L"), pl.col("ticker")]).alias("listing_id"),
        pl.lit(None, dtype=pl.String).alias("isin"),
        pl.when(_preferred_expr()).then(pl.lit("preferred")).otherwise(pl.lit("common")).alias(
            "share_class"
        ),
        pl.when(pl.col("name").str.contains("스팩|SPAC", literal=False))
        .then(pl.lit("spac_pre"))
        .otherwise(pl.lit("equity"))
        .alias("security_type"),
        pl.lit(None, dtype=pl.Date).alias("delisted_date"),
        pl.lit("unknown").alias("delisting_reason"),
    ).select(
        [
            "issuer_id",
            "security_id",
            "listing_id",
            "corp_code",
            "isin",
            "ticker",
            "name",
            "market",
            "share_class",
            "security_type",
            "listed_date",
            "delisted_date",
            "delisting_reason",
        ]
    )

    writer = ParquetDatasetWriter()
    layout = DataLakeLayout(data_root)
    paths = [
        writer.write(master, layout.singleton_path("silver.security_identity"), mode="overwrite"),
        writer.write(master, layout.singleton_path("gold.security_master"), mode="overwrite"),
    ]
    return [BuildSummary("gold.security_master", master.height, len(paths))]


def build_universe_history(data_root: Path) -> list[BuildSummary]:
    prices = _read_optional(data_root / "silver/prices/dt=*/part.parquet")
    master = _read_optional(data_root / "gold/security_master.parquet")
    if prices is None or prices.is_empty() or master is None or master.is_empty():
        return [BuildSummary("gold.universe_history", 0, 0)]
    universe = (
        prices.select(
            "date",
            "ticker",
            "is_halted",
            "is_designated",
            "is_liquidation_window",
        )
        .join(master, on="ticker", how="inner")
        .with_columns(
            pl.lit(True).alias("is_active"),
            (pl.col("security_type") == "spac_pre").alias("is_spac_pre"),
        )
        .select(
            [
                "date",
                "security_id",
                "listing_id",
                "market",
                "is_active",
                "share_class",
                "security_type",
                "is_spac_pre",
                "is_halted",
                "is_designated",
                "is_liquidation_window",
            ]
        )
    )
    return _write_by_date(data_root, "gold.universe_history", universe, "date")


def build_daily_prices_adj(data_root: Path) -> list[BuildSummary]:
    prices = _read_optional(data_root / "silver/prices/dt=*/part.parquet")
    if prices is None or prices.is_empty():
        return [BuildSummary("gold.daily_prices_adj", 0, 0)]
    adjusted = (
        prices.sort(["security_id", "date"])
        .with_columns(
            pl.col("open").alias("open_adj"),
            pl.col("high").alias("high_adj"),
            pl.col("low").alias("low_adj"),
            pl.col("close").alias("close_adj"),
            pl.col("close").pct_change().over("security_id").alias("return_1d"),
        )
        .select(
            [
                "date",
                "security_id",
                "listing_id",
                "open_adj",
                "high_adj",
                "low_adj",
                "close_adj",
                "return_1d",
                "volume",
                "trading_value",
                "market_cap",
                "listed_shares",
                "is_halted",
                "is_designated",
                "is_liquidation_window",
            ]
        )
    )
    return _write_by_date(data_root, "gold.daily_prices_adj", adjusted, "date")


def build_financials_silver(data_root: Path) -> list[BuildSummary]:
    financials = _read_optional(data_root / "bronze/dart_financials/rcept_dt=*/part.parquet")
    if financials is None or financials.is_empty():
        return [BuildSummary("silver.financials", 0, 0)]
    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is not None and not master.is_empty() and "corp_code" in financials.columns:
        financials = (
            financials.drop("security_id")
            if "security_id" in financials.columns
            else financials
        ).join(
            master.select("corp_code", "security_id").drop_nulls("corp_code").unique(),
            on="corp_code",
            how="left",
        )
    financials = (
        _cast_dates(
            financials,
            ["fiscal_period_end", "event_date", "rcept_dt", "available_date"],
        )
        .select(
            [
                "security_id",
                "corp_code",
                "fiscal_period_end",
                "event_date",
                "rcept_dt",
                "available_date",
                "report_type",
                "account_id",
                "account_name",
                "amount",
                "is_consolidated",
                "accounting_basis",
            ]
        )
        .with_columns(pl.col("fiscal_period_end").dt.year().alias("fiscal_year"))
    )
    return _write_by_partition(
        data_root,
        "silver.financials",
        financials,
        "fiscal_year",
        financials["fiscal_year"].to_list(),
    )


def build_fundamentals_pit(data_root: Path) -> list[BuildSummary]:
    financials = _read_optional(data_root / "silver/financials/fiscal_year=*/part.parquet")
    universe_pattern = data_root / "gold/universe_history/dt=*/part.parquet"
    universe_files = sorted(glob(universe_pattern.as_posix()))
    if financials is None or financials.is_empty() or not universe_files:
        return [BuildSummary("gold.fundamentals_pit", 0, 0)]
    financials = (
        _cast_dates(
            financials,
            ["fiscal_period_end", "event_date", "rcept_dt", "available_date"],
        )
        .filter(pl.col("security_id").is_not_null())
        .sort(["security_id", "account_id", "fiscal_period_end", "available_date", "rcept_dt"])
    )

    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    total_rows = 0
    total_files = 0
    for file in universe_files:
        universe = pl.read_parquet(file, hive_partitioning=True).select("date", "security_id")
        if universe.is_empty():
            continue
        as_of_date = _partition_date_from_path(Path(file), "dt")
        security_ids = universe["security_id"].drop_nulls().unique().to_list()
        if not security_ids:
            continue
        eligible = financials.filter(
            pl.col("security_id").is_in(security_ids)
            & (pl.col("available_date") <= pl.lit(as_of_date))
        )
        if eligible.is_empty():
            continue
        pit = (
            universe.join(eligible, on="security_id", how="inner")
            .sort(
                [
                    "date",
                    "security_id",
                    "account_id",
                    "fiscal_period_end",
                    "available_date",
                    "rcept_dt",
                ]
            )
            .unique(subset=["date", "security_id", "account_id"], keep="last")
            .rename({"date": "as_of_date"})
        )
        if pit.is_empty():
            continue
        writer.write(
            pit,
            layout.partition_path("gold.fundamentals_pit", as_of_date),
            mode="overwrite",
        )
        total_rows += pit.height
        total_files += 1
    return [BuildSummary("gold.fundamentals_pit", total_rows, total_files)]


def _normalize_price_frame(frame: pl.DataFrame, source: str) -> pl.DataFrame:
    frame = _cast_dates(frame, ["date"])
    if "name" not in frame.columns:
        frame = frame.with_columns(pl.col("ticker").alias("name"))
    return frame.with_columns(
        _ticker_expr("ticker").alias("ticker"),
        pl.concat_str([pl.lit("S"), _ticker_expr("ticker")]).alias("security_id"),
        pl.concat_str([pl.lit("L"), _ticker_expr("ticker")]).alias("listing_id"),
        pl.lit(source).alias("price_source"),
        pl.lit(False).alias("is_halted"),
        pl.lit(False).alias("is_designated"),
        pl.lit(False).alias("is_liquidation_window"),
    ).select(
        [
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
        ]
    )


def _read_optional(pattern: Path) -> pl.DataFrame | None:
    files = sorted(glob(pattern.as_posix()))
    if not files:
        return None
    try:
        return pl.read_parquet(files, hive_partitioning=True)
    except pl.exceptions.SchemaError:
        frames = [pl.read_parquet(file, hive_partitioning=True) for file in files]
        return pl.concat(frames, how="diagonal_relaxed")


def _latest_company_frame(data_root: Path) -> pl.DataFrame | None:
    frame = _read_optional(data_root / "bronze/dart_company/snapshot_dt=*/part.parquet")
    if frame is None or frame.is_empty():
        return None
    frame = _cast_dates(frame, ["snapshot_dt"])
    latest = frame["snapshot_dt"].max()
    return frame.filter(pl.col("snapshot_dt") == latest).with_columns(
        _ticker_expr("stock_code").alias("stock_code")
    )


def _naver_summary_frame(data_root: Path) -> pl.DataFrame | None:
    frame = _read_optional(data_root / "bronze/naver_summary/dt=*/part.parquet")
    if frame is None or frame.is_empty():
        return None
    return (
        _cast_dates(frame, ["snapshot_dt"])
        .with_columns(
            _ticker_expr("ticker").alias("ticker"),
            pl.col("name").alias("naver_name"),
            pl.col("market").alias("naver_market"),
            pl.col("market_cap").cast(pl.Int64, strict=False).alias("naver_market_cap"),
            pl.col("listed_shares").cast(pl.Int64, strict=False).alias("naver_listed_shares"),
        )
        .select(
            pl.col("snapshot_dt").alias("date"),
            "ticker",
            "naver_name",
            "naver_market",
            "naver_market_cap",
            "naver_listed_shares",
        )
        .unique(subset=["date", "ticker"], keep="last")
    )


def _enrich_prices_with_naver(
    prices: pl.DataFrame,
    naver_summary: pl.DataFrame,
) -> pl.DataFrame:
    return (
        prices.join(naver_summary, on=["date", "ticker"], how="left")
        .with_columns(
            pl.when(
                pl.col("naver_name").is_not_null()
                & (pl.col("name").is_null() | (_ticker_expr("name") == pl.col("ticker")))
            )
            .then(pl.col("naver_name"))
            .otherwise(pl.col("name"))
            .alias("name"),
            pl.coalesce(["naver_market", "market"]).alias("market"),
            pl.coalesce(["market_cap", "naver_market_cap"]).alias("market_cap"),
            pl.coalesce(["listed_shares", "naver_listed_shares"]).alias("listed_shares"),
        )
        .drop(["naver_name", "naver_market", "naver_market_cap", "naver_listed_shares"])
    )


def _preferred_expr() -> pl.Expr:
    return (
        pl.col("ticker").str.ends_with("5")
        | pl.col("ticker").str.ends_with("7")
        | pl.col("ticker").str.ends_with("9")
        | pl.col("name").str.contains("(?:\\d+)?\\uc6b0[A-Z]?$|\\uc6b0\\uc120", literal=False)
    )


def _ticker_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars().str.to_uppercase().str.zfill(6)


def _cast_dates(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    exprs = []
    for column in columns:
        if column in frame.columns:
            exprs.append(pl.col(column).cast(pl.Date, strict=False))
    return frame.with_columns(exprs) if exprs else frame


def _partition_date_from_path(path: Path, key: str) -> date:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return date.fromisoformat(part.removeprefix(prefix))
    raise ValueError(f"{path} does not contain {key}= partition")


def _write_by_date(
    data_root: Path,
    dataset: str,
    frame: pl.DataFrame,
    date_column: str,
) -> list[BuildSummary]:
    values = frame[date_column].unique().sort().to_list()
    return _write_by_partition(data_root, dataset, frame, date_column, values)


def _write_by_partition(
    data_root: Path,
    dataset: str,
    frame: pl.DataFrame,
    partition_column: str,
    values: list[object],
) -> list[BuildSummary]:
    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    files = 0
    rows = 0
    for value in sorted(set(values)):
        partition = frame.filter(pl.col(partition_column) == value)
        if partition.is_empty():
            continue
        if dataset == "silver.financials":
            path = data_root / "silver" / "financials" / f"fiscal_year={value}" / "part.parquet"
            partition = partition.drop(partition_column)
        else:
            path = layout.partition_path(dataset, value)
        writer.write(partition, path, mode="overwrite")
        files += 1
        rows += partition.height
    return [BuildSummary(dataset, rows, files)]
