from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from glob import glob
from pathlib import Path

import polars as pl

from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter

logger = logging.getLogger(__name__)

# A security whose last price row is more than this many trading days before the
# newest silver.prices date is considered delisted as of its last seen date.
DELISTING_BUFFER_TRADING_DAYS = 10

# Corporate-action detection: listed_shares must jump by at least this ratio
# (or its inverse) between consecutive observed rows to be a candidate event.
SHARES_JUMP_MIN_RATIO = 1.5
# ... and by no more than this ratio, to reject unit/data glitches.
SHARES_JUMP_MAX_RATIO = 100.0
# Cross-check: close must move inversely to shares (|close_ratio * shares_ratio - 1|).
SHARES_PRICE_CROSS_TOLERANCE = 0.10
# Market cap should stay roughly continuous across the event when both sides exist.
MARKET_CAP_CONTINUITY_TOLERANCE = 0.25
# Warn when detection over a non-trivial history yields no events at all.
CORPORATE_ACTIONS_WARN_MIN_DATES = 250
# DART filing corroboration window around the effective date (calendar days).
DART_CORROBORATION_DAYS_BEFORE = 120
DART_CORROBORATION_DAYS_AFTER = 14

_CORPORATE_ACTION_SPLIT_PATTERN = "주식분할|액면분할"
_CORPORATE_ACTION_MERGE_PATTERN = "주식병합|액면병합|감자"

# join_asof cannot verify sortedness when `by` groups are provided and emits a
# UserWarning instead. The asof inputs below are always pre-sorted by (by, on),
# so skip the check when the installed polars supports opting out (>=1.2).
_JOIN_ASOF_KWARGS: dict[str, bool] = (
    {"check_sortedness": False}
    if "check_sortedness" in inspect.signature(pl.DataFrame.join_asof).parameters
    else {}
)

# Column pruning (ST-12): every reader below asks the parquet scan for exactly
# the columns it consumes, so a full-history build decodes megabytes instead of
# gigabytes per builder. Extra columns in an explicitly shared frame are fine —
# each builder still projects with these lists before computing.
_BRONZE_PRICE_COLUMNS = [
    "date",
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
]
_MASTER_PRICE_COLUMNS = ["date", "ticker", "name", "market"]
_UNIVERSE_PRICE_COLUMNS = ["date", "ticker", "is_halted", "is_designated", "is_liquidation_window"]
_ACTION_PRICE_COLUMNS = ["date", "security_id", "ticker", "close", "listed_shares", "market_cap"]
_ADJ_PRICE_COLUMNS = [
    "date",
    "security_id",
    "listing_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
    "market_cap",
    "listed_shares",
    "price_source",
    "price_basis",
    "is_halted",
    "is_designated",
    "is_liquidation_window",
]
_MARKET_CAP_PRICE_COLUMNS = [
    "date",
    "security_id",
    "listing_id",
    "ticker",
    "name",
    "market",
    "close",
    "trading_value",
    "volume",
    "market_cap",
    "listed_shares",
    "price_source",
]
_MARCAP_COLUMNS = [
    "Date",
    "Code",
    "Name",
    "Market",
    "Rank",
    "Close",
    "Amount",
    "Volume",
    "Marcap",
    "Stocks",
]
_FINANCIALS_COLUMNS = [
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


@dataclass(frozen=True)
class BuildSummary:
    dataset: str
    rows: int
    files: int


def build_all(data_root: Path) -> list[BuildSummary]:
    return list(build_all_iter(data_root))


def build_all_iter(data_root: Path) -> Iterable[BuildSummary]:
    yield from build_silver_market_caps(data_root)
    yield from build_silver_prices(data_root)
    # Load silver.prices once (all columns) and share it with every downstream
    # consumer instead of re-reading the multi-GB history once per builder.
    prices = _read_optional(data_root / "silver/prices/dt=*/part.parquet")
    yield from build_security_master(data_root, prices=prices)
    yield from build_universe_history(data_root, prices=prices)
    yield from build_corporate_actions(data_root, prices=prices)
    yield from build_daily_prices_adj(data_root, prices=prices)
    yield from build_daily_market_caps(data_root, prices=prices)
    del prices
    yield from build_nps_holdings_silver(data_root)
    yield from build_nps_universe(data_root)
    yield from build_financials_silver(data_root)
    yield from build_fundamentals_pit(data_root)


def build_silver_prices(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    frames: list[pl.DataFrame] = []
    naver_summary = _naver_summary_frame(data_root, selected_dates)
    market_caps = _market_caps_frame(data_root, selected_dates)
    for source, pattern in [
        ("krx", "bronze/krx_daily/dt=*/part.parquet"),
        ("kis", "bronze/kis_daily/dt=*/part.parquet"),
        ("naver", "bronze/naver_daily/request_dt=*/chunk=*/part.parquet"),
        ("pre2010", "bronze/pre2010/source=*/dt=*/part.parquet"),
    ]:
        frame = (
            _read_price_source_dates(data_root, source, selected_dates)
            if selected_dates is not None
            else _read_optional(data_root / pattern, columns=_BRONZE_PRICE_COLUMNS)
        )
        if frame is not None and not frame.is_empty():
            prices = _normalize_price_frame(frame, source)
            if naver_summary is not None:
                prices = _enrich_prices_with_naver(prices, naver_summary)
            if market_caps is not None:
                prices = _enrich_prices_with_market_caps(prices, market_caps)
            prices = _deduplicate_price_candidates(prices)
            frames.append(prices)
    if not frames:
        return [BuildSummary("silver.prices", 0, 0)]

    prices = _select_preferred_price_sources(pl.concat(frames, how="diagonal_relaxed"))
    return _write_by_date(data_root, "silver.prices", prices, "date")


def build_silver_market_caps(data_root: Path) -> list[BuildSummary]:
    frame = _read_optional(data_root / "bronze/marcap/year=*/part.parquet", columns=_MARCAP_COLUMNS)
    if frame is None or frame.is_empty():
        return [BuildSummary("silver.market_caps", 0, 0)]
    market_caps = _normalize_marcap_frame(frame)
    return _write_by_date(data_root, "silver.market_caps", market_caps, "date")


def build_security_master(
    data_root: Path,
    dates: Iterable[date] | None = None,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    if selected_dates is not None:
        prices = _read_partition_dates(data_root, "silver.prices", selected_dates)
    elif prices is None:
        prices = _read_optional(
            data_root / "silver/prices/dt=*/part.parquet",
            columns=_MASTER_PRICE_COLUMNS,
        )
    if prices is None or prices.is_empty():
        return [BuildSummary("gold.security_master", 0, 0)]

    companies = _latest_company_frame(data_root)
    delisting_cutoff = _delisting_cutoff(prices) if selected_dates is None else None
    master = _security_master_from_prices(prices, companies, delisting_cutoff)
    if selected_dates is not None:
        master = _merge_security_master(data_root, master)
    if "_last_seen_date" in master.columns:
        master = master.drop("_last_seen_date")

    writer = ParquetDatasetWriter()
    layout = DataLakeLayout(data_root)
    paths = [
        writer.write(master, layout.singleton_path("silver.security_identity"), mode="overwrite"),
        writer.write(master, layout.singleton_path("gold.security_master"), mode="overwrite"),
    ]
    return [BuildSummary("gold.security_master", master.height, len(paths))]


def build_universe_history(
    data_root: Path,
    dates: Iterable[date] | None = None,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    if selected_dates is not None:
        prices = _read_partition_dates(data_root, "silver.prices", selected_dates)
    elif prices is None:
        prices = _read_optional(
            data_root / "silver/prices/dt=*/part.parquet",
            columns=_UNIVERSE_PRICE_COLUMNS,
        )
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
            (
                pl.col("delisted_date").is_null() | (pl.col("date") <= pl.col("delisted_date"))
            ).alias("is_active"),
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


def build_corporate_actions(
    data_root: Path,
    dates: Iterable[date] | None = None,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    """Detect split/merge/capital-reduction events from silver.prices share jumps.

    An event is emitted when listed_shares moves by ~N between consecutive
    observed rows, close moves inversely by ~1/N (cross-checked within
    ``SHARES_PRICE_CROSS_TOLERANCE``), and market cap stays roughly continuous.
    ``adjustment_factor`` is derived from the shares ratio. DART filings
    (주식분할/주식병합/감자) near the effective date upgrade ``confidence`` to
    "high" and refine ``action_type``. Detection needs the previous trading row
    for context, so it always scans the full history; ``dates`` is accepted for
    pipeline symmetry but ignored. Rows written by other sources (e.g. manual
    curation) are preserved and win over detected rows for the same event.
    """
    del dates
    prices = (
        prices.select(_ACTION_PRICE_COLUMNS)
        if prices is not None
        else _read_price_history_columns(data_root, _ACTION_PRICE_COLUMNS)
    )
    if prices is None or prices.is_empty():
        return [BuildSummary("silver.corporate_actions", 0, 0)]
    detected = _detect_share_jump_actions(prices)
    detected = _corroborate_actions_with_dart(data_root, detected)
    trading_days = prices["date"].n_unique()
    if detected.is_empty() and trading_days >= CORPORATE_ACTIONS_WARN_MIN_DATES:
        logger.warning(
            "silver.corporate_actions is empty after scanning %d trading days of "
            "silver.prices; gold 'adjusted' prices will equal raw prices",
            trading_days,
        )
    return _write_corporate_actions(data_root, detected)


def _detect_share_jump_actions(prices: pl.DataFrame) -> pl.DataFrame:
    observed = (
        prices.filter(
            pl.col("close").is_not_null()
            & (pl.col("close") > 0)
            & pl.col("listed_shares").is_not_null()
            & (pl.col("listed_shares") > 0)
        )
        .sort(["security_id", "date"])
        .with_columns(
            pl.col("close").shift(1).over("security_id").alias("_prev_close"),
            pl.col("listed_shares").shift(1).over("security_id").alias("_prev_shares"),
            pl.col("market_cap").shift(1).over("security_id").alias("_prev_market_cap"),
        )
        .filter(pl.col("_prev_close").is_not_null() & pl.col("_prev_shares").is_not_null())
        .with_columns(
            (pl.col("listed_shares") / pl.col("_prev_shares")).alias("_shares_ratio"),
            (pl.col("close") / pl.col("_prev_close")).alias("_close_ratio"),
        )
    )
    shares_jump = pl.max_horizontal("_shares_ratio", 1.0 / pl.col("_shares_ratio"))
    market_cap_continuous = (
        pl.col("market_cap").is_null()
        | pl.col("_prev_market_cap").is_null()
        | (pl.col("market_cap") <= 0)
        | (pl.col("_prev_market_cap") <= 0)
        | (
            (pl.col("market_cap") / pl.col("_prev_market_cap") - 1.0).abs()
            <= MARKET_CAP_CONTINUITY_TOLERANCE
        )
    )
    return (
        observed.filter(
            (shares_jump >= SHARES_JUMP_MIN_RATIO)
            & (shares_jump <= SHARES_JUMP_MAX_RATIO)
            & (
                (pl.col("_close_ratio") * pl.col("_shares_ratio") - 1.0).abs()
                <= SHARES_PRICE_CROSS_TOLERANCE
            )
            & market_cap_continuous
        )
        .select(
            pl.col("date").alias("effective_date"),
            "security_id",
            "ticker",
            pl.when(pl.col("_shares_ratio") > 1.0)
            .then(pl.lit("split"))
            .otherwise(pl.lit("merge"))
            .alias("action_type"),
            (1.0 / pl.col("_shares_ratio")).cast(pl.Float64).alias("adjustment_factor"),
            pl.lit(None, dtype=pl.String).alias("source_rcept_no"),
            pl.lit("shares_jump").alias("source"),
            pl.lit("medium").alias("confidence"),
        )
        .sort(["effective_date", "security_id"])
    )


def _corroborate_actions_with_dart(data_root: Path, actions: pl.DataFrame) -> pl.DataFrame:
    if actions.is_empty():
        return actions
    filings = _read_optional(
        data_root / "bronze/dart_filings/dt=*/part.parquet",
        columns=["stock_code", "rcept_dt", "rcept_no", "report_nm"],
    )
    if filings is None or filings.is_empty():
        return actions
    filings = (
        _cast_dates(filings, ["rcept_dt"])
        .filter(
            pl.col("stock_code").is_not_null()
            & pl.col("rcept_dt").is_not_null()
            & pl.col("report_nm")
            .cast(pl.String)
            .str.contains(f"{_CORPORATE_ACTION_SPLIT_PATTERN}|{_CORPORATE_ACTION_MERGE_PATTERN}")
        )
        .with_columns(_ticker_expr("stock_code").alias("ticker"))
        .select("ticker", "rcept_dt", "rcept_no", "report_nm")
    )
    if filings.is_empty():
        return actions
    matched = (
        actions.join(filings, on="ticker", how="inner")
        .with_columns(
            (pl.col("effective_date") - pl.col("rcept_dt")).dt.total_days().alias("_lead_days")
        )
        .filter(
            (pl.col("_lead_days") >= -DART_CORROBORATION_DAYS_AFTER)
            & (pl.col("_lead_days") <= DART_CORROBORATION_DAYS_BEFORE)
            & pl.when(pl.col("action_type") == "split")
            .then(pl.col("report_nm").str.contains(_CORPORATE_ACTION_SPLIT_PATTERN))
            .otherwise(pl.col("report_nm").str.contains(_CORPORATE_ACTION_MERGE_PATTERN))
        )
        .with_columns(pl.col("_lead_days").abs().alias("_lag_days"))
        .sort(["security_id", "effective_date", "_lag_days"])
        .unique(subset=["security_id", "effective_date"], keep="first")
        .select(
            "security_id",
            "effective_date",
            pl.col("rcept_no").alias("_matched_rcept_no"),
            pl.when(pl.col("report_nm").str.contains("감자"))
            .then(pl.lit("capital_reduction"))
            .when(pl.col("report_nm").str.contains(_CORPORATE_ACTION_SPLIT_PATTERN))
            .then(pl.lit("split"))
            .otherwise(pl.lit("merge"))
            .alias("_matched_action_type"),
        )
    )
    if matched.is_empty():
        return actions
    return (
        actions.join(matched, on=["security_id", "effective_date"], how="left")
        .with_columns(
            pl.coalesce(["_matched_rcept_no", "source_rcept_no"]).alias("source_rcept_no"),
            pl.coalesce(["_matched_action_type", "action_type"]).alias("action_type"),
            pl.when(pl.col("_matched_rcept_no").is_not_null())
            .then(pl.lit("high"))
            .otherwise(pl.col("confidence"))
            .alias("confidence"),
        )
        .drop("_matched_rcept_no", "_matched_action_type")
    )


def _write_corporate_actions(data_root: Path, detected: pl.DataFrame) -> list[BuildSummary]:
    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    detected = detected.select(
        "effective_date",
        "security_id",
        "action_type",
        "adjustment_factor",
        "source_rcept_no",
        "source",
        "confidence",
    )
    existing_paths = {
        _partition_date_from_path(Path(file), "dt"): Path(file)
        for file in glob((data_root / "silver/corporate_actions/dt=*/part.parquet").as_posix())
    }
    rows = 0
    files = 0
    detected_dates = set(detected["effective_date"].to_list())
    for value in sorted(detected_dates | set(existing_paths)):
        partition = detected.filter(pl.col("effective_date") == value)
        existing = existing_paths.get(value)
        if existing is not None:
            preserved = pl.read_parquet(existing, hive_partitioning=True)
            if "dt" in preserved.columns:
                preserved = preserved.drop("dt")
            if "source" in preserved.columns:
                preserved = preserved.filter(
                    pl.col("source").is_null() | (pl.col("source") != "shares_jump")
                )
            if not preserved.is_empty():
                partition = partition.join(
                    preserved.select("security_id"), on="security_id", how="anti"
                )
                partition = pl.concat([preserved, partition], how="diagonal_relaxed")
        if partition.is_empty():
            if existing is not None:
                existing.unlink()
                with suppress(OSError):
                    existing.parent.rmdir()
            continue
        partition = partition.sort("security_id")
        writer.write(
            partition,
            layout.partition_path("silver.corporate_actions", value),
            mode="overwrite",
        )
        rows += partition.height
        files += 1
    return [BuildSummary("silver.corporate_actions", rows, files)]


def build_nps_holdings_silver(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    bronze = (
        _read_partition_dates(data_root, "bronze.nps_holdings_raw", selected_dates)
        if selected_dates is not None
        else _read_optional(data_root / "bronze/nps_holdings/dt=*/part.parquet")
    )
    if bronze is None or bronze.is_empty():
        return [BuildSummary("silver.nps_holdings", 0, 0)]

    holdings = _normalize_nps_holdings(bronze)
    if selected_dates is not None:
        holdings = holdings.filter(pl.col("date").is_in(selected_dates))
    if holdings.is_empty():
        return [BuildSummary("silver.nps_holdings", 0, 0)]

    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is not None and not master.is_empty():
        identity = master.select(
            pl.col("ticker").cast(pl.String).alias("stock_code"),
            "security_id",
            "listing_id",
        ).unique(subset=["stock_code"], keep="last")
        holdings = holdings.join(identity, on="stock_code", how="left")
    else:
        holdings = holdings.with_columns(
            pl.lit(None, dtype=pl.String).alias("security_id"),
            pl.lit(None, dtype=pl.String).alias("listing_id"),
        )

    holdings = (
        holdings.with_columns(
            pl.when(pl.col("security_id").is_null())
            .then(pl.concat_str([pl.lit("S"), pl.col("stock_code")]))
            .otherwise(pl.col("security_id"))
            .alias("security_id"),
            pl.when(pl.col("listing_id").is_null())
            .then(pl.concat_str([pl.lit("L"), pl.col("stock_code")]))
            .otherwise(pl.col("listing_id"))
            .alias("listing_id"),
        )
        .select(
            [
                "date",
                "security_id",
                "listing_id",
                "stock_code",
                "stock_name",
                "shares",
                "ownership_pct",
                "price",
                "market_value",
                "change_pct",
                "source_rank",
                "source",
                "source_date",
                "source_market_value",
                "source_weight_pct",
                "shares_source",
                "price_date",
                "is_exact_price",
            ]
        )
        .sort(["date", "stock_code"])
        .unique(subset=["date", "stock_code"], keep="last")
    )
    return _write_by_date(data_root, "silver.nps_holdings", holdings, "date")


def build_nps_universe(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    holdings = (
        _read_partition_dates(data_root, "silver.nps_holdings", selected_dates)
        if selected_dates is not None
        else _read_optional(data_root / "silver/nps_holdings/dt=*/part.parquet")
    )
    if holdings is None or holdings.is_empty():
        return [BuildSummary("gold.nps_universe", 0, 0)]

    universe = (
        holdings.filter(pl.col("market_value").is_not_null())
        .sort(["date", "market_value", "stock_code"], descending=[False, True, False])
        .with_columns(
            pl.col("market_value")
            .rank(method="ordinal", descending=True)
            .over("date")
            .cast(pl.Int64)
            .alias("rank"),
            pl.col("date").alias("as_of"),
        )
        .select(
            [
                "date",
                "as_of",
                "rank",
                "security_id",
                "listing_id",
                "stock_code",
                "stock_name",
                "shares",
                "ownership_pct",
                "price",
                "market_value",
                "change_pct",
                "source",
                "source_date",
            ]
        )
    )
    if selected_dates is not None:
        universe = universe.filter(pl.col("date").is_in(selected_dates))
    if universe.is_empty():
        return [BuildSummary("gold.nps_universe", 0, 0)]
    return _write_by_date(data_root, "gold.nps_universe", universe, "date")


def build_daily_prices_adj(
    data_root: Path,
    dates: Iterable[date] | None = None,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    selected_dates = set(dates) if dates is not None else None
    if selected_dates is not None:
        prices = _read_partition_dates(data_root, "silver.prices", selected_dates)
    elif prices is None:
        prices = _read_optional(
            data_root / "silver/prices/dt=*/part.parquet",
            columns=_ADJ_PRICE_COLUMNS,
        )
    else:
        prices = prices.select(
            [column for column in _ADJ_PRICE_COLUMNS if column in prices.columns]
        )
    if prices is None or prices.is_empty():
        return [BuildSummary("gold.daily_prices_adj", 0, 0)]
    actions = _load_corporate_actions(data_root)
    output_dates = selected_dates
    rebuilt_rows = 0
    rebuilt_files = 0
    if selected_dates is not None:
        stale_ids = _stale_adjustment_security_ids(data_root, actions)
        if stale_ids:
            logger.info(
                "corporate action events changed for %d securities; rebuilding their full "
                "adjusted price history: %s",
                len(stale_ids),
                sorted(stale_ids),
            )
            rebuilt_rows, rebuilt_files = _rebuild_adjusted_history(data_root, stale_ids, actions)
            prices = prices.filter(~pl.col("security_id").is_in(sorted(stale_ids)))
        if not prices.is_empty():
            prices = _with_previous_gold_close(data_root, prices, selected_dates)
        if prices.is_empty():
            _write_corporate_actions_state(data_root, actions)
            return [BuildSummary("gold.daily_prices_adj", rebuilt_rows, rebuilt_files)]
    adjusted = _adjusted_price_frame(prices, actions)
    if output_dates is not None:
        adjusted = adjusted.filter(pl.col("date").is_in(output_dates))
    summary = _write_by_date(data_root, "gold.daily_prices_adj", adjusted, "date")[0]
    _write_corporate_actions_state(data_root, actions)
    return [
        BuildSummary(
            "gold.daily_prices_adj",
            summary.rows + rebuilt_rows,
            summary.files + rebuilt_files,
        )
    ]


def _adjusted_price_frame(prices: pl.DataFrame, actions: pl.DataFrame | None) -> pl.DataFrame:
    prices = _apply_corporate_action_factors(prices, actions)
    previous_source = pl.col("price_source").shift(1).over("security_id")
    source_boundary = (
        pl.col("price_source").is_not_null()
        & previous_source.is_not_null()
        & (pl.col("price_source") != previous_source)
    )
    return (
        prices.sort(["security_id", "date"])
        .with_columns(
            pl.col("open").alias("open_adj"),
            pl.col("high").alias("high_adj"),
            pl.col("low").alias("low_adj"),
            pl.col("close").alias("close_adj"),
            pl.when(
                (pl.col("volume").fill_null(0) > 0)
                & (pl.col("volume").shift(1).over("security_id").fill_null(0) > 0)
                & ~source_boundary
            )
            .then(pl.col("close").pct_change().over("security_id"))
            .otherwise(None)
            .alias("return_1d"),
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


def build_daily_market_caps(
    data_root: Path,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    market_caps = _read_optional(data_root / "silver/market_caps/dt=*/part.parquet")
    price_market_caps = _price_market_caps_frame(data_root, prices)
    frames = [
        frame
        for frame in [market_caps, price_market_caps]
        if frame is not None and not frame.is_empty()
    ]
    if not frames:
        return [BuildSummary("gold.daily_market_caps", 0, 0)]
    market_caps = (
        pl.concat(frames, how="diagonal_relaxed")
        .sort(["date", "ticker", "market_cap_source"])
        .unique(subset=["date", "ticker"], keep="first")
    )
    existing_dates = {
        _partition_date_from_path(Path(file), "dt")
        for file in glob((data_root / "gold/daily_market_caps/dt=*/part.parquet").as_posix())
    }
    if existing_dates:
        market_caps = market_caps.filter(~pl.col("date").is_in(existing_dates))
    if market_caps.is_empty():
        return [BuildSummary("gold.daily_market_caps", 0, 0)]
    return _write_by_date(data_root, "gold.daily_market_caps", market_caps, "date")


def build_financials_silver(data_root: Path) -> list[BuildSummary]:
    financials = _read_optional(
        data_root / "bronze/dart_financials/rcept_dt=*/part.parquet",
        columns=_FINANCIALS_COLUMNS,
    )
    if financials is None or financials.is_empty():
        return [BuildSummary("silver.financials", 0, 0)]
    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is not None and not master.is_empty() and "corp_code" in financials.columns:
        financials = (
            financials.drop("security_id") if "security_id" in financials.columns else financials
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
        .pipe(_normalize_financial_account_ids)
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


def _normalize_financial_account_ids(financials: pl.DataFrame) -> pl.DataFrame:
    normalized_name = (
        pl.col("account_name")
        .cast(pl.String)
        .str.replace_all(r"\s+", "")
        .str.replace(r"\(단위[:：]?원\)$", "")
    )
    return financials.with_columns(
        pl.when(normalized_name == "자산총계")
        .then(pl.lit("ifrs-full_Assets"))
        .when(normalized_name == "부채총계")
        .then(pl.lit("ifrs-full_Liabilities"))
        .when(normalized_name == "자본총계")
        .then(pl.lit("ifrs-full_Equity"))
        .when(normalized_name.is_in(["기본주당이익(손실)", "기본주당손익"]))
        .then(pl.lit("ifrs-full_BasicEarningsLossPerShare"))
        .otherwise(pl.col("account_id"))
        .alias("account_id")
    )


def build_fundamentals_pit(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    """Materialize point-in-time fundamentals per universe day.

    PIT availability policy: a filing is visible strictly AFTER its
    ``available_date`` (``available_date < as_of_date``), i.e. next-trading-day
    availability — an intraday filing can never inform a same-day decision.
    Ties within (as_of_date, security_id, account_id) are broken by the newest
    fiscal_period_end, then available_date, then rcept_dt, then consolidated
    statements over separate ones. Must stay row-for-row identical to
    ``finance_pi.pit.build_fundamentals_pit_sql``.

    Incremental behavior: with ``dates=None`` an as-of date is only rebuilt
    when its gold partition is missing or when silver.financials rows with an
    ``available_date`` strictly before it were added or removed since the last
    successful full run (per-available_date row counts recorded under
    ``data/_state/fundamentals_pit.json``). Without that marker (first run)
    every date is built, matching the previous full-rebuild behavior. With
    explicit ``dates`` exactly those partitions are rebuilt and the state
    marker is left untouched.
    """
    selected_dates = set(dates) if dates is not None else None
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

    available_date_rows = _financials_available_date_rows(financials)
    state = _load_fundamentals_pit_state(data_root) if selected_dates is None else None
    rebuild_all = selected_dates is None and state is None
    changed_after: date | None = None
    previously_empty: set[date] = set()
    if state is not None:
        previous_rows, previously_empty = state
        changed_after = _first_changed_available_date(previous_rows, available_date_rows)

    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    total_rows = 0
    total_files = 0
    skipped_dates = 0
    universe_dates: set[date] = set()
    rebuilt_empty: set[date] = set()
    rebuilt_nonempty: set[date] = set()
    for file in universe_files:
        as_of_date = _partition_date_from_path(Path(file), "dt")
        if selected_dates is not None:
            if as_of_date not in selected_dates:
                continue
        else:
            universe_dates.add(as_of_date)
            if not rebuild_all and not _fundamentals_pit_needs_rebuild(
                layout, as_of_date, changed_after, previously_empty
            ):
                skipped_dates += 1
                continue
        universe = pl.read_parquet(file, hive_partitioning=True).select("date", "security_id")
        pit = _fundamentals_pit_partition(universe, financials, as_of_date)
        if pit is None:
            rebuilt_empty.add(as_of_date)
            continue
        writer.write(
            pit,
            layout.partition_path("gold.fundamentals_pit", as_of_date),
            mode="overwrite",
        )
        rebuilt_nonempty.add(as_of_date)
        total_rows += pit.height
        total_files += 1

    if selected_dates is None:
        empty_dates = ((previously_empty | rebuilt_empty) - rebuilt_nonempty) & universe_dates
        _write_fundamentals_pit_state(data_root, available_date_rows, empty_dates)
        logger.info(
            "gold.fundamentals_pit: rebuilt %d as-of dates, skipped %d up-to-date dates",
            len(rebuilt_empty) + len(rebuilt_nonempty),
            skipped_dates,
        )
    return [BuildSummary("gold.fundamentals_pit", total_rows, total_files)]


def _fundamentals_pit_partition(
    universe: pl.DataFrame,
    financials: pl.DataFrame,
    as_of_date: date,
) -> pl.DataFrame | None:
    """Compute one gold.fundamentals_pit partition; None when it has no rows."""
    if universe.is_empty():
        return None
    security_ids = universe["security_id"].drop_nulls().unique().to_list()
    if not security_ids:
        return None
    eligible = financials.filter(
        pl.col("security_id").is_in(security_ids)
        & (pl.col("available_date") < pl.lit(as_of_date))
    )
    if eligible.is_empty():
        return None
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
                "is_consolidated",
            ]
        )
        .unique(subset=["date", "security_id", "account_id"], keep="last")
        .rename({"date": "as_of_date"})
    )
    return None if pit.is_empty() else pit


def _fundamentals_pit_needs_rebuild(
    layout: DataLakeLayout,
    as_of_date: date,
    changed_after: date | None,
    previously_empty: set[date],
) -> bool:
    # Strict-less-than visibility: a financials row available on day A can only
    # influence as-of dates strictly after A.
    if changed_after is not None and as_of_date > changed_after:
        return True
    if layout.partition_path("gold.fundamentals_pit", as_of_date).exists():
        return False
    # Missing partition: rebuild unless the last run already proved it empty.
    return as_of_date not in previously_empty


def _fundamentals_pit_state_path(data_root: Path) -> Path:
    return data_root / "_state" / "fundamentals_pit.json"


def _financials_available_date_rows(financials: pl.DataFrame) -> dict[str, int]:
    """Row count per available_date for the PIT-relevant financials rows.

    Rows with a null available_date can never satisfy ``available_date <
    as_of_date`` and are excluded, so churn in them never triggers rebuilds.
    """
    counts = (
        financials.drop_nulls("available_date").group_by("available_date").agg(pl.len())
    )
    return {
        value.isoformat(): rows
        for value, rows in zip(counts["available_date"], counts["len"], strict=True)
    }


def _first_changed_available_date(
    previous: dict[str, int],
    current: dict[str, int],
) -> date | None:
    changed = {
        key for key in previous.keys() | current.keys() if previous.get(key) != current.get(key)
    }
    if not changed:
        return None
    return date.fromisoformat(min(changed))


def _load_fundamentals_pit_state(data_root: Path) -> tuple[dict[str, int], set[date]] | None:
    path = _fundamentals_pit_state_path(data_root)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    available_date_rows = {
        str(key): int(value) for key, value in payload.get("available_date_rows", {}).items()
    }
    empty_dates = {date.fromisoformat(value) for value in payload.get("empty_dates", [])}
    return available_date_rows, empty_dates


def _write_fundamentals_pit_state(
    data_root: Path,
    available_date_rows: dict[str, int],
    empty_dates: set[date],
) -> None:
    path = _fundamentals_pit_state_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "financials_row_count": sum(available_date_rows.values()),
        "financials_max_available_date": max(available_date_rows, default=None),
        "available_date_rows": dict(sorted(available_date_rows.items())),
        "empty_dates": sorted(value.isoformat() for value in empty_dates),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _normalize_nps_holdings(frame: pl.DataFrame) -> pl.DataFrame:
    columns = set(frame.columns)
    source_name = "stock_name" if "stock_name" in columns else "name"
    if source_name not in columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("stock_name"))
        source_name = "stock_name"
    if "rank" not in columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("rank"))
    for column in (
        "source",
        "shares_source",
        "source_date",
        "source_market_value",
        "source_weight_pct",
        "price_date",
        "is_exact_price",
    ):
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias(column))

    return (
        _cast_dates(frame, ["date", "source_date", "price_date"])
        .with_columns(
            _nps_ticker_expr("stock_code").alias("stock_code"),
            pl.col(source_name).cast(pl.String).str.strip_chars().alias("stock_name"),
            pl.col("shares").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("ownership_pct").cast(pl.Float64, strict=False),
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("market_value").cast(pl.Float64, strict=False),
            pl.col("change_pct").cast(pl.Float64, strict=False),
            pl.col("rank").cast(pl.Int64, strict=False).alias("source_rank"),
            pl.col("source").cast(pl.String),
            pl.col("source_market_value").cast(pl.Float64, strict=False),
            pl.col("source_weight_pct").cast(pl.Float64, strict=False),
            pl.col("shares_source").cast(pl.String),
            pl.col("is_exact_price").cast(pl.Boolean, strict=False).fill_null(False),
        )
        .filter(
            pl.col("date").is_not_null()
            & pl.col("stock_code").is_not_null()
            & (pl.col("stock_code").str.len_chars() > 0)
        )
    )


def _normalize_price_frame(frame: pl.DataFrame, source: str) -> pl.DataFrame:
    frame = _cast_dates(frame, ["date"])
    if "name" not in frame.columns:
        frame = frame.with_columns(pl.col("ticker").alias("name"))
    return frame.with_columns(
        _ticker_expr("ticker").alias("ticker"),
        pl.concat_str([pl.lit("S"), _ticker_expr("ticker")]).alias("security_id"),
        pl.concat_str([pl.lit("L"), _ticker_expr("ticker")]).alias("listing_id"),
        pl.lit(source).alias("price_source"),
        pl.lit(_price_basis_for_source(source)).alias("price_basis"),
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
            "price_basis",
            "is_halted",
            "is_designated",
            "is_liquidation_window",
        ]
    )


def _price_basis_for_source(source: str) -> str:
    # Naver siseJson returns split-adjusted prices; KRX/KIS return raw exchange
    # prices. pre2010 provenance is mixed, so it is never factor-multiplied.
    return {"naver": "adjusted", "pre2010": "unknown"}.get(source, "raw")


def _deduplicate_price_candidates(prices: pl.DataFrame) -> pl.DataFrame:
    if prices.height > 1_000_000:
        return _deduplicate_large_price_candidates(prices)

    duplicate_keys = (
        prices.group_by(["date", "ticker"])
        .agg(pl.len().alias("_candidate_count"))
        .filter(pl.col("_candidate_count") > 1)
        .select("date", "ticker")
    )
    if duplicate_keys.is_empty():
        return prices.sort(["date", "ticker", "price_source"])

    singletons = prices.join(duplicate_keys, on=["date", "ticker"], how="anti")
    duplicates = (
        prices.join(duplicate_keys, on=["date", "ticker"], how="semi")
        .with_row_index("_candidate_id")
        .with_columns(_price_source_priority_expr().alias("_source_priority"))
    )

    duplicate_tickers = duplicates.select("ticker").unique()
    anchors = (
        singletons.join(duplicate_tickers, on="ticker", how="semi")
        .select(
            "ticker",
            pl.col("date").alias("_anchor_date"),
            pl.col("close").alias("_anchor_close"),
        )
        .sort(["ticker", "_anchor_date"])
    )
    if anchors.is_empty():
        selected = duplicates.sort(["date", "ticker", "_source_priority", "price_source"])
    else:
        duplicate_keys = duplicates.select("_candidate_id", "ticker", "date", "close").sort(
            ["ticker", "date"]
        )
        # Both sides are sorted by (ticker, date) above; polars cannot verify
        # per-group sortedness itself, so opt out of the noisy runtime check.
        previous = duplicate_keys.join_asof(
            anchors,
            left_on="date",
            right_on="_anchor_date",
            by="ticker",
            strategy="backward",
            **_JOIN_ASOF_KWARGS,
        ).select("_candidate_id", pl.col("_anchor_close").alias("_previous_close"))
        following = duplicate_keys.join_asof(
            anchors,
            left_on="date",
            right_on="_anchor_date",
            by="ticker",
            strategy="forward",
            **_JOIN_ASOF_KWARGS,
        ).select("_candidate_id", pl.col("_anchor_close").alias("_following_close"))
        selected = (
            duplicates.join(previous, on="_candidate_id", how="left")
            .join(following, on="_candidate_id", how="left")
            .with_columns(
                _continuity_penalty("close", "_previous_close").alias("_previous_penalty"),
                _continuity_penalty("close", "_following_close").alias("_following_penalty"),
            )
            .with_columns(
                pl.sum_horizontal(
                    pl.col("_previous_penalty").fill_null(0.0),
                    pl.col("_following_penalty").fill_null(0.0),
                ).alias("_continuity_score"),
                (
                    pl.col("_previous_penalty").is_not_null().cast(pl.Int8)
                    + pl.col("_following_penalty").is_not_null().cast(pl.Int8)
                ).alias("_anchor_count"),
            )
            .sort(
                ["date", "ticker", "_anchor_count", "_continuity_score", "_source_priority"],
                descending=[False, False, True, False, False],
            )
        )

    selected = selected.unique(subset=["date", "ticker"], keep="first")
    return (
        pl.concat([singletons, selected], how="diagonal_relaxed")
        .drop(
            [
                column
                for column in [
                    "_candidate_count",
                    "_source_priority",
                    "_candidate_id",
                    "_previous_close",
                    "_following_close",
                    "_previous_penalty",
                    "_following_penalty",
                    "_continuity_score",
                    "_anchor_count",
                ]
                if column in selected.columns or column in singletons.columns
            ]
        )
        .sort(["date", "ticker", "price_source"])
    )


def _deduplicate_large_price_candidates(prices: pl.DataFrame) -> pl.DataFrame:
    return (
        prices.with_columns(_price_source_priority_expr().alias("_source_priority"))
        .sort(
            ["date", "ticker", "close", "_source_priority", "price_source"],
            descending=[False, False, True, False, False],
        )
        .unique(subset=["date", "ticker"], keep="first")
        .drop("_source_priority")
        .sort(["date", "ticker", "price_source"])
    )


def _select_preferred_price_sources(prices: pl.DataFrame) -> pl.DataFrame:
    return (
        prices.with_columns(_price_source_priority_expr().alias("_source_priority"))
        .sort(["date", "ticker", "_source_priority", "price_source"])
        .unique(subset=["date", "ticker"], keep="first")
        .drop("_source_priority")
        .sort(["date", "ticker", "price_source"])
    )


def _price_source_priority_expr() -> pl.Expr:
    return (
        pl.when(pl.col("price_source") == "krx")
        .then(0)
        .when(pl.col("price_source") == "kis")
        .then(1)
        .when(pl.col("price_source") == "naver")
        .then(2)
        .when(pl.col("price_source") == "pre2010")
        .then(3)
        .otherwise(9)
    )


def _continuity_penalty(close_column: str, anchor_column: str) -> pl.Expr:
    return (
        pl.when(
            pl.col(close_column).is_not_null()
            & pl.col(anchor_column).is_not_null()
            & (pl.col(close_column) > 0)
            & (pl.col(anchor_column) > 0)
        )
        .then((pl.col(close_column) / pl.col(anchor_column)).log().abs())
        .otherwise(None)
    )


def _normalize_marcap_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        _cast_dates(frame, ["Date"])
        .with_columns(
            _ticker_expr("Code").alias("ticker"),
            pl.concat_str([pl.lit("S"), _ticker_expr("Code")]).alias("security_id"),
            pl.concat_str([pl.lit("L"), _ticker_expr("Code")]).alias("listing_id"),
            pl.col("Name").cast(pl.String).alias("name"),
            pl.col("Market").cast(pl.String).alias("market"),
            pl.col("Rank").cast(pl.Int64, strict=False).alias("rank"),
            pl.col("Close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("Amount").cast(pl.Int64, strict=False).alias("trading_value"),
            pl.col("Volume").cast(pl.Int64, strict=False).alias("volume"),
            pl.col("Marcap").cast(pl.Int64, strict=False).alias("market_cap"),
            pl.col("Stocks").cast(pl.Int64, strict=False).alias("listed_shares"),
            pl.lit("marcap").alias("market_cap_source"),
            pl.lit(False).alias("is_estimated"),
        )
        .select(
            [
                pl.col("Date").alias("date"),
                "security_id",
                "listing_id",
                "ticker",
                "name",
                "market",
                "rank",
                "close",
                "trading_value",
                "volume",
                "market_cap",
                "listed_shares",
                "market_cap_source",
                "is_estimated",
            ]
        )
        .filter(
            pl.col("date").is_not_null()
            & pl.col("ticker").is_not_null()
            & pl.col("market_cap").is_not_null()
            & pl.col("listed_shares").is_not_null()
        )
        .unique(subset=["date", "ticker"], keep="last")
        .sort(["date", "ticker"])
    )


def _read_optional(pattern: Path, columns: list[str] | None = None) -> pl.DataFrame | None:
    """Read every file matching ``pattern``, or None when the dataset is absent.

    ``columns`` prunes the scan to the requested columns (intersected with the
    dataset schema, so optional columns may be silently absent) before
    collecting, keeping memory proportional to what the caller consumes.
    """
    files = sorted(glob(pattern.as_posix()))
    if not files:
        return None
    try:
        lazy = pl.scan_parquet(files, hive_partitioning=True)
        if columns is not None:
            available = lazy.collect_schema().names()
            lazy = lazy.select([column for column in columns if column in available])
        return lazy.collect()
    except (pl.exceptions.SchemaError, pl.exceptions.ColumnNotFoundError):
        frames = [pl.read_parquet(file, hive_partitioning=True) for file in files]
        frame = pl.concat(frames, how="diagonal_relaxed")
        if columns is not None:
            frame = frame.select([column for column in columns if column in frame.columns])
        return frame


def _read_partition_dates(
    data_root: Path,
    dataset: str,
    dates: set[date],
) -> pl.DataFrame | None:
    layout = DataLakeLayout(data_root)
    files = [layout.partition_path(dataset, value) for value in dates]
    existing = [path for path in files if path.exists()]
    if not existing:
        return None
    return pl.concat(
        [pl.read_parquet(path, hive_partitioning=True) for path in existing],
        how="diagonal_relaxed",
    )


def _read_price_source_dates(
    data_root: Path,
    source: str,
    dates: set[date],
) -> pl.DataFrame | None:
    layout = DataLakeLayout(data_root)
    if source == "krx":
        return _read_existing_paths(
            [layout.partition_path("bronze.krx_daily_raw", value) for value in dates]
        )
    if source == "kis":
        return _read_existing_paths(
            [layout.partition_path("bronze.kis_daily_raw", value) for value in dates]
        )
    if source == "pre2010":
        paths = [
            Path(file)
            for value in dates
            for file in glob(
                (
                    data_root / f"bronze/pre2010/source=*/dt={value.isoformat()}/part.parquet"
                ).as_posix()
            )
        ]
        return _read_existing_paths(paths)
    if source == "naver":
        return _read_naver_daily_dates(data_root, dates)
    raise ValueError(f"unknown price source: {source}")


def _read_naver_daily_dates(data_root: Path, dates: set[date]) -> pl.DataFrame | None:
    # Naver bronze chunks are partitioned by REQUEST date, so backfilled rows for
    # an older data date can live under any request_dt: scan them all and filter
    # by the data date column instead of globbing request_dt={date}.
    files = sorted(
        glob((data_root / "bronze/naver_daily/request_dt=*/chunk=*/part.parquet").as_posix())
    )
    if not files:
        return None
    selected = sorted(dates)
    try:
        return (
            pl.scan_parquet(files, hive_partitioning=False)
            .with_columns(pl.col("date").cast(pl.Date, strict=False))
            .filter(pl.col("date").is_in(selected))
            .collect()
        )
    except pl.exceptions.SchemaError:
        frames = [pl.read_parquet(file, hive_partitioning=False) for file in files]
        frame = pl.concat(frames, how="diagonal_relaxed")
        return _cast_dates(frame, ["date"]).filter(pl.col("date").is_in(selected))


def _read_existing_paths(paths: Iterable[Path]) -> pl.DataFrame | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return pl.concat(
        [pl.read_parquet(path, hive_partitioning=True) for path in existing],
        how="diagonal_relaxed",
    )


def _latest_company_frame(data_root: Path) -> pl.DataFrame | None:
    frame = _read_optional(
        data_root / "bronze/dart_company/snapshot_dt=*/part.parquet",
        columns=["snapshot_dt", "stock_code", "corp_code", "corp_name"],
    )
    if frame is None or frame.is_empty():
        return None
    frame = _cast_dates(frame, ["snapshot_dt"])
    latest = frame["snapshot_dt"].max()
    return frame.filter(pl.col("snapshot_dt") == latest).with_columns(
        _ticker_expr("stock_code").alias("stock_code")
    )


def _naver_summary_frame(
    data_root: Path,
    dates: set[date] | None = None,
) -> pl.DataFrame | None:
    frame = (
        _read_partition_dates(data_root, "bronze.naver_summary_raw", dates)
        if dates is not None
        else _read_optional(
            data_root / "bronze/naver_summary/dt=*/part.parquet",
            columns=["snapshot_dt", "ticker", "name", "market", "market_cap", "listed_shares"],
        )
    )
    if frame is None or frame.is_empty():
        return None
    result = (
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
    if dates is not None:
        result = result.filter(pl.col("date").is_in(dates))
    return result


def _market_caps_frame(data_root: Path, dates: set[date] | None = None) -> pl.DataFrame | None:
    frame = (
        _read_partition_dates(data_root, "silver.market_caps", dates)
        if dates is not None
        else _read_optional(
            data_root / "silver/market_caps/dt=*/part.parquet",
            columns=[
                "date",
                "ticker",
                "name",
                "market",
                "trading_value",
                "market_cap",
                "listed_shares",
            ],
        )
    )
    if frame is None or frame.is_empty():
        return None
    result = frame.select(
        "date",
        "ticker",
        pl.col("name").alias("mc_name"),
        pl.col("market").alias("mc_market"),
        pl.col("trading_value").alias("mc_trading_value"),
        pl.col("market_cap").alias("mc_market_cap"),
        pl.col("listed_shares").alias("mc_listed_shares"),
    ).unique(subset=["date", "ticker"], keep="last")
    if dates is not None:
        result = result.filter(pl.col("date").is_in(dates))
    return result


def _delisting_cutoff(prices: pl.DataFrame) -> date | None:
    trading_dates = prices["date"].unique().sort().to_list()
    if len(trading_dates) <= DELISTING_BUFFER_TRADING_DAYS:
        return None
    return trading_dates[-(DELISTING_BUFFER_TRADING_DAYS + 1)]


def _security_master_from_prices(
    prices: pl.DataFrame,
    companies: pl.DataFrame | None,
    delisting_cutoff: date | None = None,
) -> pl.DataFrame:
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

    if delisting_cutoff is None:
        delisted_expr = pl.lit(None, dtype=pl.Date)
    else:
        delisted_expr = (
            pl.when(pl.col("last_seen_date") < delisting_cutoff)
            .then(pl.col("last_seen_date"))
            .otherwise(pl.lit(None, dtype=pl.Date))
        )
    return grouped.with_columns(
        pl.concat_str([pl.lit("I"), pl.coalesce(["corp_code", "ticker"])]).alias("issuer_id"),
        pl.concat_str([pl.lit("S"), pl.col("ticker")]).alias("security_id"),
        pl.concat_str([pl.lit("L"), pl.col("ticker")]).alias("listing_id"),
        pl.lit(None, dtype=pl.String).alias("isin"),
        pl.when(_preferred_expr())
        .then(pl.lit("preferred"))
        .otherwise(pl.lit("common"))
        .alias("share_class"),
        pl.when(pl.col("name").str.contains("스팩|SPAC", literal=False))
        .then(pl.lit("spac_pre"))
        .otherwise(pl.lit("equity"))
        .alias("security_type"),
        delisted_expr.alias("delisted_date"),
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
            pl.col("last_seen_date").alias("_last_seen_date"),
        ]
    )


def _merge_security_master(data_root: Path, updates: pl.DataFrame) -> pl.DataFrame:
    existing = _read_optional(data_root / "gold/security_master.parquet")
    if existing is None or existing.is_empty():
        return updates
    if updates.is_empty():
        return existing

    existing_updates = existing.join(
        updates.select(pl.col("ticker").alias("_update_ticker")),
        left_on="ticker",
        right_on="_update_ticker",
        how="semi",
    )
    if existing_updates.is_empty():
        return pl.concat([existing, updates], how="diagonal_relaxed").sort("ticker")

    adjusted_updates = (
        updates.join(
            existing_updates.select(
                "ticker",
                pl.col("listed_date").alias("_existing_listed_date"),
                pl.col("delisted_date").alias("_existing_delisted_date"),
            ),
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.min_horizontal("listed_date", "_existing_listed_date").alias("listed_date"),
            # Incremental updates only see the selected dates, so keep a known
            # delisted_date unless the update proves trading after it.
            pl.when(
                pl.col("delisted_date").is_null()
                & pl.col("_existing_delisted_date").is_not_null()
                & (pl.col("_last_seen_date") <= pl.col("_existing_delisted_date"))
            )
            .then(pl.col("_existing_delisted_date"))
            .otherwise(pl.col("delisted_date"))
            .alias("delisted_date"),
        )
        .drop("_existing_listed_date", "_existing_delisted_date")
    )
    unchanged = existing.join(
        updates.select(pl.col("ticker").alias("_update_ticker")),
        left_on="ticker",
        right_on="_update_ticker",
        how="anti",
    )
    return pl.concat([unchanged, adjusted_updates], how="diagonal_relaxed").sort("ticker")


def _load_corporate_actions(data_root: Path) -> pl.DataFrame | None:
    actions = _read_optional(
        data_root / "silver/corporate_actions/dt=*/part.parquet",
        columns=["security_id", "effective_date", "adjustment_factor"],
    )
    if actions is None or actions.is_empty():
        return None
    actions = (
        _cast_dates(actions, ["effective_date"])
        .filter(
            pl.col("security_id").is_not_null()
            & pl.col("effective_date").is_not_null()
            & pl.col("adjustment_factor").is_not_null()
            & (pl.col("adjustment_factor") > 0)
        )
        .select(
            "security_id",
            "effective_date",
            pl.col("adjustment_factor").cast(pl.Float64),
        )
        .sort(["security_id", "effective_date"])
        .unique(subset=["security_id", "effective_date"], keep="first")
    )
    return None if actions.is_empty() else actions


def _apply_corporate_action_factors(
    prices: pl.DataFrame,
    actions: pl.DataFrame | None,
) -> pl.DataFrame:
    if actions is None or actions.is_empty():
        return prices

    # adjustment_factor is a price multiplier applied to rows before effective_date.
    # For example, a 2-for-1 split uses 0.5, while a 1-for-21 reverse split uses 21
    # when the source prices are still raw. Only rows on a raw price basis are
    # multiplied: Naver history is already split-adjusted at fetch time, and
    # previous-gold-close rows injected for incremental builds are adjusted too.
    factors = (
        prices.with_row_index("_price_row")
        .select("_price_row", "date", "security_id")
        .join(actions, on="security_id", how="left")
        .with_columns(
            pl.when(pl.col("date") < pl.col("effective_date"))
            .then(pl.col("adjustment_factor"))
            .otherwise(1.0)
            .alias("_factor")
        )
        .group_by("_price_row")
        .agg(pl.col("_factor").product().alias("_price_adjustment_factor"))
    )
    return (
        prices.with_row_index("_price_row")
        .join(factors, on="_price_row", how="left")
        .with_columns(
            pl.when(_price_basis_expr(prices) == "raw")
            .then(pl.coalesce(["_price_adjustment_factor", pl.lit(1.0)]))
            .otherwise(1.0)
            .alias("_price_adjustment_factor")
        )
        .with_columns(
            (pl.col("open") * pl.col("_price_adjustment_factor")).alias("open"),
            (pl.col("high") * pl.col("_price_adjustment_factor")).alias("high"),
            (pl.col("low") * pl.col("_price_adjustment_factor")).alias("low"),
            (pl.col("close") * pl.col("_price_adjustment_factor")).alias("close"),
        )
        .drop("_price_row", "_price_adjustment_factor")
    )


def _price_basis_expr(prices: pl.DataFrame) -> pl.Expr:
    derived = (
        pl.when(pl.col("price_source") == "naver")
        .then(pl.lit("adjusted"))
        .when(pl.col("price_source") == "pre2010")
        .then(pl.lit("unknown"))
        .when(pl.col("price_source").is_null())
        .then(pl.lit("adjusted"))
        .otherwise(pl.lit("raw"))
    )
    if "price_basis" in prices.columns:
        return pl.coalesce([pl.col("price_basis"), derived])
    return derived


def _corporate_actions_state_path(data_root: Path) -> Path:
    return data_root / "_state" / "corporate_actions_applied.json"


def _corporate_action_event_keys(actions: pl.DataFrame | None) -> set[tuple[str, str, float]]:
    if actions is None or actions.is_empty():
        return set()
    return {
        (
            row["security_id"],
            row["effective_date"].isoformat(),
            round(row["adjustment_factor"], 9),
        )
        for row in actions.iter_rows(named=True)
    }


def _applied_corporate_action_keys(data_root: Path) -> set[tuple[str, str, float]]:
    path = _corporate_actions_state_path(data_root)
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        (
            event["security_id"],
            event["effective_date"],
            round(float(event["adjustment_factor"]), 9),
        )
        for event in payload.get("events", [])
    }


def _stale_adjustment_security_ids(
    data_root: Path,
    actions: pl.DataFrame | None,
) -> set[str]:
    changed = _corporate_action_event_keys(actions) ^ _applied_corporate_action_keys(data_root)
    return {security_id for security_id, _, _ in changed}


def _write_corporate_actions_state(data_root: Path, actions: pl.DataFrame | None) -> None:
    path = _corporate_actions_state_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        {"security_id": security_id, "effective_date": effective_date, "adjustment_factor": factor}
        for security_id, effective_date, factor in sorted(_corporate_action_event_keys(actions))
    ]
    path.write_text(json.dumps({"events": events}, indent=2), encoding="utf-8")


def _rebuild_adjusted_history(
    data_root: Path,
    security_ids: set[str],
    actions: pl.DataFrame | None,
) -> tuple[int, int]:
    ids = sorted(security_ids)
    prices = _read_silver_prices_for_securities(data_root, ids)
    if prices is None or prices.is_empty():
        return 0, 0
    adjusted = _adjusted_price_frame(prices, actions)
    layout = DataLakeLayout(data_root)
    writer = ParquetDatasetWriter()
    files = 0
    for value in adjusted["date"].unique().sort().to_list():
        partition = adjusted.filter(pl.col("date") == value)
        path = layout.partition_path("gold.daily_prices_adj", value)
        if path.exists():
            existing = pl.read_parquet(path, hive_partitioning=True)
            if "dt" in existing.columns:
                existing = existing.drop("dt")
            existing = existing.filter(~pl.col("security_id").is_in(ids))
            partition = pl.concat([existing, partition], how="diagonal_relaxed").sort(
                ["date", "security_id"]
            )
        writer.write(partition, path, mode="overwrite")
        files += 1
    return adjusted.height, files


def _read_silver_prices_for_securities(
    data_root: Path,
    security_ids: list[str],
) -> pl.DataFrame | None:
    files = sorted(glob((data_root / "silver/prices/dt=*/part.parquet").as_posix()))
    if not files:
        return None
    try:
        return (
            pl.scan_parquet(files, hive_partitioning=False)
            .filter(pl.col("security_id").is_in(security_ids))
            .collect()
        )
    except pl.exceptions.SchemaError:
        frames = [
            pl.read_parquet(file, hive_partitioning=False).filter(
                pl.col("security_id").is_in(security_ids)
            )
            for file in files
        ]
        return pl.concat(frames, how="diagonal_relaxed")


def _read_price_history_columns(data_root: Path, columns: list[str]) -> pl.DataFrame | None:
    files = sorted(glob((data_root / "silver/prices/dt=*/part.parquet").as_posix()))
    if not files:
        return None
    try:
        return pl.scan_parquet(files, hive_partitioning=False).select(columns).collect()
    except pl.exceptions.SchemaError:
        frames = [pl.read_parquet(file, columns=columns) for file in files]
        return pl.concat(frames, how="diagonal_relaxed")


def _with_previous_gold_close(
    data_root: Path,
    prices: pl.DataFrame,
    dates: set[date],
) -> pl.DataFrame:
    target = prices.filter(pl.col("date").is_in(dates))
    if target.is_empty():
        return target

    previous = _previous_gold_close_frame(data_root, min(dates))
    if previous is None or previous.is_empty():
        return target

    previous_prices = (
        previous.rename({"close_adj": "close"})
        .with_columns(
            pl.col("security_id").str.strip_prefix("S").alias("ticker"),
            pl.col("open_adj").alias("open"),
            pl.col("high_adj").alias("high"),
            pl.col("low_adj").alias("low"),
            pl.lit(None, dtype=pl.String).alias("name"),
            pl.lit(None, dtype=pl.String).alias("market"),
            pl.lit(None, dtype=pl.String).alias("price_source"),
            # Gold closes already carry every applied adjustment factor.
            pl.lit("adjusted").alias("price_basis"),
        )
        .select(target.columns)
    )
    return pl.concat([previous_prices, target], how="diagonal_relaxed")


def _previous_gold_close_frame(data_root: Path, before: date) -> pl.DataFrame | None:
    candidates: list[tuple[date, Path]] = []
    for file in glob((data_root / "gold/daily_prices_adj/dt=*/part.parquet").as_posix()):
        partition_date = _partition_date_from_path(Path(file), "dt")
        if partition_date < before:
            candidates.append((partition_date, Path(file)))
    if not candidates:
        return None
    _, path = max(candidates, key=lambda item: item[0])
    return pl.read_parquet(path, hive_partitioning=True)


def _price_market_caps_frame(
    data_root: Path,
    prices: pl.DataFrame | None = None,
) -> pl.DataFrame | None:
    frame = (
        prices
        if prices is not None
        else _read_optional(
            data_root / "silver/prices/dt=*/part.parquet",
            columns=_MARKET_CAP_PRICE_COLUMNS,
        )
    )
    if frame is None or frame.is_empty():
        return None
    return (
        frame.filter(pl.col("market_cap").is_not_null() & pl.col("listed_shares").is_not_null())
        .with_columns(
            pl.lit(None, dtype=pl.Int64).alias("rank"),
            pl.col("price_source").alias("market_cap_source"),
            pl.lit(False).alias("is_estimated"),
        )
        .select(
            [
                "date",
                "security_id",
                "listing_id",
                "ticker",
                "name",
                "market",
                "rank",
                "close",
                "trading_value",
                "volume",
                "market_cap",
                "listed_shares",
                "market_cap_source",
                "is_estimated",
            ]
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


def _enrich_prices_with_market_caps(
    prices: pl.DataFrame,
    market_caps: pl.DataFrame,
) -> pl.DataFrame:
    return (
        prices.join(market_caps, on=["date", "ticker"], how="left")
        .with_columns(
            pl.when(
                pl.col("mc_name").is_not_null()
                & (pl.col("name").is_null() | (_ticker_expr("name") == pl.col("ticker")))
            )
            .then(pl.col("mc_name"))
            .otherwise(pl.col("name"))
            .alias("name"),
            pl.coalesce(["market", "mc_market"]).alias("market"),
            pl.coalesce(["trading_value", "mc_trading_value"]).alias("trading_value"),
            pl.coalesce(["market_cap", "mc_market_cap"]).alias("market_cap"),
            pl.coalesce(["listed_shares", "mc_listed_shares"]).alias("listed_shares"),
        )
        .drop(["mc_name", "mc_market", "mc_trading_value", "mc_market_cap", "mc_listed_shares"])
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


def _nps_ticker_expr(column: str) -> pl.Expr:
    normalized = pl.col(column).cast(pl.String).str.strip_chars().str.to_uppercase()
    return (
        pl.when(normalized.str.contains(r"^\d+$"))
        .then(normalized.str.zfill(6))
        .otherwise(normalized)
    )


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
    selected = set(values)
    # Single partition pass instead of one full-frame filter per value.
    partitions = frame.partition_by(partition_column, as_dict=True)
    for key in sorted(key for key in partitions if key[0] is not None and key[0] in selected):
        value = key[0]
        partition = partitions[key]
        if dataset in {"silver.financials", "silver.dividends"}:
            name = dataset.split(".", maxsplit=1)[1]
            path = data_root / "silver" / name / f"fiscal_year={value}" / "part.parquet"
            partition = partition.drop(partition_column)
        else:
            path = layout.partition_path(dataset, value)
        writer.write(partition, path, mode="overwrite")
        files += 1
        rows += partition.height
    return [BuildSummary(dataset, rows, files)]
