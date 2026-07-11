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

from finance_pi.events.classify import classify_filing_events
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter

logger = logging.getLogger(__name__)

# A security whose last price row is more than this many trading days before the
# newest silver.prices date is considered delisted as of its last seen date.
DELISTING_BUFFER_TRADING_DAYS = 10

# KRX reuses ticker codes; a gap of more than this many observed trading days
# between consecutive price rows of one ticker flags a possible reuse for
# manual identity review (security_id semantics stay ticker-derived).
TICKER_REUSE_GAP_TRADING_DAYS = 250

# Full-history silver.prices rebuilds (dates=None) process the bronze sources
# in date chunks spanning at most this many calendar days (~one trading year),
# so peak memory stays proportional to a chunk instead of the multi-decade
# union (which OOM-killed a 16GB host at ~15M rows).
SILVER_PRICES_REBUILD_CHUNK_DAYS = 366

# Increment when PIT grouping/selection semantics change so stale partitions
# cannot survive merely because input row counts stayed constant.
FUNDAMENTALS_PIT_STATE_VERSION = 2

# Rows for this many extra trading days on each side of a chunk are loaded as
# dedup context but never written: _deduplicate_price_candidates anchors
# duplicate (date, ticker) candidates against the nearest singleton row per
# ticker via join_asof, and a duplicate at a chunk edge may anchor into the
# neighbouring chunk. A duplicate whose nearest anchor sits further away than
# this many trading days can score differently from a one-shot rebuild; that
# far-off an anchor carries little continuity signal anyway.
SILVER_PRICES_REBUILD_CONTEXT_TRADING_DAYS = 10

# Full gold.daily_market_caps builds chunk the new (not yet written) dates the
# same way; the merge is date-local, so chunking cannot change the output.
DAILY_MARKET_CAPS_REBUILD_CHUNK_DAYS = 366

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

# Preferred discount: trailing window (observed pair trading days) for the
# rolling mean/std of the pair discount, and the minimum observations before
# the stats (and z-score) are emitted instead of null.
PREFERRED_DISCOUNT_WINDOW = 252
PREFERRED_DISCOUNT_MIN_OBS = 120

# NPS holdings delta PIT stamping. The silver.nps_holdings snapshot ``date`` is
# the ingest capture (logical) date of already-public sources (the data.go.kr
# public dataset, the FnGuide institutional page, or the legacy value-invest
# SQLite crawl) — no official filing/publication date is captured, and
# ``source_date`` is only the disclosure's valuation reference date. Live
# crawls were public on the capture day itself, but legacy backfilled rows may
# carry a reference date instead of a crawl date, so ``available_date`` adds
# this conservative calendar-day buffer (~5 trading days) to the LATER
# snapshot's date before a delta becomes knowable.
NPS_DISCLOSURE_LAG_DAYS = 7

# KRX preferred tickers reuse the common ticker's first 5 digits with a
# non-zero 6th character: 5/7/9 for legacy classes, or an uppercase letter
# (e.g. K for 신형우선주). Anything else (fully alphanumeric short codes,
# non-6-char codes) is excluded conservatively.
_COMMON_TICKER_PATTERN = r"^\d{5}0$"
_PREFERRED_TICKER_PATTERN = r"^\d{5}[1-9A-Z]$"
# Preferred name = common name + suffix such as 우, 우B, 1우, 2우B, 우선주
# (whitespace ignored on both sides).
_PREFERRED_NAME_SUFFIX_PATTERN = r"^\d?우(선주)?[A-Z]?$"

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
_RELATION_PRICE_COLUMNS = ["date", "ticker"]
_DISCOUNT_PRICE_COLUMNS = ["date", "security_id", "close_adj"]
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
    "rcept_no",
    "report_type",
    "statement_division",
    "statement_name",
    "account_id",
    "account_name",
    "account_detail",
    "amount",
    "amount_basis",
    "current_period_name",
    "current_amount",
    "cumulative_amount",
    "prior_period_name",
    "prior_amount",
    "prior_cumulative_amount",
    "two_year_prior_period_name",
    "two_year_prior_amount",
    "sort_order",
    "currency",
    "unit",
    "is_consolidated",
    "accounting_basis",
    "is_backfilled",
]
_FILINGS_BRONZE_COLUMNS = [
    "rcept_dt",
    "corp_code",
    "corp_name",
    "stock_code",
    "rcept_no",
    "report_nm",
    "rm",
]
_FILINGS_SILVER_COLUMNS = [
    "rcept_no",
    "rcept_dt",
    "available_date",
    "stock_code",
    "security_id",
    "report_nm",
    "is_correction",
]

# Bronze price sources feeding silver.prices, in priority order (see
# _price_source_priority_expr for how ties are broken at selection time).
_BRONZE_PRICE_SOURCES: list[tuple[str, str]] = [
    ("krx", "bronze/krx_daily/dt=*/part.parquet"),
    ("kis", "bronze/kis_daily/dt=*/part.parquet"),
    ("naver", "bronze/naver_daily/request_dt=*/chunk=*/part.parquet"),
    ("pre2010", "bronze/pre2010/source=*/dt=*/part.parquet"),
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
    # Each builder re-reads silver.prices itself (prices=None) with its own
    # pruned column set, so at most one builder's working set is resident at a
    # time. Sharing one all-column frame here pinned the entire multi-GB
    # history in RAM underneath every downstream builder and OOMed a 16GB
    # host; the ``prices`` keywords stay for callers that already hold a frame.
    yield from build_security_master(data_root)
    yield from build_security_relations(data_root)
    yield from build_universe_history(data_root)
    yield from build_corporate_actions(data_root)
    yield from build_daily_prices_adj(data_root)
    yield from build_daily_market_caps(data_root)
    yield from build_preferred_discount(data_root)
    yield from build_nps_holdings_silver(data_root)
    yield from build_nps_universe(data_root)
    yield from build_nps_holdings_delta(data_root)
    yield from build_financials_silver(data_root)
    yield from build_filings_silver(data_root)
    yield from build_filing_events(data_root)
    yield from build_fundamentals_pit(data_root)


def build_silver_prices(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    if dates is None:
        return _build_silver_prices_full(data_root)
    selected_dates = set(dates)
    frames: list[pl.DataFrame] = []
    naver_summary = _naver_summary_frame(data_root, selected_dates)
    market_caps = _market_caps_frame(data_root, selected_dates)
    for source, _pattern in _BRONZE_PRICE_SOURCES:
        frame = _read_price_source_dates(data_root, source, selected_dates)
        if frame is not None and not frame.is_empty():
            frames.append(_prepare_price_candidates(frame, source, naver_summary, market_caps))
    if not frames:
        return [BuildSummary("silver.prices", 0, 0)]

    prices = _select_preferred_price_sources(pl.concat(frames, how="diagonal_relaxed"))
    return _write_by_date(data_root, "silver.prices", prices, "date")


def _build_silver_prices_full(data_root: Path) -> list[BuildSummary]:
    """Full-history rebuild of silver.prices in memory-bounded date chunks.

    The distinct data dates across all bronze price sources are split into
    chunks of at most ``SILVER_PRICES_REBUILD_CHUNK_DAYS`` calendar days. Each
    chunk loads its own rows plus ``SILVER_PRICES_REBUILD_CONTEXT_TRADING_DAYS``
    surrounding trading days of context (so duplicate candidates near a chunk
    edge keep their join_asof continuity anchors) but writes only its own
    dates, keeping peak memory proportional to one chunk instead of the whole
    history. Source selection and enrichment are date-local, so the chunked
    output matches a one-shot rebuild row for row.
    """
    all_dates = _bronze_price_source_dates(data_root)
    if not all_dates:
        return [BuildSummary("silver.prices", 0, 0)]
    chunks = _chunk_dates_by_span(all_dates, SILVER_PRICES_REBUILD_CHUNK_DAYS)
    context = SILVER_PRICES_REBUILD_CONTEXT_TRADING_DAYS
    logger.info(
        "silver.prices: full rebuild over %d trading dates in %d chunk(s)",
        len(all_dates),
        len(chunks),
    )
    rows = 0
    files = 0
    position = 0
    for index, chunk in enumerate(chunks, start=1):
        start, end = position, position + len(chunk)
        position = end
        load_dates = [
            *all_dates[max(0, start - context) : start],
            *chunk,
            *all_dates[end : end + context],
        ]
        load_set = set(load_dates)
        naver_summary = _naver_summary_frame(data_root, load_set)
        market_caps = _market_caps_frame(data_root, load_set)
        frames: list[pl.DataFrame] = []
        for source, pattern in _BRONZE_PRICE_SOURCES:
            frame = _read_price_source_chunk(data_root / pattern, load_dates)
            if frame is not None and not frame.is_empty():
                frames.append(
                    _prepare_price_candidates(frame, source, naver_summary, market_caps)
                )
        del naver_summary, market_caps
        if not frames:
            continue
        prices = _select_preferred_price_sources(pl.concat(frames, how="diagonal_relaxed"))
        del frames
        prices = prices.filter(pl.col("date").is_in(chunk))
        summary = _write_by_date(data_root, "silver.prices", prices, "date")[0]
        rows += summary.rows
        files += summary.files
        logger.info(
            "silver.prices: chunk %d/%d (%s..%s) wrote %d rows across %d partitions",
            index,
            len(chunks),
            chunk[0].isoformat(),
            chunk[-1].isoformat(),
            summary.rows,
            summary.files,
        )
    return [BuildSummary("silver.prices", rows, files)]


def _prepare_price_candidates(
    frame: pl.DataFrame,
    source: str,
    naver_summary: pl.DataFrame | None,
    market_caps: pl.DataFrame | None,
) -> pl.DataFrame:
    prices = _normalize_price_frame(frame, source)
    if naver_summary is not None:
        prices = _enrich_prices_with_naver(prices, naver_summary)
    if market_caps is not None:
        prices = _enrich_prices_with_market_caps(prices, market_caps)
    return _deduplicate_price_candidates(prices)


def build_silver_market_caps(data_root: Path) -> list[BuildSummary]:
    files = sorted(glob((data_root / "bronze/marcap/year=*/part.parquet").as_posix()))
    if not files:
        return [BuildSummary("silver.market_caps", 0, 0)]
    rows = 0
    written = 0
    # One bronze marcap file per calendar year: normalizing and writing per
    # file keeps peak memory at one year of rows instead of the full history,
    # and the (date, ticker) dedupe cannot span year files because each
    # calendar date lives in exactly one of them.
    for file in files:
        frame = pl.read_parquet(file, hive_partitioning=True)
        frame = frame.select([column for column in _MARCAP_COLUMNS if column in frame.columns])
        if frame.is_empty():
            continue
        market_caps = _normalize_marcap_frame(frame)
        summary = _write_by_date(data_root, "silver.market_caps", market_caps, "date")[0]
        rows += summary.rows
        written += summary.files
        logger.info(
            "silver.market_caps: %s wrote %d rows across %d partitions",
            Path(file).parent.name,
            summary.rows,
            summary.files,
        )
    return [BuildSummary("silver.market_caps", rows, written)]


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
    summaries = [BuildSummary("gold.security_master", master.height, len(paths))]
    if selected_dates is None:
        summaries.extend(_write_identity_review(data_root, prices))
    return summaries


def _write_identity_review(data_root: Path, prices: pl.DataFrame) -> list[BuildSummary]:
    """Detect ticker-reuse candidates and write them to gold.identity_review.

    KRX reuses delisted ticker codes and ``security_id`` is derived from the
    ticker, so a reused code silently chains two different companies. This
    only surfaces the suspects for review — identity semantics are unchanged.
    Detection needs the full price history, so incremental (per-date) master
    builds skip it.
    """
    review = _detect_ticker_reuse(prices)
    if not review.is_empty():
        logger.warning(
            "gold.identity_review: %d possible ticker reuse gap(s) over %d trading days detected",
            review.height,
            TICKER_REUSE_GAP_TRADING_DAYS,
        )
    ParquetDatasetWriter().write(
        review,
        DataLakeLayout(data_root).singleton_path("gold.identity_review"),
        mode="overwrite",
    )
    return [BuildSummary("gold.identity_review", review.height, 1)]


def _detect_ticker_reuse(prices: pl.DataFrame) -> pl.DataFrame:
    axis = (
        prices.select(pl.col("date").unique().sort())
        .with_row_index("_day_index")
        .with_columns(pl.col("_day_index").cast(pl.Int64))
    )
    return (
        prices.select("date", "ticker", "name")
        .unique(subset=["ticker", "date"], keep="last")
        .join(axis, on="date", how="left")
        .sort(["ticker", "date"])
        .with_columns(
            pl.col("date").shift(1).over("ticker").alias("gap_start"),
            pl.col("name").shift(1).over("ticker").alias("name_before"),
            pl.col("_day_index").shift(1).over("ticker").alias("_prev_day_index"),
        )
        .filter(
            (pl.col("_day_index") - pl.col("_prev_day_index")) > TICKER_REUSE_GAP_TRADING_DAYS
        )
        .select(
            "ticker",
            "gap_start",
            pl.col("date").alias("gap_end"),
            "name_before",
            pl.col("name").alias("name_after"),
            (pl.col("name_before") != pl.col("name")).fill_null(False).alias("name_changed"),
        )
        .sort(["ticker", "gap_start"])
    )


def build_security_relations(
    data_root: Path,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    """Map preferred shares to their common share into silver.security_relations.

    Candidates come from gold.security_master: a preferred-class ticker
    matching ``^\\d{5}[1-9A-Z]$`` pairs with the common-class ticker sharing
    its first five digits (6th char ``0``). ``confidence`` is ``"high"`` when
    the preferred name also equals the common name plus a recognised suffix
    (우/우B/1우/2우B/우선주 …, whitespace ignored), and ``"low"`` when only
    the ticker prefix matches — still emitted but flagged for review.
    ``first_seen_date``/``last_seen_date`` are the overlap of both legs'
    observed silver.prices trading ranges (null when the legs never overlap
    or prices are unavailable). The dataset is a singleton parquet rebuilt
    from the full history, like gold.security_master.
    """
    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is None or master.is_empty():
        return [BuildSummary("silver.security_relations", 0, 0)]
    if prices is None:
        prices = _read_optional(
            data_root / "silver/prices/dt=*/part.parquet",
            columns=_RELATION_PRICE_COLUMNS,
        )
    relations = _preferred_relations_frame(master, prices)
    ParquetDatasetWriter().write(
        relations,
        DataLakeLayout(data_root).singleton_path("silver.security_relations"),
        mode="overwrite",
    )
    return [BuildSummary("silver.security_relations", relations.height, 1)]


def _preferred_relations_frame(
    master: pl.DataFrame,
    prices: pl.DataFrame | None,
) -> pl.DataFrame:
    securities = master.select(
        "security_id",
        pl.col("ticker").cast(pl.String),
        pl.col("name").cast(pl.String),
        "share_class",
    )
    common = securities.filter(
        (pl.col("share_class") == "common")
        & pl.col("ticker").str.contains(_COMMON_TICKER_PATTERN)
    ).select(
        pl.col("ticker").str.slice(0, 5).alias("_ticker_prefix"),
        pl.col("security_id").alias("common_security_id"),
        pl.col("ticker").alias("common_ticker"),
        pl.col("name").alias("_common_name"),
    )
    preferred = securities.filter(
        (pl.col("share_class") == "preferred")
        & pl.col("ticker").str.contains(_PREFERRED_TICKER_PATTERN)
    ).select(
        pl.col("ticker").str.slice(0, 5).alias("_ticker_prefix"),
        pl.col("security_id").alias("preferred_security_id"),
        pl.col("ticker").alias("preferred_ticker"),
        pl.col("name").alias("preferred_name"),
    )
    common_name = pl.col("_common_name").str.replace_all(r"\s+", "")
    preferred_name = pl.col("preferred_name").str.replace_all(r"\s+", "")
    name_matches = preferred_name.str.starts_with(common_name) & (
        preferred_name.str.strip_prefix(common_name).str.contains(
            _PREFERRED_NAME_SUFFIX_PATTERN
        )
    )
    pairs = (
        preferred.join(common, on="_ticker_prefix", how="inner")
        .with_columns(
            pl.when(name_matches.fill_null(False))
            .then(pl.lit("high"))
            .otherwise(pl.lit("low"))
            .alias("confidence"),
            pl.lit("preferred_of").alias("relation_type"),
        )
        .sort(["preferred_ticker", "confidence"])
        .unique(subset=["preferred_security_id"], keep="first")
    )
    return (
        _with_relation_span(pairs, prices)
        .select(
            [
                "common_security_id",
                "preferred_security_id",
                "common_ticker",
                "preferred_ticker",
                "preferred_name",
                "relation_type",
                "confidence",
                "first_seen_date",
                "last_seen_date",
            ]
        )
        .sort("preferred_ticker")
    )


def _with_relation_span(pairs: pl.DataFrame, prices: pl.DataFrame | None) -> pl.DataFrame:
    if prices is None or prices.is_empty():
        return pairs.with_columns(
            pl.lit(None, dtype=pl.Date).alias("first_seen_date"),
            pl.lit(None, dtype=pl.Date).alias("last_seen_date"),
        )
    spans = prices.group_by("ticker").agg(
        pl.col("date").min().alias("_first_date"),
        pl.col("date").max().alias("_last_date"),
    )
    overlapping = pl.col("first_seen_date") <= pl.col("last_seen_date")
    return (
        pairs.join(
            spans.select(
                pl.col("ticker").alias("preferred_ticker"),
                pl.col("_first_date").alias("_preferred_first"),
                pl.col("_last_date").alias("_preferred_last"),
            ),
            on="preferred_ticker",
            how="left",
        )
        .join(
            spans.select(
                pl.col("ticker").alias("common_ticker"),
                pl.col("_first_date").alias("_common_first"),
                pl.col("_last_date").alias("_common_last"),
            ),
            on="common_ticker",
            how="left",
        )
        .with_columns(
            pl.max_horizontal("_preferred_first", "_common_first").alias("first_seen_date"),
            pl.min_horizontal("_preferred_last", "_common_last").alias("last_seen_date"),
        )
        .with_columns(
            pl.when(overlapping).then(pl.col("first_seen_date")).alias("first_seen_date"),
            pl.when(overlapping).then(pl.col("last_seen_date")).alias("last_seen_date"),
        )
        .drop("_preferred_first", "_preferred_last", "_common_first", "_common_last")
    )


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


def build_nps_holdings_delta(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    """Per-security ownership change between consecutive NPS disclosure snapshots.

    One row per security per consecutive snapshot pair: ``delta_ratio =
    holding_ratio - prev_holding_ratio`` (silver ``ownership_pct``). A security
    disclosed for the first time gets ``entered=True`` (previous ratio treated
    as 0); a security that disappears gets an ``exited=True`` row with
    ``holding_ratio`` 0 — NPS selling out is itself a signal. PIT stamping: a
    delta is knowable only once the LATER snapshot became public, so
    ``available_date = snapshot_date + NPS_DISCLOSURE_LAG_DAYS`` (see the
    constant for the evidence behind that assumption). Consumers must apply
    the same strict ``available_date < as_of`` policy as gold.fundamentals_pit.

    ``dates`` is accepted for orchestrator symmetry only: each delta depends on
    the previous snapshot and the dataset is tiny (one row set per disclosure
    snapshot), so every invocation performs a full rebuild.
    """
    del dates  # deltas need consecutive snapshots; a full rebuild is cheap.
    holdings = _read_optional(
        data_root / "silver/nps_holdings/dt=*/part.parquet",
        columns=["date", "security_id", "stock_code", "ownership_pct"],
    )
    if holdings is None or holdings.is_empty():
        return [BuildSummary("gold.nps_holdings_delta", 0, 0)]
    delta = _nps_holdings_delta_frame(holdings)
    if delta.is_empty():
        return [BuildSummary("gold.nps_holdings_delta", 0, 0)]
    return _write_by_date(data_root, "gold.nps_holdings_delta", delta, "snapshot_date")


def _nps_holdings_delta_frame(holdings: pl.DataFrame) -> pl.DataFrame:
    holdings = (
        _cast_dates(holdings, ["date"])
        .filter(pl.col("date").is_not_null() & pl.col("security_id").is_not_null())
        .with_columns(pl.col("ownership_pct").cast(pl.Float64, strict=False).fill_null(0.0))
        .unique(subset=["date", "security_id"], keep="last")
    )
    snapshot_dates = holdings["date"].unique().sort().to_list()
    if len(snapshot_dates) < 2:
        return pl.DataFrame()
    pairs = pl.DataFrame(
        {
            "snapshot_date": snapshot_dates[1:],
            "prev_snapshot_date": snapshot_dates[:-1],
        },
        schema={"snapshot_date": pl.Date, "prev_snapshot_date": pl.Date},
    )
    current = holdings.select(
        pl.col("date").alias("snapshot_date"),
        "security_id",
        pl.col("stock_code").alias("ticker"),
        pl.col("ownership_pct").alias("holding_ratio"),
    ).join(pairs, on="snapshot_date", how="inner")
    previous = holdings.select(
        pl.col("date").alias("prev_snapshot_date"),
        "security_id",
        pl.col("stock_code").alias("prev_ticker"),
        pl.col("ownership_pct").alias("prev_holding_ratio"),
    ).join(pairs, on="prev_snapshot_date", how="inner")
    return (
        current.join(
            previous,
            on=["snapshot_date", "prev_snapshot_date", "security_id"],
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.col("prev_holding_ratio").is_null().alias("entered"),
            pl.col("holding_ratio").is_null().alias("exited"),
            pl.coalesce(["ticker", "prev_ticker"]).alias("ticker"),
            pl.col("holding_ratio").fill_null(0.0),
            pl.col("prev_holding_ratio").fill_null(0.0),
        )
        .with_columns(
            (pl.col("holding_ratio") - pl.col("prev_holding_ratio")).alias("delta_ratio"),
            pl.col("snapshot_date")
            .dt.offset_by(f"{NPS_DISCLOSURE_LAG_DAYS}d")
            .alias("available_date"),
        )
        .select(
            [
                "security_id",
                "ticker",
                "snapshot_date",
                "prev_snapshot_date",
                "available_date",
                "holding_ratio",
                "prev_holding_ratio",
                "delta_ratio",
                "entered",
                "exited",
            ]
        )
        .sort(["snapshot_date", "security_id"])
    )


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
    # The raw frame (15 columns over the full history on a rebuild) is no
    # longer needed; release it before the partitioned write allocates.
    del prices
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


def build_preferred_discount(
    data_root: Path,
    dates: Iterable[date] | None = None,
    *,
    prices: pl.DataFrame | None = None,
    include_low_confidence: bool = False,
) -> list[BuildSummary]:
    """Build the daily preferred-vs-common discount series (gold.preferred_discount).

    ``discount = 1 - preferred_close_adj / common_close_adj`` per
    silver.security_relations pair (positive = preferred trades below the
    common, the typical case). Rolling mean/std over the trailing
    ``PREFERRED_DISCOUNT_WINDOW`` observed pair days (row-based: only days
    where BOTH legs traded count) feed ``discount_z``; both stats stay null
    until ``PREFERRED_DISCOUNT_MIN_OBS`` observations exist, and the z-score
    additionally requires a positive std. The ``discount_*_252`` column names
    are fixed even if the window constant is overridden. Only high-confidence
    pairs are used unless ``include_low_confidence=True``.

    ``prices`` optionally shares a gold.daily_prices_adj-shaped frame (date,
    security_id, close_adj); the caller must then include enough history for
    the trailing window. With ``dates``, only those partitions are rewritten,
    but the trailing ``PREFERRED_DISCOUNT_WINDOW`` gold partitions before
    min(dates) are loaded as lookback so incremental z-scores match a full
    rebuild.
    """
    selected_dates = set(dates) if dates is not None else None
    relations = _load_preferred_relations(data_root, include_low_confidence)
    if relations is None or relations.is_empty():
        return [BuildSummary("gold.preferred_discount", 0, 0)]
    if prices is not None:
        adj = prices.select(
            [column for column in _DISCOUNT_PRICE_COLUMNS if column in prices.columns]
        )
    elif selected_dates is not None:
        adj = _read_adj_prices_with_lookback(data_root, selected_dates)
    else:
        adj = _read_optional(
            data_root / "gold/daily_prices_adj/dt=*/part.parquet",
            columns=_DISCOUNT_PRICE_COLUMNS,
        )
    if adj is None or adj.is_empty():
        return [BuildSummary("gold.preferred_discount", 0, 0)]
    discount = _preferred_discount_frame(adj, relations)
    if selected_dates is not None:
        discount = discount.filter(pl.col("date").is_in(selected_dates))
    if discount.is_empty():
        return [BuildSummary("gold.preferred_discount", 0, 0)]
    return _write_by_date(data_root, "gold.preferred_discount", discount, "date")


def _load_preferred_relations(
    data_root: Path,
    include_low_confidence: bool,
) -> pl.DataFrame | None:
    relations = _read_optional(
        data_root / "silver/security_relations/part.parquet",
        columns=[
            "common_security_id",
            "preferred_security_id",
            "preferred_ticker",
            "relation_type",
            "confidence",
        ],
    )
    if relations is None or relations.is_empty():
        return None
    relations = relations.filter(pl.col("relation_type") == "preferred_of")
    if not include_low_confidence:
        relations = relations.filter(pl.col("confidence") == "high")
    return relations.drop("relation_type")


def _read_adj_prices_with_lookback(
    data_root: Path,
    dates: set[date],
) -> pl.DataFrame | None:
    partition_dates = sorted(
        _partition_date_from_path(Path(file), "dt")
        for file in glob((data_root / "gold/daily_prices_adj/dt=*/part.parquet").as_posix())
    )
    if not partition_dates:
        return None
    target_min = min(dates)
    lookback = [value for value in partition_dates if value < target_min]
    lookback = lookback[-PREFERRED_DISCOUNT_WINDOW:]
    selected = set(lookback) | (set(partition_dates) & dates)
    frame = _read_partition_dates(data_root, "gold.daily_prices_adj", selected)
    if frame is None:
        return None
    return frame.select([column for column in _DISCOUNT_PRICE_COLUMNS if column in frame.columns])


def _preferred_discount_frame(adj: pl.DataFrame, relations: pl.DataFrame) -> pl.DataFrame:
    adj = adj.filter(pl.col("close_adj").is_not_null() & (pl.col("close_adj") > 0))
    common = adj.select(
        "date",
        pl.col("security_id").alias("common_security_id"),
        pl.col("close_adj").alias("common_close"),
    )
    preferred = adj.select(
        "date",
        pl.col("security_id").alias("preferred_security_id"),
        pl.col("close_adj").alias("preferred_close"),
    )
    window = PREFERRED_DISCOUNT_WINDOW
    min_obs = PREFERRED_DISCOUNT_MIN_OBS
    return (
        relations.join(preferred, on="preferred_security_id", how="inner")
        .join(common, on=["date", "common_security_id"], how="inner")
        .with_columns(
            (1.0 - pl.col("preferred_close") / pl.col("common_close")).alias("discount")
        )
        .sort(["preferred_security_id", "date"])
        .with_columns(
            pl.col("discount")
            .rolling_mean(window_size=window, min_samples=min_obs)
            .over("preferred_security_id")
            .alias("discount_mean_252"),
            pl.col("discount")
            .rolling_std(window_size=window, min_samples=min_obs)
            .over("preferred_security_id")
            .alias("discount_std_252"),
        )
        .with_columns(
            pl.when(pl.col("discount_std_252") > 0)
            .then(
                (pl.col("discount") - pl.col("discount_mean_252")) / pl.col("discount_std_252")
            )
            .otherwise(None)
            .alias("discount_z")
        )
        .select(
            [
                "date",
                "common_security_id",
                "preferred_security_id",
                "preferred_ticker",
                "common_close",
                "preferred_close",
                "discount",
                "discount_mean_252",
                "discount_std_252",
                "discount_z",
                "confidence",
            ]
        )
        .sort(["date", "preferred_ticker"])
    )


def build_daily_market_caps(
    data_root: Path,
    *,
    prices: pl.DataFrame | None = None,
) -> list[BuildSummary]:
    """Merge silver.market_caps with price-derived caps into gold.daily_market_caps.

    Existing gold partitions are never rewritten (append-only), so only dates
    without a gold partition are considered at all. Those new dates are then
    processed in ``DAILY_MARKET_CAPS_REBUILD_CHUNK_DAYS`` calendar chunks so a
    first-time full build stays memory-bounded; the per-(date, ticker) source
    preference is date-local, so chunking cannot change the output.
    """
    existing_dates = {
        _partition_date_from_path(Path(file), "dt")
        for file in glob((data_root / "gold/daily_market_caps/dt=*/part.parquet").as_posix())
    }
    candidate_dates = {
        _partition_date_from_path(Path(file), "dt")
        for file in glob((data_root / "silver/market_caps/dt=*/part.parquet").as_posix())
    }
    if prices is not None:
        candidate_dates.update(prices["date"].drop_nulls().unique().to_list())
    else:
        candidate_dates.update(
            _partition_date_from_path(Path(file), "dt")
            for file in glob((data_root / "silver/prices/dt=*/part.parquet").as_posix())
        )
    new_dates = sorted(candidate_dates - existing_dates)
    if not new_dates:
        return [BuildSummary("gold.daily_market_caps", 0, 0)]
    rows = 0
    files = 0
    for chunk in _chunk_dates_by_span(new_dates, DAILY_MARKET_CAPS_REBUILD_CHUNK_DAYS):
        chunk_set = set(chunk)
        market_caps = _read_partition_dates(data_root, "silver.market_caps", chunk_set)
        price_frame = (
            prices.filter(pl.col("date").is_in(chunk))
            if prices is not None
            else _read_partition_dates(data_root, "silver.prices", chunk_set)
        )
        if price_frame is not None and not price_frame.is_empty():
            price_frame = _price_market_caps_frame(
                price_frame.select(
                    [
                        column
                        for column in _MARKET_CAP_PRICE_COLUMNS
                        if column in price_frame.columns
                    ]
                )
            )
        frames = [
            frame
            for frame in [market_caps, price_frame]
            if frame is not None and not frame.is_empty()
        ]
        if not frames:
            continue
        merged = (
            pl.concat(frames, how="diagonal_relaxed")
            .sort(["date", "ticker", "market_cap_source"])
            .unique(subset=["date", "ticker"], keep="first")
        )
        summary = _write_by_date(data_root, "gold.daily_market_caps", merged, "date")[0]
        rows += summary.rows
        files += summary.files
    return [BuildSummary("gold.daily_market_caps", rows, files)]


def build_financials_silver(data_root: Path) -> list[BuildSummary]:
    """Normalize bronze DART financial rows into silver.financials.

    ``is_backfilled`` marks rows fetched by a historical bulk backfill rather
    than the daily filings-scheduled ingest. OpenDART fnlttSinglAcntAll always
    returns the LATEST (post-correction) figures, so a backfilled amount may
    not match what was knowable on ``available_date``; PIT-sensitive research
    can exclude or discount such rows. Legacy bronze partitions written before
    the column existed read as False.
    """
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
    if "is_backfilled" not in financials.columns:
        financials = financials.with_columns(pl.lit(False).alias("is_backfilled"))
    optional_columns = {
        "rcept_no": pl.String,
        "statement_division": pl.String,
        "statement_name": pl.String,
        "account_detail": pl.String,
        "amount_basis": pl.String,
        "current_period_name": pl.String,
        "current_amount": pl.Float64,
        "cumulative_amount": pl.Float64,
        "prior_period_name": pl.String,
        "prior_amount": pl.Float64,
        "prior_cumulative_amount": pl.Float64,
        "two_year_prior_period_name": pl.String,
        "two_year_prior_amount": pl.Float64,
        "sort_order": pl.Int64,
        "currency": pl.String,
        "unit": pl.String,
    }
    for column, dtype in optional_columns.items():
        if column not in financials.columns:
            financials = financials.with_columns(pl.lit(None, dtype=dtype).alias(column))
    financials = (
        _cast_dates(
            financials,
            ["fiscal_period_end", "event_date", "rcept_dt", "available_date"],
        )
        .pipe(_normalize_financial_account_ids)
        .pipe(_remove_legacy_refreshed_financial_rows)
        .with_columns(pl.col("is_backfilled").cast(pl.Boolean, strict=False).fill_null(False))
        .select(
            [
                "security_id",
                "corp_code",
                "fiscal_period_end",
                "event_date",
                "rcept_dt",
                "available_date",
                "rcept_no",
                "report_type",
                "statement_division",
                "statement_name",
                "account_id",
                "account_name",
                "account_detail",
                "amount",
                "amount_basis",
                "current_period_name",
                "current_amount",
                "cumulative_amount",
                "prior_period_name",
                "prior_amount",
                "prior_cumulative_amount",
                "two_year_prior_period_name",
                "two_year_prior_amount",
                "sort_order",
                "currency",
                "unit",
                "is_consolidated",
                "accounting_basis",
                "is_backfilled",
            ]
        )
        .with_columns(pl.col("fiscal_period_end").dt.year().alias("fiscal_year"))
        .unique(
            subset=[
                "corp_code",
                "rcept_no",
                "fiscal_period_end",
                "report_type",
                "statement_division",
                "account_id",
                "account_detail",
                "sort_order",
                "is_consolidated",
            ],
            keep="last",
        )
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


def _remove_legacy_refreshed_financial_rows(financials: pl.DataFrame) -> pl.DataFrame:
    """Prefer refreshed source-grain rows over legacy normalized equivalents."""

    coarse_grain = [
        "corp_code",
        "fiscal_period_end",
        "available_date",
        "report_type",
        "account_id",
        "is_consolidated",
    ]
    refreshed = financials.filter(pl.col("rcept_no").is_not_null())
    if refreshed.is_empty():
        return financials
    refreshed_keys = refreshed.select(coarse_grain).unique()
    legacy = financials.filter(pl.col("rcept_no").is_null()).join(
        refreshed_keys,
        on=coarse_grain,
        how="anti",
    )
    return pl.concat([legacy, refreshed], how="diagonal_relaxed")


def build_filings_silver(data_root: Path) -> list[BuildSummary]:
    """Normalize bronze DART filing-list rows into silver.filings.

    ``rcept_no`` is the unique key: overlapping ingest chunks can rewrite a
    receipt, so duplicates keep the newest ``rcept_dt`` row (last input row on
    ties). ``available_date`` is exposed as ``rcept_dt`` unchanged — the same
    convention as silver.financials, whose ``available_date`` also carries the
    receipt date verbatim; consumers (gold.fundamentals_pit, the event
    pipeline) apply the strict ``available_date < as_of`` visibility rule, so
    an intraday filing can never inform a same-day decision. ``security_id``
    uses the lake-wide ticker-derived ``"S" + stock_code`` convention and
    stays null for unlisted filers. ``is_correction`` mirrors the bronze
    ``rm`` remark flag (see ``DartFilingRow.is_correction``). Filing volume is
    modest (hundreds of thousands of rows), so like silver.financials this is
    a full single-pass rebuild over a pruned-column bronze scan.
    """
    filings = _read_optional(
        data_root / "bronze/dart_filings/dt=*/part.parquet",
        columns=_FILINGS_BRONZE_COLUMNS,
    )
    if filings is None or filings.is_empty():
        return [BuildSummary("silver.filings", 0, 0)]
    for column in ("corp_code", "corp_name", "stock_code", "report_nm", "rm"):
        if column not in filings.columns:
            filings = filings.with_columns(pl.lit(None, dtype=pl.String).alias(column))
    stock_code = pl.col("stock_code").cast(pl.String).str.strip_chars().str.to_uppercase()
    listed = stock_code.str.contains(r"^[0-9A-Z]{6}$")
    filings = (
        _cast_dates(filings, ["rcept_dt"])
        .filter(pl.col("rcept_no").is_not_null() & pl.col("rcept_dt").is_not_null())
        .with_columns(
            pl.when(listed)
            .then(stock_code)
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("stock_code"),
            pl.col("rm")
            .cast(pl.String)
            .str.contains("정", literal=True)
            .fill_null(False)
            .alias("is_correction"),
        )
        .with_columns(
            pl.when(pl.col("stock_code").is_not_null())
            .then(pl.concat_str([pl.lit("S"), pl.col("stock_code")]))
            .otherwise(pl.lit(None, dtype=pl.String))
            .alias("security_id"),
            pl.col("rcept_dt").alias("available_date"),
        )
        .sort(["rcept_no", "rcept_dt"], maintain_order=True)
        .unique(subset=["rcept_no"], keep="last", maintain_order=True)
        .select(
            [
                "rcept_no",
                "rcept_dt",
                "available_date",
                "corp_code",
                "corp_name",
                "stock_code",
                "security_id",
                "report_nm",
                "is_correction",
            ]
        )
        .sort(["rcept_dt", "rcept_no"])
    )
    return _write_by_date(data_root, "silver.filings", filings, "rcept_dt")


def build_filing_events(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    """Classify silver.filings into gold.filing_events.

    Classification and correction-folding semantics live in
    ``finance_pi.events.classify.classify_filing_events``. ``dates`` is
    accepted for orchestrator symmetry only: folding a correction needs each
    security's full same-type filing history and the dataset is small, so
    every invocation performs a full rebuild.
    """
    del dates
    filings = _read_optional(
        data_root / "silver/filings/dt=*/part.parquet",
        columns=_FILINGS_SILVER_COLUMNS,
    )
    if filings is None or filings.is_empty():
        return [BuildSummary("gold.filing_events", 0, 0)]
    events = classify_filing_events(_cast_dates(filings, ["rcept_dt", "available_date"]))
    if events.is_empty():
        return [BuildSummary("gold.filing_events", 0, 0)]
    return _write_by_date(data_root, "gold.filing_events", events, "event_date")


def build_fundamentals_pit(
    data_root: Path,
    dates: Iterable[date] | None = None,
) -> list[BuildSummary]:
    """Materialize point-in-time fundamentals per universe day.

    PIT availability policy: a filing is visible strictly AFTER its
    ``available_date`` (``available_date < as_of_date``), i.e. next-trading-day
    availability — an intraday filing can never inform a same-day decision.
    Annual and interim report types remain independently addressable. Within
    each report/statement/account grain, the newest fiscal period wins, then
    available_date, rcept_dt, and consolidated statements break ties. Must stay
    row-for-row identical to
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
    if "statement_division" not in financials.columns:
        financials = financials.with_columns(
            pl.lit(None, dtype=pl.String).alias("statement_division")
        )
    financials = (
        _cast_dates(
            financials,
            ["fiscal_period_end", "event_date", "rcept_dt", "available_date"],
        )
        .filter(pl.col("security_id").is_not_null())
        .sort(
            [
                "security_id",
                "account_id",
                "fiscal_period_end",
                "report_type",
                "statement_division",
                "available_date",
                "rcept_dt",
            ]
        )
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
                "report_type",
                "statement_division",
                "available_date",
                "rcept_dt",
                "is_consolidated",
            ]
        )
        .unique(
            subset=[
                "date",
                "security_id",
                "report_type",
                "statement_division",
                "account_id",
            ],
            keep="last",
        )
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
    if payload.get("version") != FUNDAMENTALS_PIT_STATE_VERSION:
        return None
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
        "version": FUNDAMENTALS_PIT_STATE_VERSION,
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


def _bronze_price_source_dates(data_root: Path) -> list[date]:
    """Distinct data dates across all bronze price sources, cheaply.

    ``dt=``-partitioned sources contribute their partition dates straight from
    the directory names (the ingest writes one data date per partition, which
    the incremental path already relies on); only request-date-partitioned
    sources (naver) are scanned, projected down to the ``date`` column.
    """
    values: set[date] = set()
    for _source, pattern in _BRONZE_PRICE_SOURCES:
        unpartitioned: list[str] = []
        for file in sorted(glob((data_root / pattern).as_posix())):
            try:
                values.add(_partition_date_from_path(Path(file), "dt"))
            except ValueError:
                unpartitioned.append(file)
        values.update(_scanned_price_dates(unpartitioned))
    return sorted(values)


def _scanned_price_dates(files: list[str]) -> set[date]:
    if not files:
        return set()
    try:
        dates = (
            pl.scan_parquet(files, hive_partitioning=True)
            .select(pl.col("date").cast(pl.Date, strict=False))
            .unique()
            .collect()["date"]
        )
        return set(dates.drop_nulls().to_list())
    except (pl.exceptions.SchemaError, pl.exceptions.ColumnNotFoundError):
        values: set[date] = set()
        for file in files:
            lazy = pl.scan_parquet(file, hive_partitioning=True)
            if "date" not in lazy.collect_schema().names():
                continue
            frame = _cast_dates(lazy.select("date").collect(), ["date"])
            values.update(frame["date"].drop_nulls().to_list())
        return values


def _read_price_source_chunk(pattern: Path, load_dates: list[date]) -> pl.DataFrame | None:
    """Read one bronze price source pruned to ``load_dates`` and price columns.

    ``dt=``-partitioned files outside the requested dates are dropped from the
    file list before scanning; everything else relies on the lazy scan's
    column projection and date predicate, so only the chunk's rows are ever
    materialized.
    """
    files = _filter_files_by_partition_dates(sorted(glob(pattern.as_posix())), set(load_dates))
    if not files:
        return None
    try:
        lazy = pl.scan_parquet(files, hive_partitioning=True)
        available = lazy.collect_schema().names()
        return (
            lazy.select([column for column in _BRONZE_PRICE_COLUMNS if column in available])
            .with_columns(pl.col("date").cast(pl.Date, strict=False))
            .filter(pl.col("date").is_in(load_dates))
            .collect()
        )
    except (pl.exceptions.SchemaError, pl.exceptions.ColumnNotFoundError):
        frames: list[pl.DataFrame] = []
        for file in files:
            frame = pl.read_parquet(file, hive_partitioning=True)
            if "date" not in frame.columns:
                continue
            frame = frame.select(
                [column for column in _BRONZE_PRICE_COLUMNS if column in frame.columns]
            )
            frame = _cast_dates(frame, ["date"]).filter(pl.col("date").is_in(load_dates))
            if not frame.is_empty():
                frames.append(frame)
        if not frames:
            return None
        return pl.concat(frames, how="diagonal_relaxed")


def _filter_files_by_partition_dates(files: list[str], dates: set[date]) -> list[str]:
    """Keep dt=-partitioned files matching ``dates``; pass other files through."""
    selected: list[str] = []
    for file in files:
        try:
            partition_date = _partition_date_from_path(Path(file), "dt")
        except ValueError:
            selected.append(file)
            continue
        if partition_date in dates:
            selected.append(file)
    return selected


def _chunk_dates_by_span(dates: list[date], span_days: int) -> list[list[date]]:
    """Split sorted dates into consecutive chunks spanning <= span_days each."""
    chunks: list[list[date]] = []
    current: list[date] = []
    for value in dates:
        if current and (value - current[0]).days >= span_days:
            chunks.append(current)
            current = []
        current.append(value)
    if current:
        chunks.append(current)
    return chunks


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


def _price_market_caps_frame(frame: pl.DataFrame) -> pl.DataFrame:
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
    # Walk zero-copy slices of the frame sorted by the partition column instead
    # of partition_by(as_dict=True), which materializes a full copy of every
    # partition simultaneously (~2x frame memory at full-history scale). The
    # stable sort keeps the original row order within each partition (matching
    # partition_by), and the ordered group sizes yield each partition's
    # (offset, length) in ascending key order like the old sorted() loop.
    if frame.height and not frame[partition_column].is_sorted():
        frame = frame.sort(partition_column, maintain_order=True)
    group_sizes = frame.group_by(partition_column, maintain_order=True).agg(
        pl.len().alias("_len")
    )
    offset = 0
    for value, length in zip(group_sizes[partition_column], group_sizes["_len"], strict=True):
        partition = frame.slice(offset, length)
        offset += length
        if value is None or value not in selected:
            continue
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
