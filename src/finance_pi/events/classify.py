"""Rule-based classification of DART filing titles into corporate events.

``EVENT_PATTERNS`` is a declarative (pattern -> event_type, expected_sign)
table matched against the prefix-stripped ``report_nm``. The FIRST matching
row wins, so more specific patterns must precede broader ones (e.g.
``stock_split``/``stock_merge`` before ``merger``'s generic 합병/분할 결정).

This table is the deliberate extension point for a future LLM-assisted
classifier: an LLM stage can append or refine rows (or emit per-filing
overrides keyed by ``rcept_no``) without touching the event pipeline, which
only consumes the (event_type, expected_sign) outputs of
:func:`classify_filing_events`.

Known rule limitations (accepted until the LLM stage lands):

- 유무상증자 결정 (mixed rights + bonus issue) matches ``bonus_issue`` because
  무상증자 is a substring of 유무상증자 while 유상증자 is not.
- ``expected_sign`` is the *typical* announcement-day direction from the
  Korean event-study literature; individual filings can obviously differ.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class EventPattern:
    """One classification rule: regex over the prefix-stripped report_nm."""

    event_type: str
    pattern: str
    # +1 = typically bullish, -1 = typically bearish, 0 = ambiguous/unsigned.
    expected_sign: int


EVENT_PATTERNS: tuple[EventPattern, ...] = (
    # Treasury-share actions. 처분 first so a disposal title can never fall
    # through to the buyback rules.
    EventPattern("buyback_disposal", r"자기주식\s*처분", -1),
    EventPattern("buyback", r"자기주식\s*취득\s*결정", 1),
    EventPattern("buyback", r"자기주식\s*취득\s*신탁계약\s*체결", 1),
    # Capital raises / capital structure.
    EventPattern("rights_issue", r"유상증자\s*결정", -1),
    EventPattern("bonus_issue", r"무상증자\s*결정", 1),
    EventPattern("convertible_bond", r"(전환사채|신주인수권부사채|교환사채).{0,8}발행\s*결정", -1),
    EventPattern("capital_reduction", r"감자\s*결정", -1),
    # 주식분할/주식병합 must precede merger so 주식분할결정 never matches 분할결정.
    EventPattern("stock_split", r"주식\s*분할\s*결정", 1),
    EventPattern("stock_merge", r"주식\s*병합\s*결정", 0),
    EventPattern("merger", r"(합병|분할)\s*결정", 0),
    # Distributions and governance.
    EventPattern("dividend", r"(현금|현물).{0,4}배당\s*결정", 1),
    EventPattern("major_holder_change", r"최대주주\s*변경", 0),
    EventPattern("supply_contract", r"(단일판매|공급계약).{0,10}체결", 1),
    # Red flags.
    EventPattern("embezzlement", r"횡령|배임", -1),
    EventPattern("delisting_risk", r"관리종목|상장폐지|감사의견", -1),
)

# event_type -> expected_sign (rules of one type always share a sign).
EVENT_SIGNS: dict[str, int] = {rule.event_type: rule.expected_sign for rule in EVENT_PATTERNS}

# Leading DART title tags such as [기재정정], [첨부추가], [기재정정][첨부정정].
_REPORT_PREFIX_PATTERN = r"^(?:\s*\[[^\]]*\]\s*)+"
# A bracketed 정정 tag marks the filing as a correction of an earlier receipt.
_CORRECTION_TAG_PATTERN = r"\[[^\]]*정정[^\]]*\]"


def stripped_report_name_expr(column: str = "report_nm") -> pl.Expr:
    """report_nm with leading ``[...]`` tags removed, for pattern matching."""
    return pl.col(column).cast(pl.String).str.replace(_REPORT_PREFIX_PATTERN, "").str.strip_chars()


def event_type_expr(stripped: pl.Expr) -> pl.Expr:
    """First-match-wins event_type over ``EVENT_PATTERNS`` (null when unmatched)."""
    chained: pl.Expr | None = None
    for rule in EVENT_PATTERNS:
        matched = stripped.str.contains(rule.pattern)
        if chained is None:
            chained = pl.when(matched).then(pl.lit(rule.event_type))
        else:
            chained = chained.when(matched).then(pl.lit(rule.event_type))
    assert chained is not None  # EVENT_PATTERNS is a non-empty constant
    return chained.otherwise(pl.lit(None, dtype=pl.String))


def classify_filing_events(filings: pl.DataFrame) -> pl.DataFrame:
    """Classify silver.filings rows into gold.filing_events rows.

    Only listed filers (non-null ``security_id``) can produce events. A filing
    counts as a correction when the silver ``is_correction`` flag (from the
    DART ``rm`` remark) is set OR its title carries a ``[...정정...]`` tag.

    Correction folding: the DART list API does not link a correction to the
    receipt it amends, so within (security_id, event_type) — the closest
    observable proxy for "the same base filing" — any correction that follows
    an earlier filing of the same type is folded into that event and dropped.
    The kept row is the ORIGINAL, so ``event_date`` stays the original
    ``rcept_dt``: the market learned about the event when the original was
    filed; corrections only amend details. A correction whose original
    predates the ingested history is kept and stands in as the event itself
    (with its own ``rcept_dt`` and ``is_correction=True``) rather than losing
    the event entirely.
    """
    if filings.is_empty():
        return filings
    stripped = stripped_report_name_expr("report_nm")
    return (
        filings.filter(pl.col("security_id").is_not_null())
        .with_columns(
            event_type_expr(stripped).alias("event_type"),
            (
                pl.col("is_correction").cast(pl.Boolean, strict=False).fill_null(False)
                | pl.col("report_nm").cast(pl.String).str.contains(_CORRECTION_TAG_PATTERN)
            ).alias("is_correction"),
        )
        .filter(pl.col("event_type").is_not_null())
        .with_columns(
            pl.col("event_type")
            .replace_strict(EVENT_SIGNS, return_dtype=pl.Int32)
            .alias("expected_sign")
        )
        .sort(["security_id", "event_type", "rcept_dt", "rcept_no"], maintain_order=True)
        .with_columns(
            pl.col("rcept_no").cum_count().over(["security_id", "event_type"]).alias("_order")
        )
        .filter(~pl.col("is_correction") | (pl.col("_order") == 1))
        .select(
            pl.col("rcept_dt").alias("event_date"),
            "available_date",
            "security_id",
            pl.col("stock_code").alias("ticker"),
            "event_type",
            "expected_sign",
            "rcept_no",
            "report_nm",
            "is_correction",
        )
        .sort(["event_date", "security_id", "event_type", "rcept_no"])
    )


def describe_event_patterns() -> list[dict[str, object]]:
    """Listing rows (event_type, expected_sign, pattern count) in table order."""
    counts: dict[str, int] = {}
    for rule in EVENT_PATTERNS:
        counts[rule.event_type] = counts.get(rule.event_type, 0) + 1
    return [
        {"event_type": name, "expected_sign": EVENT_SIGNS[name], "patterns": count}
        for name, count in counts.items()
    ]
