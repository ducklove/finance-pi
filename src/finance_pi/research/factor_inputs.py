"""복합 팩터 입력 검증. 기존 PIT 산출물을 신뢰 표시만으로 승인하지 않는다."""

from datetime import date, timedelta
from pathlib import Path

import polars as pl

METRICS = {
    "ifrs-full_ProfitLossAttributableToOwnersOfParent": "earnings",
    "ifrs-full_ProfitLoss": "earnings",
    "ifrs-full_Assets": "assets",
    "ifrs-full_EquityAttributableToOwnersOfParent": "equity",
    "ifrs-full_Equity": "equity",
}
REQUIRED = {
    "as_of_date",
    "security_id",
    "fiscal_period_end",
    "available_date",
    "rcept_dt",
    "rcept_no",
    "report_type",
    "account_id",
    "amount",
    "amount_basis",
    "currency",
    "unit",
    "is_backfilled",
    "is_consolidated",
}


def inspect_inputs(frame: pl.DataFrame, as_of: date) -> dict:
    missing = sorted(REQUIRED - set(frame.columns))
    if missing:
        return {"status": "missing_fields", "missing_fields": missing, "eligible_securities": None}
    core = frame.filter(
        (pl.col("report_type") == "11011") & pl.col("account_id").is_in(METRICS)
    ).with_columns(pl.col("account_id").replace_strict(METRICS).alias("metric"))
    receipt = pl.col("rcept_no").str.slice(0, 8).str.to_date("%Y%m%d", strict=False)
    rules = {
        "as_of_mismatch": pl.col("as_of_date") != as_of,
        "unknown_or_future_publication": (
            (pl.col("available_date") >= as_of)
            | (pl.col("rcept_dt") >= as_of)
            | pl.col("available_date").is_null()
            | pl.col("rcept_dt").is_null()
        ),
        "invalid_receipt": (
            ~pl.col("rcept_no").str.contains(r"^[0-9]{14}$")
            | receipt.is_null()
            | (receipt != pl.col("rcept_dt"))
        ),
        "historical_or_unknown_provenance": pl.col("is_backfilled") != False,  # noqa: E712
        "unknown_or_foreign_units": (pl.col("currency") != "KRW") | (pl.col("unit") != "KRW"),
        "unknown_amount_basis": ~pl.col("amount_basis").is_in(["current", "cumulative"]),
        "invalid_amount": ~pl.col("amount").is_finite(),
        "stale_or_future_period": (
            (pl.col("fiscal_period_end") < as_of - timedelta(days=550))
            | (pl.col("fiscal_period_end") > as_of)
        ),
        "unknown_scope": pl.col("is_consolidated").is_null(),
    }
    marked = core.with_columns(
        [condition.fill_null(True).alias(name) for name, condition in rules.items()]
    )
    clean = marked.filter(~pl.any_horizontal([pl.col(name) for name in rules]))
    # 세 지표를 서로 다른 보고서/연결 범위에서 섞어서 완전하다고 판정하지 않는다.
    complete = (
        clean.group_by("security_id", "fiscal_period_end", "rcept_no", "is_consolidated")
        .agg(pl.col("metric").n_unique().alias("metrics"))
        .filter(pl.col("metrics") == 3)
    )
    return {
        "status": "review_required",
        "core_rows": core.height,
        "securities": core["security_id"].n_unique(),
        "eligible_rows": clean.height,
        "eligible_securities": clean["security_id"].n_unique(),
        "complete_securities": complete["security_id"].n_unique(),
        "exclusions": {name: int(marked[name].sum() or 0) for name in rules},
        "excluded_rows": core.height - clean.height,
        "exclusion_counts_overlap": True,
    }


def audit(data_root: Path, as_of: date | None = None) -> dict:
    root = data_root / "gold/fundamentals_pit"
    if as_of is None:
        paths = sorted(root.glob("dt=*/part.parquet"))
        if not paths:
            return {"status": "missing_data", "eligible_securities": None, "live_eligible": False}
        path = paths[-1]
        as_of = date.fromisoformat(path.parent.name.removeprefix("dt="))
    else:
        path = root / f"dt={as_of}/part.parquet"
    if not path.is_file():
        raise ValueError("해당 시점의 재무 자료가 없습니다.")
    frame = pl.scan_parquet(path)
    fields = frame.collect_schema().names()
    selected = frame.select([c for c in REQUIRED if c in fields]).collect()
    prices = sorted((data_root / "gold/daily_prices_adj").glob("dt=*/part.parquet"))
    latest_price = prices[-1].parent.name.removeprefix("dt=") if prices else None
    return {
        **inspect_inputs(selected, as_of),
        "version": "factor-input-audit-1",
        "as_of": str(as_of),
        "latest_price_date": latest_price,
        "aligned_with_latest_prices": str(as_of) == latest_price,
        "live_eligible": False,
        "note": (
            "연차 이익·자산·자본의 입력 검사입니다. 평가일 당일·이후 접수 자료, "
            "역사적 일괄 수집, 불명확한 단위와 오래된 재무를 제외합니다. "
            "통과 후보도 원공시·당시 종목군·가격 및 계정 우선순위 검증 전이며 "
            "복합 팩터 백테스트나 운용 승인을 뜻하지 않습니다. 제외 사유는 중복 집계됩니다."
        ),
    }
