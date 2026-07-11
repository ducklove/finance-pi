from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
from typing import Literal

import polars as pl

from finance_pi.calendar import TradingCalendar

Status = Literal["PASS", "WARN", "FAIL"]
Grade = Literal["A", "C", "F"]


@dataclass(frozen=True)
class ReportCheck:
    name: str
    status: Status
    message: str


@dataclass(frozen=True)
class DataQualityReport:
    report_date: date
    checks: tuple[ReportCheck, ...]
    scorecard: pl.DataFrame | None = None

    def has_failures(self) -> bool:
        return any(check.status == "FAIL" for check in self.checks)

    def worst_grade(self) -> Grade | None:
        if self.scorecard is None or self.scorecard.is_empty():
            return None
        grades = set(self.scorecard["grade"].to_list())
        for grade in ("F", "C", "A"):
            if grade in grades:
                return grade  # type: ignore[return-value]
        return None

    def to_html(self) -> str:
        rows = "\n".join(
            "<tr>"
            f"<td>{escape(check.name)}</td>"
            f"<td class='{check.status.lower()}'>{check.status}</td>"
            f"<td>{escape(check.message)}</td>"
            "</tr>"
            for check in self.checks
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>finance-pi data quality {self.report_date.isoformat()}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #1f2937; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #d1d5db; padding: 8px 10px; text-align: left; }}
    .pass, .grade-a {{ color: #047857; font-weight: 700; }}
    .warn, .grade-c {{ color: #b45309; font-weight: 700; }}
    .fail, .grade-f {{ color: #b91c1c; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Data Quality</h1>
  <p>Report date: {self.report_date.isoformat()}</p>
  <h2>Dataset Reliability Scorecard</h2>
  {self._scorecard_html()}
  <h2>Checks</h2>
  <table>
    <thead><tr><th>Check</th><th>Status</th><th>Message</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
"""

    def _scorecard_html(self) -> str:
        if self.scorecard is None or self.scorecard.is_empty():
            return "<p>No scorecard rows.</p>"
        rows = "\n".join(
            "<tr>"
            f"<td>{escape(str(row['dataset']))}</td>"
            f"<td class='grade-{str(row['grade']).lower()}'>{escape(str(row['grade']))}</td>"
            f"<td>{escape(str(row['detail']))}</td>"
            "</tr>"
            for row in self.scorecard.iter_rows(named=True)
        )
        return f"""<table>
    <thead><tr><th>Dataset</th><th>Grade</th><th>Detail</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>"""

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_html(), encoding="utf-8")
        return path


def empty_data_quality_report(report_date: date) -> DataQualityReport:
    return DataQualityReport(
        report_date=report_date,
        checks=(
            ReportCheck(
                "catalog",
                "PASS",
                "Report scaffold generated; attach dataset checks next.",
            ),
        ),
    )


def build_data_quality_report(data_root: Path, report_date: date) -> DataQualityReport:
    checks: list[ReportCheck] = []
    catalog = data_root / "catalog" / "finance_pi.duckdb"
    checks.append(
        ReportCheck(
            "catalog",
            "PASS" if catalog.exists() else "WARN",
            f"{catalog} {'exists' if catalog.exists() else 'has not been built'}",
        )
    )

    scorecard = build_dataset_scorecard(data_root, report_date)
    _write_scorecard_artifact(data_root, report_date, scorecard)

    prices = _read_optional(
        data_root / "silver/prices" / f"dt={report_date.isoformat()}" / "part.parquet"
    )
    if prices is None or prices.is_empty():
        checks.append(ReportCheck("silver_prices", "WARN", "No silver price rows yet."))
        return DataQualityReport(report_date, tuple(checks), scorecard)

    day_prices = prices.filter(pl.col("date") == report_date)
    checks.append(
        ReportCheck(
            "price_rows",
            "PASS" if day_prices.height > 0 else "WARN",
            f"{day_prices.height} silver price rows for {report_date.isoformat()}",
        )
    )
    duplicate_prices = day_prices.height - day_prices.unique(
        subset=["date", "security_id"], keep="last"
    ).height
    checks.append(
        ReportCheck(
            "price_key_duplicates",
            "PASS" if duplicate_prices == 0 else "FAIL",
            f"{duplicate_prices} duplicate (date, security_id) price rows",
        )
    )
    invalid_ohlc = day_prices.filter(
        (pl.col("close") <= 0)
        | (pl.col("low") > pl.min_horizontal("open", "close"))
        | (pl.col("high") < pl.max_horizontal("open", "close"))
        | (pl.col("high") < pl.col("low"))
    ).height
    checks.append(
        ReportCheck(
            "price_ohlc_invariants",
            "PASS" if invalid_ohlc == 0 else "WARN",
            f"{invalid_ohlc} rows violate positive-close or OHLC bounds",
        )
    )

    jump_prices = _with_previous_prices(data_root, prices, report_date)
    jumps = (
        jump_prices.sort(["security_id", "date"])
        .with_columns(pl.col("close").pct_change().over("security_id").alias("return_1d"))
        .filter((pl.col("date") == report_date) & (pl.col("return_1d").abs() > 0.30))
    )
    checks.append(
        ReportCheck(
            "large_price_jumps",
            "PASS" if jumps.is_empty() else "WARN",
            f"{jumps.height} rows with absolute close-to-close return above 30%",
        )
    )

    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is not None and not master.is_empty():
        unknown = master.filter(pl.col("corp_code").is_null())
        checks.append(
            ReportCheck(
                "unknown_identity",
                "PASS" if unknown.is_empty() else "WARN",
                f"{unknown.height} securities do not have DART corp_code mapping",
            )
        )

    krx_kis = _krx_kis_mismatch(prices, report_date)
    if krx_kis is not None:
        checks.append(
            ReportCheck(
                "krx_kis_close_mismatch",
                "PASS" if krx_kis == 0 else "WARN",
                f"{krx_kis} KRX/KIS close mismatches above 0.1%",
            )
        )

    financials = _latest_partition_frame(data_root / "silver/financials/fiscal_year=*/part.parquet")
    if financials is not None and not financials.is_empty():
        core_nulls = financials.filter(
            pl.any_horizontal(
                pl.col("corp_code").is_null(),
                pl.col("fiscal_period_end").is_null(),
                pl.col("report_type").is_null(),
                pl.col("account_id").is_null(),
                pl.col("amount").is_null(),
            )
        ).height
        checks.append(
            ReportCheck(
                "financial_core_nulls",
                "PASS" if core_nulls == 0 else "FAIL",
                f"{core_nulls} newest-fiscal-partition rows have null core financial keys",
            )
        )
        if "rcept_no" in financials.columns:
            refreshed = financials.filter(pl.col("rcept_no").is_not_null())
            grain = [
                "corp_code",
                "rcept_no",
                "fiscal_period_end",
                "report_type",
                "statement_division",
                "account_id",
                "account_detail",
                "sort_order",
                "is_consolidated",
            ]
            grain = [column for column in grain if column in refreshed.columns]
            duplicates = refreshed.height - refreshed.unique(subset=grain, keep="last").height
            checks.append(
                ReportCheck(
                    "financial_source_grain_duplicates",
                    "PASS" if duplicates == 0 else "FAIL",
                    f"{duplicates} refreshed financial rows duplicate the source grain",
                )
            )

    dividends = _read_optional(data_root / "silver/dividends/fiscal_year=*/part.parquet")
    if dividends is not None and not dividends.is_empty():
        dividend_key = ["fiscal_year", "corp_code", "stock_kind", "source_rcept_no"]
        duplicate_dividends = dividends.height - dividends.unique(
            subset=dividend_key, keep="last"
        ).height
        checks.append(
            ReportCheck(
                "dividend_key_duplicates",
                "PASS" if duplicate_dividends == 0 else "FAIL",
                f"{duplicate_dividends} duplicate dividend business-key rows",
            )
        )

    return DataQualityReport(report_date, tuple(checks), scorecard)


def _krx_kis_mismatch(prices: pl.DataFrame, report_date: date) -> int | None:
    day = prices.filter(
        (pl.col("date") == report_date) & pl.col("price_source").is_in(["krx", "kis"])
    )
    if day.filter(pl.col("price_source") == "kis").is_empty():
        return None
    pivot = day.pivot(
        index="ticker",
        on="price_source",
        values="close",
        aggregate_function="first",
    )
    if "krx" not in pivot.columns or "kis" not in pivot.columns:
        return None
    return pivot.filter(((pl.col("krx") - pl.col("kis")).abs() / pl.col("krx")) > 0.001).height


def _with_previous_prices(data_root: Path, prices: pl.DataFrame, report_date: date) -> pl.DataFrame:
    previous = _previous_price_frame(data_root, report_date)
    if previous is None or previous.is_empty():
        return prices
    shared = [column for column in prices.columns if column in previous.columns]
    return pl.concat([previous.select(shared), prices], how="diagonal_relaxed")


def _previous_price_frame(data_root: Path, report_date: date) -> pl.DataFrame | None:
    from glob import glob

    candidates: list[tuple[date, Path]] = []
    for name in glob((data_root / "silver/prices/dt=*/part.parquet").as_posix()):
        path = Path(name)
        for part in path.parts:
            if part.startswith("dt="):
                try:
                    logical_date = date.fromisoformat(part.removeprefix("dt="))
                except ValueError:
                    continue
                if logical_date < report_date:
                    candidates.append((logical_date, path))
    if not candidates:
        return None
    _, path = max(candidates, key=lambda item: item[0])
    return pl.read_parquet(path, hive_partitioning=True)


def _read_optional(pattern: Path) -> pl.DataFrame | None:
    from glob import glob

    file_names = sorted(glob(pattern.as_posix()))
    files = [Path(name) for name in file_names]
    if not files:
        return None
    return pl.read_parquet([file.as_posix() for file in files], hive_partitioning=True)


def _latest_partition_frame(pattern: Path) -> pl.DataFrame | None:
    from glob import glob

    files = sorted(Path(name) for name in glob(pattern.as_posix()))
    if not files:
        return None
    return pl.read_parquet(files[-1], hive_partitioning=True)


@dataclass(frozen=True)
class _DatasetScorecardSpec:
    """Per-dataset expectations used by :func:`build_dataset_scorecard`."""

    name: str
    relative_glob: str
    partition_prefix: str  # e.g. "dt=" or "fiscal_year=" preceding the value in the path
    date_column: str | None  # column to compute freshness/max-date from; None = no freshness check
    # max age in trading days for freshness; None means informational only (no freshness grade)
    max_age_trading_days: int | None
    # if True, an empty/missing dataset grades C with an explanatory message instead of F
    may_be_legitimately_empty: bool = False
    empty_explanation: str = ""


_SCORECARD_DATASETS: tuple[_DatasetScorecardSpec, ...] = (
    _DatasetScorecardSpec(
        name="silver.prices",
        relative_glob="silver/prices/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=2,
    ),
    _DatasetScorecardSpec(
        name="silver.market_caps",
        relative_glob="silver/market_caps/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=130,
    ),
    _DatasetScorecardSpec(
        name="gold.daily_prices_adj",
        relative_glob="gold/daily_prices_adj/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=2,
    ),
    _DatasetScorecardSpec(
        name="gold.daily_market_caps",
        relative_glob="gold/daily_market_caps/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=2,
    ),
    _DatasetScorecardSpec(
        name="gold.fundamentals_pit",
        relative_glob="gold/fundamentals_pit/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="as_of_date",
        max_age_trading_days=45,
    ),
    _DatasetScorecardSpec(
        name="silver.financials",
        relative_glob="silver/financials/fiscal_year=*/part.parquet",
        partition_prefix="fiscal_year=",
        date_column="available_date",
        max_age_trading_days=45,
    ),
    _DatasetScorecardSpec(
        name="silver.filings",
        relative_glob="silver/filings/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="rcept_dt",
        max_age_trading_days=10,
    ),
    _DatasetScorecardSpec(
        name="silver.dividends",
        relative_glob="silver/dividends/fiscal_year=*/part.parquet",
        partition_prefix="fiscal_year=",
        date_column="available_date",
        max_age_trading_days=260,
    ),
    _DatasetScorecardSpec(
        name="silver.share_counts",
        relative_glob="silver/share_counts/fiscal_year=*/part.parquet",
        partition_prefix="fiscal_year=",
        date_column="available_date",
        max_age_trading_days=260,
    ),
    _DatasetScorecardSpec(
        name="silver.nps_holdings",
        relative_glob="silver/nps_holdings/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=130,
        may_be_legitimately_empty=True,
        empty_explanation=(
            "NPS holdings are optional until a public NPS disclosure file is ingested."
        ),
    ),
    _DatasetScorecardSpec(
        name="silver.corporate_actions",
        relative_glob="silver/corporate_actions/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="effective_date",
        max_age_trading_days=None,
        may_be_legitimately_empty=True,
        empty_explanation=(
            "silver.corporate_actions may legitimately be empty when no corporate action "
            "has been detected yet (e.g. before the first split/rights-issue detection)."
        ),
    ),
    _DatasetScorecardSpec(
        name="gold.universe_history",
        relative_glob="gold/universe_history/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=2,
    ),
    _DatasetScorecardSpec(
        name="gold.filing_events",
        relative_glob="gold/filing_events/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="event_date",
        max_age_trading_days=10,
    ),
    _DatasetScorecardSpec(
        name="gold.nps_universe",
        relative_glob="gold/nps_universe/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=130,
        may_be_legitimately_empty=True,
        empty_explanation="NPS universe is optional until source holdings are available.",
    ),
    _DatasetScorecardSpec(
        name="gold.preferred_discount",
        relative_glob="gold/preferred_discount/dt=*/part.parquet",
        partition_prefix="dt=",
        date_column="date",
        max_age_trading_days=2,
    ),
)


def build_dataset_scorecard(data_root: Path, report_date: date) -> pl.DataFrame:
    """Grade each key dataset on freshness, completeness, and emptiness.

    Grades:
      - A: fresh, complete, non-empty.
      - C: warning (stale, incomplete, or legitimately-empty-with-explanation).
      - F: failing (empty when it should not be, or missing entirely, or very stale).
    """

    rows: list[dict[str, object]] = []
    for spec in _SCORECARD_DATASETS:
        rows.append(_grade_dataset(data_root, report_date, spec))
    return pl.DataFrame(
        rows,
        schema={"dataset": pl.Utf8, "grade": pl.Utf8, "detail": pl.Utf8},
    )


def _grade_dataset(data_root: Path, report_date: date, spec: _DatasetScorecardSpec) -> dict:
    partitions = _partition_dates(data_root, spec)
    if not partitions:
        if spec.may_be_legitimately_empty:
            return {"dataset": spec.name, "grade": "C", "detail": spec.empty_explanation}
        return {
            "dataset": spec.name,
            "grade": "F",
            "detail": f"{spec.name} has no partitions at {spec.relative_glob}",
        }

    newest_partition_value, newest_path = max(partitions, key=lambda item: item[0])
    newest_frame = pl.read_parquet(newest_path.as_posix(), hive_partitioning=True)

    if newest_frame.is_empty():
        if spec.may_be_legitimately_empty:
            return {"dataset": spec.name, "grade": "C", "detail": spec.empty_explanation}
        return {
            "dataset": spec.name,
            "grade": "F",
            "detail": f"{spec.name} newest partition {newest_partition_value} has zero rows",
        }

    messages: list[str] = []
    worst: Grade = "A"

    freshness_grade, freshness_message = _freshness_grade(spec, newest_frame, report_date)
    if freshness_message:
        messages.append(freshness_message)
    worst = _worse_grade(worst, freshness_grade)

    completeness_grade, completeness_message = _completeness_grade(
        data_root, spec, partitions, report_date
    )
    if completeness_message:
        messages.append(completeness_message)
    worst = _worse_grade(worst, completeness_grade)

    if not messages:
        messages.append(f"{spec.name} is fresh and complete as of {report_date.isoformat()}")

    return {"dataset": spec.name, "grade": worst, "detail": "; ".join(messages)}


def _worse_grade(current: Grade, candidate: Grade) -> Grade:
    order = {"A": 0, "C": 1, "F": 2}
    return candidate if order[candidate] > order[current] else current


def _age_in_trading_days(max_date: date, report_date: date) -> int:
    """Trading-day age of max_date relative to report_date.

    Years without an explicit KRX holiday calendar fall back to the
    weekday-only rule inside TradingCalendar; that fallback is fine for a
    freshness estimate, so suppress the per-year UserWarning locally instead
    of spamming report builds (and test runs) that touch old dates.
    """

    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="No KRX holiday calendar for")
        return len(TradingCalendar.krx_trading_days(max_date, report_date).dates) - 1


def _freshness_grade(
    spec: _DatasetScorecardSpec, newest_frame: pl.DataFrame, report_date: date
) -> tuple[Grade, str | None]:
    if spec.max_age_trading_days is None:
        return "A", None
    if spec.date_column is None or spec.date_column not in newest_frame.columns:
        return "A", None
    max_date = newest_frame[spec.date_column].drop_nulls().max()
    if max_date is None:
        return "C", f"{spec.name} has no non-null {spec.date_column} values"
    if isinstance(max_date, str):
        max_date = date.fromisoformat(max_date)
    if hasattr(max_date, "date") and not isinstance(max_date, date):
        max_date = max_date.date()
    if max_date > report_date:
        max_date = report_date
    age_trading_days = _age_in_trading_days(max_date, report_date)
    if age_trading_days > spec.max_age_trading_days:
        return (
            "F" if age_trading_days > spec.max_age_trading_days * 3 else "C",
            f"{spec.name} newest {spec.date_column}={max_date.isoformat()} is "
            f"{age_trading_days} trading days old (expected <= {spec.max_age_trading_days})",
        )
    return "A", None


def _completeness_grade(
    data_root: Path,
    spec: _DatasetScorecardSpec,
    partitions: list[tuple[str, Path]],
    report_date: date,
) -> tuple[Grade, str | None]:
    if len(partitions) < 2:
        return "A", None
    ordered = sorted(partitions, key=lambda item: item[0])
    newest_value, newest_path = ordered[-1]
    if spec.partition_prefix == "fiscal_year=" and newest_value == str(report_date.year):
        return "A", None
    trailing = ordered[-21:-1] if len(ordered) > 1 else []
    if not trailing:
        return "A", None
    newest_rows = pl.read_parquet(newest_path.as_posix(), hive_partitioning=True).height
    trailing_counts = [
        pl.read_parquet(path.as_posix(), hive_partitioning=True).height for _, path in trailing
    ]
    trailing_counts = [count for count in trailing_counts if count > 0]
    if not trailing_counts:
        return "A", None
    median_count = sorted(trailing_counts)[len(trailing_counts) // 2]
    if median_count == 0:
        return "A", None
    ratio = newest_rows / median_count
    if ratio < 0.5:
        return (
            "F" if ratio < 0.1 else "C",
            f"{spec.name} newest partition ({newest_value}) has {newest_rows} rows, "
            f"{ratio:.0%} of trailing {len(trailing_counts)}-partition median ({median_count})",
        )
    return "A", None


def _partition_dates(
    data_root: Path, spec: _DatasetScorecardSpec
) -> list[tuple[str, Path]]:
    from glob import glob

    results: list[tuple[str, Path]] = []
    for name in glob((data_root / spec.relative_glob).as_posix()):
        path = Path(name)
        for part in path.parts:
            if part.startswith(spec.partition_prefix):
                value = part.removeprefix(spec.partition_prefix)
                results.append((value, path))
                break
    return results


def _write_scorecard_artifact(data_root: Path, report_date: date, scorecard: pl.DataFrame) -> Path:
    path = data_root / "reports" / "dq" / f"scorecard-{report_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    scorecard.write_parquet(path)
    return path
