from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from finance_pi.calendar import TradingCalendar
from finance_pi.reports.data_quality import build_data_quality_report, build_dataset_scorecard
from finance_pi.reports.fraud import build_fraud_report


def _write_universe(tmp_path, report_date: date, size: int) -> None:
    # size == 0 simulates a partition-content mismatch: the file exists (so the
    # earlier "universe" empty-file guard does not trigger) but no row matches
    # report_date once filtered.
    row_date = report_date if size > 0 else report_date - timedelta(days=1)
    count = size if size > 0 else 1
    partition = tmp_path / "gold" / "universe_history" / f"dt={report_date.isoformat()}"
    partition.mkdir(parents=True)
    rows = [
        {
            "date": row_date,
            "security_id": f"S{i:04d}",
            "is_spac_pre": False,
        }
        for i in range(count)
    ]
    pl.DataFrame(rows).write_parquet(partition / "part.parquet")


def _tiny_universe_check(report):
    return next(check for check in report.checks if check.name == "tiny_universe")


def test_tiny_universe_zero_rows_warns(tmp_path) -> None:
    report_date = date(2026, 1, 5)
    _write_universe(tmp_path, report_date, 0)

    report = build_fraud_report(tmp_path, report_date)

    check = _tiny_universe_check(report)
    assert check.status == "WARN"
    assert "empty" in check.message


def test_tiny_universe_below_threshold_warns(tmp_path) -> None:
    report_date = date(2026, 1, 5)
    _write_universe(tmp_path, report_date, 29)

    report = build_fraud_report(tmp_path, report_date)

    check = _tiny_universe_check(report)
    assert check.status == "WARN"
    assert "29" in check.message


def test_tiny_universe_at_threshold_passes(tmp_path) -> None:
    report_date = date(2026, 1, 5)
    _write_universe(tmp_path, report_date, 30)

    report = build_fraud_report(tmp_path, report_date)

    check = _tiny_universe_check(report)
    assert check.status == "PASS"
    assert "30" in check.message


def _write_daily_partition(
    tmp_path,
    relative: str,
    partition_date: date,
    date_column: str,
    rows: int,
) -> None:
    partition = tmp_path / relative / f"dt={partition_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            date_column: [partition_date] * rows,
            "security_id": [f"S{i:04d}" for i in range(rows)],
        }
    )
    frame.write_parquet(partition / "part.parquet")


def _trading_days_ending(end: date, count: int) -> list[date]:
    calendar = TradingCalendar.krx_trading_days(end - timedelta(days=count * 3), end)
    return list(calendar.dates[-count:])


def _grade_for(scorecard: pl.DataFrame, dataset: str) -> str:
    row = scorecard.filter(pl.col("dataset") == dataset)
    assert row.height == 1, f"expected exactly one scorecard row for {dataset}"
    return row.select("grade").item()


def test_scorecard_grades_fresh_dataset_a(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    for partition_date in _trading_days_ending(report_date, 21):
        _write_daily_partition(tmp_path, "silver/prices", partition_date, "date", 50)

    scorecard = build_dataset_scorecard(tmp_path, report_date)

    assert _grade_for(scorecard, "silver.prices") == "A"


def test_scorecard_grades_stale_partition_c_or_f(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    for partition_date in _trading_days_ending(date(2026, 5, 20), 21):
        _write_daily_partition(tmp_path, "gold/daily_prices_adj", partition_date, "date", 50)

    scorecard = build_dataset_scorecard(tmp_path, report_date)

    grade = _grade_for(scorecard, "gold.daily_prices_adj")
    assert grade in ("C", "F")
    detail = scorecard.filter(pl.col("dataset") == "gold.daily_prices_adj").select("detail").item()
    assert "trading days old" in detail


def test_scorecard_grades_empty_dataset_f(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    partition = tmp_path / "gold" / "daily_market_caps" / f"dt={report_date.isoformat()}"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {"date": pl.Series([], dtype=pl.Date), "security_id": pl.Series([], dtype=pl.Utf8)}
    ).write_parquet(partition / "part.parquet")

    scorecard = build_dataset_scorecard(tmp_path, report_date)

    assert _grade_for(scorecard, "gold.daily_market_caps") == "F"


def test_scorecard_grades_missing_dataset_f(tmp_path) -> None:
    scorecard = build_dataset_scorecard(tmp_path, date(2026, 6, 22))

    assert _grade_for(scorecard, "gold.daily_prices_adj") == "F"


def test_scorecard_grades_absent_corporate_actions_c_with_explanation(tmp_path) -> None:
    scorecard = build_dataset_scorecard(tmp_path, date(2026, 6, 22))

    assert _grade_for(scorecard, "silver.corporate_actions") == "C"
    detail = (
        scorecard.filter(pl.col("dataset") == "silver.corporate_actions").select("detail").item()
    )
    assert "legitimately" in detail.lower()


def test_scorecard_grades_incomplete_partition_below_half_median(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    for offset in range(21):
        partition_date = date(2026, 5, 25) + timedelta(days=offset)
        rows = 10 if offset == 20 else 100
        _write_daily_partition(tmp_path, "gold/universe_history", partition_date, "date", rows)

    scorecard = build_dataset_scorecard(tmp_path, report_date)

    grade = _grade_for(scorecard, "gold.universe_history")
    assert grade in ("C", "F")


def test_scorecard_artifact_parquet_written(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    for offset in range(21):
        partition_date = date(2026, 5, 25) + timedelta(days=offset)
        _write_daily_partition(tmp_path, "silver/prices", partition_date, "date", 50)

    build_data_quality_report(tmp_path, report_date)

    artifact = tmp_path / "reports" / "dq" / f"scorecard-{report_date.isoformat()}.parquet"
    assert artifact.exists()
    scorecard = pl.read_parquet(artifact)
    assert set(scorecard.columns) == {"dataset", "grade", "detail"}
    assert scorecard.height > 0


def test_data_quality_report_includes_scorecard_html_section(tmp_path) -> None:
    report_date = date(2026, 6, 22)
    for offset in range(21):
        partition_date = date(2026, 5, 25) + timedelta(days=offset)
        _write_daily_partition(tmp_path, "silver/prices", partition_date, "date", 50)

    report = build_data_quality_report(tmp_path, report_date)
    html = report.to_html()

    assert "Dataset Reliability Scorecard" in html
    assert "silver.prices" in html
    assert report.scorecard is not None
    assert report.worst_grade() in ("A", "C", "F")
