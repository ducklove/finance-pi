from __future__ import annotations

from datetime import date, timedelta

import polars as pl

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
