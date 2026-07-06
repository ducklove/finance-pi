from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from finance_pi.events import classify_filing_events, describe_event_patterns, run_event_study
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import build_filing_events


def _silver_filing_row(
    security_id: str | None,
    report_nm: str,
    *,
    rcept_dt: date = date(2024, 1, 5),
    rcept_no: str = "20240105000001",
    is_correction: bool = False,
) -> dict:
    return {
        "rcept_no": rcept_no,
        "rcept_dt": rcept_dt,
        "available_date": rcept_dt,
        "stock_code": security_id.removeprefix("S") if security_id else None,
        "security_id": security_id,
        "report_nm": report_nm,
        "is_correction": is_correction,
    }


def test_classify_covers_every_pattern_class() -> None:
    cases = [
        ("주요사항보고서(자기주식취득결정)", "buyback", 1),
        ("자기주식취득신탁계약체결결정", "buyback", 1),
        ("주요사항보고서(자기주식처분결정)", "buyback_disposal", -1),
        ("주요사항보고서(유상증자결정)", "rights_issue", -1),
        ("주요사항보고서(무상증자결정)", "bonus_issue", 1),
        ("주요사항보고서(전환사채권발행결정)", "convertible_bond", -1),
        ("주요사항보고서(신주인수권부사채권발행결정)", "convertible_bond", -1),
        ("주요사항보고서(감자결정)", "capital_reduction", -1),
        ("주요사항보고서(주식분할결정)", "stock_split", 1),
        ("주요사항보고서(주식병합결정)", "stock_merge", 0),
        ("주요사항보고서(회사합병결정)", "merger", 0),
        ("주요사항보고서(회사분할결정)", "merger", 0),
        ("현금ㆍ현물배당결정", "dividend", 1),
        ("최대주주변경", "major_holder_change", 0),
        ("단일판매ㆍ공급계약체결", "supply_contract", 1),
        ("횡령ㆍ배임혐의발생", "embezzlement", -1),
        ("상장폐지사유발생", "delisting_risk", -1),
        ("감사의견비적정설에대한조회공시요구", "delisting_risk", -1),
    ]
    rows = [
        _silver_filing_row(
            f"S{str(index).zfill(6)}",
            report_nm,
            rcept_no=f"2024010500{str(index).zfill(4)}",
        )
        for index, (report_nm, _, _) in enumerate(cases)
    ]
    rows.append(
        _silver_filing_row("S999999", "사업보고서 (2023.12)", rcept_no="20240105009999")
    )

    events = classify_filing_events(pl.DataFrame(rows))

    by_security = {row["security_id"]: row for row in events.iter_rows(named=True)}
    for index, (report_nm, event_type, sign) in enumerate(cases):
        row = by_security[f"S{str(index).zfill(6)}"]
        assert row["event_type"] == event_type, report_nm
        assert row["expected_sign"] == sign, report_nm
        assert row["event_date"] == date(2024, 1, 5)
        assert row["available_date"] == date(2024, 1, 5)
    # Unmatched report names never become events.
    assert "S999999" not in by_security


def test_classify_requires_listed_filer() -> None:
    events = classify_filing_events(
        pl.DataFrame([_silver_filing_row(None, "주요사항보고서(자기주식취득결정)")])
    )

    assert events.is_empty()


def test_classify_folds_correction_into_original_event() -> None:
    rows = [
        _silver_filing_row(
            "S005930",
            "주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 1, 5),
            rcept_no="20240105000001",
        ),
        # Correction flagged only by the [기재정정] title tag (rm empty in silver).
        _silver_filing_row(
            "S005930",
            "[기재정정]주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 1, 20),
            rcept_no="20240120000002",
        ),
        # A later genuine second buyback stays its own event.
        _silver_filing_row(
            "S005930",
            "주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 6, 3),
            rcept_no="20240603000003",
        ),
    ]

    events = classify_filing_events(pl.DataFrame(rows)).sort("event_date")

    assert events.height == 2
    assert events["event_date"].to_list() == [date(2024, 1, 5), date(2024, 6, 3)]
    assert events["rcept_no"].to_list() == ["20240105000001", "20240603000003"]
    assert events["is_correction"].to_list() == [False, False]


def test_classify_keeps_correction_without_visible_original() -> None:
    events = classify_filing_events(
        pl.DataFrame(
            [
                _silver_filing_row(
                    "S111110",
                    "[기재정정]주요사항보고서(무상증자결정)",
                    rcept_dt=date(2024, 2, 1),
                    rcept_no="20240201000001",
                )
            ]
        )
    )

    assert events.height == 1
    row = events.row(0, named=True)
    assert row["event_type"] == "bonus_issue"
    assert row["is_correction"] is True
    assert row["event_date"] == date(2024, 2, 1)


def test_build_filing_events_reads_silver_and_writes_gold(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    by_date: dict[date, list[dict]] = {}
    for row in [
        _silver_filing_row(
            "S005930",
            "주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 1, 5),
            rcept_no="20240105000001",
        ),
        _silver_filing_row(
            "S000660",
            "주요사항보고서(유상증자결정)",
            rcept_dt=date(2024, 1, 5),
            rcept_no="20240105000002",
        ),
        _silver_filing_row(
            None,
            "주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 1, 5),
            rcept_no="20240105000003",
        ),
        _silver_filing_row(
            "S005930",
            "[기재정정]주요사항보고서(자기주식취득결정)",
            rcept_dt=date(2024, 1, 20),
            rcept_no="20240120000004",
        ),
    ]:
        by_date.setdefault(row["rcept_dt"], []).append(row)
    for rcept_dt, rows in by_date.items():
        writer.write(
            pl.DataFrame(rows),
            layout.partition_path("silver.filings", rcept_dt),
            mode="overwrite",
        )

    summary = build_filing_events(tmp_path)[0]

    assert summary.dataset == "gold.filing_events"
    assert summary.rows == 2
    events = pl.read_parquet(
        tmp_path / "gold" / "filing_events" / "dt=2024-01-05" / "part.parquet"
    ).sort("security_id")
    assert events["security_id"].to_list() == ["S000660", "S005930"]
    assert events["event_type"].to_list() == ["rights_issue", "buyback"]
    assert events["expected_sign"].to_list() == [-1, 1]
    assert events["ticker"].to_list() == ["000660", "005930"]
    # The correction was folded into the original event: no 01-20 partition.
    assert not (tmp_path / "gold" / "filing_events" / "dt=2024-01-20").exists()


def _write_study_fixture(
    tmp_path,
    *,
    s1_day11_return: float | None = 0.02,
    include_edge_event: bool = False,
) -> list[date]:
    """3 securities x 40 days of gold.daily_prices_adj plus buyback events.

    Base return is 0.01 for everyone; overrides: S000010 on day 10 (0.04) and
    day 11 (``s1_day11_return``), S000020 on day 20 (-0.02). Events: S000010
    available on day 9 (day 0 = day 10) and S000020 available on day 19
    (day 0 = day 20); optionally S000030 available on day 37, whose window is
    cut short by the end of the axis.
    """
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(40)]
    overrides: dict[tuple[str, date], float | None] = {
        ("S000010", days[10]): 0.04,
        ("S000010", days[11]): s1_day11_return,
        ("S000020", days[20]): -0.02,
    }
    for day in days:
        rows = [
            {
                "date": day,
                "security_id": security_id,
                "return_1d": overrides.get((security_id, day), 0.01),
            }
            for security_id in ("S000010", "S000020", "S000030")
        ]
        schema = {"date": pl.Date, "security_id": pl.String, "return_1d": pl.Float64}
        writer.write(
            pl.DataFrame(rows, schema=schema),
            layout.partition_path("gold.daily_prices_adj", day),
            mode="overwrite",
        )
    events = [("S000010", days[9]), ("S000020", days[19])]
    if include_edge_event:
        events.append(("S000030", days[37]))
    for security_id, available in events:
        writer.write(
            pl.DataFrame(
                [
                    {
                        "event_date": available,
                        "available_date": available,
                        "security_id": security_id,
                        "ticker": security_id.removeprefix("S"),
                        "event_type": "buyback",
                        "expected_sign": 1,
                        "rcept_no": f"R{security_id}",
                        "report_nm": "주요사항보고서(자기주식취득결정)",
                        "is_correction": False,
                    }
                ]
            ),
            layout.partition_path("gold.filing_events", available),
            mode="overwrite",
        )
    return days


def test_run_event_study_hand_computed(tmp_path) -> None:
    _write_study_fixture(tmp_path)

    result = run_event_study(
        tmp_path, "buyback", window_pre=2, window_post=3, min_events=2
    )

    by_rel = {row["rel_day"]: row for row in result.by_rel_day.iter_rows(named=True)}
    assert sorted(by_rel) == [-2, -1, 0, 1, 2, 3]
    assert all(row["n"] == 2 for row in by_rel.values())
    # Day 0 is the first trading day STRICTLY after available_date: the S000010
    # spike (0.04 on day 10, market 0.02) must land on rel_day 0, not +1.
    assert by_rel[0]["mean_ar"] == pytest.approx((0.02 - 0.02) / 2)
    assert by_rel[0]["mean_car"] == pytest.approx(0.0)
    assert by_rel[-1]["mean_ar"] == pytest.approx(0.0)
    ar_s1_rel1 = 0.02 - 0.04 / 3  # S000010 day-11 return minus that day's market mean
    assert by_rel[1]["mean_ar"] == pytest.approx(ar_s1_rel1 / 2)
    assert by_rel[1]["mean_car"] == pytest.approx((0.02 + ar_s1_rel1 - 0.02) / 2)
    assert by_rel[3]["mean_car"] == pytest.approx((0.02 + ar_s1_rel1 - 0.02) / 2)
    assert by_rel[3]["median_car"] == pytest.approx((0.02 + ar_s1_rel1 - 0.02) / 2)

    summary = result.summary
    assert summary["n_events"] == 2
    assert summary["car_pre"] == pytest.approx(0.0)
    assert summary["ar_day0"] == pytest.approx(0.0)
    assert summary["car_post"] == pytest.approx(ar_s1_rel1 / 2)
    assert summary["car_full"] == pytest.approx(
        summary["car_pre"] + summary["ar_day0"] + summary["car_post"]
    )
    assert summary["hit_rate"] == pytest.approx(0.5)
    assert result.start == date(2024, 1, 10)  # min event_date
    assert result.end == date(2024, 1, 20)

    report = result.write(tmp_path / "reports" / "event_study" / "buyback-test.html")
    assert report.exists()
    text = report.read_text(encoding="utf-8")
    assert "buyback" in text
    assert "hit_rate" in text


def test_run_event_study_missing_returns_and_window_coverage(tmp_path) -> None:
    _write_study_fixture(tmp_path, s1_day11_return=None, include_edge_event=True)

    result = run_event_study(
        tmp_path, "buyback", window_pre=2, window_post=3, min_events=2
    )

    # The edge event (only 4 of 6 window days exist, < 70%) was dropped, and
    # the S000010 null return removed only that observation, not the event.
    assert result.summary["n_events"] == 2
    by_rel = {row["rel_day"]: row for row in result.by_rel_day.iter_rows(named=True)}
    assert by_rel[1]["n"] == 1
    assert by_rel[0]["n"] == 2
    assert result.summary["car_post"] == pytest.approx(0.0)
    assert result.summary["hit_rate"] == pytest.approx(0.0)


def test_run_event_study_min_events_guard(tmp_path) -> None:
    _write_study_fixture(tmp_path)

    with pytest.raises(ValueError, match="need at least 3"):
        run_event_study(tmp_path, "buyback", window_pre=2, window_post=3, min_events=3)


def test_run_event_study_unknown_type_raises(tmp_path) -> None:
    _write_study_fixture(tmp_path)

    with pytest.raises(ValueError, match="no 'merger' events"):
        run_event_study(tmp_path, "merger", min_events=1)


def test_describe_event_patterns_lists_all_types() -> None:
    rows = {row["event_type"]: row for row in describe_event_patterns()}
    assert rows["buyback"]["patterns"] == 2
    assert rows["buyback"]["expected_sign"] == 1
    assert rows["rights_issue"]["expected_sign"] == -1
    assert set(rows) == {
        "buyback",
        "buyback_disposal",
        "rights_issue",
        "bonus_issue",
        "convertible_bond",
        "capital_reduction",
        "stock_split",
        "stock_merge",
        "merger",
        "dividend",
        "major_holder_change",
        "supply_contract",
        "embezzlement",
        "delisting_risk",
    }


def test_cli_events_list_types_prints_table(capsys) -> None:
    from finance_pi.cli.app import list_event_types

    list_event_types()

    output = capsys.readouterr().out
    assert "buyback\tsign=+1\tpatterns=2" in output
    assert "delisting_risk\tsign=-1\tpatterns=1" in output
