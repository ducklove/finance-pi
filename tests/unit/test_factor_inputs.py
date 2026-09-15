from datetime import date

import polars as pl
import pytest

from finance_pi.research.factor_inputs import METRICS, audit, inspect_inputs


def rows():
    return [
        {
            "as_of_date": date(2026, 4, 1),
            "security_id": "S005930",
            "fiscal_period_end": date(2025, 12, 31),
            "available_date": date(2026, 3, 20),
            "rcept_dt": date(2026, 3, 20),
            "rcept_no": "20260320000001",
            "report_type": "11011",
            "account_id": account,
            "amount": 100.0,
            "amount_basis": "current",
            "currency": "KRW",
            "unit": "KRW",
            "is_backfilled": False,
            "is_consolidated": True,
        }
        for account in METRICS
    ]


def test_complete_input_is_only_a_review_candidate():
    r = inspect_inputs(pl.DataFrame(rows()), date(2026, 4, 1))
    assert r["status"] == "review_required"
    assert r["complete_securities"] == 1
    assert r["excluded_rows"] == 0


@pytest.mark.parametrize(
    "changes,reason",
    [
        (
            {"rcept_dt": date(2026, 6, 19), "rcept_no": "20260619000667"},
            "unknown_or_future_publication",
        ),
        ({"available_date": date(2026, 4, 1)}, "unknown_or_future_publication"),
        ({"currency": "USD"}, "unknown_or_foreign_units"),
        ({"unit": None}, "unknown_or_foreign_units"),
        ({"is_backfilled": True}, "historical_or_unknown_provenance"),
        ({"amount": float("nan")}, "invalid_amount"),
        ({"fiscal_period_end": date(2020, 12, 31)}, "stale_or_future_period"),
        ({"rcept_no": "20260230000001"}, "invalid_receipt"),
    ],
)
def test_unsafe_input_is_excluded(changes, reason):
    records = [{**r, **changes} for r in rows()]
    r = inspect_inputs(pl.DataFrame(records), date(2026, 4, 1))
    assert r["eligible_rows"] == 0
    assert r["complete_securities"] == 0
    assert r["exclusions"][reason] == len(records)


def test_different_statement_scopes_cannot_be_combined():
    records = rows()
    records[2]["is_consolidated"] = False  # 자산 계정만 별도 재무제표
    r = inspect_inputs(pl.DataFrame(records), date(2026, 4, 1))
    assert r["eligible_securities"] == 1
    assert r["complete_securities"] == 0


def test_missing_schema_is_unknown_not_zero():
    r = inspect_inputs(pl.DataFrame({"security_id": ["S005930"]}), date(2026, 4, 1))
    assert r["status"] == "missing_fields"
    assert r["eligible_securities"] is None


def test_audit_reads_requested_partition_without_mutation(tmp_path):
    path = tmp_path / "gold/fundamentals_pit/dt=2026-04-01/part.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows()).write_parquet(path)
    before = path.read_bytes()
    r = audit(tmp_path, date(2026, 4, 1))
    assert r["as_of"] == "2026-04-01"
    assert r["live_eligible"] is False
    assert path.read_bytes() == before
    with pytest.raises(ValueError):
        audit(tmp_path, date(2026, 4, 2))
