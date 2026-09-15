import json
from datetime import date

import pytest

from finance_pi.research.readiness import price_readiness


def setup_marker(root, failures):
    path = root / "_state/daily/2026-09-15.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "status": "complete_with_failures",
                "price_date": "2026-09-15",
                "gold_price_partition": True,
                "failures": failures,
            }
        ),
        encoding="utf-8",
    )
    return {
        "status": "not_ready",
        "checks": {
            **{
                k: True
                for k in (
                    "data_root",
                    "catalog",
                    "catalog_query",
                    "catalog_datasets",
                    "latest_price_rows",
                    "price_fresh",
                )
            },
            "latest_price_date": "2026-09-15",
            "incomplete_daily_count": 1,
            "incomplete_daily_dates": ["2026-09-15"],
        },
    }


def test_only_known_macro_failures_can_be_nonblocking(tmp_path):
    global_ready = setup_marker(tmp_path, ["Macro US_CPI_ALL ingest failed: timed out"])
    result = price_readiness(tmp_path, global_ready, date(2026, 9, 16))
    assert result["status"] == "ready"
    assert result["global_status"] == "not_ready"
    assert result["unrelated_macro_failures"] == 1
    assert global_ready["status"] == "not_ready"


@pytest.mark.parametrize(
    "failures",
    [
        [],
        ["Naver price failed"],
        ["unknown"],
        ["Macro US_CPI_ALL ingest failed: timeout", "Gold failed"],
    ],
)
def test_price_or_unknown_failures_block_observation(tmp_path, failures):
    payload = setup_marker(tmp_path, failures)
    assert price_readiness(tmp_path, payload, date(2026, 9, 16))["status"] == "not_ready"


def test_stale_prices_and_truncated_failure_list_block(tmp_path):
    payload = setup_marker(tmp_path, ["Macro US_CPI_ALL ingest failed: timeout"])
    assert price_readiness(tmp_path, payload, date(2026, 9, 17))["status"] == "not_ready"
    payload["checks"]["incomplete_daily_count"] = 11
    assert price_readiness(tmp_path, payload, date(2026, 9, 16))["status"] == "not_ready"


def test_invalid_or_missing_marker_blocks(tmp_path):
    payload = setup_marker(tmp_path, ["Macro US_CPI_ALL ingest failed: timeout"])
    path = tmp_path / "_state/daily/2026-09-15.json"
    path.write_text("null", encoding="utf-8")
    assert price_readiness(tmp_path, payload, date(2026, 9, 16))["status"] == "not_ready"
    path.unlink()
    assert price_readiness(tmp_path, payload, date(2026, 9, 16))["status"] == "not_ready"
