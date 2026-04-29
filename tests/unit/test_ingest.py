from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.ingest import ResponseCache, request_hash
from finance_pi.ingest.models import IngestUnit, RawBatch


def test_request_hash_is_stable_for_param_order() -> None:
    left = request_hash("krx", "daily", {"b": 2, "a": 1})
    right = request_hash("krx", "daily", {"a": 1, "b": 2})
    assert left == right


def test_response_cache_roundtrip(tmp_path) -> None:
    cache = ResponseCache(tmp_path)
    cache.write_json("krx", "abc", {"rows": [{"ticker": "005930"}]})
    assert cache.exists("krx", "abc")
    assert cache.read_json("krx", "abc") == {"rows": [{"ticker": "005930"}]}


def test_raw_batch_to_frame_accepts_null_then_string() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"stock_code": None},
            {"stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"stock_code": pl.String})

    assert frame["stock_code"].to_list() == [None, "036720"]


def test_raw_batch_to_frame_casts_json_date_strings() -> None:
    batch = RawBatch(
        IngestUnit("opendart", date(2026, 4, 29), "corpCode.xml"),
        [
            {"snapshot_dt": "2026-04-28", "stock_code": None},
            {"snapshot_dt": "2026-04-28", "stock_code": "036720"},
        ],
    )

    frame = batch.to_frame({"snapshot_dt": pl.Date, "stock_code": pl.String})

    assert frame["snapshot_dt"].dtype == pl.Date
    assert frame["snapshot_dt"].to_list() == [date(2026, 4, 28), date(2026, 4, 28)]
    assert frame["stock_code"].to_list() == [None, "036720"]
