from __future__ import annotations

from finance_pi.ingest import ResponseCache, request_hash


def test_request_hash_is_stable_for_param_order() -> None:
    left = request_hash("krx", "daily", {"b": 2, "a": 1})
    right = request_hash("krx", "daily", {"a": 1, "b": 2})
    assert left == right


def test_response_cache_roundtrip(tmp_path) -> None:
    cache = ResponseCache(tmp_path)
    cache.write_json("krx", "abc", {"rows": [{"ticker": "005930"}]})
    assert cache.exists("krx", "abc")
    assert cache.read_json("krx", "abc") == {"rows": [{"ticker": "005930"}]}
