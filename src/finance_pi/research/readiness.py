"""일봉만 사용하는 연구 관찰의 의존 데이터 검사. 전체 readiness는 유지한다."""

import json
import re
from datetime import timedelta

from finance_pi.calendar import TradingCalendar


def price_readiness(data_root, global_ready, today):
    checks = global_ready.get("checks", {})
    failures = []
    unrelated = 0
    for key in (
        "data_root",
        "catalog",
        "catalog_query",
        "catalog_datasets",
        "latest_price_rows",
        "price_fresh",
    ):
        if checks.get(key) is not True:
            failures.append(key)
    completed = TradingCalendar.krx_trading_days(
        today - timedelta(days=20), today - timedelta(days=1)
    ).dates
    expected = str(completed[-1]) if completed else None
    latest = checks.get("latest_price_date")
    if not expected or latest != expected:
        failures.append("latest_completed_price_date")
    pending = checks.get("incomplete_daily_dates", [])
    if checks.get("incomplete_daily_count") != len(pending):
        failures.append("incomplete_daily_list_truncated")
    for day in sorted(set(pending + ([latest] if latest else []))):
        try:
            # 날짜만 경로로 사용한다. 외부 입력이나 임의 경로는 허용하지 않는다.
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
                raise ValueError("invalid date")
            marker = json.loads(
                (data_root / "_state/daily" / f"{day}.json").read_text(encoding="utf-8")
            )
            status, errors = marker.get("status"), marker.get("failures", [])
            if marker.get("gold_price_partition") is not True or marker.get("price_date") != day:
                failures.append("price_marker")
            elif status == "complete" and not errors:
                continue
            elif (
                status == "complete_with_failures"
                and isinstance(errors, list)
                and errors
                and all(
                    isinstance(e, str) and re.match(r"^Macro [A-Z0-9_]+ ingest failed:", e)
                    for e in errors
                )
            ):
                unrelated += len(errors)
            else:
                failures.append("unclassified_or_price_failure")
        except (OSError, ValueError, AttributeError, TypeError):
            failures.append("invalid_daily_marker")
    return {
        "status": "ready" if not failures else "not_ready",
        "scope": "pair_daily_prices",
        "global_status": global_ready.get("status"),
        "checks": {"latest_price_date": latest, "expected_price_date": expected},
        "blocking_checks": sorted(set(failures)),
        "unrelated_macro_failures": unrelated,
        "note": "대상 종목의 가격·중복·거래상태·스냅샷 검사는 연구 응답에서 추가 수행합니다.",
    }
