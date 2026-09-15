"""설정 선택 없이 고정된 전략을 시간순 세 구간에서 재평가한다."""

from datetime import date


def period_validation(snapshot, config, signal_rows):
    from finance_pi.research.pairs import digest, simulate

    bars = snapshot["bars"]
    dates = [b["date"] for b in bars if str(config.start) <= b["date"] <= str(config.end)]
    report = {
        "version": "fixed-period-1",
        "method": "chronological_three_equal_observation_blocks",
        "benchmark": "mixed",
        "minimum_observations_per_period": 63,
        "out_of_sample_claim": False,
        "note": (
            "사후 기간 분할 진단입니다. 미관측 검증이나 수익성 합격 판정이 아닙니다. "
            "각 구간은 초기 현금으로 다시 시작합니다."
        ),
        "periods": [],
    }
    if len(dates) < 189:
        return {**report, "status": "insufficient_data", "observations": len(dates)}
    edges = [0, len(dates) // 3, len(dates) * 2 // 3, len(dates)]
    for index in range(3):
        first, last = dates[edges[index]], dates[edges[index + 1] - 1]
        period_config = config.model_copy(
            update={"start": date.fromisoformat(first), "end": date.fromisoformat(last)}
        )
        # 미래 구간의 가격·신호는 시뮬레이터에 전달하지 않는다.
        prefix = [b for b in bars if b["date"] <= last]
        past_signals = signal_rows[: len(prefix)]
        strategy = simulate(prefix, past_signals, period_config, "switch")
        benchmark = simulate(prefix, past_signals, period_config, "mixed")
        report["periods"].append(
            {
                "start": first,
                "end": last,
                "observations": edges[index + 1] - edges[index],
                "config_hash": digest(period_config.model_dump(mode="json")),
                "return_pct": strategy["return_pct"],
                "benchmark_return_pct": benchmark["return_pct"],
                "excess_return_pct": strategy["return_pct"] - benchmark["return_pct"],
                "max_drawdown_pct": strategy["max_drawdown_pct"],
                "cost": strategy["cost"],
                "trade_count": strategy["trade_count"],
            }
        )
    return {
        **report,
        "status": "available",
        "positive_excess_periods": sum(p["excess_return_pct"] > 0 for p in report["periods"]),
    }
