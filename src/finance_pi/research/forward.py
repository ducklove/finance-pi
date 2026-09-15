"""사전에 고정한 시작일 이후의 일봉 재생. 실제 호가 체결과 구분한다."""

from datetime import date

from finance_pi.research.pairs import PairConfig, digest, signals, simulate

FORWARD_VERSION = "daily-forward-1"


def forward_analysis(snapshot: dict, config: PairConfig, start: date) -> dict:
    if start <= config.start:
        raise ValueError("전진 평가 시작일은 원래 연구 시작일 이후여야 합니다.")
    rows = signals(snapshot["bars"], config)
    selected = [
        (b, s) for b, s in zip(snapshot["bars"], rows, strict=True) if b["date"] >= str(start)
    ]
    result = {
        "version": FORWARD_VERSION,
        "start": str(start),
        "status": "available" if selected else "waiting",
        "scenarios": [],
        "orders_sent": 0,
        "execution_model": "next_observation_close_replay",
        "note": (
            "등록 이후 완료 일봉으로 재생한 가상 원장입니다. "
            "최초 일봉은 신호만 생성하며 다음 일봉부터 집행합니다. "
            "중단 기간도 재생하며 실제 호가·체결·배당은 반영하지 않습니다."
        ),
    }
    if selected:
        bars, selected_signals = map(list, zip(*selected, strict=True))
        evaluation = config.model_copy(update={"start": start})
        result["scenarios"] = [
            simulate(bars, selected_signals, evaluation, mode) for mode in ("switch", "mixed")
        ]
    result["ledger_hash"] = digest(result)
    return result
