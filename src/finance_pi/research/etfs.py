"""공식 자료로 기초지수를 확인한 연구 후보. 역사적 등가성이나 실행 승인은 아니다."""

ETF_ENGINE_VERSION = "etf-switch-1"
ETF_PAIRS = [
    {
        "common": "069500",
        "preferred": "102110",
        "name": "KODEX 200 / TIGER 200",
        "strategy": "etf_switch",
        "benchmark": "KOSPI 200",
        "classification": "same_index_research_candidate",
        "reviewed_at": "2026-09-16",
        "point_in_time_verified": False,
        "execution_eligible": False,
        "sources": [
            "https://www.samsungfund.com/etf/product/view.do?id=2ETF01",
            "https://kind.krx.co.kr/disclosure/etfisudetail.do?method=searchEtfIsuSummary&strIsurCd=10211",
        ],
        "limitations": "과거 분배·복제·비용 정책의 일치와 iNAV는 검증 전입니다.",
    }
]


def etf_pairs():
    return [dict(pair) for pair in ETF_PAIRS]
