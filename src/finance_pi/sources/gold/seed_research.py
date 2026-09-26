"""Reviewed factual snapshots. Rerun only after reviewing the linked source releases."""

import json
from pathlib import Path

SUPPLY = "https://www.gold.org/goldhub/research/gold-demand-trends/gold-demand-trends-full-year-2025/supply"
MINING = "https://pubs.usgs.gov/periodicals/mcs2026/mcs2026.pdf"
data = {
    "reviewedAt": "2026-09-26",
    "supply": {
        "source": SUPPLY,
        "publishedAt": "2026-01-29",
        "rows": [
            {"year": 2024, "mining": 3650.4, "recycled": 1365.3, "hedging": -53.8, "total": 4961.9},
            {"year": 2025, "mining": 3671.6, "recycled": 1404.3, "hedging": -73.6, "total": 5002.3},
        ],
    },
    "mining": {
        "source": MINING,
        "publication": "USGS Mineral Commodity Summaries 2026",
        "unit": "tonnes",
        "estimatedYear": 2025,
        "world": {"2024": 3280, "2025": 3300},
        "countries": [
            {"name": n, "2024": a, "2025": b}
            for n, a, b in [
                ("중국", 377, 380),
                ("러시아", 310, 310),
                ("호주", 284, 280),
                ("캐나다", 200, 200),
                ("미국", 163, 160),
                ("가나", 149, 150),
                ("멕시코", 140, 140),
                ("카자흐스탄", 130, 130),
                ("우즈베키스탄", 129, 130),
                ("페루", 108, 110),
                ("남아프리카공화국", 90, 90),
                ("인도네시아", 94, 90),
                ("브라질", 82, 80),
            ]
        ],
    },
    "reserves": [
        {
            "country": "미국",
            "tonnes": 8133.46,
            "asOf": "2026-08-31",
            "institution": "미 재무부 · 정부 소유 금",
            "source": "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/gold_reserve?filter=record_date:eq:2026-08-31",
            "note": "8개 보관 항목 합계 × 31.1034768 / 1,000,000 (troy oz → t)",
        },
        {
            "country": "독일",
            "tonnes": 3350.285,
            "asOf": "2025-12-31",
            "institution": "독일연방은행",
            "source": "https://publikationen.bundesbank.de/publikationen-en/reports-studies/annual-reports/annual-report-2025-974388?article=annual-accounts-of-the-deutsche-bundesbank-for-2025-988046",
        },
        {
            "country": "이탈리아",
            "tonnes": 2452,
            "asOf": None,
            "institution": "이탈리아은행",
            "source": "https://www.bancaditalia.it/compiti/riserve-portafoglio-rischi/riserve-oro/index.html?com.dotmarketing.htmlpage.language=1",
            "note": "상시 안내 페이지 수치. 기준일 미표기; 2026-09-26 확인.",
        },
        {
            "country": "프랑스",
            "tonnes": 2437,
            "asOf": "2026-03-25",
            "institution": "프랑스은행",
            "source": "https://www.banque-france.fr/fr/actualites/resultats-2025-de-la-banque-de-france",
            "note": "2025 결산 관련 2026-03-25 발표 시점의 보유량",
        },
        {
            "country": "중국",
            "tonnes": 2306,
            "asOf": "2025-12-31",
            "institution": "중국인민은행 · WGC 집계",
            "source": "https://www.gold.org/goldhub/gold-focus/2026/01/china-gold-market-update-december-demand-rebounds",
        },
        {
            "country": "한국",
            "tonnes": 104.4,
            "asOf": "2025-05-31",
            "institution": "한국은행 · 국회예산정책처 인용",
            "source": "https://nabo.go.kr/board/file/down.do?fid=33318597",
            "note": "NABO 대외경제동향 & 이슈 제2호",
        },
    ],
    "etfs": [
        {
            "ticker": "411060",
            "name": "ACE KRX금현물",
            "market": "한국",
            "type": "현물형",
            "exposure": "KRX 금현물지수",
            "currency": "KRW",
            "note": "국내 금 현물 가격에 노출. KRX 금시장 직접 거래와는 다른 상품.",
            "source": "https://kind.krx.co.kr/disclosure/etfisudetail.do?method=searchEtfIsuSummary&strIsurCd=41106",
        },
        {
            "ticker": "132030",
            "name": "KODEX 골드선물(H)",
            "market": "한국",
            "type": "선물형",
            "exposure": "금 선물 · 환헤지형",
            "currency": "KRW",
            "note": "선물 만기 교체와 환헤지 비용이 가격 추이에 영향을 줄 수 있음.",
            "source": "https://www.samsungfund.com/upload/kodex/newsroom/20251016084306186.pdf",
        },
        {
            "ticker": "GLD",
            "name": "SPDR Gold Shares",
            "market": "미국",
            "type": "현물형",
            "exposure": "실물 금 보유 신탁",
            "currency": "USD",
            "note": "금괴 가격에서 신탁 비용을 차감한 성과 추구.",
            "source": "https://www.ssga.com/us/en/individual/etfs/spdr-gold-shares-gld",
        },
        {
            "ticker": "GLDM",
            "name": "SPDR Gold MiniShares Trust",
            "market": "미국",
            "type": "현물형",
            "exposure": "실물 금 보유 신탁",
            "currency": "USD",
            "note": "실물 금 기반. 보수·거래 스프레드는 공식 자료에서 확인.",
            "source": "https://www.ssga.com/us/en/individual/capabilities/alternatives/gold",
        },
        {
            "ticker": "IAU",
            "name": "iShares Gold Trust",
            "market": "미국",
            "type": "현물형",
            "exposure": "실물 금 보유 신탁",
            "currency": "USD",
            "note": "미국 1940년 투자회사법 등록 펀드와 법적 구조가 다름.",
            "source": "https://www.ishares.com/us/literature/fact-sheet/iau-ishares-gold-trust-fund-fact-sheet-en-us.pdf",
        },
        {
            "ticker": "GDX",
            "name": "VanEck Gold Miners ETF",
            "market": "미국",
            "type": "금광주형",
            "exposure": "금 채굴 기업 주식",
            "currency": "USD",
            "note": "금 현물 대신 기업에 투자. 생산원가·경영·주식시장 위험에 노출.",
            "source": "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/",
        },
    ],
}
Path(__file__).with_name("research.json").write_text(
    json.dumps(data, ensure_ascii=False, indent=2) + "\n"
)
