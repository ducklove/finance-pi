# 전문 연구 데이터 연결

보통주·우선주 관계·권리는 common_preferred_spread, ETF 조건·비교 후보는 EIAYN이 소유한다.
`research/catalogs.py`가 고정된 공급자 URL만 읽고, 상대 스냅샷 경로·SHA-256·JSON 중복 키·
실행 제한·종목 코드·기준일과 게시 시각을 검사한다. 수집 시각은 별도 receipt에 기록한다.
시세·조정주가·거래상태·유동성과 계산은 기존 finance-pi 데이터레이크가 담당한다.

`GET /api/research/pairs`는 공급자 버전과 후보 목록을 반환한다. 우선주는 high-confidence
security_relations와 교집합만 허용하며 source/accepted/excluded 수를 함께 반환한다.
`pair-analysis`와 `pair-forward`는 `catalog_snapshot_id`를 반드시 지정해야 한다.
서버가 수집하지 않은 버전, 공급자 장애·14일 초과 자료에는 로컬 대체 후보를 사용하지 않는다.

입력은 `data/research/catalogs/<provider>/<sha>.json`에 불변 보관한다. 연구 결과에는
공급자·고정 버전·종목 검토·대상 ETF 상품 정보를 담는다. 이후에는 최신 공급 자료와
고정된 종목 검토를 비교하고, 상품 관계·보수·기초지수 등 변경 시 새 연구를 요구한다.
단순 일일 카탈로그 발행은 기존 연구 버전을 바꾸지 않는다.

엔진 계약은 preferred-switch-3 / etf-switch-3다. 계산 방식은 v2와 같고 입력 출처 계약만 바뀐다.
기존 v2 보고서는 보존되지만 새 관찰·전진 평가는 재연구해야 한다.
라이브러리의 catalog 미전달 우선주 로드는 격리된 계산 검사에만 사용하며 운영 HTTP는
반드시 전문 공급자 검증을 수행한다. ETF 하드코딩 목록은 제거했다.
배당·분배금은 총수익에 새로 합산하지 않는다. 현재 조건은 시점별 역사 자료가 아니다.
