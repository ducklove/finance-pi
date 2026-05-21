# KOSPI 시가총액 데이터 품질 보강 방안

작성일: 2026-05-03

## 목표

KOSPI 종목의 일자별 `market_cap`과 `listed_shares` 품질을 높여서, 시가총액 가중 분석, 유니버스 필터링, 데이터 품질 리포트, 백테스트 검증을 안정적으로 수행할 수 있게 한다.

이번 보강의 1차 범위는 1995-05-02 이후다. 1991-1994년은 무료 공개 원천으로 품질을 보장하기 어렵기 때문에 일단 제외하고, 유료/기관 원천 확보가 결정될 때 별도 프로젝트로 다룬다.

## 현재 부족한 데이터

로컬 가격 데이터는 1990-01-03부터 존재하지만, 보강 전에는 `market_cap`과 `listed_shares`가 2026-04-29부터 2026-04-30까지만 채워져 있었다. 따라서 1995년 이후의 시가총액 가중 분석, 대형주 필터, 시장 전체 커버리지 점검, 지수 재구성 검증을 위해 marcap 기반 보강 레이어를 추가한다.

## 필요한 Gold 데이터셋

### `gold/daily_market_caps`

일자별 종목 시가총액 테이블. 모든 시가총액 가중 분석의 기준 입력이다.

필수 컬럼:

- `date`
- `security_id`
- `listing_id`
- `ticker`
- `name`
- `market`
- `close`
- `listed_shares`
- `market_cap`
- `market_cap_source`
- `price_source`
- `is_estimated`

계산 규칙:

```text
market_cap = close * listed_shares
```

단, KRX 또는 marcap 원천에 `market_cap`이 있으면 원천값을 보존하고, `close * listed_shares`와의 차이를 품질 점검 대상으로 둔다.

### `gold/kospi_membership`

일자별 KOSPI 분석 가능 종목 테이블. 정확한 공식 지수 구성종목 이력이 없을 때는 KOSPI 시장의 보통주 universe를 1차 근사로 사용한다.

필수 컬럼:

- `date`
- `security_id`
- `listing_id`
- `ticker`
- `market`
- `share_class`
- `security_type`
- `is_active`
- `membership_source`

### `gold/kospi_index`

시장 레벨 검증용 KOSPI 지수 시계열.

필수 컬럼:

- `date`
- `index_code`
- `index_name`
- `close`
- `return_1d`
- `source`

## 원천 우선순위

### 1순위: KRX OpenAPI / KRX 정보데이터시스템

역할:

- 2026년 이후 일별 운영 적재
- 가능한 과거 기간의 공식 백필
- `close`, `listed_shares`, `market_cap`, `volume`, `trading_value`의 기준값

작업:

1. KRX OpenAPI 키 권한 복구 또는 재발급
2. `bronze/krx_daily/dt=*/part.parquet` 적재 재개
3. Silver/Gold 변환에서 `market_cap`, `listed_shares` null 금지 검증 추가
4. KRX 정보데이터시스템 CSV 다운로드 경로로 OpenAPI 불가 과거 구간 보완

### 2순위: FinanceData marcap

역할:

- 1995-05-02 이후 과거 시가총액/상장주식수 대량 백필
- KRX OpenAPI 백필이 막힌 구간의 현실적인 대체 원천

marcap의 핵심 컬럼:

- `Date`
- `Code`
- `Name`
- `Open`, `High`, `Low`, `Close`
- `Volume`, `Amount`
- `Marcap`
- `Stocks`
- `Market`

적재 위치:

```text
bronze/marcap/year=YYYY/part.parquet
silver/market_caps
gold/daily_market_caps
```

주의:

- 라이선스가 비상업 조건일 수 있으므로 사용 목적을 확인한다.
- 1991-1994년은 이번 보강 범위에서 제외한다.
- 1995-01-01부터 1995-05-01까지의 일부 공백은 시가총액 분석 범위 밖으로 둔다.

### 보류: 유료/기관 데이터

역할:

- 1991-1994년 구간 보강
- 상장폐지 종목, 종목코드 변경, 합병/분할 이벤트 검증

후보:

- FnGuide DataGuide
- KIS-Value
- NICE / Kisline 계열 데이터
- 기관용 Compustat Global 또는 유사 데이터

이번 단계에서는 사용하지 않는다. 필요성이 다시 생기면 별도 예산/계약 의사결정 후 진행한다.

## 단계별 실행안

### Phase 1: 1995-05-02 이후 시가총액 품질 복구

1. marcap 데이터를 연도별로 가져온다.
2. `Code`를 `security_master`의 `ticker`와 매핑한다.
3. `Marcap`, `Stocks`, `Close`를 `gold/daily_market_caps`로 승격한다.
4. 기존 가격 데이터의 `market_cap`, `listed_shares` 결측을 보강한다.
5. KOSPI 시장 전체 시총, 종목별 시총 순위, 연간 회전율, 대형주 커버리지를 리포트한다.
6. KOSPI 지수 수익률과 시총가중 재구성 수익률의 오차를 품질 지표로 리포트한다.

성공 기준:

- 1995년 이후 거래일별 KOSPI 보통주 `market_cap` null 비율 1% 미만
- 1995년 이후 거래일별 KOSPI 보통주 `listed_shares` null 비율 1% 미만
- `market_cap`과 `close * listed_shares` 차이가 허용오차를 넘는 row가 리포트에 표시됨
- 연간 재구성 수익률과 KOSPI 지수 수익률 차이가 품질 지표로 추적됨

### Phase 2: KRX 공식 데이터로 운영 적재 정상화

1. KRX OpenAPI 권한 문제를 해결한다.
2. 최신 일자부터 `bronze.krx_daily_raw`를 매일 적재한다.
3. marcap과 KRX가 겹치는 구간에서 `close`, `listed_shares`, `market_cap` 차이를 비교한다.
4. KRX 원천을 우선하고 marcap은 백필/검증 원천으로 둔다.

성공 기준:

- 신규 거래일의 `market_cap` null 비율 0%
- KRX와 보조 원천의 종가/시총 불일치 리포트 자동 생성

### Phase 3: 분석 기능 확장

1. `gold.daily_market_caps`를 백테스트와 팩터 엔진에서 읽을 수 있게 한다.
2. 시가총액 분위, 대형주 universe, 유동성 필터를 `market_cap` 기반으로 전환한다.
3. KOSPI 기여도 분석은 품질 검증이 끝난 뒤 별도 리포트로 붙인다.
4. 1991-1994년 확장은 별도 프로젝트로 분리한다.

성공 기준:

- `market_cap` 기반 universe 필터가 결측으로 인한 종목 누락을 리포트함
- 시가총액 분위/랭킹 계산이 1995년 이후 구간에서 동작함
- 기여도 분석은 `gold.daily_market_caps`만 읽고 별도 원천 의존이 없음

## 데이터 품질 점검

필수 체크:

- `market_cap` null 비율
- `listed_shares` null 비율
- `abs(market_cap - close * listed_shares) / market_cap`
- 종목코드-일자 기준 중복
- KOSPI 아닌 ETF/ETN/리츠/스팩 혼입
- 보통주와 우선주 분리 여부
- 거래일별/연도별 커버리지 변화
- 시장별 총 시가총액 시계열 급변
- 종목별 상장주식수 급변
- 시가총액 순위 급변
- 재구성 KOSPI-like 수익률과 실제 KOSPI 수익률 오차

## KOSPI 기여도 분석 산식

시가총액 데이터 품질 보강이 끝난 뒤, 다음 산식으로 KOSPI 기여도 분석을 수행할 수 있다.

```text
stock_return_i = close_i,end / close_i,start - 1
contribution_i = market_cap_i,start * stock_return_i
top_contributor = argmax(contribution_i)
```

최대 기여 종목 제외 수익률:

```text
kospi_like_return = sum(contribution_i) / sum(market_cap_i,start)
ex_top_return =
  (sum(contribution_i) - contribution_top)
  / (sum(market_cap_i,start) - market_cap_top,start)
```

## 구현 작업 목록

1. `bronze.marcap_raw` DatasetSpec 추가
2. marcap 연도별 CSV/GZ ingest 스크립트 추가
3. `silver.market_caps` 변환 추가
4. `gold.daily_market_caps` 변환 추가
5. KOSPI membership view 추가
6. KOSPI index series ingest 추가
7. 기존 가격 Gold 테이블에 `market_cap`, `listed_shares` 보강 조인 추가
8. 커버리지/오차 리포트 추가
9. 기여도 시뮬레이션 스크립트가 `gold.daily_market_caps`를 우선 읽도록 수정

## 실행 명령

1995년 이후 marcap 원천을 Bronze에 적재한다. 연도별 파일이 존재하는 최신 연도까지 실행한다.

```bash
python -m finance_pi.cli.app ingest marcap --start-year 1995 --end-year 2026 --root .
```

시가총액 Silver/Gold만 먼저 빌드한다.

```bash
python -m finance_pi.cli.app build market-caps --root .
```

기존 가격 Gold까지 `market_cap`, `listed_shares` 보강을 반영하려면 전체 빌드를 실행한다.

```bash
python -m finance_pi.cli.app build all --root .
```

marcap 최신 파일보다 뒤의 일자는 KRX 원천으로 보강한다. 이 경로는 `pykrx`의 KRX 로그인 세션을 사용하므로 `.env` 또는 환경 변수에 `KRX_ID`, `KRX_PW`가 필요하다.

```bash
python -m finance_pi.cli.app ingest pykrx-market-caps \
  --since 2026-02-23 \
  --until 2026-04-28 \
  --root .
python -m finance_pi.cli.app build market-caps --root .
python -m finance_pi.cli.app catalog build --root .
```
