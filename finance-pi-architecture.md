# finance-pi 아키텍처 설계 문서

> 한국 주식시장 데이터 파이프라인 + 팩터 백테스트 플랫폼
> 작성 기준일: 2026-04-27 / 상태: 초안 v0.1

---

## 1. 개요

### 1.1 목표

`finance-pi`는 **한국 주식시장의 quant 연구를 위한 풀스택 데이터·백테스트 플랫폼**이다. 단순한 데이터 수집기가 아니라, 다음을 동시에 만족해야 한다.

1. **재현 가능성(reproducibility)** — 같은 일자에 같은 백테스트를 돌리면 같은 결과가 나와야 한다. 즉 데이터의 모든 스냅샷은 시계열로 보존되며, "오늘 본 재무제표"가 아니라 "그 시점에 알 수 있었던 재무제표"가 사용된다.
2. **Survivorship bias 차단** — 현재 상장 종목만이 아니라, 과거에 상장됐던 모든 종목(상장폐지·합병·재상장 포함)이 universe에 포함된다.
3. **Look-ahead bias 차단** — 모든 비-가격 데이터(재무제표·공시·기업행동)는 공시일(`rcept_dt`) 또는 효력 발생일을 기준으로만 사용 가능하다.
4. **종목 정체성(security identity) 추적** — 우선주↔보통주, 스팩↔합병법인, 종목코드 변경/재사용을 명시적으로 관리한다.
5. **연구자 친화 인터페이스** — DuckDB + Polars 기반의 빠른 인터랙티브 분석.

### 1.2 비목표 (out of scope)

- **실시간(틱·호가) 트레이딩 시스템** — 일봉이 최소 단위. 분/초봉은 후순위.
- **자동주문/체결 시스템** — 백테스트만 다루며, 실거래 라우팅은 다루지 않는다.
- **글로벌 시장(미국·홍콩 등)** — KRX(KOSPI·KOSDAQ·KONEX)에 한정.
- **기업가치평가/재무모델링 도구** — 데이터 제공까지가 책임 범위.

### 1.3 핵심 설계 원칙

| 원칙 | 의미 |
|---|---|
| **Point-in-time(PIT) by default** | 모든 분석 API는 `as_of_date`를 받고, 그 시점에 알 수 있던 데이터만 반환한다. |
| **Append-only raw layer** | 원천 응답은 절대 덮어쓰지 않는다. 정정공시·재계산은 새 row로 들어온다. |
| **Identity ≠ Ticker** | 종목코드는 재사용·변경되므로 도메인 ID가 별도로 존재한다. |
| **Fail loud on schema drift** | 외부 API 스키마가 바뀌면 silent하게 NULL이 되는 게 아니라 적재가 실패해야 한다. |
| **Read path is Polars/DuckDB only** | Pandas는 노트북 일회성 외 금지. 메모리 안 터지게. |

---

## 2. 시스템 아키텍처 개관

### 2.1 레이어 구조

```
┌─────────────────────────────────────────────────────────────────┐
│  Application Layer                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │ Backtest     │  │ Factor       │  │ Quality / Fraud      │   │
│  │ Engine       │  │ Library      │  │ Reports              │   │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘   │
└─────────┼─────────────────┼─────────────────────┼───────────────┘
          │                 │                     │
┌─────────▼─────────────────▼─────────────────────▼───────────────┐
│  Analytics Layer (Polars LazyFrames over DuckDB views)          │
│  - PIT join helpers, universe filters, factor primitives        │
└─────────┬───────────────────────────────────────────────────────┘
          │
┌─────────▼───────────────────────────────────────────────────────┐
│  Gold (curated, query-ready)                                    │
│  - daily_prices_adj, fundamentals_pit, universe_history,        │
│    security_master, identity_links, corporate_actions           │
└─────────┬───────────────────────────────────────────────────────┘
          │ DuckDB views / materializations
┌─────────▼───────────────────────────────────────────────────────┐
│  Silver (cleaned, conformed)                                    │
│  - prices_silver, filings_silver, financials_silver,            │
│    security_identity_silver                                     │
└─────────┬───────────────────────────────────────────────────────┘
          │ dbt-style transforms (Python + SQL)
┌─────────▼───────────────────────────────────────────────────────┐
│  Bronze (raw + minimal typing, append-only Parquet)             │
│  - krx_daily_raw, kis_daily_raw, dart_filings_raw,              │
│    dart_financials_raw, pre2010_raw                             │
└─────────┬───────────────────────────────────────────────────────┘
          │
┌─────────▼───────────────────────────────────────────────────────┐
│  Source Adapters                                                │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌─────────────────┐    │
│  │ KRX     │  │ KIS     │  │ OpenDART │  │ Pre-2010 sources│    │
│  └─────────┘  └─────────┘  └──────────┘  └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 메달리언(Bronze / Silver / Gold) 적용 이유

- **Bronze**: 외부 API의 응답을 가능한 한 손대지 않고 보존한다. 정정공시·재계산·스키마 진화에 대비. 모든 row에 `_ingested_at`, `_source`, `_source_request_hash`가 붙는다.
- **Silver**: 정규화·타입 캐스팅·도메인 ID 매핑까지 끝낸 깨끗한 테이블. 단, 비즈니스 로직은 최소한.
- **Gold**: 분석 친화적인 wide table·PIT 조인 결과·유니버스 등. 백테스트가 직접 읽는 곳.

> 같은 종목이라도 Bronze에는 KRX·KIS·DART의 표현이 각각 들어가지만, Silver부터는 단일 `security_id`로 통합된다.

---

## 3. 데이터 소스 레이어

### 3.1 KRX OpenAPI (요구사항 1)

**역할**: 일별 가격(시·고·저·종·거래량·거래대금·시가총액·상장주식수)의 1차 출처.

**구현 노트**

- KRX 정보데이터시스템(OPEN API)은 일자별 전종목 시세(`MDCSTAT01701` 등)를 제공한다. 종목 단위 루프보다 일자 단위 루프가 효율적이다.
- 거래소 휴장일 처리: 거래일 캘린더(`trading_calendar`) 테이블을 별도 관리.
- 적재 단위: **하루 = 1 Parquet 파일**, Bronze 디렉토리는 `bronze/krx_daily/dt=YYYY-MM-DD/part.parquet` Hive partitioning.
- 멱등성: 이미 적재된 일자는 `_ingested_at` 비교 후 skip 또는 새 row append (덮어쓰기 X).
- 백필: 2010-01-04 ~ 현재. 최초 백필은 일자 단위 병렬 처리(`asyncio` + 토큰버킷 rate limiter).

**주의사항**

- KRX API는 시점에 따라 **수정주가(adjusted)와 무수정주가(raw)** 의 정의가 미묘하게 다르다. Bronze에는 **무수정주가**를 받고, Silver에서 corporate actions로 직접 조정한다(아래 §4.4).
- ETF·ETN·ELW·신주인수권증권은 별도 endpoint·별도 테이블로 분리. 본 문서는 보통주·우선주·스팩에 한정.

### 3.2 KIS Open API — 보조 가격 데이터 (요구사항 2)

**역할**: KRX 데이터의 **검증·보강** 용도. 1차 출처를 KIS로 하지 않는 이유:

1. KIS는 회원사(증권사) 인증 기반 — 계정·토큰 발급·갱신 운영 부담.
2. Rate limit이 KRX OPEN API보다 빡빡한 구간이 있음.
3. 호가·체결강도 등 KRX OPEN API에 없는 보조 정보를 제공 — 이쪽이 차별 가치.

**PoC 범위**

- OAuth2 토큰 발급 어댑터 (`KisAuthClient`).
- 일봉 조회(`/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice`)로 KRX와 cross-check.
- 일치율 리포트(`reports/source_consistency_report.html`)에서 KRX vs KIS 종가·거래량 일치율 추적.
- 불일치 시: KRX 우선, KIS는 alert만 발생. 단, KRX가 결측인 경우 KIS로 폴백 시도(이 폴백은 Silver layer에서 명시 기록).

### 3.3 OpenDART — 기업·공시·재무 (요구사항 3)

**역할**: 비-가격 데이터의 정전소(power plant). 세 영역으로 나뉜다.

| 영역 | API | Bronze 테이블 |
|---|---|---|
| 기업개요 | `/api/company.json` + `corpCode.xml` | `dart_company_raw` |
| 공시목록 | `/api/list.json` | `dart_filings_raw` |
| 재무제표 | `/api/fnlttSinglAcntAll.json`, `/api/fnlttCmpnyDartXBRL` | `dart_financials_raw` |

**핵심 설계 결정**

- **`corp_code`(8자리)가 종목 식별의 골격이다.** 종목코드(6자리)는 변경·재사용이 잦지만 `corp_code`는 회사가 사라지지 않는 한 유지된다(상장폐지 후에도). `security_master`의 외부 키로 `corp_code`를 1순위, ISIN을 2순위, 종목코드를 3순위로 둔다.
- 공시목록은 매일 증분 적재(`bgn_de = 어제`, `end_de = 오늘`). 정정공시(`rm`에 '정' 포함)는 새 row로 들어가며, 동일 `rcept_no`의 원공시와 link.
- 재무제표는 **공시일(`rcept_dt`) 기준으로 PIT 색인**된다(§5).

### 3.4 2010년 이전 가격 데이터 (요구사항 4)

이 부분은 **PoC 단계의 조사·통합 작업**으로 명시적으로 분리한다. 2010년 이전 데이터는 단일·신뢰 출처가 없기 때문이다.

**후보 출처 비교 (1차 조사)**

| 출처 | 커버리지 | 라이선스 | 신뢰도 | 통합 난이도 |
|---|---|---|---|---|
| KRX 정보데이터시스템 (수기 다운로드 / 일별 통계) | 1995~ 일부, 종목별 일봉 제공 | 비상업 사용 가능 | 높음 (1차 출처) | 중 (CSV 파싱 + 상장폐지 종목 누락 검증) |
| 키움 OpenAPI+ (구 OCX, COM 기반) | 1985년경~ 가능 (계좌별) | 회원사 약관 | 중~상 | 높음 (Windows COM, Python 32비트 브릿지) |
| 대신증권 CYBOS Plus | 1990년대~ 일부 | 회원사 약관 | 중~상 | 높음 (COM 기반, 동일) |
| FnGuide / KIS-Value | 1980년대~ | **유료, 학술 라이선스** | 매우 높음 | 낮음 (CSV/엑셀, 정제됨) |
| Yahoo Finance (티커.KS) | 1996년경~ 일부 | 비공식 (ToS 회색) | 낮음 (결측·이상치 다수) | 매우 낮음 |
| 학술 데이터베이스 (DataGuide, Compustat Global) | 1980년대~ | 기관 라이선스 | 매우 높음 | 낮음 |
| 한국은행 ECOS, 통계청 KOSIS | 지수·매크로만 | 공공데이터 | 높음 | 낮음 |

**통합 PoC 권장 경로**

1. **KRX 일별 통계 CSV의 자동 다운로드 가능 범위 확정** — 거래소가 제공하는 가장 빠른 시점은 종목·항목별로 다르다. 가능한 만큼은 무조건 1차 출처로.
2. **상장폐지 종목 데이터 보강 우선** — 학술 라이선스가 있다면 FnGuide/DataGuide에서 폐지 종목만 분리 수입(보통주 가격은 Yahoo/Naver와 cross-check).
3. **출처 표지(`price_source`) 컬럼을 Silver에 보존** — `krx`, `kis`, `fnguide`, `kiwoom`, `daishin`, `manual`. 백테스트 사기 탐지 리포트(§9)에서 `price_source != 'krx'` 비중이 높은 기간을 경고한다.
4. **PoC 결과물**: `reports/pre2010_coverage.html` (종목별·연도별 데이터 가용성 히트맵).

> 이 영역은 **계약·예산 의사결정**이 결과를 좌우한다. 설계상으로는 `pre2010_raw` 단일 테이블에 `source`만 식별 가능하면 된다.

---

## 4. 도메인 모델: Security Identity (요구사항 5·6·7)

이 섹션이 본 시스템의 핵심이다. 가격 데이터를 모으는 것보다 **"이 가격이 누구의 가격인가"** 를 정의하는 게 훨씬 어렵다.

### 4.1 핵심 엔티티

```
Issuer (발행회사)        — corp_code (DART) 기준, 영구 식별자
  ├── Security (증권)    — security_id (자체 발급), 보통주/우선주/스팩 구분
  │     └── ListedTicker — listing_id, 종목코드(6자리), 상장기간 [start, end]
  └── ...
```

세 단계로 분리한 이유:

- **회사가 사라져도** corp_code는 남는다(상장폐지 후 비상장 → 재상장 케이스 추적).
- **하나의 회사가 여러 증권을 발행**한다(보통주 + 우선주 + 신형우선주 …).
- **하나의 증권이 종목코드를 바꿀 수 있다**(액면병합·합병·재상장).

### 4.2 보통주 ↔ 우선주 관계 (요구사항 6)

- 한국 우선주는 종목코드가 보통주 코드의 마지막 자리를 5(구형우선)·7(신형우선)·9(2우선) 등으로 바꾼 형태인 경우가 많지만, **이 규칙에만 의존하지 않는다**(예외 다수).
- 명시적 매핑 테이블: `security_relations(parent_security_id, child_security_id, relation_type, effective_from, effective_to)`.
  - `relation_type ∈ {preferred_to_common, common_to_preferred, spac_pre_merger, spac_post_merger, …}`.
- 1차 매핑 소스: KRX 종목정보 + DART 회사개황. 자동 매핑 후 **검수 큐**(`identity_review_queue`)로 보내 수동 확인 후 승격.
- API 측 영향: `factor.compute(...)`에는 기본적으로 `share_class='common'`을 적용. 우선주를 포함하려면 명시적으로 `include_preferred=True`.

### 4.3 스팩(SPAC) 합병 정체성 변화 (요구사항 7)

스팩은 **합병 전후가 사실상 다른 회사**다. 그러나 종목코드와 corp_code는 합병 후 상속·변경되는 경우가 많아 단순 join이 위험하다.

**모델링**

```
SPAC 상장:    security_id=S001 (type=spac_pre)   listing_id=L001  ticker=123456
합병 발효일:  identity_event(kind=spac_merger, from=S001 → to=S002)
합병 후:      security_id=S002 (type=spac_post)  listing_id=L002  ticker=123456 또는 신규
```

- 백테스트의 기본 정책: **`spac_pre` 종목은 universe에서 제외**(설정으로 토글). 합병 후 `spac_post`는 일반 보통주처럼 취급.
- 가격 시계열을 합치지 **않는다**. 두 종목의 OHLC를 인위적으로 이어붙이면 합병가액·전환비율 때문에 거짓 수익률이 발생한다.
- 진단 리포트(§9)에서 "스팩 합병 직후 진입 시그널"을 별도 카운트(전형적인 사기 패턴).

### 4.4 종목코드 재사용·상장폐지 (요구사항 5)

- 한국 종목코드(6자리)는 **재사용된다**. 즉 같은 코드가 다른 회사를 가리킬 수 있다.
- 키 전략: **(`ticker`, `as_of_date`) → `listing_id` → `security_id`** 의 lookup.
- `listings(listing_id, security_id, ticker, market, listed_date, delisted_date, delisting_reason)` 테이블이 진실의 원천.
- 상장폐지 사유(`delisting_reason`) 분류:
  - `merger` — 합병으로 소멸
  - `delisting_for_cause` — 관리종목·감사의견 거절 등
  - `voluntary` — 자진상폐
  - `transfer` — KOSDAQ ↔ KOSPI 이전
  - `unknown` — 1차 적재 후 검수 큐로
- **`delisting_reason='delisting_for_cause'`인 종목의 백테스트 처리**는 큰 함정이다. 보통 폐지 직전 90일은 거래정지·정리매매 구간이라 수익률이 -90% 단위로 찍힌다. 이를 누락하면 survivorship bias, 포함하면 비현실적 손실. 본 시스템은 **포함이 디폴트**, 정리매매 구간은 별도 플래그(`is_liquidation_window`)로 표시.

### 4.5 Historical Universe 구축 절차

```
1. DART corp_code 전체 다운로드 → issuers
2. KRX 상장종목 + 상장폐지종목(현재·과거) 목록 수집
3. ISIN·corp_code·ticker 3중 매칭 → security_master, listings
4. 자동 매칭 실패 row → identity_review_queue (수동 검수)
5. 보통주↔우선주, 스팩 pre↔post 관계 → security_relations
6. 일자별 universe view 생성:
     universe_history(date, security_id, market, is_active, share_class, …)
```

`universe_history`는 매 거래일에 대해 그날 살아 있던 종목 집합을 반환한다. 백테스트는 **반드시** 이 뷰를 통해서만 종목을 선택한다(현재 활성 종목 리스트 직접 사용 금지).

---

## 5. Look-ahead Bias 방지 (요구사항 8)

### 5.1 PIT 원칙

- 모든 비-가격 fact 테이블은 두 시간 컬럼을 동시에 갖는다.
  - `event_date` — 사건이 실제 일어난/회계상 시점 (예: 사업연도말 `2024-12-31`)
  - `available_date` — 그 정보를 알 수 있게 된 시점 (예: 공시일 `2025-03-14`)
- 분석 API는 `available_date <= as_of` 만을 사용한다.

### 5.2 재무제표의 공시일 기반 as-of join

```sql
-- gold.fundamentals_pit 의 정의 (개념)
SELECT
    p.date AS as_of_date,
    p.security_id,
    f.fiscal_period_end,
    f.rcept_dt   AS available_date,
    f.account_id,
    f.amount
FROM gold.daily_calendar p
ASOF JOIN silver.financials_silver f
  ON f.security_id = p.security_id
 AND f.rcept_dt   <= p.date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.security_id, f.account_id, f.fiscal_period_end
    ORDER BY f.rcept_dt DESC
) = 1
```

- DuckDB의 `ASOF JOIN`은 정확히 이 패턴을 위한 구문.
- **정정공시 처리**: 같은 `fiscal_period_end`에 대해 후속 정정 공시(`rcept_dt`가 더 늦은 row)가 있으면, `as_of_date`가 정정공시일 이후일 때만 정정값을 반환. 그 전에는 원공시 값을 반환.
- 즉 **백테스트는 "당시 실제로 알 수 있던 숫자"를 본다**. 사후 보정된 깨끗한 숫자가 아니다.

### 5.3 잠재 함정

- **분기 재무제표의 사업보고서 vs 분기보고서 일관성** — 같은 분기를 두 보고서가 다르게 기록할 수 있다. `report_type` 우선순위(`사업보고서 > 반기 > 분기 > 분기 잠정`)로 해결.
- **연결 vs 별도** — 디폴트는 `consolidated`. `is_consolidated=False`는 별도 호출 시에만.
- **회계기준 변경** — K-IFRS 도입 이전(2010 이전 일부 회사)은 K-GAAP. `accounting_basis` 컬럼으로 명시.

---

## 6. 저장소 레이어 (요구사항 9)

### 6.1 디렉토리 레이아웃

```
data/
├── bronze/
│   ├── krx_daily/             dt=YYYY-MM-DD/part.parquet
│   ├── kis_daily/             dt=YYYY-MM-DD/part.parquet
│   ├── dart_filings/          dt=YYYY-MM-DD/part.parquet
│   ├── dart_financials/       rcept_dt=YYYY-MM-DD/part.parquet
│   ├── dart_company/          snapshot_dt=YYYY-MM-DD/part.parquet
│   └── pre2010/               source=<x>/dt=YYYY-MM-DD/part.parquet
├── silver/
│   ├── prices/                dt=YYYY-MM-DD/part.parquet
│   ├── financials/            fiscal_year=YYYY/part.parquet
│   ├── filings/               dt=YYYY-MM-DD/part.parquet
│   ├── corporate_actions/     dt=YYYY-MM-DD/part.parquet
│   └── security_identity/     part.parquet (전체)
├── gold/
│   ├── daily_prices_adj/      dt=YYYY-MM-DD/part.parquet
│   ├── fundamentals_pit/      dt=YYYY-MM-DD/part.parquet
│   ├── universe_history/      dt=YYYY-MM-DD/part.parquet
│   └── security_master.parquet
├── catalog/
│   └── finance_pi.duckdb       — DuckDB 카탈로그(뷰만, 데이터는 Parquet)
└── reports/
    ├── data_quality/
    └── backtest_fraud/
```

### 6.2 파티셔닝 전략

- **시계열 데이터**: 거래일(`dt`) 단위 Hive partition. DuckDB의 `read_parquet('.../dt=*/part.parquet', hive_partitioning=true)` 가 그대로 동작.
- **재무제표**: 공시일(`rcept_dt`) 단위 — PIT join이 자연스럽게 partition pruning 혜택을 받는다.
- **마스터 테이블**: 단일 파일. 크기가 작아 partition 불필요.
- 작은 파일(<10MB) 누적 시 주간 compaction 잡(`scripts/compact.py`).

### 6.3 DuckDB 카탈로그

DuckDB는 **데이터 저장소가 아니라 쿼리 엔진**으로만 쓴다. 모든 영속 데이터는 Parquet. DuckDB 파일에는 view·매크로만 둔다.

```sql
CREATE OR REPLACE VIEW gold.daily_prices_adj AS
SELECT * FROM read_parquet('data/gold/daily_prices_adj/dt=*/part.parquet',
                           hive_partitioning = true);
```

이렇게 하면:
- 저장소 마이그레이션(예: S3) 시 view만 바꾸면 된다.
- DuckDB 파일 손상이 데이터 손실로 이어지지 않는다.
- Polars에서 `pl.scan_parquet(...)`로도 같은 데이터에 접근 가능.

### 6.4 증분 적재(idempotent ingest)

각 어댑터는 다음 인터페이스를 따른다.

```python
class SourceAdapter(Protocol):
    name: str
    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]: ...
    def fetch(self, unit: IngestUnit) -> RawBatch: ...
    def write_bronze(self, batch: RawBatch) -> WriteResult: ...
```

- 중복 적재는 `(source, primary_key, _source_request_hash)`로 차단.
- 실패 시 재시도는 **응답 캐시**(`data/_cache/<source>/<hash>.json.gz`) 기반. 외부 API를 다시 때리지 않는다.

---

## 7. 분석 레이어 — Polars (요구사항 10)

### 7.1 팩터 계산 인터페이스

```python
import polars as pl
from finance_pi.factors import Factor, factor_registry

@factor_registry.register("momentum_12_1")
class Momentum12_1(Factor):
    """12개월 수익률에서 최근 1개월을 제외한 모멘텀."""
    requires = ["daily_prices_adj"]
    rebalance = "monthly"

    def compute(self, ctx) -> pl.LazyFrame:
        prices = ctx.scan("gold.daily_prices_adj")
        return (
            prices
            .sort(["security_id", "date"])
            .with_columns(
                ret_12m = pl.col("close_adj").pct_change(252).over("security_id"),
                ret_1m  = pl.col("close_adj").pct_change(21).over("security_id"),
            )
            .with_columns(score = (1 + pl.col("ret_12m")) / (1 + pl.col("ret_1m")) - 1)
            .select(["date", "security_id", "score"])
        )
```

- **모든 팩터는 LazyFrame을 반환**. 백테스트 엔진에서 `as_of`·universe 필터를 함께 push down.
- `ctx.scan(name)`은 DuckDB view든 Parquet 직접 스캔이든 같은 추상화.
- `requires`는 데이터 의존성 그래프 — quality 리포트가 이 그래프를 따라 결측·이상 origin을 추적.

### 7.2 lazy 실행과 pushdown

- 거래일 필터는 항상 **partition column(`dt`)** 으로 표현 → Polars/DuckDB 둘 다 partition pruning.
- 종목 필터는 universe view를 inner join → universe 외 종목은 스캔 자체를 스킵.

---

## 8. 백테스트 엔진 (요구사항 11)

### 8.1 워크플로 (월간 리밸런싱)

```
for each rebalance_date in monthly_calendar:
    # 1) signal_date = rebalance_date - signal_lag (디폴트 1영업일)
    # 2) 그 시점 universe 적용 (universe_history(signal_date))
    # 3) 팩터 점수 계산 (PIT)
    # 4) 점수 기반 선택/가중 (top decile, equal weight 등)
    # 5) target_weights 산출
    # 6) entry_date = signal_date + entry_lag (디폴트 1영업일, 시초가/종가 옵션)
    # 7) 거래비용·슬리피지 적용 → realized_weights, cash
    # 8) 보유 기간 동안 일별 수익률 계산
```

### 8.2 핵심 분리

| 일자 | 의미 |
|---|---|
| `signal_date` | 팩터를 계산한 날(이 날까지의 정보만 사용) |
| `rebalance_date` | 의사결정 시점 |
| `entry_date` | 실제 체결되는 날 |
| `exit_date` | 다음 entry 직전 |

- 디폴트는 `signal_date = T`, `rebalance_date = T+1 close`, `entry_date = T+1`. 모두 설정 가능.
- **같은 날에 신호 산출과 진입을 동시에 하지 않는 것이 디폴트** — 가장 흔한 look-ahead 함정.

### 8.3 거래비용·슬리피지

- 비용 모델은 swappable: 고정 bps · 회전율 비례 · 거래량 점유율 기반.
- 슬리피지 디폴트: 종가 대비 10bps + sqrt(participation) 항. 상한·하한가 진입 시 체결 실패(`fill_status='untradable'`).
- 거래정지(`is_halted`)·관리종목 진입(`is_designated`)·정리매매(`is_liquidation_window`) 종목은 디폴트 거래 금지.

### 8.4 결과물

- `BacktestResult` (Polars DataFrame): 일별 NAV·수익률·turnover·position 수.
- `attribution` 테이블: 종목별·섹터별 기여도.
- `ledger`: 모든 매매의 row-level 기록(재현 검증용).

---

## 9. 데이터 품질 + 백테스트 사기 탐지 리포트 (요구사항 12)

### 9.1 데이터 품질 검사

자동 일일 잡(`scripts/run_dq.py`) → `reports/data_quality/<date>.html`.

| 검사 | 임계 |
|---|---|
| 거래일에 가격 결측인 종목 수 | > 5% of universe |
| 일중 |return| > 30% & 거래정지 아님 | 0건 디폴트, 발생 시 검수 큐 |
| 무수정주가의 점프 (split·배당 미반영 의심) | 코퍼레이트 액션과 cross-check |
| 우선주 ⟷ 보통주 가격 비율 > 5 또는 < 0.05 | 매핑 오류 의심 |
| KRX vs KIS 종가 불일치 | > 0.1% bps 차이 시 alert |
| 재무제표 row 수 급변 | 전기 대비 ±50% 이상 |
| `unknown` 식별자 큐 길이 | > 0이면 표시 |

### 9.2 백테스트 사기 탐지 (Backtest Fraud) 리포트

별도 리포트로 분리한다. 데이터가 아닌 **백테스트 그 자체**를 의심하기 위해서. 현실에서는 좋은 백테스트가 망가지는 가장 흔한 원인이다.

| 검사 | 신호 |
|---|---|
| **Survivorship**: universe에 상장폐지 종목이 0인 기간 | 데이터 누락 또는 잘못된 universe 필터 |
| **Look-ahead**: `signal_date`에 alive 아닌 종목 보유 | 코드 버그 |
| **Look-ahead 재무**: 사용된 재무 row의 `available_date > signal_date` | 5.2 위반 |
| **Tiny universe**: 평균 universe 크기 < 30 | 결과 신뢰성 낮음 |
| **Liquidity**: 평균 거래대금 하위 10% 종목이 포지션의 >30% | 슬리피지 모델로 흡수 안 됨 |
| **Concentration**: 단일 종목 >20% in any rebalance | 실거래 부적합 |
| **Penny stocks**: 종가 < 1000원 종목 비중 | high-noise zone |
| **SPAC pre-merger 진입** | §4.3 위반 |
| **Post-delisting return**: 폐지 후 일자에 수익률 발생 | 계산 버그 |
| **Turnover spike**: 평균 turnover의 5배 초과 월 | 비용 모델 점검 |
| **Sharpe inflation by edge dates**: 상위 5거래일 제거 시 Sharpe < 0 | 이상치 의존 |

리포트는 PASS / WARN / FAIL을 명시하고, FAIL이 있으면 백테스트 결과 헤더에 빨간 배너를 강제 출력한다.

---

## 10. 모듈/패키지 구조

```
finance_pi/
├── pyproject.toml
├── src/
│   └── finance_pi/
│       ├── sources/
│       │   ├── krx/        (api client, schemas, ingest)
│       │   ├── kis/
│       │   ├── opendart/
│       │   └── pre2010/
│       ├── ingest/         (orchestration, retry, cache)
│       ├── identity/       (issuers, securities, listings, relations, review queue)
│       ├── calendar/       (trading_calendar, holidays)
│       ├── corporate_actions/
│       ├── pit/            (asof joins, fundamentals_pit builder)
│       ├── storage/        (parquet writers, duckdb catalog, compaction)
│       ├── factors/        (registry, base classes, library)
│       ├── backtest/       (engine, costs, ledger, attribution)
│       ├── reports/        (data quality, fraud detection, html templates)
│       └── cli/            (typer-based: ingest / dq / backtest / report)
├── tests/
│   ├── unit/
│   ├── integration/        (실제 API 일부, vcr.py로 응답 녹화)
│   └── golden/             (작은 PIT 데이터셋으로 회귀 테스트)
├── scripts/
│   ├── backfill.py
│   ├── run_dq.py
│   └── compact.py
└── data/                   (로컬 데이터 lake, gitignore)
```

---

## 11. 기술 스택 결정 근거

| 결정 | 이유 | 대안 / 기각 사유 |
|---|---|---|
| Python 3.11+ | Polars/DuckDB 모두 우선 지원, 타입 힌트 성숙 | Rust(코어 일부는 추후 PyO3 전환 가능) |
| **Polars** for in-memory 분석 | Lazy 실행, 메모리 효율, group_by_dynamic | Pandas — 메모리·속도 모두 부적합 |
| **DuckDB** as 쿼리 엔진 | Parquet 직접 스캔, ASOF JOIN, 단일 바이너리 | SQLite — 컬럼지향 아님 / Postgres — 운영 부담 |
| **Parquet** as 영속 포맷 | 컬럼지향, 압축, 호환성 | Arrow IPC — 더 빠르나 호환성 약함 |
| `httpx` + `tenacity` + `aiolimiter` | 비동기, 재시도, 토큰버킷 | requests — 동기, 백필 느림 |
| `pydantic v2` for raw schemas | 외부 응답 검증 + 스키마 drift 감지 | dataclasses — 검증 부족 |
| `typer` for CLI | 의존성 적고 친화적 | click — 동등하지만 typing 약함 |
| `pytest` + `vcrpy` | API 응답 녹화로 통합 테스트 안정 | mock 전체 — 회귀 못 잡음 |
| `dbt` 미사용 | Python 변환 + DuckDB SQL이면 충분, 의존성 절약 | 향후 도입 여지 있음 |

---

## 12. 단계별 구현 로드맵

| 단계 | 산출물 | 기간 가이드 |
|---|---|---|
| **0. 부트스트랩** | repo·CI·pyproject·기본 lint·DuckDB 카탈로그 골격 | 1주 |
| **1. KRX 가격 백필** | 2010~현재 일별 가격 Bronze→Silver, trading_calendar | 2주 |
| **2. Security identity v1** | corp_code 기반 issuer/security/listing 골격, 자동 매칭 | 2주 |
| **3. Corporate actions** | 액면분할·무상증자·배당 → close_adj 산출 | 1.5주 |
| **4. OpenDART 적재** | 회사·공시·재무 Bronze + financials Silver | 2주 |
| **5. Universe history** | 상장폐지 포함 일자별 universe view | 1주 |
| **6. PIT fundamentals** | fundamentals_pit Gold 빌더 | 1주 |
| **7. 우선주/스팩 매핑** | security_relations + 검수 큐 UI(터미널) | 1.5주 |
| **8. KIS 보조 데이터** | 어댑터 + cross-check 리포트 | 1주 |
| **9. Pre-2010 PoC** | coverage 리포트 + KRX 일별 통계 자동화 | 2주 (별도 트랙) |
| **10. Polars 팩터 라이브러리** | Momentum/Value/Quality 3종 + 등록 메커니즘 | 1.5주 |
| **11. 백테스트 엔진 v1** | 월간 리밸런싱·비용·슬리피지·ledger | 2.5주 |
| **12. DQ + Fraud 리포트** | HTML 리포트 자동 생성, CI에서 게이트 | 1.5주 |

> 1~6은 직렬, 7·8·9는 병렬 가능, 10~12는 6 이후 병렬.

---

## 13. 리스크 및 오픈 이슈

| # | 항목 | 영향 | 완화 |
|---|---|---|---|
| R1 | KRX OPEN API 응답 스키마/엔드포인트 변경 | 적재 중단 | pydantic 검증 + 대체 엔드포인트(KIS) 폴백 |
| R2 | 2010년 이전 데이터 라이선스 비용 | PoC 범위 축소 가능 | KRX 1차 출처 + 무료 보강 우선, 유료는 의사결정 게이트 후 |
| R3 | 보통주↔우선주 자동매핑 정확도 | look-ahead 또는 잘못된 universe | 검수 큐 + 매뉴얼 승격, 기본은 보수적(common only) |
| R4 | 정정공시의 잦은 발생 | PIT 관점에서 백테스트가 시점마다 달라짐 | 정정공시 수 메트릭을 fraud 리포트에 노출 |
| R5 | DuckDB 단일 프로세스 한계 | 대규모 병렬 쿼리 어려움 | 분석은 batch + 잡 단위 격리, 동시성은 이후 단계 |
| R6 | 코퍼레이트 액션 누락 | 가격 점프로 false signal | DQ에서 점프 감지 + DART 공시 cross-check |
| R7 | 종목코드 재사용 | 잘못된 종목 매핑 | (ticker, date) 기반 lookup 강제, 직접 ticker join 금지 (린트로 차단) |
| R8 | 스팩 합병 후 가격시계열 결합 유혹 | 거짓 수익률 | API에서 자동 결합 차단, 명시 plumb-through만 허용 |
| R9 | 거래정지·정리매매 처리 | 비현실적 손익 | 별도 플래그 + 디폴트 거래 차단 |
| R10 | 시간대 / 영업일 캘린더 | off-by-one bias | 단일 trading_calendar 진실원천 + 모든 시점 timezone-aware |

---

## 14. 결론

`finance-pi`의 진짜 가치는 **데이터를 모으는 능력보다 "무엇이 그 시점에 알 수 있던 진실인가"를 정의하는 도메인 모델**에 있다. 따라서 본 설계는 다음을 가장 우선한다.

1. Security identity (issuer / security / listing 분리)
2. PIT 원칙의 일관된 적용
3. 모든 출처에 대한 append-only Bronze 보존
4. 백테스트 자체에 대한 사기 탐지 리포트

이 네 축이 흔들리지 않으면, 팩터·백테스트·리포트는 그 위에서 자연스럽게 확장된다.
