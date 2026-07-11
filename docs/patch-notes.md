# Finance Pi reliability patch notes

운영 신뢰도 개선 작업의 변경 사항, 검증 결과, 배포 상태를 단계별로 기록한다. 점수는 데이터 정합성 35점, 수집 완성도 20점, 서빙 안정성 20점, 보안 10점, 성능 10점, 운영 복구성 5점의 합계다.

## 2026-07-11 - 기준선 평가

- 운영 신뢰도: **48/100**
- 검증 기준: 로컬 전체 테스트와 커버리지, Raspberry Pi 서비스/데이터셋/디스크 상태, 실제 API 응답, Parquet 및 DuckDB 정합성 질의
- 주요 위험: 재무제표 원본 grain 손실과 PIT 충돌, 배당 증분 수집 시 동년도 데이터 유실, 수집 예외 은폐, 실패한 과거 일자의 영구 건너뛰기, 일별 시가총액 미갱신, 프록시 경유 POST 인증 우회, MCP의 임의 로컬 파일 조회, 얕은 데이터 품질 검사와 미검증 백업

## 2026-07-11 - 1단계: 즉시 손실 및 장애 은폐 차단

### 데이터 정합성 및 수집

- 배당 저장을 동년도 전체 교체 방식에서 기존 행과 신규 행의 키 기반 upsert 방식으로 변경했다. 한 기업의 증분 수집이 같은 회계연도의 다른 기업 배당을 삭제하지 않는다.
- OpenDART 공시 수집에 `--refresh-latest`를 추가했다. 일일 수집은 최근 7일 파티션을 다시 조회하고 `rcept_no` 기준으로 병합해 늦게 등록된 공시를 놓치지 않는다.
- 어댑터가 반환한 `error:` 결과를 CLI 성공으로 처리하지 않고 예외로 승격한다. 각 파티션의 격리는 유지하되 명령과 systemd 실행은 실패 상태가 된다.
- 매크로 수집 실패를 일일 실패 목록과 marker에 포함한다.
- `complete_with_failures` marker를 완료로 보지 않고 다음 실행에서 재시도한다.
- 일일 빌드에 `gold.daily_market_caps` 갱신을 포함했다.

### 서빙 및 보안

- `/api/jobs` POST는 루프백과 사설망 요청도 항상 `X-Admin-Token`을 요구한다. 토큰이 설정되지 않았으면 변경 작업을 거부한다.
- DuckDB MCP 연결에서 외부 파일 접근을 비활성화해 `read_text`, `read_parquet` 등을 통한 카탈로그 밖 로컬 파일 조회를 차단했다.
- MCP 결과 바이트 한도를 첫 행부터 강제해 단일 대형 행이 제한을 우회하지 못하게 했다.
- Apache reverse proxy에 `/files` 경로를 추가해 관리 UI의 파일 다운로드 경로를 일치시켰다.

### 운영 안전장치

- admin 및 daily systemd 서비스에 `UMask=0077`을 적용했다.
- daily 서비스의 `--no-strict`를 제거하고 90분 실행 제한, 5 GiB soft limit, 6 GiB hard limit을 추가했다.
- 배포 시 `.env` 권한을 `0600`으로 강제한다.
- 배포 시 저장소의 systemd user unit과 Apache proxy 설정을 설치·재로드해 코드와 운영 설정이 함께 반영되도록 했다.
- Windows 배포 래퍼가 stdin 끝에 CRLF를 덧붙여 정상 배포를 실패로 보고하던 문제를 직접 stdin 스트리밍 방식으로 수정했다.
- 위 동작을 고정하는 배당 upsert, 최근 공시 refresh, 수집 실패 전파, macro 실패, marker 재시도, 시가총액 빌드, POST 인증, MCP 외부 접근/바이트 제한, 운영 설정 회귀 테스트를 추가했다.

### 검증 및 반영 상태

- 로컬 테스트: **312/312 통과**, Ruff 통과, statement/branch 종합 coverage 68%
- Raspberry Pi 코드 배포: `035fa39`, 서버 전체 테스트 312개 및 admin health 통과
- 운영 설정: daily strict 실행, 90분 timeout, MemoryHigh 5 GiB, MemoryMax 6 GiB, admin/daily `UMask=0077`, Apache `/files` proxy, `.env` mode `0600` 실적용 확인
- 보안 실검증: Apache 경유 무토큰 `POST /api/jobs`는 401, MCP 정상 카탈로그 질의는 성공하고 데이터 루트 밖 `read_text('README.md')`는 차단
- 시가총액 복구: `silver.market_caps` 15,214,118행, `gold.daily_market_caps` 최신 2026-07-10까지 재생성
- 배당 복구: OpenDART 8,915건을 연도별 처리해 전체 11,538행으로 복원. 2023/2024/2025 회계연도는 각각 3,720/2,539/1,325행, 1,165/1,146/1,156개 기업이며 업무 키 중복 0건
- 단계 종료 운영 신뢰도: **68/100**
  - 데이터 정합성 21/35: 배당·시가총액 손상은 복구했으나 재무제표 원본 grain과 PIT 충돌이 남음
  - 수집 완성도 14/20: 실패 전파·재시도·최신 공시 refresh를 확보했으나 과거 실패 backfill과 빈 데이터셋이 남음
  - 서빙 안정성 15/20: strict/timeout/memory limit과 경로 정합성을 확보했으나 readiness와 원자적 catalog publish가 남음
  - 보안 8/10: POST와 MCP 경계를 닫았으나 세분화된 권한과 감사 로그 강화가 남음
  - 성능 7/10: 현재 부하는 안정적이나 대형 screener 응답 최적화가 남음
  - 운영 복구성 3/5: 배포 재현성은 개선했으나 검증된 백업·복구 훈련이 남음

## 2026-07-11 - 2단계: 재무제표 grain 및 연간/PIT 의미 복구

### 데이터 정합성

- OpenDART 재무제표 Bronze/Silver에 `rcept_no`, 재무제표 구분·명칭, 계정 상세, 정렬 순서, 당기·전기·전전기 기간명과 금액, 당기 누적금액, 통화·단위를 보존한다.
- 손익계산서 분기 자료에 누적금액(`thstrm_add_amount`)이 있으면 계산용 `amount`가 누적금액을 사용하고, 원래 당기금액도 별도 컬럼에 유지한다.
- 재무제표 중복 키를 접수번호·회계기간·보고서·재무제표·계정 상세·정렬 순서·연결 범위까지 확장했다.
- 새 grain으로 refresh된 행이 있으면 같은 coarse grain의 구형 행을 Silver 빌드에서 제외해 마이그레이션 중 이중 계상을 막는다.

### PIT 및 API 의미

- `gold.fundamentals_pit`가 같은 계정의 연간/Q1/반기/Q3를 서로 덮어쓰지 않고 보고서·재무제표별 최신 회계기간을 각각 보존한다.
- Polars PIT 빌더와 DuckDB PIT SQL을 동일한 키·우선순위로 변경했다.
- PIT 상태 버전을 2로 올려 기존 row-count marker가 남아 있어도 의미 변경 시 전체 파티션을 자동 재생성한다.
- `/api/fundamentals/basic`, 전체 종목 screener, MCP `get_fundamentals`가 실제 최신 연간 보고서만 선택하도록 수정했다. 더 최신인 Q1이 연간 수치로 노출되지 않는다.
- 재무제표 bulk ingest에 `--refresh`를 추가해 완료 marker가 있어도 원문 grain을 다시 받을 수 있게 했다.

### 검증 및 반영 상태

- 로컬 테스트: **314/314 통과**, Ruff 통과
- Raspberry Pi 배포, 최근 연간 재무제표 refresh, Silver/PIT/catalog 재빌드: 진행 중
- 단계 종료 점수: 실데이터 재검증 후 기록 예정
