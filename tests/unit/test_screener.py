"""가치 스크리너 엔드포인트(/api/fundamentals/screener) 테스트.

fixture parquet 으로 security_master / daily_prices_adj / silver.financials /
silver.dividends / bronze.naver_summary 를 시드하고, parquet 폴백 쿼리 경로로
전 유니버스 스크리너 행을 검증한다.

검증 핵심:
- KOSPI/KOSDAQ 일반주식은 포함, SPAC/리츠/ETF 는 제외
- PER 은 naver 원본 우선, roe 도 naver 우선, pbr 은 market_cap/equity 계산
- 빈 데이터(파일 없음) → 빈 결과(에러 아님)
"""

from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.admin.server import AdminState
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter

AS_OF = date(2026, 4, 29)
FIN_DATE = date(2026, 3, 31)


def _master_row(security_id, ticker, name, market, security_type="equity"):
    return {
        "security_id": security_id,
        "ticker": ticker,
        "name": name,
        "market": market,
        "share_class": "common",
        "security_type": security_type,
    }


def _price_row(security_id, listing_id, close, market_cap, listed_shares):
    return {
        "date": AS_OF,
        "security_id": security_id,
        "listing_id": listing_id,
        "open_adj": close,
        "high_adj": close + 1000,
        "low_adj": close - 1000,
        "close_adj": close,
        "return_1d": 0.0,
        "volume": 1000,
        "trading_value": close * 1000,
        "market_cap": market_cap,
        "listed_shares": listed_shares,
        "is_halted": False,
        "is_designated": False,
        "is_liquidation_window": False,
    }


def _fin_row(account_id, amount):
    return {
        "security_id": "S005930",
        "corp_code": "C001",
        "fiscal_period_end": FIN_DATE,
        "event_date": FIN_DATE,
        "rcept_dt": FIN_DATE,
        "available_date": FIN_DATE,
        "report_type": "annual",
        "account_id": account_id,
        "account_name": "metric",
        "amount": float(amount),
        "is_consolidated": True,
        "accounting_basis": "consolidated",
    }


def _naver_row(ticker, name, per, roe, market_cap, listed_shares):
    return {
        "snapshot_dt": AS_OF,
        "ticker": ticker,
        "name": name,
        "market": "KOSPI",
        "close": int(per * 1000),
        "change_abs": 0,
        "change_rate_pct": 0.0,
        "par_value": 100,
        "market_cap": market_cap,
        "listed_shares": listed_shares,
        "foreign_ownership_pct": 50.0,
        "volume": 1000,
        "per": per,
        "roe": roe,
        "_ingested_at": "2026-04-29T00:00:00",
        "_source": "naver",
        "_source_request_hash": "",
    }


def _seed_screener_fixture(tmp_path) -> None:
    """스크리너 조회에 필요한 5개 데이터셋을 최소 시드한다."""
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    for sub in ("gold", "silver", "bronze"):
        (data_root / sub).mkdir(parents=True, exist_ok=True)

    # security_master: 일반주식 2개 + SPAC 1개 + 리츠 1개
    pl.DataFrame(
        [
            _master_row("S005930", "005930", "삼성전자", "KOSPI"),
            _master_row("S000660", "000660", "SK하이닉스", "KOSPI"),
            _master_row("S400000", "400000", "스팩제1호", "KOSPI", "spac_pre"),
            _master_row("S300000", "300000", "에이치리츠", "KOSDAQ"),
        ]
    ).write_parquet(data_root / "gold" / "security_master.parquet")

    # daily_prices_adj: 최신 거래일 2026-04-29
    writer.write(
        pl.DataFrame(
            [
                _price_row("S005930", "L005930", 70000, 4_000_000_000_000, 60_000_000),
                _price_row("S000660", "L000660", 150000, 10_000_000_000_000, 72_000_000),
                _price_row("S400000", "L400000", 2000, 200_000_000, 100_000),
                _price_row("S300000", "L300000", 8000, 800_000_000, 100_000),
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", AS_OF),
    )

    # silver/financials: 삼성전자 equity/net_income/revenue/operating_profit
    fin_dir = data_root / "silver" / "financials" / "fiscal_year=2026"
    fin_dir.mkdir(parents=True, exist_ok=True)
    writer.write(
        pl.DataFrame(
            [
                _fin_row("ifrs-full_EquityAttributableToOwnersOfParent", 400_000_000_000_000),
                _fin_row("ifrs-full_ProfitLossAttributableToOwnersOfParent", 40_000_000_000_000),
                _fin_row("ifrs-full_Revenue", 280_000_000_000_000),
                _fin_row("dart_OperatingIncomeLoss", 30_000_000_000_000),
            ]
        ),
        fin_dir / "part.parquet",
    )

    # silver/dividends: 삼성전자 배당수익률
    div_dir = data_root / "silver" / "dividends" / "fiscal_year=2025"
    div_dir.mkdir(parents=True, exist_ok=True)
    writer.write(
        pl.DataFrame(
            [
                {
                    "fiscal_year": 2025,
                    "fiscal_period_end": date(2025, 12, 31),
                    "rcept_dt": FIN_DATE,
                    "available_date": FIN_DATE,
                    "corp_code": "C001",
                    "corp_name": "삼성전자",
                    "security_id": "S005930",
                    "ticker": "005930",
                    "share_class": "common",
                    "stock_kind": "common",
                    "cash_dividend_per_share": 1440.0,
                    "stock_dividend_per_share": 0.0,
                    "cash_dividend_yield_pct": 2.0,
                    "currency": "KRW",
                    "source_rcept_no": "",
                    "report_type": "annual",
                    "source": "dart",
                    "is_estimated": False,
                }
            ]
        ),
        div_dir / "part.parquet",
    )

    # bronze/naver_summary: PER/ROE 원본값
    naver_dir = data_root / "bronze" / "naver_summary" / "dt=2026-04-29"
    naver_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            _naver_row("005930", "삼성전자", 12.5, 10.0, 4_000_000_000_000, 60_000_000),
            _naver_row("000660", "SK하이닉스", 9.0, 15.0, 10_000_000_000_000, 72_000_000),
        ]
    ).write_parquet(naver_dir / "part.parquet")


def test_screener_returns_kospi_kosdaq_excluding_spac_and_reit(tmp_path) -> None:
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})

    tickers = {row["ticker"] for row in payload["rows"]}
    assert "005930" in tickers
    assert "000660" in tickers
    assert "400000" not in tickers  # SPAC
    assert "300000" not in tickers  # 리츠
    assert payload["as_of"] == "2026-04-29"


def test_screener_prefers_naver_per_and_roe(tmp_path) -> None:
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    samsung = next(row for row in payload["rows"] if row["ticker"] == "005930")
    assert samsung["per"] == 12.5
    assert samsung["roe"] == 10.0


def test_screener_computes_pbr_from_market_cap_and_equity(tmp_path) -> None:
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    samsung = next(row for row in payload["rows"] if row["ticker"] == "005930")
    # pbr = market_cap / equity = 4e12 / 400e12 = 0.01
    assert samsung["pbr"] is not None
    assert abs(samsung["pbr"] - 0.01) < 1e-6


def test_screener_computes_operating_margin(tmp_path) -> None:
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    samsung = next(row for row in payload["rows"] if row["ticker"] == "005930")
    expected = 30_000_000_000_000 / 280_000_000_000_000 * 100
    assert samsung["operating_margin"] is not None
    assert abs(samsung["operating_margin"] - expected) < 1e-3


def test_screener_includes_dividend_yield(tmp_path) -> None:
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    samsung = next(row for row in payload["rows"] if row["ticker"] == "005930")
    assert samsung["dividend_yield"] == 2.0


def test_screener_without_fundamentals_still_returns_price_and_per(tmp_path) -> None:
    """재무 데이터가 없는 종목도 시세 + naver PER/ROE 는 노출된다(LEFT JOIN)."""
    _seed_screener_fixture(tmp_path)
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    hynix = next(row for row in payload["rows"] if row["ticker"] == "000660")
    assert hynix["per"] == 9.0
    assert hynix["roe"] == 15.0
    assert hynix["pbr"] is None
    assert hynix["equity"] is None


def test_screener_returns_empty_when_no_data(tmp_path) -> None:
    """데이터 파일이 전혀 없으면 빈 결과(에러 아님)."""
    payload = AdminState(tmp_path).screener({"as_of": ["2026-04-29"]})
    assert payload["count"] == 0
    assert payload["rows"] == []


def test_screener_defaults_as_of_to_today(tmp_path) -> None:
    """as_of 생략 시 오늘 날짜로 동작(데이터 없어도 에러 아님)."""
    payload = AdminState(tmp_path).screener({})
    assert payload["count"] == 0
    assert payload["as_of"] == date.today().isoformat()
