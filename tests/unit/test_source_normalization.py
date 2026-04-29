from __future__ import annotations

from datetime import date

from finance_pi.sources.kis import normalize_kis_daily_row
from finance_pi.sources.krx import normalize_krx_daily_row
from finance_pi.sources.opendart import DartCompanyRow, DartFilingRow


def test_normalize_krx_daily_row_accepts_krx_fields() -> None:
    row = normalize_krx_daily_row(
        {
            "ISU_SRT_CD": "5930",
            "ISU_CD": "KR7005930003",
            "ISU_ABBRV": "삼성전자",
            "MKT_NM": "KOSPI",
            "TDD_OPNPRC": "70,000",
            "TDD_HGPRC": "71,000",
            "TDD_LWPRC": "69,000",
            "TDD_CLSPRC": "70,500",
            "ACC_TRDVOL": "1,000",
            "ACC_TRDVAL": "70,500,000",
        },
        date(2024, 1, 2),
    )

    assert row["ticker"] == "005930"
    assert row["close"] == 70500.0
    assert row["volume"] == 1000


def test_normalize_kis_daily_row_accepts_output2_fields() -> None:
    row = normalize_kis_daily_row(
        "005930",
        {
            "stck_bsop_date": "20240102",
            "stck_oprc": "70000",
            "stck_hgpr": "71000",
            "stck_lwpr": "69000",
            "stck_clpr": "70500",
            "acml_vol": "1000",
        },
    )

    assert row["date"] == date(2024, 1, 2)
    assert row["ticker"] == "005930"
    assert row["close"] == 70500.0


def test_opendart_non_numeric_stock_codes_are_not_fatal() -> None:
    company = DartCompanyRow.model_validate(
        {
            "snapshot_dt": date(2026, 4, 29),
            "corp_code": "12345678",
            "corp_name": "Non Equity Corp",
            "stock_code": "0068Y0",
            "modify_date": "20260429",
        }
    )
    filing = DartFilingRow.model_validate(
        {
            "rcept_dt": date(2026, 4, 29),
            "corp_code": "12345678",
            "corp_name": "Non Equity Corp",
            "stock_code": "0068Y0",
            "rcept_no": "20260429000001",
            "report_nm": "Report",
        }
    )

    assert company.stock_code is None
    assert filing.stock_code is None
