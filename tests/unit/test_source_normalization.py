from __future__ import annotations

from datetime import date

from finance_pi.cli.app import _parse_dart_financial_report
from finance_pi.sources.kis import normalize_kis_daily_row
from finance_pi.sources.krx import normalize_krx_daily_row
from finance_pi.sources.naver import parse_daily_price_payload, parse_market_sum_page
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
            "hts_kor_isnm": "Samsung Electronics",
            "stck_oprc": "70000",
            "stck_hgpr": "71000",
            "stck_lwpr": "69000",
            "stck_clpr": "70500",
            "acml_vol": "1000",
        },
    )

    assert row["date"] == date(2024, 1, 2)
    assert row["ticker"] == "005930"
    assert row["name"] == "Samsung Electronics"
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


def test_parse_naver_market_sum_page_converts_units() -> None:
    html = """
    <table><tbody>
    <tr>
      <td class="no">1</td>
      <td><a href="/item/main.naver?code=005930" class="tltle">Samsung</a></td>
      <td class="number">70,500</td>
      <td class="number"><span class="blind">상승</span>1,000</td>
      <td class="number">+1.44%</td>
      <td class="number">100</td>
      <td class="number">1,234,567</td>
      <td class="number">5,846,279</td>
      <td class="number">49.19</td>
      <td class="number">12,345</td>
      <td class="number">15.3</td>
      <td class="number">10.1</td>
      <td></td>
    </tr>
    </tbody></table>
    """

    row = parse_market_sum_page(html, date(2026, 4, 29), "KOSPI")[0]

    assert row["ticker"] == "005930"
    assert row["market_cap"] == 1_234_567 * 100_000_000
    assert row["listed_shares"] == 5_846_279_000
    assert row["foreign_ownership_pct"] == 49.19


def test_parse_naver_daily_price_payload() -> None:
    payload = """
    [['날짜', '시가', '고가', '저가', '종가', '거래량', '외국인소진율'],
     ['20260428', 224000, 226000, 221500, 222000, 18444490, 49.19]]
    """

    row = parse_daily_price_payload(payload, "005930")[0]

    assert row["date"] == date(2026, 4, 28)
    assert row["ticker"] == "005930"
    assert row["close"] == 222000.0
    assert row["volume"] == 18444490


def test_parse_dart_financial_report_names() -> None:
    assert _parse_dart_financial_report(
        "\uc0ac\uc5c5\ubcf4\uace0\uc11c (2025.12)",
        date(2026, 3, 15),
    ) == (2025, "11011")
    assert _parse_dart_financial_report(
        "\ubc18\uae30\ubcf4\uace0\uc11c (2026.06)",
        date(2026, 8, 14),
    ) == (2026, "11012")
    assert _parse_dart_financial_report(
        "\ubd84\uae30\ubcf4\uace0\uc11c (2026.09)",
        date(2026, 11, 14),
    ) == (2026, "11014")
