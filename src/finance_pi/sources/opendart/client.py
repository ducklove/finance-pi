from __future__ import annotations

import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from typing import Any

from finance_pi.config import format_yyyymmdd
from finance_pi.http import HttpJsonClient, SourceApiError
from finance_pi.sources.opendart.schemas import DartCompanyRow, DartFilingRow, DartFinancialRow
from finance_pi.sources.parsing import parse_float, parse_int, value_for

REPORT_PERIOD_END = {
    "11013": (3, 31),
    "11012": (6, 30),
    "11014": (9, 30),
    "11011": (12, 31),
}


@dataclass(frozen=True)
class OpenDartClient:
    api_key: str
    http: HttpJsonClient

    def fetch_corp_codes(self, snapshot_dt: date) -> list[dict[str, Any]]:
        content = self.http.get_bytes("/api/corpCode.xml", params={"crtfc_key": self.api_key})
        with zipfile.ZipFile(BytesIO(content)) as archive:
            xml_name = archive.namelist()[0]
            root = ET.fromstring(archive.read(xml_name))
        rows: list[dict[str, Any]] = []
        for item in root.findall("list"):
            row = {
                "snapshot_dt": snapshot_dt,
                "corp_code": _xml_text(item, "corp_code"),
                "corp_name": _xml_text(item, "corp_name"),
                "stock_code": _empty_to_none(_xml_text(item, "stock_code")),
                "modify_date": _empty_to_none(_xml_text(item, "modify_date")),
            }
            rows.append(DartCompanyRow.model_validate(row).model_dump(mode="json"))
        return rows

    def fetch_filings(self, since: date, until: date) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page_no = 1
        page_count = 100
        while True:
            payload = self.http.get_json(
                "/api/list.json",
                params={
                    "crtfc_key": self.api_key,
                    "bgn_de": format_yyyymmdd(since),
                    "end_de": format_yyyymmdd(until),
                    "page_no": page_no,
                    "page_count": page_count,
                },
            )
            status = str(payload.get("status", "000"))
            if status == "013":
                return rows
            if status != "000":
                raise SourceApiError(
                    "opendart",
                    str(payload.get("message", status)),
                    payload=payload,
                )
            for item in payload.get("list", []):
                row = {
                    "rcept_dt": _dart_date(item.get("rcept_dt")),
                    "corp_code": item.get("corp_code"),
                    "corp_name": item.get("corp_name"),
                    "stock_code": _empty_to_none(item.get("stock_code")),
                    "rcept_no": item.get("rcept_no"),
                    "report_nm": item.get("report_nm"),
                    "rm": _empty_to_none(item.get("rm")),
                }
                rows.append(DartFilingRow.model_validate(row).model_dump(mode="json"))
            total_count = parse_int(payload.get("total_count"), default=len(rows)) or len(rows)
            if page_no * page_count >= total_count:
                return rows
            page_no += 1

    def fetch_financials(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str,
        *,
        available_date: date,
        fs_div: str = "CFS",
        is_backfilled: bool = False,
    ) -> list[dict[str, Any]]:
        payload = self.http.get_json(
            "/api/fnlttSinglAcntAll.json",
            params={
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
        status = str(payload.get("status", "000"))
        if status == "013":
            return []
        if status != "000":
            raise SourceApiError("opendart", str(payload.get("message", status)), payload=payload)
        fiscal_period_end = _period_end(bsns_year, reprt_code)
        rows: list[dict[str, Any]] = []
        for item in payload.get("list", []):
            amount = parse_float(
                value_for(item, "thstrm_amount", "amount", "thstrm_add_amount"),
                default=None,
            )
            if amount is None:
                continue
            row = {
                "corp_code": corp_code,
                "security_id": None,
                "fiscal_period_end": fiscal_period_end,
                "event_date": fiscal_period_end,
                # The first 8 digits of rcept_no are the true DART receipt date;
                # fall back to the scheduling date only when they are missing.
                "rcept_dt": _receipt_date(item.get("rcept_no")) or available_date,
                "available_date": available_date,
                "report_type": reprt_code,
                "account_id": value_for(item, "account_id", "account_nm"),
                "account_name": value_for(item, "account_nm", "account_id"),
                "amount": amount,
                "is_consolidated": fs_div.upper() == "CFS",
                "accounting_basis": value_for(item, "frmtrm_nm", default=None),
                "is_backfilled": is_backfilled,
            }
            rows.append(DartFinancialRow.model_validate(row).model_dump(mode="json"))
        return rows

    def fetch_dividend_matters(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
    ) -> list[dict[str, Any]]:
        payload = self.http.get_json(
            "/api/alotMatter.json",
            params={
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
        )
        status = str(payload.get("status", "000"))
        if status == "013":
            return []
        if status != "000":
            raise SourceApiError("opendart", str(payload.get("message", status)), payload=payload)
        return [
            {
                "rcept_no": item.get("rcept_no"),
                "corp_code": item.get("corp_code") or corp_code,
                "corp_name": item.get("corp_name"),
                "se": item.get("se"),
                "stock_knd": _empty_to_none(item.get("stock_knd")),
                "thstrm": item.get("thstrm"),
                "frmtrm": item.get("frmtrm"),
                "lwfr": item.get("lwfr"),
                "stlm_dt": _dart_date_dash(item.get("stlm_dt")) if item.get("stlm_dt") else None,
            }
            for item in payload.get("list", [])
        ]

    def fetch_stock_total_quantity(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
    ) -> list[dict[str, Any]]:
        payload = self.http.get_json(
            "/api/stockTotqySttus.json",
            params={
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
        )
        status = str(payload.get("status", "000"))
        if status == "013":
            return []
        if status != "000":
            raise SourceApiError("opendart", str(payload.get("message", status)), payload=payload)
        return [
            {
                "rcept_no": item.get("rcept_no"),
                "corp_code": item.get("corp_code") or corp_code,
                "corp_name": item.get("corp_name"),
                "se": item.get("se"),
                "isu_stock_totqy": item.get("isu_stock_totqy"),
                "now_to_isu_stock_totqy": item.get("now_to_isu_stock_totqy"),
                "now_to_dcrs_stock_totqy": item.get("now_to_dcrs_stock_totqy"),
                "redc": item.get("redc"),
                "profit_incnr": item.get("profit_incnr"),
                "rdmstk_repy": item.get("rdmstk_repy"),
                "etc": item.get("etc"),
                "istc_totqy": item.get("istc_totqy"),
                "tesstk_co": item.get("tesstk_co"),
                "distb_stock_co": item.get("distb_stock_co"),
                "stlm_dt": _dart_date_dash(item.get("stlm_dt")) if item.get("stlm_dt") else None,
            }
            for item in payload.get("list", [])
        ]


def _xml_text(item: ET.Element, tag: str) -> str | None:
    found = item.find(tag)
    return found.text.strip() if found is not None and found.text else None


def _empty_to_none(value: Any) -> Any:
    return None if value in (None, "", " ") else value


def _dart_date(value: Any) -> date:
    text = str(value)
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _receipt_date(rcept_no: Any) -> date | None:
    text = str(rcept_no or "")
    if len(text) < 8 or not text[:8].isdigit():
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _dart_date_dash(value: Any) -> date:
    text = str(value)
    return date.fromisoformat(text)


def _period_end(bsns_year: int, reprt_code: str) -> date:
    month, day = REPORT_PERIOD_END.get(reprt_code, (12, 31))
    return date(bsns_year, month, day)
