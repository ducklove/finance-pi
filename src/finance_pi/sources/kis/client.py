from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from finance_pi.config import format_yyyymmdd
from finance_pi.http import HttpJsonClient, SourceApiError
from finance_pi.sources.parsing import parse_date, parse_float, parse_int, value_for


@dataclass(frozen=True)
class KisDailyPriceClient:
    http: HttpJsonClient
    app_key: str
    app_secret: str
    access_token: str

    def fetch_daily_prices(self, ticker: str, since: date, until: date) -> list[dict[str, Any]]:
        payload = self.http.get_json(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers={
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "FHKST03010100",
                "custtype": "P",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": format_yyyymmdd(since),
                "FID_INPUT_DATE_2": format_yyyymmdd(until),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "1",
            },
        )
        if str(payload.get("rt_cd", "0")) != "0":
            raise SourceApiError("kis", str(payload.get("msg1", "request failed")), payload=payload)
        metadata = payload.get("output1", {})
        metadata = metadata if isinstance(metadata, dict) else {}
        rows = payload.get("output2", [])
        if not isinstance(rows, list):
            raise SourceApiError("kis", "expected output2 list", payload=payload)
        return [
            normalize_kis_daily_row(ticker, {**metadata, **row})
            for row in rows
            if isinstance(row, dict)
        ]


def normalize_kis_daily_row(ticker: str, row: dict[str, Any]) -> dict[str, Any]:
    normalized_ticker = ticker.zfill(6)
    return {
        "date": parse_date(value_for(row, "stck_bsop_date", "date")),
        "ticker": normalized_ticker,
        "isin": None,
        "name": str(value_for(row, "hts_kor_isnm", "prdt_name", "name", default=normalized_ticker)),
        "market": "KRX",
        "open": parse_float(value_for(row, "stck_oprc", "open"), default=0.0),
        "high": parse_float(value_for(row, "stck_hgpr", "high"), default=0.0),
        "low": parse_float(value_for(row, "stck_lwpr", "low"), default=0.0),
        "close": parse_float(value_for(row, "stck_clpr", "close"), default=0.0),
        "volume": parse_int(value_for(row, "acml_vol", "volume"), default=0),
        "trading_value": parse_int(value_for(row, "acml_tr_pbmn", "trading_value"), default=0),
        "market_cap": None,
        "listed_shares": None,
    }
