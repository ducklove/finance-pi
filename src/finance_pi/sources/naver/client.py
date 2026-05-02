from __future__ import annotations

import ast
import html
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from finance_pi.config import format_yyyymmdd, parse_yyyymmdd
from finance_pi.http import HttpJsonClient
from finance_pi.sources.parsing import parse_float, parse_int

_ITEM_RE = re.compile(
    r'<a\s+href="/item/main\.naver\?code=(?P<ticker>[0-9A-Z]{6})"[^>]*class="tltle"[^>]*>'
    r"(?P<name>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_ROW_RE = re.compile(r"<tr[^>]*>.*?</tr>", re.IGNORECASE | re.DOTALL)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_PAGE_RE = re.compile(r"page=(\d+)")
_MARKETS = {"KOSPI": 0, "KOSDAQ": 1}


@dataclass(frozen=True)
class NaverFinanceClient:
    http: HttpJsonClient
    user_agent: str = "Mozilla/5.0 finance-pi/0.1"

    def fetch_market_summary(
        self,
        snapshot_date: date,
        *,
        markets: tuple[str, ...] = ("KOSPI", "KOSDAQ"),
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for market in markets:
            rows.extend(self._fetch_one_market(snapshot_date, market.upper()))
        return rows

    def _fetch_one_market(self, snapshot_date: date, market: str) -> list[dict[str, Any]]:
        if market not in _MARKETS:
            raise ValueError(f"unsupported Naver market: {market}")
        first = self._fetch_page(market, 1)
        rows = parse_market_sum_page(first, snapshot_date, market)
        for page in range(2, _last_page(first) + 1):
            rows.extend(
                parse_market_sum_page(self._fetch_page(market, page), snapshot_date, market)
            )
        return rows

    def _fetch_page(self, market: str, page: int) -> str:
        return self.http.get_text(
            "/sise/sise_market_sum.naver",
            headers={"User-Agent": self.user_agent},
            params={
                "sosok": _MARKETS[market],
                "page": page,
                "fieldIds": [
                    "market_sum",
                    "listed_stock_cnt",
                    "frgn_rate",
                ],
            },
        )


@dataclass(frozen=True)
class NaverDailyPriceClient:
    http: HttpJsonClient
    user_agent: str = "Mozilla/5.0 finance-pi/0.1"

    def fetch_daily_prices(self, ticker: str, since: date, until: date) -> list[dict[str, Any]]:
        payload = self.http.get_text(
            "/siseJson.naver",
            headers={"User-Agent": self.user_agent},
            params={
                "symbol": ticker.zfill(6),
                "requestType": 1,
                "startTime": format_yyyymmdd(since),
                "endTime": format_yyyymmdd(until),
                "timeframe": "day",
            },
        )
        return parse_daily_price_payload(payload, ticker)


def parse_market_sum_page(
    text: str,
    snapshot_date: date,
    market: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_row in _ROW_RE.findall(text):
        item = _ITEM_RE.search(raw_row)
        if not item:
            continue
        cells = [_clean_cell(cell) for cell in _TD_RE.findall(raw_row)]
        if len(cells) < 12:
            continue
        market_cap_eok = _parse_int(cells[6])
        listed_thousand = _parse_int(cells[7])
        rows.append(
            {
                "snapshot_dt": snapshot_date,
                "ticker": _normalize_ticker_symbol(item.group("ticker")),
                "name": _clean_cell(item.group("name")),
                "market": market,
                "close": _parse_int(cells[2]),
                "change_abs": _parse_int(cells[3]),
                "change_rate_pct": _parse_percent(cells[4]),
                "par_value": _parse_int(cells[5]),
                "market_cap": market_cap_eok * 100_000_000
                if market_cap_eok is not None
                else None,
                "listed_shares": listed_thousand * 1_000
                if listed_thousand is not None
                else None,
                "foreign_ownership_pct": _parse_percent(cells[8]),
                "volume": _parse_int(cells[9]),
                "per": _parse_float(cells[10]),
                "roe": _parse_float(cells[11]),
            }
        )
    return rows


def parse_daily_price_payload(payload: str, ticker: str) -> list[dict[str, Any]]:
    parsed = ast.literal_eval(payload.strip())
    if not isinstance(parsed, list) or not parsed:
        return []
    rows: list[dict[str, Any]] = []
    for raw in parsed[1:]:
        if not isinstance(raw, list) or len(raw) < 6:
            continue
        rows.append(
            {
                "date": parse_yyyymmdd(str(raw[0])),
                "ticker": _normalize_ticker_symbol(ticker),
                "isin": None,
                "name": _normalize_ticker_symbol(ticker),
                "market": "KRX",
                "open": parse_float(raw[1], default=0.0),
                "high": parse_float(raw[2], default=0.0),
                "low": parse_float(raw[3], default=0.0),
                "close": parse_float(raw[4], default=0.0),
                "volume": parse_int(raw[5], default=0),
                "trading_value": None,
                "market_cap": None,
                "listed_shares": None,
            }
        )
    return rows


def _clean_cell(value: str) -> str:
    text = html.unescape(_TAG_RE.sub(" ", value))
    return " ".join(text.split()).strip()


def _parse_int(value: str) -> int | None:
    return parse_int(_normalize_number(value), default=None)


def _parse_float(value: str) -> float | None:
    return parse_float(_normalize_number(value), default=None)


def _parse_percent(value: str) -> float | None:
    return _parse_float(value.replace("%", ""))


def _normalize_number(value: str) -> str:
    text = value.replace(",", "").replace("%", "").strip()
    if text in {"", "-", "N/A"}:
        return text
    sign = "-" if "하락" in text or text.startswith("-") else ""
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return text
    return f"{sign}{match.group(0)}"


def _normalize_ticker_symbol(value: str) -> str:
    text = value.strip().upper()
    return text.zfill(6) if text.isdigit() else text


def _last_page(text: str) -> int:
    pages = [int(page) for page in _PAGE_RE.findall(text)]
    return max(pages, default=1)
