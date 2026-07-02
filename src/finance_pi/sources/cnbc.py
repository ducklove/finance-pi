from __future__ import annotations

import json
from datetime import UTC, date, datetime
from urllib.parse import quote
from urllib.request import Request, urlopen

CNBC_QUOTE_TIMEOUT_SECONDS = 15

CNBC_INDEX_SERIES = [
    {
        "series_id": "SP500",
        "cnbc_symbol": ".SPX",
        "country": "US",
        "name": "S&P 500 Index",
        "category": "equity_index",
        "currency": "USD",
    },
    {
        "series_id": "NASDAQ",
        "cnbc_symbol": ".IXIC",
        "country": "US",
        "name": "NASDAQ Composite Index",
        "category": "equity_index",
        "currency": "USD",
    },
    {
        "series_id": "DOW_JONES",
        "cnbc_symbol": ".DJI",
        "country": "US",
        "name": "Dow Jones Industrial Average",
        "category": "equity_index",
        "currency": "USD",
    },
    {
        "series_id": "NIKKEI_225",
        "cnbc_symbol": ".N225",
        "country": "JP",
        "name": "Nikkei 225",
        "category": "equity_index",
        "currency": "JPY",
    },
    {
        "series_id": "HANG_SENG",
        "cnbc_symbol": ".HSI",
        "country": "HK",
        "name": "Hang Seng Index",
        "category": "equity_index",
        "currency": "HKD",
    },
    {
        "series_id": "SSE_COMPOSITE",
        "cnbc_symbol": ".SSEC",
        "country": "CN",
        "name": "Shanghai Composite Index",
        "category": "equity_index",
        "currency": "CNY",
    },
    {
        "series_id": "US_DOLLAR_INDEX",
        "cnbc_symbol": ".DXY",
        "country": "US",
        "name": "US Dollar Index",
        "category": "fx_index",
        "currency": "USD",
    },
]
CNBC_RATE_SERIES = [
    {
        "series_id": "US_TREASURY_2Y",
        "cnbc_symbol": "US2Y",
        "country": "US",
        "name": "U.S. 2 Year Treasury",
        "frequency": "D",
        "tenor": "2Y",
        "unit": "percent",
    },
    {
        "series_id": "US_TREASURY_10Y",
        "cnbc_symbol": "US10Y",
        "country": "US",
        "name": "U.S. 10 Year Treasury",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
    {
        "series_id": "US_TREASURY_30Y",
        "cnbc_symbol": "US30Y",
        "country": "US",
        "name": "U.S. 30 Year Treasury",
        "frequency": "D",
        "tenor": "30Y",
        "unit": "percent",
    },
    {
        "series_id": "KR_GOVT_5Y",
        "cnbc_symbol": "KR5Y",
        "country": "KR",
        "name": "Korea Treasury Bond 5-Year Yield",
        "frequency": "D",
        "tenor": "5Y",
        "unit": "percent",
    },
    {
        "series_id": "KR_GOVT_10Y_DAILY",
        "cnbc_symbol": "KR10Y",
        "country": "KR",
        "name": "Korea Treasury Bond 10-Year Yield",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
    {
        "series_id": "JP_GOVT_10Y",
        "cnbc_symbol": "JP10Y",
        "country": "JP",
        "name": "Japan 10 Year Treasury",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
    {
        "series_id": "DE_GOVT_10Y",
        "cnbc_symbol": "DE10Y",
        "country": "DE",
        "name": "Germany 10 Year Bund",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
    {
        "series_id": "FR_GOVT_10Y",
        "cnbc_symbol": "FR10Y",
        "country": "FR",
        "name": "France 10 Year Bond",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
    {
        "series_id": "GB_GOVT_10Y",
        "cnbc_symbol": "GB10Y",
        "country": "GB",
        "name": "British 10 Year Gilt",
        "frequency": "D",
        "tenor": "10Y",
        "unit": "percent",
    },
]
CNBC_COMMODITY_SERIES = [
    {
        "series_id": "GOLD_USD_OZ",
        "cnbc_symbol": "@GC.1",
        "name": "Gold COMEX",
        "commodity": "gold",
        "unit": "troy_oz",
        "currency": "USD",
    },
    {
        "series_id": "SILVER_USD_OZ",
        "cnbc_symbol": "@SI.1",
        "name": "Silver COMEX",
        "commodity": "silver",
        "unit": "troy_oz",
        "currency": "USD",
    },
    {
        "series_id": "WTI_USD_BBL",
        "cnbc_symbol": "@CL.1",
        "name": "WTI Crude",
        "commodity": "wti",
        "unit": "barrel",
        "currency": "USD",
    },
    {
        "series_id": "BRENT_USD_BBL",
        "cnbc_symbol": "@LCO.1",
        "name": "ICE Brent Crude",
        "commodity": "brent",
        "unit": "barrel",
        "currency": "USD",
    },
]
CNBC_FX_SERIES = [
    {
        "series_id": "USD_KRW",
        "cnbc_symbol": "KRW=",
        "base_currency": "USD",
        "quote_currency": "KRW",
    },
    {
        "series_id": "EUR_KRW",
        "cnbc_symbol": "EURKRW=",
        "base_currency": "EUR",
        "quote_currency": "KRW",
    },
    {
        "series_id": "JPY_KRW",
        "cnbc_symbol": "JPYKRW=",
        "base_currency": "JPY",
        "quote_currency": "KRW",
    },
    {
        "series_id": "CNY_KRW",
        "cnbc_symbol": "CNYKRW=",
        "base_currency": "CNY",
        "quote_currency": "KRW",
    },
    {
        "series_id": "AUD_KRW",
        "cnbc_symbol": "AUDKRW=",
        "base_currency": "AUD",
        "quote_currency": "KRW",
    },
    {
        "series_id": "USD_VND",
        "cnbc_symbol": "VND=",
        "base_currency": "USD",
        "quote_currency": "VND",
    },
]
CNBC_DERIVED_FX_SERIES = [
    {
        "series_id": "VND_KRW",
        "numerator_symbol": "KRW=",
        "denominator_symbol": "VND=",
        "base_currency": "VND",
        "quote_currency": "KRW",
    },
]


def fetch_cnbc_quotes(symbols: list[str]) -> dict[str, dict[str, str]]:
    if not symbols:
        return {}
    encoded_symbols = quote("|".join(symbols), safe="")
    url = (
        "https://quote.cnbc.com/quote-html-webservice/quote.htm"
        f"?symbols={encoded_symbols}"
        "&requestMethod=quick"
        "&exthrs=1"
        "&noform=1"
        "&partnerId=2"
        "&fund=1"
        "&output=json"
    )
    request = Request(url, headers={"User-Agent": "finance-pi/0.1"})
    with urlopen(request, timeout=CNBC_QUOTE_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    quotes = payload.get("QuickQuoteResult", {}).get("QuickQuote", [])
    if isinstance(quotes, dict):
        quotes = [quotes]
    result = {}
    for item in quotes:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if symbol and item.get("code") == "0" and item.get("last"):
            result[str(symbol)] = item
    return result


def cnbc_index_rows(
    series_list: list[dict[str, str]],
    quotes: dict[str, dict[str, str]],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    rows = []
    for series in series_list:
        quote_item = quotes.get(series["cnbc_symbol"])
        parsed = cnbc_quote_date_value(quote_item, since, until)
        if parsed is None:
            continue
        logical_date, value = parsed
        rows.append(
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": quote_item.get("name") or series["name"],
                "frequency": "D",
                "category": series["category"],
                "value": value,
                "currency": series["currency"],
                "return_1d": cnbc_return_1d(quote_item, value),
                "return_1m": None,
                "source": "cnbc",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def cnbc_rate_rows(
    series_list: list[dict[str, str]],
    quotes: dict[str, dict[str, str]],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    rows = []
    for series in series_list:
        quote_item = quotes.get(series["cnbc_symbol"])
        parsed = cnbc_quote_date_value(quote_item, since, until)
        if parsed is None:
            continue
        logical_date, value = parsed
        rows.append(
            {
                "date": logical_date,
                "country": series["country"],
                "series_id": series["series_id"],
                "name": quote_item.get("name") or series["name"],
                "frequency": series["frequency"],
                "tenor": series["tenor"],
                "value": value,
                "unit": series["unit"],
                "source": "cnbc",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def cnbc_commodity_rows(
    series_list: list[dict[str, str]],
    quotes: dict[str, dict[str, str]],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    rows = []
    for series in series_list:
        quote_item = quotes.get(series["cnbc_symbol"])
        parsed = cnbc_quote_date_value(quote_item, since, until)
        if parsed is None:
            continue
        logical_date, value = parsed
        rows.append(
            {
                "date": logical_date,
                "series_id": series["series_id"],
                "name": quote_item.get("name") or series["name"],
                "commodity": series["commodity"],
                "value": value,
                "unit": series["unit"],
                "currency": series["currency"],
                "source": "cnbc",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def cnbc_fx_rows(
    series_list: list[dict[str, str]],
    quotes: dict[str, dict[str, str]],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    rows = []
    for series in series_list:
        quote_item = quotes.get(series["cnbc_symbol"])
        parsed = cnbc_quote_date_value(quote_item, since, until)
        if parsed is None:
            continue
        logical_date, value = parsed
        rows.append(
            {
                "date": logical_date,
                "series_id": series["series_id"],
                "base_currency": series["base_currency"],
                "quote_currency": series["quote_currency"],
                "value": value,
                "source": "cnbc",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def cnbc_derived_fx_rows(
    series_list: list[dict[str, str]],
    quotes: dict[str, dict[str, str]],
    since: date,
    until: date,
) -> list[dict[str, object]]:
    rows = []
    for series in series_list:
        numerator = cnbc_quote_date_value(quotes.get(series["numerator_symbol"]), since, until)
        denominator = cnbc_quote_date_value(
            quotes.get(series["denominator_symbol"]),
            since,
            until,
        )
        if numerator is None or denominator is None:
            continue
        numerator_date, numerator_value = numerator
        denominator_date, denominator_value = denominator
        if numerator_date != denominator_date or denominator_value == 0:
            continue
        rows.append(
            {
                "date": numerator_date,
                "series_id": series["series_id"],
                "base_currency": series["base_currency"],
                "quote_currency": series["quote_currency"],
                "value": numerator_value / denominator_value,
                "source": "cnbc_derived",
                "updated_at": datetime.now(UTC),
            }
        )
    return rows


def cnbc_quote_date_value(
    quote_item: dict[str, str] | None,
    since: date,
    until: date,
) -> tuple[date, float] | None:
    if quote_item is None:
        return None
    raw_date = quote_item.get("last_time") or quote_item.get("reg_last_time")
    raw_value = quote_item.get("last")
    if raw_date is None or raw_value is None:
        return None
    logical_date = parse_cnbc_datetime(raw_date).date()
    if logical_date < since or logical_date > until:
        return None
    return logical_date, parse_cnbc_number(raw_value)


def cnbc_return_1d(quote_item: dict[str, str], value: float) -> float | None:
    previous = quote_item.get("previous_day_closing") or quote_item.get("prev_prev_closing")
    if previous is None:
        return None
    return cnbc_pct_change(value, parse_cnbc_number(previous))


def parse_cnbc_datetime(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        return datetime.fromisoformat(value)


def parse_cnbc_number(value: object) -> float:
    return float(str(value).replace(",", ""))


def cnbc_pct_change(value: float, previous: float | None) -> float | None:
    if previous in (None, 0):
        return None
    return (value / previous - 1.0) * 100.0
