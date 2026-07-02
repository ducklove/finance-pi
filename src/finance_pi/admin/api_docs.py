# ruff: noqa: E501
from __future__ import annotations

from typing import Any


def build_api_docs_payload(
    *,
    generated_at: str,
    workspace: str,
    max_request_threads: int,
    max_admin_jobs: int,
    max_price_queries: int,
    price_query_wait_seconds: float,
    max_price_tickers: int,
    max_price_days: int,
    max_price_cells: int,
    default_daily_price_fields: list[str],
    daily_price_fields: list[str],
    basic_fundamental_metrics: list[str],
    capital_action_metrics: list[str],
    macro_table_columns: dict[str, list[str]],
) -> dict[str, Any]:
    base_url = "/api"
    # Unauthenticated endpoint: no absolute filesystem paths in the response.
    return {
        "service": "finance-pi admin API",
        "generated_at": generated_at,
        "workspace": workspace,
        "auth": {
            "local_network": "LAN clients are allowed without a token",
            "token": "Use X-Admin-Token or token query parameter outside local networks",
        },
        "limits": {
            "max_request_threads": max_request_threads,
            "max_admin_jobs": max_admin_jobs,
            "max_price_queries": max_price_queries,
            "price_query_wait_seconds": price_query_wait_seconds,
            "max_price_tickers": max_price_tickers,
            "max_price_days": max_price_days,
            "max_price_cells": max_price_cells,
        },
        "endpoints": {
            "health": {
                "method": "GET",
                "path": f"{base_url}/health",
                "description": "Lightweight service health check.",
            },
            "close_prices": {
                "method": "GET",
                "path": f"{base_url}/prices/close",
                "description": (
                    "Adjusted close prices. Supports one ticker or batched tickers. "
                    f"tickers x days must not exceed {max_price_cells}."
                ),
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "since": "YYYY-MM-DD inclusive.",
                    "until": "YYYY-MM-DD inclusive.",
                },
                "examples": [
                    f"{base_url}/prices/close?ticker=005930&since=2026-04-29&until=2026-04-30",
                    f"{base_url}/prices/close?tickers=005930,000660&since=2026-04-29&until=2026-04-30",
                ],
            },
            "daily_prices": {
                "method": "GET",
                "path": f"{base_url}/prices/daily",
                "description": (
                    "Daily adjusted OHLCV rows with optional market fields. "
                    f"tickers x days must not exceed {max_price_cells}."
                ),
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "since": "YYYY-MM-DD inclusive.",
                    "until": "YYYY-MM-DD inclusive.",
                    "fields": "Optional comma-separated subset. Defaults to open,high,low,close,volume,trading_value.",
                },
                "fields": {
                    "default": default_daily_price_fields,
                    "available": daily_price_fields,
                },
                "examples": [
                    f"{base_url}/prices/daily?tickers=005930,000660&since=2026-04-29&until=2026-04-30",
                    f"{base_url}/prices/daily?ticker=005930&since=2026-04-29&until=2026-04-30&fields=close,volume",
                ],
            },
            "quotes": {
                "method": "GET",
                "path": f"{base_url}/quotes",
                "description": "Domestic and overseas quote snapshots. Korean numeric tickers use local Naver/gold data; other symbols use CNBC quotes.",
                "query": {
                    "symbol": "Single ticker/symbol, e.g. 005930 or AAPL.",
                    "symbols": "Comma-separated tickers/symbols, e.g. 005930,AAPL,.SPX.",
                },
                "examples": [
                    f"{base_url}/quotes?symbols=005930,000660",
                    f"{base_url}/quotes?symbols=AAPL,MSFT,.SPX,US10Y",
                ],
            },
            "security_search": {
                "method": "GET",
                "path": f"{base_url}/securities/search",
                "description": "Batch search local Korean securities by ticker/name and validate overseas CNBC symbols.",
                "query": {
                    "q": "Search query. Can be repeated or comma-separated.",
                    "queries": "Alternative comma-separated batch query parameter.",
                    "limit": "Optional max results per query, 1..50. Defaults to 10.",
                },
                "examples": [
                    f"{base_url}/securities/search?q=삼성전자&q=000660",
                    f"{base_url}/securities/search?queries=AAPL,MSFT,.SPX&limit=5",
                ],
            },
            "nps_universe": {
                "method": "GET",
                "path": f"{base_url}/universe/nps",
                "description": "Point-in-time NPS holdings universe ranked by market value.",
                "query": {
                    "date": "Optional requested date as YYYY-MM-DD. Uses the latest NPS snapshot on or before this date.",
                    "top": "Optional max rows, 1..1000. Defaults to 100.",
                },
                "examples": [
                    f"{base_url}/universe/nps?date=2024-12-31&top=100",
                    f"{base_url}/universe/nps?date=2026-04-30&top=50",
                ],
            },
            "basic_fundamentals": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/basic",
                "description": "Latest available annual basic financial metrics by ticker, with per-share BPS/EPS values when DART share-denominator data is available.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "fiscal_year": "Optional fiscal year filter.",
                },
                "metrics": basic_fundamental_metrics,
                "per_share": {
                    "object": "Returned per ticker as fundamentals.<ticker>.per_share when denominator data is available.",
                    "fields": {
                        "bps": "Latest balance-sheet equity divided by issuer-level DART outstanding shares when available.",
                        "eps_annual": "Latest annual net income divided by issuer-level DART outstanding shares when available.",
                        "eps_ttm": "Trailing-twelve-month net income divided by issuer-level DART outstanding shares when available. Annual latest rows equal eps_annual; interim latest rows use latest cumulative + prior annual - prior matching interim.",
                        "eps_forward": "Forward EPS placeholder. value is null and available is false until a forward earnings estimate source is added.",
                    },
                    "denominator": {
                        "share_basis": "dart_distributed_shares",
                        "issuer_key": "corp_code",
                        "preferred_shares": "Included by summing DART common/preferred rows for the same issuer.",
                        "treasury_shares": "DART stock total quantity rows are preferred; when available, shares are distb_stock_co and treasury_shares_excluded is true.",
                    },
                },
                "examples": [
                    f"{base_url}/fundamentals/basic?tickers=005930,000660",
                    f"{base_url}/fundamentals/basic?ticker=005930&as_of=2026-04-30&fiscal_year=2025",
                ],
            },
            "capital_actions": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/capital-actions",
                "description": "Annual dividend and treasury-share related financial rows by ticker.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "start_year": "Optional first fiscal year.",
                    "end_year": "Optional last fiscal year.",
                },
                "metrics": capital_action_metrics,
                "examples": [
                    f"{base_url}/fundamentals/capital-actions?tickers=005930,000660&start_year=2024&end_year=2025",
                    f"{base_url}/fundamentals/capital-actions?ticker=005930&as_of=2026-04-30",
                ],
            },
            "dividends": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/dividends",
                "description": "Per-share dividend rows by listed security, including common/preferred stock distinctions when OpenDART can map them unambiguously.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 005935.",
                    "tickers": "Comma-separated tickers, e.g. 005930,005935.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "start_year": "Optional first fiscal year.",
                    "end_year": "Optional last fiscal year.",
                },
                "examples": [
                    f"{base_url}/fundamentals/dividends?tickers=005930,005935&start_year=2022&end_year=2024",
                    f"{base_url}/fundamentals/dividends?ticker=005935&as_of=2026-04-30",
                ],
            },
            "cpi": {
                "method": "GET",
                "path": f"{base_url}/macro/cpi",
                "description": "Consumer price index observations from macro.cpi.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional CPI series id.",
                },
                "columns": macro_table_columns["cpi"],
                "examples": [
                    f"{base_url}/macro/cpi?country=KR&since=2024-01-01",
                    f"{base_url}/macro/cpi?series_id=KOR_CPI_ALL&since=2024-01-01&until=2024-12-31",
                ],
            },
            "rates": {
                "method": "GET",
                "path": f"{base_url}/macro/rates",
                "description": "Interest-rate observations from macro.rates.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional rate series id.",
                },
                "columns": macro_table_columns["rates"],
                "examples": [
                    f"{base_url}/macro/rates?country=KR&since=2024-01-01",
                    f"{base_url}/macro/rates?series_id=KOR_BASE_RATE",
                ],
            },
            "indices": {
                "method": "GET",
                "path": f"{base_url}/macro/indices",
                "description": "Market and macro index observations from macro.indices.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional index series id.",
                },
                "columns": macro_table_columns["indices"],
                "examples": [
                    f"{base_url}/macro/indices?country=KR&since=2024-01-01",
                    f"{base_url}/macro/indices?series_id=KOSPI",
                ],
            },
            "daily_indices": {
                "method": "GET",
                "path": f"{base_url}/daily-indices",
                "description": "Alias for daily market index observations from macro.indices.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. JP or US.",
                    "series_id": "Optional index series id, e.g. SP500, NASDAQ, DOW_JONES, NIKKEI_225, HANG_SENG, SSE_COMPOSITE.",
                },
                "columns": macro_table_columns["indices"],
                "examples": [
                    f"{base_url}/daily-indices?series_id=SP500&since=2024-01-01",
                    f"{base_url}/daily-indices?country=JP&since=2024-01-01",
                ],
            },
            "realtime_indicators": {
                "method": "GET",
                "path": f"{base_url}/realtime/indicators",
                "description": "Live CNBC quote snapshots for configured indices, rates, commodities, and FX indicators. Responses are cached briefly in-process.",
                "query": {
                    "category": "Optional group: indices, rates, commodities, or fx.",
                    "series_id": "Optional configured series id, e.g. SP500, USD_KRW, US_TREASURY_10Y.",
                },
                "examples": [
                    f"{base_url}/realtime/indicators",
                    f"{base_url}/realtime/indicators?category=indices",
                    f"{base_url}/realtime/indicators?series_id=USD_KRW",
                ],
            },
            "commodities": {
                "method": "GET",
                "path": f"{base_url}/macro/commodities",
                "description": "Commodity observations such as gold and silver from macro.commodities.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "series_id": "Optional commodity series id.",
                    "commodity": "Optional commodity code, e.g. gold or silver.",
                },
                "columns": macro_table_columns["commodities"],
                "examples": [
                    f"{base_url}/macro/commodities?commodity=gold&since=2024-01-01",
                    f"{base_url}/macro/commodities?series_id=GOLD_USD_OZ",
                ],
            },
            "fx": {
                "method": "GET",
                "path": f"{base_url}/macro/fx",
                "description": "Foreign-exchange observations from macro.fx.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "series_id": "Optional FX series id.",
                    "base_currency": "Optional base currency, e.g. USD.",
                    "quote_currency": "Optional quote currency, e.g. KRW.",
                },
                "columns": macro_table_columns["fx"],
                "examples": [
                    f"{base_url}/macro/fx?base_currency=USD&quote_currency=KRW&since=2024-01-01",
                    f"{base_url}/macro/fx?series_id=USD_KRW",
                ],
            },
            "economic_indicators": {
                "method": "GET",
                "path": f"{base_url}/macro/economic-indicators",
                "description": "General FRED macroeconomic observations from macro.economic_indicators.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. US.",
                    "series_id": "Optional FRED series id, e.g. UNRATE or VIXCLS.",
                    "category": "Optional category, e.g. labor, growth, risk, credit, inflation.",
                    "frequency": "Optional frequency code, e.g. D, W, M, Q.",
                },
                "columns": macro_table_columns["economic_indicators"],
                "examples": [
                    f"{base_url}/macro/economic-indicators?category=labor&since=2024-01-01",
                    f"{base_url}/macro/economic-indicators?series_id=VIXCLS&since=2024-01-01",
                ],
            },
        },
    }
