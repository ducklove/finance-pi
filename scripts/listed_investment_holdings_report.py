from __future__ import annotations

# ruff: noqa: I001

import concurrent.futures as futures
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill_dart_web_financials import (
    MAIN_URL,
    VIEWER_URL,
    TableParser,
    _http_text,
    _parse_sections,
)


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = DATA / "reports"
CACHE = DATA / "_cache" / "opendart_listed_investments_2025"
BSNS_YEAR = "2025"
REPRT_CODE = "11011"


def load_env_key() -> str:
    env_path = ROOT / ".env"
    for line in env_path.read_text().splitlines():
        if line.startswith("OPENDART_API_KEY="):
            return line.split("=", 1)[1].strip()
    key = os.environ.get("OPENDART_API_KEY")
    if key:
        return key
    raise RuntimeError("OPENDART_API_KEY is not configured")


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "-", "nan", "None"}:
        return None
    text = text.replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "-", "nan", "None"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_name(name: Any) -> str:
    text = str(name or "").upper()
    text = re.sub(r"\s+", "", text)
    text = text.replace("㈜", "")
    text = re.sub(r"^\(주\)|^\(株\)|^주식회사|^유한회사|^사단법인|^재단법인", "", text)
    text = re.sub(r"\(주\)$|\(株\)$|주식회사$", "", text)
    text = re.sub(r"[^0-9A-Z가-힣]", "", text)
    return text


def fetch_json(endpoint: str, corp_code: str, key: str) -> dict[str, Any]:
    CACHE.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE / f"{endpoint}_{corp_code}_{BSNS_YEAR}_{REPRT_CODE}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    if os.environ.get("FINANCE_PI_OFFLINE_CACHE_ONLY") == "1":
        return {"status": "OFFLINE_CACHE_MISS", "message": "cache miss", "list": []}

    params = urllib.parse.urlencode(
        {
            "crtfc_key": key,
            "corp_code": corp_code,
            "bsns_year": BSNS_YEAR,
            "reprt_code": REPRT_CODE,
        }
    )
    url = f"https://opendart.fss.or.kr/api/{endpoint}.json?{params}"
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            cache_path.write_text(json.dumps(payload, ensure_ascii=False))
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"{endpoint} {corp_code} failed: {last_error}")


@dataclass(frozen=True)
class DartPayload:
    corp_code: str
    investments: list[dict[str, Any]]
    stock_rows: list[dict[str, Any]]
    status: str


def fetch_company(corp_code: str, key: str) -> DartPayload:
    inv = fetch_json("otrCprInvstmntSttus", corp_code, key)
    stock = fetch_json("stockTotqySttus", corp_code, key)
    return DartPayload(
        corp_code=corp_code,
        investments=inv.get("list") or [],
        stock_rows=stock.get("list") or [],
        status=f"{inv.get('status')}:{stock.get('status')}",
    )


def latest_annual_filing_map(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = con.sql(
        """
        select corp_code, rcept_no
        from (
          select
            cast(corp_code as varchar) corp_code,
            cast(rcept_no as varchar) rcept_no,
            row_number() over (partition by corp_code order by rcept_dt desc, rcept_no desc) rn
          from read_parquet(
            'data/bronze/dart_filings/dt=*/part.parquet',
            hive_partitioning=true,
            union_by_name=true
          )
          where report_nm like '%사업보고서%'
            and report_nm like '%2025.12%'
            and stock_code is not null
        )
        where rn = 1
        """
    ).fetchall()
    return {corp_code: rcept_no for corp_code, rcept_no in rows}


def fetch_investments(corp_code: str, key: str, rcept_no: str | None = None) -> DartPayload:
    inv = fetch_json("otrCprInvstmntSttus", corp_code, key)
    rows = inv.get("list") or []
    status = str(inv.get("status"))
    if not rows and rcept_no and os.environ.get("FINANCE_PI_DISABLE_WEB_FALLBACK") != "1":
        rows = fetch_investments_from_web(rcept_no)
        if rows:
            status = "WEB"
    return DartPayload(
        corp_code=corp_code,
        investments=rows,
        stock_rows=[],
        status=status,
    )


def fetch_investments_from_web(rcept_no: str) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            return _fetch_investments_from_web_once(rcept_no)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"dart web investment table {rcept_no} failed: {last_error}")


def _fetch_investments_from_web_once(rcept_no: str) -> list[dict[str, Any]]:
    main_html = _http_text(MAIN_URL, {"rcpNo": rcept_no})
    sections = _parse_sections(main_html)
    matches = [section for section in sections if section.text == "타법인출자 현황(상세)"]
    if not matches:
        return []
    section = matches[-1]
    html = _http_text(
        VIEWER_URL,
        {
            "rcpNo": section.rcp_no,
            "dcmNo": section.dcm_no,
            "eleId": section.ele_id,
            "offset": section.offset,
            "length": section.length,
            "dtd": section.dtd,
        },
    )
    parser = TableParser()
    parser.feed(html)
    table = max(parser.tables, key=len, default=[])
    rows: list[dict[str, Any]] = []
    for cells in table:
        if len(cells) < 16 or cells[0] in {"법인명", "수량", "합 계", "합계"}:
            continue
        rows.append(
            {
                "rcept_no": rcept_no,
                "corp_code": None,
                "corp_name": None,
                "inv_prm": cells[0],
                "frst_acqs_de": cells[2],
                "invstmnt_purps": cells[3],
                "frst_acqs_amount": cells[4],
                "bsis_blce_qy": cells[5],
                "bsis_blce_qota_rt": cells[6],
                "bsis_blce_acntbk_amount": cells[7],
                "incrs_dcrs_acqs_dsps_qy": cells[8],
                "incrs_dcrs_acqs_dsps_amount": cells[9],
                "incrs_dcrs_evl_lstmn": cells[10],
                "trmend_blce_qy": cells[11],
                "trmend_blce_qota_rt": cells[12],
                "trmend_blce_acntbk_amount": cells[13],
                "recent_bsns_year_fnnr_sttus_tot_assets": cells[14],
                "recent_bsns_year_fnnr_sttus_thstrm_ntpf": cells[15],
                "stlm_dt": "2025-12-31",
                "_source": "dart_web_viewer",
            }
        )
    return rows


def fetch_stock_rows(corp_code: str, key: str) -> DartPayload:
    stock = fetch_json("stockTotqySttus", corp_code, key)
    return DartPayload(
        corp_code=corp_code,
        investments=[],
        stock_rows=stock.get("list") or [],
        status=str(stock.get("status")),
    )


def latest_prices(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, date]:
    prices = con.sql(
        """
        with p as (
          select *, row_number() over (partition by ticker order by date desc) rn
          from read_parquet(
            'data/bronze/kis_daily/dt=*/part.parquet',
            hive_partitioning=true,
            union_by_name=true
          )
        )
        select ticker, name, market, close, date
        from p
        where rn = 1 and close > 0
        """
    ).fetchdf()
    return prices, prices["date"].max()


def latest_marcap_stocks(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    df = con.sql(
        """
        with m as (
          select Code ticker, Stocks,
                 row_number() over (partition by Code order by Date desc) rn
          from read_parquet(
            'data/bronze/marcap/year=*/part.parquet',
            hive_partitioning=true,
            union_by_name=true
          )
          where Stocks is not null and Stocks > 0
        )
        select ticker, Stocks from m where rn = 1
        """
    ).fetchdf()
    return {str(row.ticker): int(row.Stocks) for row in df.itertuples(index=False)}


def current_listed_companies(con: duckdb.DuckDBPyConnection, prices: pd.DataFrame) -> pd.DataFrame:
    tickers = tuple(sorted(prices["ticker"].dropna().unique().tolist()))
    return con.sql(
        f"""
        select corp_code, ticker, name, market
        from read_parquet('data/gold/security_master.parquet')
        where corp_code is not null
          and ticker in {tickers}
          and security_type = 'equity'
          and share_class = 'common'
          and (delisted_date is null or delisted_date > current_date)
        qualify row_number() over (partition by corp_code order by listed_date desc nulls last) = 1
        """
    ).fetchdf()


def build_report() -> tuple[pd.DataFrame, pd.DataFrame, Path, Path]:
    key = load_env_key()
    con = duckdb.connect()
    prices, price_date = latest_prices(con)
    price_label = pd.Timestamp(price_date).date().isoformat()
    marcap_stocks = latest_marcap_stocks(con)
    filing_map = latest_annual_filing_map(con)
    companies = current_listed_companies(con, prices)
    con.close()
    print(f"price_date={price_date} tickers={len(prices)} companies={len(companies)}", flush=True)

    price_by_ticker = prices.set_index("ticker").to_dict("index")
    listed_by_norm: dict[str, list[dict[str, Any]]] = {}
    for row in companies.to_dict("records"):
        price = price_by_ticker.get(row["ticker"])
        if not price:
            continue
        row = {**row, **price}
        norm = normalize_name(row["name"])
        if norm:
            listed_by_norm.setdefault(norm, []).append(row)

    corp_codes = companies["corp_code"].dropna().drop_duplicates().tolist()
    payloads: list[DartPayload] = []
    errors: list[dict[str, str]] = []
    done = 0
    with futures.ThreadPoolExecutor(max_workers=6) as pool:
        for offset in range(0, len(corp_codes), 100):
            batch = corp_codes[offset : offset + 100]
            submitted = {
                pool.submit(fetch_investments, corp_code, key, filing_map.get(corp_code)): corp_code
                for corp_code in batch
            }
            for fut in futures.as_completed(submitted):
                corp_code = submitted[fut]
                try:
                    payloads.append(fut.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append({"corp_code": corp_code, "error": str(exc)})
                done += 1
                if done % 25 == 0:
                    print(f"fetched {done}/{len(corp_codes)}", flush=True)

    company_by_corp = companies.set_index("corp_code").to_dict("index")

    candidate_rows: list[dict[str, Any]] = []
    for payload in payloads:
        holder = company_by_corp.get(payload.corp_code)
        if not holder:
            continue
        for inv in payload.investments:
            if normalize_name(inv.get("inv_prm")) in {"합계", "계"}:
                continue
            quantity = to_int(inv.get("trmend_blce_qy"))
            if not quantity or quantity <= 0:
                continue
            norm = normalize_name(inv.get("inv_prm"))
            matches = listed_by_norm.get(norm, [])
            if not matches:
                continue
            # Prefer common shares when a normal name collides with preferred shares.
            investee = sorted(
                matches,
                key=lambda r: (str(r["name"]).endswith("우"), r["ticker"]),
            )[0]
            if investee["ticker"] == holder["ticker"]:
                continue
            candidate_rows.append(
                {
                    "payload": payload,
                    "holder": holder,
                    "investee": investee,
                    "inv": inv,
                    "quantity": quantity,
                }
            )

    stock_needed = sorted(
        {row["payload"].corp_code for row in candidate_rows}
        | {
            row["investee"].get("corp_code")
            for row in candidate_rows
            if row["investee"].get("corp_code")
        }
    )
    stock_payloads: list[DartPayload] = []
    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        submitted = {
            pool.submit(fetch_stock_rows, corp_code, key): corp_code
            for corp_code in stock_needed
        }
        for fut in futures.as_completed(submitted):
            corp_code = submitted[fut]
            try:
                stock_payloads.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                errors.append({"corp_code": corp_code, "error": str(exc)})

    free_float_shares: dict[str, int] = {}
    issued_shares: dict[str, int] = {}
    stock_dates: dict[str, str] = {}
    for payload in stock_payloads:
        for stock_row in payload.stock_rows:
            if stock_row.get("se") == "보통주":
                issued = to_int(stock_row.get("istc_totqy"))
                shares = to_int(stock_row.get("distb_stock_co"))
                if issued:
                    issued_shares[payload.corp_code] = issued
                if shares:
                    free_float_shares[payload.corp_code] = shares
                    stock_dates[payload.corp_code] = str(stock_row.get("stlm_dt") or "")
                break

    holding_rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        payload = row["payload"]
        holder = row["holder"]
        investee = row["investee"]
        inv = row["inv"]
        quantity = row["quantity"]
        stake_pct = to_float(inv.get("trmend_blce_qota_rt"))
        market_issued = marcap_stocks.get(investee["ticker"])
        investee_issued = issued_shares.get(investee.get("corp_code")) or market_issued
        if investee_issued and market_issued and investee_issued > market_issued * 2:
            investee_issued = market_issued
        if stake_pct and investee_issued:
            inferred = round(investee_issued * stake_pct / 100)
            # Some DART rows inherit the note unit and report shares scaled by
            # 1,000 or 1,000,000. Use the disclosed stake when the raw count is
            # impossible relative to issued shares.
            if inferred > 0 and quantity > investee_issued:
                quantity = inferred
        value = quantity * float(investee["close"])
        holding_rows.append(
            {
                "holder_corp_code": payload.corp_code,
                "holder_ticker": holder["ticker"],
                "holder_name": holder["name"],
                "holder_market": holder["market"],
                "investee_ticker": investee["ticker"],
                "investee_name": investee["name"],
                "investee_market": investee["market"],
                "shares_held": quantity,
                "investee_close": float(investee["close"]),
                "holding_value": value,
                "book_value": to_int(inv.get("trmend_blce_acntbk_amount")),
                "stake_pct": inv.get("trmend_blce_qota_rt"),
                "purpose": inv.get("invstmnt_purps"),
                "rcept_no": inv.get("rcept_no"),
                "statement_date": inv.get("stlm_dt"),
                "price_date": price_label,
            }
        )

    holdings = pd.DataFrame(holding_rows)
    if holdings.empty:
        raise RuntimeError("No listed holdings matched")

    totals = holdings.groupby(
        ["holder_corp_code", "holder_ticker", "holder_name", "holder_market"],
        as_index=False,
    ).agg(
        listed_holding_value=("holding_value", "sum"),
        listed_holding_count=("investee_ticker", "nunique"),
    )
    totals["free_float_shares"] = totals["holder_corp_code"].map(free_float_shares)
    totals["market_listed_shares"] = totals["holder_ticker"].map(marcap_stocks)
    totals["free_float_shares"] = totals["free_float_shares"].fillna(
        totals["market_listed_shares"]
    )
    scaled = totals["market_listed_shares"].notna() & (
        totals["free_float_shares"] > totals["market_listed_shares"] * 2
    )
    totals.loc[scaled, "free_float_shares"] = totals.loc[scaled, "market_listed_shares"]
    totals["holder_close"] = totals["holder_ticker"].map(
        lambda ticker: price_by_ticker.get(ticker, {}).get("close")
    )
    totals["ex_treasury_market_cap"] = totals["free_float_shares"] * totals["holder_close"]
    totals["ratio"] = totals["listed_holding_value"] / totals["ex_treasury_market_cap"]
    totals["stock_status_date"] = totals["holder_corp_code"].map(stock_dates)
    totals = (
        totals.dropna(subset=["ratio"])
        .query("ex_treasury_market_cap > 0")
        .sort_values("ratio", ascending=False)
    )

    REPORTS.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS / f"listed_investment_holdings_{BSNS_YEAR}_{price_label}.csv"
    detail_path = REPORTS / f"listed_investment_holdings_detail_{BSNS_YEAR}_{price_label}.csv"
    totals.to_csv(csv_path, index=False)
    holdings.sort_values(["holder_ticker", "holding_value"], ascending=[True, False]).to_csv(
        detail_path,
        index=False,
    )
    if errors:
        (REPORTS / f"listed_investment_holdings_errors_{BSNS_YEAR}_{price_label}.csv").write_text(
            pd.DataFrame(errors).to_csv(index=False)
        )
    return totals, holdings, csv_path, detail_path


def krw_eok(value: float) -> float:
    return value / 100_000_000


def main() -> None:
    totals, holdings, csv_path, detail_path = build_report()
    show = totals.head(50).copy()
    show["listed_holding_value_eok"] = show["listed_holding_value"].map(krw_eok)
    show["ex_treasury_market_cap_eok"] = show["ex_treasury_market_cap"].map(krw_eok)
    show["ratio_pct"] = show["ratio"] * 100
    cols = [
        "holder_ticker",
        "holder_name",
        "listed_holding_count",
        "listed_holding_value_eok",
        "ex_treasury_market_cap_eok",
        "ratio_pct",
        "holder_close",
        "free_float_shares",
    ]
    print(show[cols].to_string(index=False, formatters={
        "listed_holding_value_eok": "{:,.0f}".format,
        "ex_treasury_market_cap_eok": "{:,.0f}".format,
        "ratio_pct": "{:.2f}".format,
        "holder_close": "{:,.0f}".format,
        "free_float_shares": "{:,.0f}".format,
    }))
    print(f"\nsummary_csv={csv_path}")
    print(f"detail_csv={detail_path}")
    print(f"holders={len(totals)} holdings={len(holdings)}")


if __name__ == "__main__":
    main()
