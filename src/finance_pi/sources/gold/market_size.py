"""Estimated global gold stock/value and calendar-year-end US federal debt.

Past stock is a transparent reconstruction, not WGC's published historical series.
The USGS DS140 chart remains unchanged; newer mining editions are used only here.
"""

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path

from finance_pi.sources.gold.prices import get

TROY_OZ_PER_TONNE = 1_000_000 / 31.1034768
DEBT_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/"
    "accounting/od/debt_to_penny?filter=record_calendar_month:eq:12"
    "&fields=record_date,tot_pub_debt_out_amt&sort=record_date&page[size]=10000"
)
DEBT_SOURCE = "https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/"


def year_end_debt(payload, now):
    """Last reported December business day, completed calendar years only, USD."""
    rows = payload["data"]
    if int(payload["meta"]["total-count"]) != len(rows):
        raise ValueError("Incomplete Treasury pagination")
    years = {}
    for row in rows:
        date = datetime.strptime(row["record_date"], "%Y-%m-%d")
        if date.year >= now.year or date.month != 12:
            continue
        value = float(row["tot_pub_debt_out_amt"])
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid Treasury debt amount")
        previous = years.get(date.year)
        if previous is None or row["record_date"] > previous["observedAt"]:
            years[date.year] = {
                "date": f"{date.year}-12",
                "value": value,
                "observedAt": row["record_date"],
            }
    if any(int(point["observedAt"][-2:]) < 28 for point in years.values()):
        raise ValueError("Incomplete December debt observations")
    return [years[year] for year in sorted(years)]


def reconstruct_stock(mining, anchor):
    """S_y = S_anchor - sum(mine output after y), assuming no permanent losses."""
    output = {int(p["date"][:4]): p["value"] for p in mining}
    end = anchor["year"]
    if end not in output or not output:
        raise ValueError("Missing anchor-year mine output")
    if any(year not in output for year in range(min(output), end + 1)):
        raise ValueError("Cannot reconstruct across a missing mining year")
    stock = float(anchor["tonnes"])
    points = []
    for year in range(end, min(output) - 1, -1):
        if not math.isfinite(output[year]) or output[year] <= 0 or stock <= output[year]:
            raise ValueError("Invalid mining/stock quantities")
        points.append({"date": f"{year}-12", "value": round(stock, 6)})
        stock -= output[year]
    return list(reversed(points))


def derive(history, trends, inputs, debt, now):
    world = next(s for s in trends["mining"] if s["id"] == "world")
    production = {p["date"]: dict(p) for p in world["points"]}
    for item in inputs["miningExtensions"]:
        date = f"{item['year']}-12"
        if date in production:
            raise ValueError("Mining extension overlaps historical edition")
        production[date] = {"date": date, "value": item["tonnes"]}
    anchor = inputs["stockAnchor"]
    if anchor["year"] >= now.year:
        raise ValueError("Stock anchor must describe a completed year")
    production = {d: p for d, p in production.items() if int(d[:4]) <= anchor["year"]}
    stock = reconstruct_stock(list(production.values()), anchor)
    gold = next(a for a in history["assets"] if a["id"] == "gold")
    december_prices = {p["date"]: p["value"] for p in gold["points"] if p["date"].endswith("-12")}
    debt_map = {p["date"]: p["value"] for p in debt}
    cap, ratio, mining_ratio, stock_to_flow = [], [], [], []
    for point in stock:
        date, tonnes = point["date"], point["value"]
        mining_ratio.append({"date": date, "value": production[date]["value"] / tonnes * 100})
        stock_to_flow.append({"date": date, "value": tonnes / production[date]["value"]})
        if date in december_prices:
            value = tonnes * TROY_OZ_PER_TONNE * december_prices[date]
            cap.append({"date": date, "value": round(value, 2)})
            if date in debt_map:
                ratio.append({"date": date, "value": value / debt_map[date] * 100})
    if len(cap) < 20 or len(ratio) < 20:
        raise ValueError("Insufficient market-size overlap")
    return {
        "schemaVersion": 1,
        "generatedAt": now.isoformat(),
        "frequency": "annual",
        "stock": stock,
        "mining": [production[d] for d in sorted(production)],
        "marketCap": cap,
        "usDebt": debt,
        "goldDebtRatioPct": ratio,
        "miningStockRatioPct": mining_ratio,
        "stockToFlowYears": stock_to_flow,
        "inputs": inputs,
        "sources": {
            "goldPrice": gold["source"],
            "historicalMining": world["source"],
            "usDebt": DEBT_SOURCE,
            "usDebtApi": DEBT_URL,
        },
        "methodology": {
            "stock": "Reconstructed backward from the WGC end-2025 estimate "
            "using USGS mine output. "
            "Assumes no permanent losses; excludes recycling and below-ground reserves. "
            "Not WGC historical observations. Editions differ; "
            "past stock and values are estimates.",
            "marketCap": "End-year reconstructed tonnes * (1,000,000 / 31.1034768) * "
            "World Bank December average USD/troy oz. A nominal value proxy, "
            "not a closing valuation "
            "or freely traded market capitalization.",
            "usDebt": "Total Public Debt Outstanding, including debt held by the public and "
            "intragovernmental holdings; face value, not market value. Last December business day. "
            "Calendar-year observations, not fiscal-year September totals.",
            "goldDebtRatioPct": "Global gold value proxy / US total federal debt * 100. "
            "Not US gold reserves, collateral coverage, or a debt repayment measure.",
            "miningStockRatioPct": "Annual USGS mine output / reconstructed END-year stock * 100; "
            "not the percentage change in stock or total gold supply including recycling.",
        },
    }


def collect_market_size(history, trends):
    now = datetime.now(UTC)
    inputs = json.loads(Path(__file__).with_name("market_size_inputs.json").read_text())
    raw = get(DEBT_URL)
    debt = year_end_debt(json.loads(raw), now)
    result = derive(history, trends, inputs, debt, now)
    result["debtSha256"] = hashlib.sha256(raw).hexdigest()
    return result
