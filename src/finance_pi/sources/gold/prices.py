"""World Bank spot monthly averages + Yahoo daily-close monthly averages; atomic publish."""

import hashlib
import json
import math
import re
from collections import defaultdict
from datetime import UTC, datetime
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

from finance_pi.http import HttpJsonClient
from finance_pi.sources.gold.xlsx_values import rows

WB_URL = "https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx"
ASSETS = [
    ("bitcoin", "비트코인", "BTC-USD", "USD / BTC", "#e98b3a", "비트코인 일별 종가의 월평균"),
    ("dollar", "달러 지수", "DX-Y.NYB", "index", "#5c80d9", "미국 달러 지수 일별 종가의 월평균"),
    ("usdkrw", "달러/원", "KRW=X", "KRW / USD", "#39a59b", "달러/원 일별 종가의 월평균"),
]


def get(url):
    parsed = urlsplit(url)
    with HttpJsonClient(
        source="gold-research",
        base_url=f"{parsed.scheme}://{parsed.netloc}",
        timeout=45,
        default_headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        return client.get_bytes(parsed.path + ("?" + parsed.query if parsed.query else ""))


def parse_months(result, now):
    """Legacy monthly-close parser retained for source regression tests."""
    tz = ZoneInfo(result["meta"].get("exchangeTimezoneName", "UTC"))
    current_month = now.astimezone(tz).strftime("%Y-%m")
    points = {}
    for stamp, value in zip(
        result["timestamp"], result["indicators"]["quote"][0]["close"], strict=True
    ):
        month = datetime.fromtimestamp(stamp, tz).strftime("%Y-%m")
        if month < current_month and value is not None and math.isfinite(value) and value > 0:
            points[month] = round(value, 6)
    if len(points) < 24:
        raise ValueError("Insufficient monthly observations")
    return [{"date": month, "value": value} for month, value in sorted(points.items())]


def parse_daily_averages(result, now):
    tz = ZoneInfo(result["meta"].get("exchangeTimezoneName", "UTC"))
    cutoff = now.astimezone(tz).strftime("%Y-%m")
    groups = defaultdict(list)
    first_date = None
    for stamp, value in zip(
        result["timestamp"], result["indicators"]["quote"][0]["close"], strict=True
    ):
        date = datetime.fromtimestamp(stamp, tz)
        month = date.strftime("%Y-%m")
        if value is not None and math.isfinite(value) and value > 0 and month < cutoff:
            first_date = first_date or date
            groups[month].append(value)
    # The first source month may start halfway through a month (e.g. BTC in Sep 2014).
    if first_date and first_date.day > 7:
        groups.pop(first_date.strftime("%Y-%m"), None)
    if len(groups) < 24:
        raise ValueError("Insufficient daily history")
    return [
        {"date": month, "value": round(sum(v) / len(v), 6), "observations": len(v)}
        for month, v in sorted(groups.items())
    ]


def world_bank_assets(blob, now):
    table = list(rows(blob, "Monthly Prices"))
    headers = next(row for row in table if "Gold" in row and "Silver" in row)
    output = []
    for key, name, column, color in [
        ("gold", "금", "Gold", "#b68a25"),
        ("silver", "은", "Silver", "#8294ac"),
    ]:
        index = headers.index(column)
        points = []
        for row in table:
            if not row or not isinstance(row[0], str) or not re.fullmatch(r"\d{4}M\d{2}", row[0]):
                continue
            date = row[0].replace("M", "-")
            value = row[index] if len(row) > index else None
            if (
                date < now.strftime("%Y-%m")
                and isinstance(value, (float, int))
                and math.isfinite(value)
                and value > 0
            ):
                points.append({"date": date, "value": round(value, 6)})
        if not points or points[0]["date"] != "1960-01":
            raise ValueError("World Bank historical coverage changed")
        output.append(
            dict(
                id=key,
                name=name,
                symbol="WB " + column,
                unit="USD / troy oz",
                color=color,
                description=f"세계은행 {name} 현물 월평균",
                source="https://www.worldbank.org/en/research/commodity-markets",
                sourceFile=WB_URL,
                frequency="monthly",
                aggregation="monthly-average",
                points=points,
            )
        )
    return output


def collect_history():
    now = datetime.now(UTC)
    blob = get(WB_URL)
    assets = world_bank_assets(blob, now)
    for key, name, symbol, unit, color, description in ASSETS:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol, safe='')}"
            f"?period1=0&period2={int(now.timestamp())}&interval=1d"
        )
        result = json.loads(get(url))["chart"]["result"][0]
        points = parse_daily_averages(result, now)
        assets.append(
            dict(
                id=key,
                name=name,
                symbol=symbol,
                unit=unit,
                color=color,
                description=description,
                source=f"https://finance.yahoo.com/quote/{quote(symbol, safe='')}/history/",
                aggregation="mean-of-daily-closes",
                points=points,
            )
        )
    for a in assets:
        print(
            f"{a['id']}: {len(a['points'])} months, "
            f"{a['points'][0]['date']} → {a['points'][-1]['date']}"
        )
    output = dict(
        schemaVersion=2,
        generatedAt=now.isoformat(),
        frequency="monthly",
        worldBankSha256=hashlib.sha256(blob).hexdigest(),
        methodology=(
            "Gold/silver: World Bank monthly average spot prices. "
            "Other assets: mean of available daily closes. No filling or futures splicing."
        ),
        assets=assets,
    )
    return output
