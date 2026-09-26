"""Fetch USGS long-run mine output and IMF annual gold volumes via DBnomics."""

import hashlib
import json
from datetime import UTC, datetime

from finance_pi.sources.gold.prices import get
from finance_pi.sources.gold.xlsx_values import rows

USGS = "https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/ds140-gold-2022.xlsx"
COUNTRIES = [
    ("US", "미국", "#b68a25"),
    ("DE", "독일", "#5c80d9"),
    ("IT", "이탈리아", "#8e6db8"),
    ("FR", "프랑스", "#39a59b"),
    ("CN", "중국", "#e98b3a"),
    ("KR", "한국", "#d35470"),
]


def imf_points(doc):
    if "Millions" not in doc["series_name"] or "Gold" not in doc["series_name"]:
        raise ValueError("Unexpected IMF unit/series")
    # Source values are millions of fine troy ounces. 1 million oz = 31.1034768 tonnes.
    return [
        {"date": f"{year}-12", "value": round(value * 31.1034768, 6)}
        for year, value in zip(doc["period"], doc["value"], strict=True)
        if isinstance(value, (float, int)) and value >= 0
    ]


def collect_trends():
    blob = get(USGS)
    table = list(rows(blob, "Gold"))
    mining = []
    for key, name, index, color in [("world", "세계", 8, "#b68a25"), ("us", "미국", 1, "#5c80d9")]:
        points = [
            {"date": f"{int(row[0])}-12", "value": row[index]}
            for row in table
            if row
            and isinstance(row[0], (int, float))
            and len(row) > index
            and isinstance(row[index], (int, float))
        ]
        mining.append(
            dict(
                id=key,
                name=name,
                color=color,
                points=points,
                source="https://www.usgs.gov/media/files/gold-historical-statistics-data-series-140",
            )
        )
    reserves = []
    for code, name, color in COUNTRIES:
        url = (
            f"https://api.db.nomics.world/v22/series/IMF/IFS/A.{code}.RAFAGOLDV_OZT?observations=1"
        )
        payload = json.loads(get(url))
        doc = payload["series"]["docs"][0]
        points = imf_points(doc)
        if len(points) < 20:
            raise ValueError(f"Insufficient IMF history: {code}")
        reserves.append(
            dict(
                id=code,
                name=name,
                color=color,
                points=points,
                source=f"https://db.nomics.world/IMF/IFS/A.{code}.RAFAGOLDV_OZT",
                provider="IMF IFS",
                distributor="DBnomics",
                sourceUnit="millions of fine troy ounces",
                sourceSha256=hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest(),
            )
        )
        print(name, points[0]["date"], points[-1]["date"])
    output = dict(
        schemaVersion=1,
        generatedAt=datetime.now(UTC).isoformat(),
        frequency="annual",
        unit="tonnes",
        mining=mining,
        reserves=reserves,
        miningSha256=hashlib.sha256(blob).hexdigest(),
        miningNote=(
            "USGS DS140 2022 edition; 1900–2022; calculated, estimated or reported. "
            "Later releases in the snapshot are not spliced."
        ),
        reservesNote=(
            "IMF IFS annual year-end gold volumes distributed by DBnomics; "
            "excludes missing values without interpolation. Latest annual data 2024. "
            "Gold deposits/swaps may be included."
        ),
    )
    return output
