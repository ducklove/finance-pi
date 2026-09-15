"""전문 공급자의 불변 연구 카탈로그. 실패 시 로컬 후보로 대체하지 않는다."""

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta, timezone

import httpx

PROVIDERS = {"preferred_switch": "common_preferred_spread", "etf_switch": "eiayn"}
BASES = {
    "common_preferred_spread": "https://raw.githubusercontent.com/ducklove/common_preferred_spread/master/data/research/v1/",
    "eiayn": "https://ducklove.github.io/eiayn/data/research/v1/",
}
MAX_BYTES = 3_000_000


def fetch_bytes(url):
    with httpx.stream("GET", url, timeout=20, follow_redirects=False) as response:
        response.raise_for_status()
        content = bytearray()
        for chunk in response.iter_bytes():
            content.extend(chunk)
            if len(content) > MAX_BYTES:
                raise ValueError("연구 카탈로그 크기 제한을 초과했습니다.")
        return bytes(content)


def decode(raw):
    def invalid(value):
        raise ValueError("비정상 숫자입니다.")

    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("중복 JSON 키입니다.")
            result[key] = value
        return result

    return json.loads(raw, parse_constant=invalid, object_pairs_hook=unique)


def validate(raw, provider, sha):
    if not re.fullmatch(r"[a-f0-9]{64}", sha) or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError("전문 데이터 스냅샷 해시가 일치하지 않습니다.")
    data = decode(raw)
    if (
        data.get("schema_version") != 1
        or data.get("provider") != provider
        or data.get("execution_eligible") is not False
        or data.get("point_in_time_verified") is not False
    ):
        raise ValueError("지원하지 않는 전문 데이터 계약입니다.")
    pairs, seen = data.get("pairs"), set()
    if not isinstance(pairs, list) or not 0 < len(pairs) <= 2000:
        raise ValueError("연구 대상 목록이 유효하지 않습니다.")
    for pair in pairs:
        a, b = pair.get("common", ""), pair.get("preferred", "")
        if (
            not re.fullmatch(r"[0-9]{6}", a)
            or not re.fullmatch(r"[0-9A-Z]{6}", b)
            or a == b
            or (a, b) in seen
            or PROVIDERS.get(pair.get("strategy")) != provider
            or pair.get("execution_eligible") is not False
            or pair.get("point_in_time_verified") is not False
        ):
            raise ValueError("전문 데이터의 종목 관계 또는 연구 제한이 유효하지 않습니다.")
        seen.add((a, b))
    return data


def fresh(value, now):
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone(timedelta(hours=9)))
    if not -timedelta(minutes=10) <= now - stamp <= timedelta(days=14):
        raise ValueError("전문 데이터가 오래됐거나 미래 시각입니다. 새 연구·관찰을 보류합니다.")


def archive(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise ValueError("보관된 연구 카탈로그가 변조됐습니다.")
        return
    fd, name = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(raw)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def current(data_root, strategy):
    provider = PROVIDERS[strategy]
    base = BASES[provider]
    try:
        manifest = decode(fetch_bytes(base + "manifest.json"))
        sha = manifest.get("snapshot_id", "")
        if (
            manifest.get("schema_version") != 1
            or manifest.get("provider") != provider
            or not re.fullmatch(r"[a-f0-9]{64}", sha)
            or manifest.get("path") != f"snapshots/{sha}.json"
        ):
            raise ValueError("전문 데이터 manifest 계약이 올바르지 않습니다.")
        now = datetime.now(UTC)
        fresh(manifest["published_at"], now)
        fresh(manifest["data_as_of"], now)
        raw = fetch_bytes(base + manifest["path"])
        data = validate(raw, provider, sha)
        if data.get("data_as_of") != manifest["data_as_of"]:
            raise ValueError("전문 데이터 기준일이 일치하지 않습니다.")
        directory = data_root / "research/catalogs" / provider
        archive(directory / f"{sha}.json", raw)
        receipt = directory / f"{sha}.receipt.json"
        if not receipt.exists():
            archive(
                receipt, json.dumps({"ingested_at": now.isoformat(), "manifest": manifest}).encode()
            )
        return data, sha
    except (httpx.HTTPError, OSError, KeyError, TypeError, AttributeError) as exc:
        raise ValueError(
            f"{provider} 연구 API를 검증하지 못했습니다. 기존 결과는 유지됩니다."
        ) from exc


def candidates(data_root, strategy, relations):
    data, sha = current(data_root, strategy)
    allowed = {(p["common"], p["preferred"]) for p in relations}
    pairs = [
        p
        for p in data["pairs"]
        if strategy == "etf_switch" or (p["common"], p["preferred"]) in allowed
    ]
    return [{**p, "catalog_snapshot_id": sha} for p in pairs], {
        "provider": data["provider"],
        "snapshot_id": sha,
        "data_as_of": data["data_as_of"],
        "source_pairs": len(data["pairs"]),
        "accepted_pairs": len(pairs),
        "excluded_pairs": len(data["pairs"]) - len(pairs),
    }


def selection(data_root, config, relations):
    if not config.catalog_snapshot_id:
        raise ValueError("전문 데이터 버전을 선택해 새 연구를 등록해 주세요.")
    data, _ = current(data_root, config.strategy)
    provider = PROVIDERS[config.strategy]
    sha = config.catalog_snapshot_id
    path = data_root / "research/catalogs" / provider / f"{sha}.json"
    if not path.is_file():
        raise ValueError(
            "서버에서 확인하지 않은 카탈로그 버전입니다. 연구 목록을 새로고침해 주세요."
        )
    pinned = validate(path.read_bytes(), provider, sha)
    key = (config.common, config.preferred)
    old = next((p for p in pinned["pairs"] if (p["common"], p["preferred"]) == key), None)
    new = next((p for p in data["pairs"] if (p["common"], p["preferred"]) == key), None)
    if not old or old != new:
        raise ValueError("종목 관계·상품 검토가 변경됐습니다. 새 연구로 재검증해 주세요.")
    if config.strategy == "preferred_switch" and key not in {
        (p["common"], p["preferred"]) for p in relations
    }:
        raise ValueError("finance-pi 종목 관계와 전문 공급자의 연구 대상이 일치하지 않습니다.")
    instruments = []
    if config.strategy == "etf_switch":

        def products(catalog):
            items = [p for p in catalog.get("instruments", []) if p.get("id") in key]
            if len(items) != 2 or {p["id"] for p in items} != set(key):
                raise ValueError("ETF 상품 정보가 불완전합니다.")
            return sorted(items, key=lambda p: p["id"])

        instruments = products(pinned)

        def comparable(items):
            return [
                {k: v for k, v in p.items() if k not in {"profile_as_of", "source_refs"}}
                for p in items
            ]

        if comparable(instruments) != comparable(products(data)):
            raise ValueError("ETF 보수·기초지수·상품 조건이 변경됐습니다. 재검증해 주세요.")
    return {
        "provider": provider,
        "catalog_snapshot_id": sha,
        "data_as_of": pinned["data_as_of"],
        "pair": old,
        "instruments": instruments,
    }
