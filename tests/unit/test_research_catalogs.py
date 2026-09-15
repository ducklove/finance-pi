import hashlib
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from finance_pi.research import catalogs
from finance_pi.research.pairs import PairConfig


def source(monkeypatch, *, change=None, manifest_change=None):
    now = datetime.now(UTC).isoformat()
    pair = {
        "common": "005930",
        "preferred": "005935",
        "strategy": "preferred_switch",
        "name": "삼성전자",
        "execution_eligible": False,
        "point_in_time_verified": False,
    }
    data = {
        "schema_version": 1,
        "provider": "common_preferred_spread",
        "data_as_of": now,
        "pairs": [pair],
        "execution_eligible": False,
        "point_in_time_verified": False,
    }
    if change:
        change(data)
    raw = json.dumps(data).encode()
    sha = hashlib.sha256(raw).hexdigest()
    manifest = {
        "schema_version": 1,
        "provider": data["provider"],
        "snapshot_id": sha,
        "path": f"snapshots/{sha}.json",
        "published_at": now,
        "data_as_of": now,
    }
    if manifest_change:
        manifest.update(manifest_change)
    monkeypatch.setattr(
        catalogs,
        "fetch_bytes",
        lambda url: json.dumps(manifest).encode() if url.endswith("manifest.json") else raw,
    )
    return data, sha


def config(sha):
    return PairConfig(
        common="005930",
        preferred="005935",
        start="2024-01-01",
        end="2025-01-01",
        catalog_snapshot_id=sha,
    )


def test_archive_intersection_and_pinned_metadata_survive_daily_refresh(tmp_path, monkeypatch):
    data, sha = source(monkeypatch)
    rows, info = catalogs.candidates(tmp_path, "preferred_switch", data["pairs"])
    assert len(rows) == info["accepted_pairs"] == 1
    assert rows[0]["catalog_snapshot_id"] == sha
    original = catalogs.selection(tmp_path, config(sha), data["pairs"])
    source(monkeypatch)  # 공개 시각·전체 해시 변경은 고정된 상품 검토를 바꾸지 않는다.
    assert catalogs.selection(tmp_path, config(sha), data["pairs"]) == original
    rows, info = catalogs.candidates(tmp_path, "preferred_switch", [])
    assert rows == [] and info["excluded_pairs"] == 1
    with pytest.raises(ValueError, match="일치하지"):
        catalogs.selection(tmp_path, config(sha), [])


@pytest.mark.parametrize(
    "change",
    [
        {"path": "../../etc/passwd"},
        {"provider": "other"},
        {"snapshot_id": "a" * 64},
        {"published_at": (datetime.now(UTC) - timedelta(days=15)).isoformat()},
        {"data_as_of": (datetime.now(UTC) + timedelta(days=1)).isoformat()},
    ],
)
def test_manifest_rejects_untrusted_paths_hashes_and_stale_data(tmp_path, monkeypatch, change):
    source(monkeypatch, manifest_change=change)
    with pytest.raises(ValueError):
        catalogs.current(tmp_path, "preferred_switch")
    assert not (tmp_path / "research/catalogs").exists()


def test_product_change_and_removed_pair_block_existing_observation(tmp_path, monkeypatch):
    data, sha = source(monkeypatch)
    catalogs.current(tmp_path, "preferred_switch")
    source(monkeypatch, change=lambda d: d["pairs"][0].update(rights={"convertible": True}))
    with pytest.raises(ValueError, match="변경"):
        catalogs.selection(tmp_path, config(sha), data["pairs"])


def test_corrupt_archive_and_offline_source_never_fall_back(tmp_path, monkeypatch):
    data, sha = source(monkeypatch)
    catalogs.current(tmp_path, "preferred_switch")
    path = tmp_path / "research/catalogs/common_preferred_spread" / f"{sha}.json"
    path.write_bytes(b"{}")
    with pytest.raises(ValueError, match="변조"):
        catalogs.current(tmp_path, "preferred_switch")

    def offline(url):
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(catalogs, "fetch_bytes", offline)
    with pytest.raises(ValueError, match="검증하지"):
        catalogs.selection(tmp_path, config(sha), data["pairs"])


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d["pairs"].append(d["pairs"][0]),
        lambda d: d.update(execution_eligible=True),
        lambda d: d["pairs"][0].update(strategy="etf_switch"),
        lambda d: d["pairs"][0].update(preferred="../foo"),
    ],
)
def test_invalid_catalog_is_not_archived(tmp_path, monkeypatch, change):
    source(monkeypatch, change=change)
    with pytest.raises(ValueError):
        catalogs.current(tmp_path, "preferred_switch")


def test_duplicate_json_keys_and_nonfinite_values_rejected():
    for raw in (b'{"a":1,"a":2}', b'{"a":NaN}'):
        with pytest.raises(ValueError):
            catalogs.decode(raw)
