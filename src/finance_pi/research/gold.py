"""Versioned gold research snapshots; external collection never runs in a GET request."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from finance_pi.sources.gold.market_size import collect_market_size
from finance_pi.sources.gold.prices import collect_history
from finance_pi.sources.gold.trends import collect_trends

# Run timestamps that change on every collection without the data changing.
VOLATILE_TOP = ("publishedAt",)
VOLATILE_SECTION = ("generatedAt",)


def content_fingerprint(payload: dict) -> str:
    """sha256 of the snapshot without its run timestamps (canonical JSON)."""
    stable = {}
    for key, value in payload.items():
        if key in VOLATILE_TOP:
            continue
        if isinstance(value, dict):
            value = {k: v for k, v in value.items() if k not in VOLATILE_SECTION}
        stable[key] = value
    blob = json.dumps(
        stable, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(blob.encode()).hexdigest()


def _latest_release(releases: Path) -> tuple[Path, str] | None:
    candidates = sorted(releases.glob("*.json")) if releases.is_dir() else []
    if not candidates:
        return None
    latest = candidates[-1]
    try:
        return latest, content_fingerprint(json.loads(latest.read_bytes()))
    except (OSError, ValueError, AttributeError):
        return None  # unreadable latest release: publish a fresh one


def publish(
    data_root: Path, history: dict, trends: dict, research: dict, market_size: dict | None = None
) -> dict:
    """Publish one complete generation. Old current remains valid on any prior failure."""
    payload = {
        "schemaVersion": 1,
        "provider": "finance-pi",
        "publishedAt": datetime.now(UTC).isoformat(),
        "history": history,
        "trends": trends,
        "research": research,
    }
    if market_size is not None:
        payload["marketSize"] = market_size
    directory = data_root / "research" / "gold"
    directory.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode()
    fd, temporary = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        # Immutable releases make point-in-time reproduction possible. A run whose
        # content matches the latest release (only run timestamps differ) adds no
        # release; current.json is still refreshed so publishedAt reports the
        # latest successful collection (all-about-gold checks its age).
        releases = directory / "releases"
        releases.mkdir(exist_ok=True)
        latest = _latest_release(releases)
        if latest is not None and latest[1] == content_fingerprint(payload):
            print(
                f"gold research content unchanged; keeping release {latest[0].name}",
                file=sys.stderr,
            )
        else:
            release = releases / (datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
            release.write_bytes(content)
        os.replace(temporary, directory / "current.json")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return payload


def read_snapshot(data_root: Path) -> dict[str, Any]:
    path = data_root / "research" / "gold" / "current.json"
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise KeyError("gold research snapshot not collected") from exc
    if result.get("provider") != "finance-pi" or result.get("schemaVersion") != 1:
        raise ValueError("unsupported gold research snapshot")
    return result


def refresh(data_root: Path) -> dict:
    from finance_pi.sources.gold import prices

    history = collect_history()
    trends = collect_trends()
    research = json.loads(Path(prices.__file__).with_name("research.json").read_text())
    market_size = collect_market_size(history, trends)
    return publish(data_root, history, trends, research, market_size)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    result = refresh(args.root / "data")
    print(f"Published finance-pi gold research: {result['publishedAt']}")


if __name__ == "__main__":
    main()
