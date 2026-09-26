"""Versioned gold research snapshots; external collection never runs in a GET request."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from finance_pi.sources.gold.prices import collect_history
from finance_pi.sources.gold.trends import collect_trends


def publish(data_root: Path, history: dict, trends: dict, research: dict) -> dict:
    """Publish one complete generation. Old current remains valid on any prior failure."""
    payload = {
        "schemaVersion": 1,
        "provider": "finance-pi",
        "publishedAt": datetime.now(UTC).isoformat(),
        "history": history,
        "trends": trends,
        "research": research,
    }
    directory = data_root / "research" / "gold"
    directory.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode()
    fd, temporary = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        # Immutable releases make point-in-time reproduction possible.
        releases = directory / "releases"
        releases.mkdir(exist_ok=True)
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
    return publish(data_root, history, trends, research)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    result = refresh(args.root / "data")
    print(f"Published finance-pi gold research: {result['publishedAt']}")


if __name__ == "__main__":
    main()
