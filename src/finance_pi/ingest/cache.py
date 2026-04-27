from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ResponseCache:
    root: Path

    def path_for(self, source: str, request_hash: str) -> Path:
        return self.root / source / f"{request_hash}.json.gz"

    def exists(self, source: str, request_hash: str) -> bool:
        return self.path_for(source, request_hash).exists()

    def read_json(self, source: str, request_hash: str) -> Any:
        path = self.path_for(source, request_hash)
        with gzip.open(path, mode="rt", encoding="utf-8") as handle:
            return json.load(handle)

    def write_json(self, source: str, request_hash: str, payload: Any) -> Path:
        path = self.path_for(source, request_hash)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, mode="wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        return path
