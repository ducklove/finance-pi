from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import polars as pl


@dataclass(frozen=True)
class IngestUnit:
    source: str
    logical_date: date
    endpoint: str
    params: Mapping[str, Any] = field(default_factory=dict)

    @property
    def request_hash(self) -> str:
        return request_hash(self.source, self.endpoint, self.params)


@dataclass(frozen=True)
class RawBatch:
    unit: IngestUnit
    rows: list[dict[str, Any]]

    def to_frame(self, schema_overrides: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
        frame = pl.DataFrame(self.rows, infer_schema_length=None)
        if not schema_overrides:
            return frame

        casts = [
            pl.col(column).cast(dtype, strict=False)
            for column, dtype in schema_overrides.items()
            if column in frame.columns
        ]
        return frame.with_columns(casts) if casts else frame


@dataclass(frozen=True)
class WriteResult:
    path: Path
    rows: int
    skipped: bool = False
    reason: str | None = None


class SourceAdapter(Protocol):
    name: str

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        ...

    def fetch(self, unit: IngestUnit) -> RawBatch:
        ...

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        ...


def request_hash(source: str, endpoint: str, params: Mapping[str, Any]) -> str:
    payload = {
        "source": source,
        "endpoint": endpoint,
        "params": dict(sorted(params.items())),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()
