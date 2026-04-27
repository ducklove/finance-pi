from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from finance_pi.storage.datasets import dataset_registry


@dataclass(frozen=True)
class ParquetFactorContext:
    data_root: Path

    def scan(self, name: str) -> pl.LazyFrame:
        spec = dataset_registry[name]
        return pl.scan_parquet(
            str(self.data_root / spec.relative_glob),
            hive_partitioning=spec.hive_partitioning,
        )


@dataclass(frozen=True)
class InMemoryFactorContext:
    frames: dict[str, pl.DataFrame]

    def scan(self, name: str) -> pl.LazyFrame:
        try:
            return self.frames[name].lazy()
        except KeyError as exc:
            raise KeyError(f"missing in-memory dataset {name!r}") from exc
