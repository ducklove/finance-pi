from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl

WriteMode = Literal["fail", "overwrite"]


@dataclass(frozen=True)
class ParquetDatasetWriter:
    """Small, explicit Parquet writer for partitioned medallion datasets."""

    compression: str = "zstd"

    def write(
        self,
        frame: pl.DataFrame,
        path: Path,
        *,
        mode: WriteMode = "fail",
        source: str | None = None,
        request_hash: str | None = None,
        include_ingest_metadata: bool = False,
    ) -> Path:
        if path.exists() and mode == "fail":
            raise FileExistsError(
                f"{path} already exists; append-only writes do not overwrite by default"
            )

        path.parent.mkdir(parents=True, exist_ok=True)
        output = frame
        if include_ingest_metadata:
            if source is None or request_hash is None:
                raise ValueError(
                    "source and request_hash are required when include_ingest_metadata=True"
                )
            output = output.with_columns(
                pl.lit(datetime.now(UTC)).alias("_ingested_at"),
                pl.lit(source).alias("_source"),
                pl.lit(request_hash).alias("_source_request_hash"),
            )
        output.write_parquet(path, compression=self.compression)
        return path
