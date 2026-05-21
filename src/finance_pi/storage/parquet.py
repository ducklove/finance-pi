from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

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
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        output = frame
        try:
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
            output.write_parquet(temp_path, compression=self.compression)
            if mode == "overwrite":
                os.replace(temp_path, path)
            else:
                os.link(temp_path, path)
                temp_path.unlink()
        finally:
            if temp_path.exists():
                temp_path.unlink()
        return path
