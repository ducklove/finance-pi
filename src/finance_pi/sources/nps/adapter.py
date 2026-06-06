from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.nps.client import NpsHoldingsClient
from finance_pi.sources.nps.schemas import NPS_HOLDINGS_SCHEMA
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class NpsHoldingsAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: NpsHoldingsClient
    legacy_sqlite_path: Path | None = None
    force: bool = False
    name: str = "nps_holdings"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        dates = (
            self.client.legacy_dates(self.legacy_sqlite_path, since, until)
            if self.legacy_sqlite_path is not None
            else _calendar_dates(since, until)
        )
        for logical_date in dates:
            unit = IngestUnit(
                self.name,
                logical_date,
                "legacy-sqlite" if self.legacy_sqlite_path is not None else "holdings",
                {
                    "date": logical_date.isoformat(),
                    "legacy_sqlite_path": str(self.legacy_sqlite_path)
                    if self.legacy_sqlite_path is not None
                    else None,
                },
            )
            if self.force or not self._path(logical_date).exists():
                yield unit

    def fetch(self, unit: IngestUnit) -> RawBatch:
        if self.legacy_sqlite_path is not None:
            rows = self.client.fetch_legacy_sqlite_holdings(
                self.legacy_sqlite_path,
                unit.logical_date,
            )
        else:
            rows = self.client.fetch_holdings(unit.logical_date)
        return RawBatch(unit, rows)

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self._path(batch.unit.logical_date)
        if path.exists() and not self.force:
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze partition exists")
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no NPS holdings rows")
        self.writer.write(
            batch.to_frame(NPS_HOLDINGS_SCHEMA),
            path,
            mode="overwrite" if self.force else "fail",
            source="nps",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(batch.rows))

    def _path(self, logical_date: date) -> Path:
        return self.layout.partition_path("bronze.nps_holdings_raw", logical_date)


def _calendar_dates(since: date, until: date) -> tuple[date, ...]:
    if until < since:
        return ()
    values: list[date] = []
    current = since
    while current <= until:
        values.append(current)
        current += timedelta(days=1)
    return tuple(values)
