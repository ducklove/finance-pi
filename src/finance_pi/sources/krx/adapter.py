from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta

from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.krx.schemas import KrxDailyPriceRow
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class KrxDailyAdapter:
    """KRX daily-price adapter shell.

    Network fetch is intentionally injected so schema validation and Bronze writes
    can be tested without live credentials or unstable endpoint details.
    """

    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    name: str = "krx"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            yield IngestUnit(
                source=self.name,
                logical_date=current,
                endpoint="MDCSTAT01701",
                params={"date": current.isoformat()},
            )
            current += timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        raise NotImplementedError("configure a concrete KRX HTTP client before live ingest")

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        rows = [KrxDailyPriceRow.model_validate(row).model_dump(mode="json") for row in batch.rows]
        frame = RawBatch(batch.unit, rows).to_frame()
        path = self.layout.partition_path("bronze.krx_daily_raw", batch.unit.logical_date)
        self.writer.write(
            frame,
            path,
            source=self.name,
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(rows))
