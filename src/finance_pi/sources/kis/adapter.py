from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.kis.client import KisDailyPriceClient
from finance_pi.sources.schemas import PRICE_SCHEMA
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class KisDailyAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: KisDailyPriceClient
    ticker: str
    name: str = "kis_daily"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        yield IngestUnit(
            self.name,
            until,
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            {"ticker": self.ticker, "since": since.isoformat(), "until": until.isoformat()},
        )

    def fetch(self, unit: IngestUnit) -> RawBatch:
        since = date.fromisoformat(str(unit.params["since"]))
        until = date.fromisoformat(str(unit.params["until"]))
        return RawBatch(unit, self.client.fetch_daily_prices(self.ticker, since, until))

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        if not batch.rows:
            return WriteResult(
                path=self.layout.partition_path("bronze.kis_daily_raw", batch.unit.logical_date),
                rows=0,
                skipped=True,
                reason="no KIS rows",
            )
        by_date: dict[date, list[dict[str, object]]] = {}
        for row in batch.rows:
            by_date.setdefault(date.fromisoformat(str(row["date"])), []).append(row)
        last_path = self.layout.partition_path("bronze.kis_daily_raw", batch.unit.logical_date)
        total = 0
        for logical_date, rows in by_date.items():
            path = self.layout.partition_path("bronze.kis_daily_raw", logical_date)
            if path.exists():
                continue
            self.writer.write(
                RawBatch(batch.unit, rows).to_frame(PRICE_SCHEMA),
                path,
                source="kis",
                request_hash=batch.unit.request_hash,
                include_ingest_metadata=True,
            )
            last_path = path
            total += len(rows)
        return WriteResult(
            path=last_path,
            rows=total,
            skipped=total == 0,
            reason="bronze partitions exist" if total == 0 else None,
        )
