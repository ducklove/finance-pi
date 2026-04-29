from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.naver.client import NaverFinanceClient
from finance_pi.sources.schemas import NAVER_SUMMARY_SCHEMA
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class NaverSummaryAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: NaverFinanceClient
    snapshot_date: date
    markets: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    name: str = "naver_summary"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        yield IngestUnit(
            self.name,
            self.snapshot_date,
            "/sise/sise_market_sum.naver",
            {"markets": ",".join(self.markets)},
        )

    def fetch(self, unit: IngestUnit) -> RawBatch:
        return RawBatch(
            unit,
            self.client.fetch_market_summary(unit.logical_date, markets=self.markets),
        )

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.naver_summary_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no Naver summary rows")
        if path.exists():
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze partition exists")
        self.writer.write(
            batch.to_frame(NAVER_SUMMARY_SCHEMA),
            path,
            source="naver",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(batch.rows))
