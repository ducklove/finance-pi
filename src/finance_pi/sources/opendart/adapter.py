from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.opendart.client import OpenDartClient
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter


@dataclass(frozen=True)
class DartCompanyAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: OpenDartClient
    snapshot_date: date
    name: str = "opendart_company"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        yield IngestUnit(self.name, self.snapshot_date, "/api/corpCode.xml", {})

    def fetch(self, unit: IngestUnit) -> RawBatch:
        return RawBatch(unit, self.client.fetch_corp_codes(unit.logical_date))

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.dart_company_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no company rows")
        if path.exists():
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze partition exists")
        self.writer.write(
            batch.to_frame(),
            path,
            source="opendart",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(batch.rows))


@dataclass(frozen=True)
class DartFilingsAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: OpenDartClient
    name: str = "opendart_filings"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        yield IngestUnit(
            self.name,
            until,
            "/api/list.json",
            {"bgn_de": since.isoformat(), "end_de": until.isoformat()},
        )

    def fetch(self, unit: IngestUnit) -> RawBatch:
        since = date.fromisoformat(str(unit.params["bgn_de"]))
        until = date.fromisoformat(str(unit.params["end_de"]))
        return RawBatch(unit, self.client.fetch_filings(since, until))

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        if not batch.rows:
            return WriteResult(
                path=self.layout.partition_path("bronze.dart_filings_raw", batch.unit.logical_date),
                rows=0,
                skipped=True,
                reason="no filings",
            )
        by_date: dict[date, list[dict[str, object]]] = {}
        for row in batch.rows:
            by_date.setdefault(date.fromisoformat(str(row["rcept_dt"])), []).append(row)
        last_path = self.layout.partition_path("bronze.dart_filings_raw", batch.unit.logical_date)
        total = 0
        for rcept_dt, rows in by_date.items():
            path = self.layout.partition_path("bronze.dart_filings_raw", rcept_dt)
            if path.exists():
                continue
            self.writer.write(
                RawBatch(batch.unit, rows).to_frame(),
                path,
                source="opendart",
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


@dataclass(frozen=True)
class DartFinancialsAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: OpenDartClient
    corp_code: str
    bsns_year: int
    reprt_code: str
    available_date: date
    fs_div: str = "CFS"
    name: str = "opendart_financials"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        yield IngestUnit(
            self.name,
            self.available_date,
            "/api/fnlttSinglAcntAll.json",
            {
                "corp_code": self.corp_code,
                "bsns_year": self.bsns_year,
                "reprt_code": self.reprt_code,
                "fs_div": self.fs_div,
            },
        )

    def fetch(self, unit: IngestUnit) -> RawBatch:
        return RawBatch(
            unit,
            self.client.fetch_financials(
                self.corp_code,
                self.bsns_year,
                self.reprt_code,
                available_date=self.available_date,
                fs_div=self.fs_div,
            ),
        )

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.dart_financials_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no financial rows")
        if path.exists():
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze partition exists")
        self.writer.write(
            batch.to_frame(),
            path,
            source="opendart",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(batch.rows))
