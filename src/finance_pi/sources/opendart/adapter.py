from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import sleep

import polars as pl

from finance_pi.http import SourceApiError
from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.opendart.client import OpenDartClient
from finance_pi.storage.layout import DataLakeLayout
from finance_pi.storage.parquet import ParquetDatasetWriter

FINANCIAL_SCHEMA = {
    "security_id": pl.String,
    "corp_code": pl.String,
    "fiscal_period_end": pl.Date,
    "event_date": pl.Date,
    "rcept_dt": pl.Date,
    "available_date": pl.Date,
    "report_type": pl.String,
    "account_id": pl.String,
    "account_name": pl.String,
    "amount": pl.Float64,
    "is_consolidated": pl.Boolean,
    "accounting_basis": pl.String,
    "is_backfilled": pl.Boolean,
}

FINANCIAL_DEDUP_COLUMNS = [
    "corp_code",
    "fiscal_period_end",
    "rcept_dt",
    "report_type",
    "account_id",
    "is_consolidated",
]


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
            batch.to_frame(
                {
                    "snapshot_dt": pl.Date,
                    "corp_code": pl.String,
                    "corp_name": pl.String,
                    "stock_code": pl.String,
                    "modify_date": pl.String,
                }
            ),
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
    chunk_days: int = 7
    refresh_latest: bool = False
    name: str = "opendart_filings"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            chunk_until = min(until, current + timedelta(days=max(1, self.chunk_days) - 1))
            unit = IngestUnit(
                self.name,
                chunk_until,
                "/api/list.json",
                {"bgn_de": current.isoformat(), "end_de": chunk_until.isoformat()},
            )
            refresh = self.refresh_latest and chunk_until == until
            if refresh or not self._marker_path(unit).exists():
                yield unit
            current = chunk_until + timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        since = date.fromisoformat(str(unit.params["bgn_de"]))
        until = date.fromisoformat(str(unit.params["end_de"]))
        return RawBatch(unit, self.client.fetch_filings(since, until))

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        marker = self._marker_path(batch.unit)
        if not batch.rows:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("no_rows\n", encoding="utf-8")
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
        written_partitions = 0
        for rcept_dt, rows in by_date.items():
            path = self.layout.partition_path("bronze.dart_filings_raw", rcept_dt)
            incoming = _with_ingest_metadata(
                RawBatch(batch.unit, rows).to_frame(
                    {
                        "rcept_dt": pl.Date,
                        "corp_code": pl.String,
                        "corp_name": pl.String,
                        "stock_code": pl.String,
                        "rcept_no": pl.String,
                        "report_nm": pl.String,
                        "rm": pl.String,
                    }
                ),
                batch.unit.request_hash,
            )
            before_rows = 0
            merged = incoming
            mode = "fail"
            if path.exists():
                existing = pl.read_parquet(path)
                before_rows = existing.height
                merged = pl.concat([existing, incoming], how="diagonal_relaxed").unique(
                    subset=["rcept_no"],
                    keep="last",
                    maintain_order=True,
                )
                mode = "overwrite"
            self.writer.write(merged, path, mode=mode)
            last_path = path
            total += max(0, merged.height - before_rows)
            written_partitions += 1
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("ok\n", encoding="utf-8")
        return WriteResult(
            path=last_path,
            rows=total,
            skipped=written_partitions == 0,
            reason="refreshed existing filing partitions" if total == 0 else None,
        )

    def _marker_path(self, unit: IngestUnit):
        return (
            self.layout.root
            / "_cache"
            / "opendart_filings"
            / f"chunk={unit.request_hash[:16]}.done"
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
    is_backfilled: bool = False
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
                is_backfilled=self.is_backfilled,
            ),
        )

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.dart_financials_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no financial rows")
        return _write_financial_rows_by_rcept_date(self.layout, self.writer, batch)


@dataclass(frozen=True)
class DartFinancialsBulkAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: OpenDartClient
    requests: tuple[dict[str, object], ...]
    fs_div: str = "CFS"
    batch_size: int = 25
    sleep_seconds: float = 0.05
    is_backfilled: bool = False
    name: str = "opendart_financials_bulk"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        batch_size = max(1, self.batch_size)
        requests_by_id = {_request_id(request): request for request in self.requests}
        for start in range(0, len(self.requests), batch_size):
            requests = self.requests[start : start + batch_size]
            unit = IngestUnit(
                self.name,
                until,
                "/api/fnlttSinglAcntAll.json",
                {
                    "requests": requests,
                    "request_start": start,
                    "request_count": len(requests),
                    "total_request_count": len(self.requests),
                    "fs_div": self.fs_div,
                },
            )
            marker = self._marker_path(unit)
            if not marker.exists():
                yield unit
                continue
            status, failed_ids = _read_marker(marker)
            if status != "partial":
                continue
            retry_requests = tuple(
                requests_by_id[request_id]
                for request_id in failed_ids
                if request_id in requests_by_id
            )
            if retry_requests:
                yield IngestUnit(
                    self.name,
                    until,
                    "/api/fnlttSinglAcntAll.json",
                    {
                        "requests": retry_requests,
                        "request_start": start,
                        "request_count": len(retry_requests),
                        "total_request_count": len(self.requests),
                        "fs_div": self.fs_div,
                        "retry_of": unit.request_hash,
                    },
                )

    def fetch(self, unit: IngestUnit) -> RawBatch:
        requests = tuple(unit.params["requests"])
        rows: list[dict[str, object]] = []
        failures: list[str] = []
        for index, request in enumerate(requests, start=1):
            corp_code = str(request["corp_code"])
            bsns_year = int(request["bsns_year"])
            report_code = str(request["report_code"])
            available_date = _coerce_date(request["available_date"])
            try:
                fetched = self.client.fetch_financials(
                    corp_code,
                    bsns_year,
                    report_code,
                    available_date=available_date,
                    fs_div=self.fs_div,
                    is_backfilled=self.is_backfilled,
                )
                if not fetched and self.fs_div.upper() == "CFS":
                    fetched = self.client.fetch_financials(
                        corp_code,
                        bsns_year,
                        report_code,
                        available_date=available_date,
                        fs_div="OFS",
                        is_backfilled=self.is_backfilled,
                    )
                rows.extend(fetched)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{corp_code}/{bsns_year}/{report_code}:{exc}")
            if self.sleep_seconds > 0 and index < len(requests):
                sleep(self.sleep_seconds)

        if failures and not rows:
            sample = "; ".join(failures[:5])
            raise SourceApiError(
                "opendart",
                f"all financial requests failed ({len(failures)}); sample={sample}",
            )
        if failures:
            rows.append({"_failures": failures})
        return RawBatch(unit, rows)

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        is_retry = "retry_of" in batch.unit.params
        marker = (
            self._marker_path_for_hash(str(batch.unit.params["retry_of"]))
            if is_retry
            else self._marker_path(batch.unit)
        )
        if not is_retry and marker.exists():
            return WriteResult(path=marker, rows=0, skipped=True, reason="bronze chunk exists")

        failures = [row for row in batch.rows if "_failures" in row]
        financial_rows = [row for row in batch.rows if "_failures" not in row]
        failed_ids = [_failure_id(entry) for entry in failures[0]["_failures"]] if failures else []

        if not financial_rows:
            marker.parent.mkdir(parents=True, exist_ok=True)
            if failed_ids:
                _write_partial_marker(marker, failed_ids)
                return WriteResult(path=marker, rows=0, skipped=True, reason="all retries failed")
            marker.write_text("no_rows\n", encoding="utf-8")
            return WriteResult(path=marker, rows=0, skipped=True, reason="no financial rows")

        result = _write_financial_rows_by_rcept_date(
            self.layout,
            self.writer,
            RawBatch(batch.unit, financial_rows),
        )
        marker.parent.mkdir(parents=True, exist_ok=True)
        if failed_ids:
            _write_partial_marker(marker, failed_ids)
            reason_parts = [result.reason] if result.reason else []
            reason_parts.append(f"partial OpenDART failures: {len(failed_ids)}")
            return WriteResult(path=result.path, rows=result.rows, reason="; ".join(reason_parts))
        marker.write_text("ok\n", encoding="utf-8")
        return result

    def _marker_path(self, unit: IngestUnit):
        return self._marker_path_for_hash(unit.request_hash)

    def _marker_path_for_hash(self, request_hash: str):
        return (
            self.layout.root
            / "_cache"
            / "opendart_financials"
            / f"chunk={request_hash[:16]}.done"
        )


def _write_financial_rows_by_rcept_date(
    layout: DataLakeLayout,
    writer: ParquetDatasetWriter,
    batch: RawBatch,
) -> WriteResult:
    by_date: dict[date, list[dict[str, object]]] = {}
    for row in batch.rows:
        by_date.setdefault(_coerce_date(row["rcept_dt"]), []).append(row)

    last_path = layout.partition_path("bronze.dart_financials_raw", batch.unit.logical_date)
    total_new_rows = 0
    merged_partitions = 0
    for rcept_dt, rows in sorted(by_date.items()):
        path = layout.partition_path("bronze.dart_financials_raw", rcept_dt)
        new_frame = _financial_frame(RawBatch(batch.unit, rows))
        new_frame = _with_ingest_metadata(new_frame, batch.unit.request_hash)
        written = new_frame
        before_rows = 0
        if path.exists():
            existing = pl.read_parquet(path)
            before_rows = existing.height
            dedup_columns = [
                column for column in FINANCIAL_DEDUP_COLUMNS if column in new_frame.columns
            ]
            written = pl.concat([existing, new_frame], how="diagonal_relaxed").unique(
                subset=dedup_columns,
                keep="last",
            )
            merged_partitions += 1
        writer.write(written, path, mode="overwrite")
        total_new_rows += max(0, written.height - before_rows)
        last_path = path

    reason = None
    if merged_partitions:
        reason = f"merged existing financial partitions: {merged_partitions}"
    return WriteResult(path=last_path, rows=total_new_rows, reason=reason)


def _financial_frame(batch: RawBatch) -> pl.DataFrame:
    frame = batch.to_frame(FINANCIAL_SCHEMA)
    for column, dtype in FINANCIAL_SCHEMA.items():
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(column))
    return frame.select(list(FINANCIAL_SCHEMA))


def _with_ingest_metadata(frame: pl.DataFrame, request_hash: str) -> pl.DataFrame:
    return frame.with_columns(
        pl.lit(datetime.now(UTC)).alias("_ingested_at"),
        pl.lit("opendart").alias("_source"),
        pl.lit(request_hash).alias("_source_request_hash"),
    )


def _coerce_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _request_id(request: dict[str, object]) -> str:
    return f"{request['corp_code']}/{request['bsns_year']}/{request['report_code']}"


def _failure_id(failure_entry: str) -> str:
    return failure_entry.split(":", 1)[0]


def _write_partial_marker(marker: Path, failed_ids: list[str]) -> None:
    marker.write_text("partial\n" + "\n".join(failed_ids) + "\n", encoding="utf-8")


def _read_marker(marker: Path) -> tuple[str, list[str]]:
    lines = marker.read_text(encoding="utf-8").splitlines()
    if not lines:
        return "", []
    return lines[0], lines[1:]
