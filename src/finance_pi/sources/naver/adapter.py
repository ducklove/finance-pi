from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import sleep

import polars as pl

from finance_pi.http import SourceApiError
from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.naver.client import NaverDailyPriceClient, NaverFinanceClient
from finance_pi.sources.schemas import NAVER_SUMMARY_SCHEMA, PRICE_SCHEMA
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


@dataclass(frozen=True)
class NaverDailyBackfillAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: NaverDailyPriceClient
    tickers: tuple[str, ...]
    chunk_days: int = 3650
    ticker_batch_size: int = 50
    sleep_seconds: float = 0.02
    name: str = "naver_daily"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            chunk_until = min(until, current + timedelta(days=max(1, self.chunk_days) - 1))
            for start in range(0, len(self.tickers), max(1, self.ticker_batch_size)):
                tickers = self.tickers[start : start + max(1, self.ticker_batch_size)]
                unit = IngestUnit(
                    self.name,
                    chunk_until,
                    "/siseJson.naver",
                    {
                        "since": current.isoformat(),
                        "until": chunk_until.isoformat(),
                        "tickers": tickers,
                        "ticker_start": start,
                        "ticker_count": len(tickers),
                        "total_ticker_count": len(self.tickers),
                    },
                )
                path = self._path(unit)
                sidecar = self._sidecar_path(path)
                if not path.exists() and not sidecar.exists():
                    yield unit
                elif sidecar.exists():
                    failed_tickers = tuple(_read_failures_sidecar(sidecar))
                    if failed_tickers:
                        yield IngestUnit(
                            self.name,
                            chunk_until,
                            "/siseJson.naver",
                            {
                                "since": current.isoformat(),
                                "until": chunk_until.isoformat(),
                                "tickers": failed_tickers,
                                "ticker_start": start,
                                "ticker_count": len(failed_tickers),
                                "total_ticker_count": len(self.tickers),
                                "retry_of": unit.request_hash,
                            },
                        )
            current = chunk_until + timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        since = date.fromisoformat(str(unit.params["since"]))
        until = date.fromisoformat(str(unit.params["until"]))
        tickers = tuple(str(ticker) for ticker in unit.params["tickers"])
        rows: list[dict[str, object]] = []
        failures: list[str] = []
        for index, ticker in enumerate(tickers, start=1):
            try:
                rows.extend(self.client.fetch_daily_prices(ticker, since, until))
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{ticker}:{exc}")
            if self.sleep_seconds > 0 and index < len(tickers):
                sleep(self.sleep_seconds)
        if failures and not rows:
            sample = "; ".join(failures[:5])
            raise SourceApiError(
                "naver",
                f"all ticker requests failed ({len(failures)}); sample={sample}",
            )
        if failures:
            rows.append({"_failures": failures})
        return RawBatch(unit, rows)

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        is_retry = "retry_of" in batch.unit.params
        path = self._path(batch.unit)
        if not is_retry and path.exists():
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze chunk exists")

        failures = [row for row in batch.rows if "_failures" in row]
        price_rows = [row for row in batch.rows if "_failures" not in row]
        failed_tickers = list(failures[0]["_failures"]) if failures else []

        if is_retry:
            # Original chunk already occupies `path`; write successes into a
            # sibling chunk directory so bronze globs (chunk=*) still pick it up.
            write_path = self._path(batch.unit, suffix="retry")
            source_path = self._path_from_source_unit(batch.unit)
            sidecar = self._sidecar_path(source_path)
        else:
            write_path = path
            sidecar = self._sidecar_path(path)

        if not price_rows:
            if failed_tickers:
                # Do not write an empty chunk; that would mark the unit done
                # and mask the failure. Only the sidecar records the retry.
                self._write_sidecar(sidecar, failed_tickers)
                return WriteResult(
                    path=write_path, rows=0, skipped=True, reason="all retries failed"
                )
            self.writer.write(
                pl.DataFrame(schema=PRICE_SCHEMA),
                write_path,
                source="naver",
                request_hash=batch.unit.request_hash,
                include_ingest_metadata=True,
            )
            return WriteResult(path=write_path, rows=0, skipped=True, reason="no Naver daily rows")

        self.writer.write(
            RawBatch(batch.unit, price_rows).to_frame(PRICE_SCHEMA),
            write_path,
            source="naver",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )

        reason = None
        if failed_tickers:
            self._write_sidecar(sidecar, failed_tickers)
            reason = f"partial Naver failures: {len(failed_tickers)}"
        elif sidecar.exists():
            sidecar.unlink()
        return WriteResult(path=write_path, rows=len(price_rows), reason=reason)

    def _path(self, unit: IngestUnit, *, suffix: str = ""):
        request_dt = unit.logical_date.isoformat()
        chunk = unit.request_hash[:12] + (f"-{suffix}" if suffix else "")
        return (
            self.layout.root
            / "bronze"
            / "naver_daily"
            / f"request_dt={request_dt}"
            / f"chunk={chunk}"
            / "part.parquet"
        )

    def _path_from_source_unit(self, retry_unit: IngestUnit) -> Path:
        source_hash = str(retry_unit.params["retry_of"])
        request_dt = retry_unit.logical_date.isoformat()
        return (
            self.layout.root
            / "bronze"
            / "naver_daily"
            / f"request_dt={request_dt}"
            / f"chunk={source_hash[:12]}"
            / "part.parquet"
        )

    @staticmethod
    def _sidecar_path(chunk_path: Path) -> Path:
        return chunk_path.with_name(chunk_path.name + ".failures.json")

    @staticmethod
    def _write_sidecar(sidecar: Path, failed_tickers: list[str]) -> None:
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        tickers = [entry.split(":", 1)[0] for entry in failed_tickers]
        sidecar.write_text(
            json.dumps({"tickers": tickers, "errors": failed_tickers}, ensure_ascii=False),
            encoding="utf-8",
        )


def _read_failures_sidecar(sidecar: Path) -> list[str]:
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    return [str(ticker) for ticker in payload.get("tickers", [])]
