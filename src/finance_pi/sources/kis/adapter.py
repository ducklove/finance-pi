from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from time import sleep

import polars as pl

from finance_pi.http import SourceApiError
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
        return _write_price_rows_by_date(self.layout, self.writer, batch, batch.rows)


@dataclass(frozen=True)
class KisUniverseDailyAdapter:
    layout: DataLakeLayout
    writer: ParquetDatasetWriter
    client: KisDailyPriceClient
    tickers: tuple[str, ...]
    chunk_days: int = 90
    sleep_seconds: float = 0.05
    ticker_batch_size: int = 50
    name: str = "kis_universe_daily"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            chunk_until = min(until, current + timedelta(days=max(1, self.chunk_days) - 1))
            tickers = self.tickers
            if current == chunk_until:
                existing = _existing_tickers_for_date(self.layout, current)
                tickers = tuple(ticker for ticker in self.tickers if ticker not in existing)
            batch_size = max(1, self.ticker_batch_size)
            for start in range(0, len(tickers), batch_size):
                batch_tickers = tickers[start : start + batch_size]
                yield IngestUnit(
                    self.name,
                    chunk_until,
                    "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                    {
                        "ticker_count": len(batch_tickers),
                        "total_ticker_count": len(self.tickers),
                        "ticker_start": start,
                        "tickers": batch_tickers,
                        "since": current.isoformat(),
                        "until": chunk_until.isoformat(),
                    },
                )
            current = chunk_until + timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        since = date.fromisoformat(str(unit.params["since"]))
        until = date.fromisoformat(str(unit.params["until"]))
        tickers = tuple(str(ticker) for ticker in unit.params.get("tickers", self.tickers))
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
                "kis",
                f"all ticker requests failed ({len(failures)}); sample={sample}",
            )
        if failures:
            rows.append({"_failures": failures})
        return RawBatch(unit, rows)

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        failures = [row for row in batch.rows if "_failures" in row]
        price_rows = [row for row in batch.rows if "_failures" not in row]
        if failures:
            sample = "; ".join(failures[0]["_failures"][:5])
            if not price_rows:
                return WriteResult(
                    path=self.layout.partition_path(
                        "bronze.kis_daily_raw",
                        batch.unit.logical_date,
                    ),
                    rows=0,
                    skipped=True,
                    reason=f"KIS failures: {sample}",
                )
        if not price_rows:
            return WriteResult(
                path=self.layout.partition_path("bronze.kis_daily_raw", batch.unit.logical_date),
                rows=0,
                skipped=True,
                reason="no KIS rows",
            )

        result = _write_price_rows_by_date(self.layout, self.writer, batch, price_rows)
        if failures and not result.skipped:
            return WriteResult(
                path=result.path,
                rows=result.rows,
                skipped=False,
                reason=f"partial KIS failures: {len(failures[0]['_failures'])}",
            )
        return result


def _write_price_rows_by_date(
    layout: DataLakeLayout,
    writer: ParquetDatasetWriter,
    batch: RawBatch,
    price_rows: list[dict[str, object]],
) -> WriteResult:
    if not price_rows:
        return WriteResult(
            path=layout.partition_path("bronze.kis_daily_raw", batch.unit.logical_date),
            rows=0,
            skipped=True,
            reason="no KIS rows",
        )

    by_date: dict[date, list[dict[str, object]]] = {}
    for row in price_rows:
        row_date = row["date"]
        logical_date = row_date if isinstance(row_date, date) else date.fromisoformat(str(row_date))
        by_date.setdefault(logical_date, []).append(row)

    last_path = layout.partition_path("bronze.kis_daily_raw", batch.unit.logical_date)
    total_new_rows = 0
    skipped_dates = 0
    merged_dates = 0
    for logical_date, rows in by_date.items():
        path = layout.partition_path("bronze.kis_daily_raw", logical_date)
        new_frame = _align_price_frame(RawBatch(batch.unit, rows).to_frame(PRICE_SCHEMA))
        output = new_frame
        mode = "fail"
        new_rows = new_frame.height
        if path.exists():
            existing = _align_price_frame(pl.read_parquet(path))
            output = (
                pl.concat([existing, new_frame], how="diagonal_relaxed")
                .sort(["date", "ticker"])
                .unique(subset=["date", "ticker"], keep="last")
            )
            new_rows = max(output.height - existing.height, 0)
            if new_rows == 0:
                skipped_dates += 1
                continue
            mode = "overwrite"
            merged_dates += 1

        writer.write(
            output,
            path,
            mode=mode,
            source="kis",
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        last_path = path
        total_new_rows += new_rows

    reason = None
    if total_new_rows == 0:
        reason = "bronze partitions already contain requested KIS rows"
    elif merged_dates:
        reason = f"merged existing KIS partitions: {merged_dates}"
    elif skipped_dates:
        reason = f"skipped existing KIS partitions: {skipped_dates}"
    return WriteResult(
        path=last_path,
        rows=total_new_rows,
        skipped=total_new_rows == 0,
        reason=reason,
    )


def _align_price_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        [
            (
                pl.col(column).cast(dtype, strict=False).alias(column)
                if column in frame.columns
                else pl.lit(None, dtype=dtype).alias(column)
            )
            for column, dtype in PRICE_SCHEMA.items()
        ]
    )


def _existing_tickers_for_date(layout: DataLakeLayout, logical_date: date) -> set[str]:
    path = layout.partition_path("bronze.kis_daily_raw", logical_date)
    if not path.exists():
        return set()
    try:
        frame = pl.read_parquet(path, columns=["ticker"])
    except (OSError, pl.exceptions.PolarsError):
        return set()
    return set(frame["ticker"].cast(pl.String).str.zfill(6).to_list())
