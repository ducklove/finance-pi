from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from finance_pi.config import format_yyyymmdd
from finance_pi.http import HttpJsonClient, SourceApiError
from finance_pi.ingest.models import IngestUnit, RawBatch, WriteResult
from finance_pi.sources.krx.schemas import KrxDailyPriceRow
from finance_pi.sources.parsing import parse_float, parse_int, value_for
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
    client: HttpJsonClient | None = None
    daily_path: str = "/svc/apis/sto/stk_bydd_trd"
    name: str = "krx"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            yield IngestUnit(
                source=self.name,
                logical_date=current,
                endpoint=self.daily_path,
                params={"date": current.isoformat()},
            )
            current += timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        if self.client is None:
            raise RuntimeError("configure a KRX HTTP client before live ingest")
        payload = self.client.get_json(
            self.daily_path,
            params={"basDd": format_yyyymmdd(unit.logical_date)},
        )
        rows = _extract_rows(payload)
        return RawBatch(
            unit=unit,
            rows=[normalize_krx_daily_row(row, unit.logical_date) for row in rows],
        )

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.krx_daily_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no KRX rows")
        rows = [KrxDailyPriceRow.model_validate(row).model_dump(mode="json") for row in batch.rows]
        frame = RawBatch(batch.unit, rows).to_frame()
        if path.exists():
            return WriteResult(path=path, rows=0, skipped=True, reason="bronze partition exists")
        self.writer.write(
            frame,
            path,
            source=self.name,
            request_hash=batch.unit.request_hash,
            include_ingest_metadata=True,
        )
        return WriteResult(path=path, rows=len(rows))


def normalize_krx_daily_row(row: dict[str, Any], logical_date: date) -> dict[str, Any]:
    ticker = str(
        value_for(row, "ISU_SRT_CD", "isu_srt_cd", "ticker", "종목코드", default="")
    ).zfill(6)
    name = str(value_for(row, "ISU_ABBRV", "isu_abbrv", "name", "종목명", default=ticker))
    return {
        "date": logical_date,
        "ticker": ticker,
        "isin": value_for(row, "ISU_CD", "isu_cd", "isin"),
        "name": name,
        "market": str(value_for(row, "MKT_NM", "mkt_nm", "market", "시장구분", default="KRX")),
        "open": parse_float(value_for(row, "TDD_OPNPRC", "open", "시가"), default=0.0),
        "high": parse_float(value_for(row, "TDD_HGPRC", "high", "고가"), default=0.0),
        "low": parse_float(value_for(row, "TDD_LWPRC", "low", "저가"), default=0.0),
        "close": parse_float(value_for(row, "TDD_CLSPRC", "close", "종가"), default=0.0),
        "volume": parse_int(value_for(row, "ACC_TRDVOL", "volume", "거래량"), default=0),
        "trading_value": parse_int(
            value_for(row, "ACC_TRDVAL", "trading_value", "거래대금"),
            default=0,
        ),
        "market_cap": parse_int(value_for(row, "MKTCAP", "market_cap", "시가총액"), default=None),
        "listed_shares": parse_int(
            value_for(row, "LIST_SHRS", "listed_shares", "상장주식수"),
            default=None,
        ),
    }


def _extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ["OutBlock_1", "output", "data", "list"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    if payload.get("result") and not isinstance(payload["result"], list):
        raise SourceApiError("krx", "unexpected result payload", payload=payload)
    return []
