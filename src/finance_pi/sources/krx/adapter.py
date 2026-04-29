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
from finance_pi.sources.schemas import PRICE_SCHEMA
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
    market_paths: dict[str, str] | None = None
    name: str = "krx"

    def list_pending(self, since: date, until: date) -> Iterable[IngestUnit]:
        current = since
        while current <= until:
            yield IngestUnit(
                source=self.name,
                logical_date=current,
                endpoint="krx_daily",
                params={"date": current.isoformat()},
            )
            current += timedelta(days=1)

    def fetch(self, unit: IngestUnit) -> RawBatch:
        if self.client is None:
            raise RuntimeError("configure a KRX HTTP client before live ingest")
        rows: list[dict[str, Any]] = []
        for market, path in self._market_paths().items():
            payload = self.client.get_json(
                path,
                params={"basDd": format_yyyymmdd(unit.logical_date)},
            )
            rows.extend(_tag_market(row, market) for row in _extract_rows(payload))
        return RawBatch(
            unit=unit,
            rows=[normalize_krx_daily_row(row, unit.logical_date) for row in rows],
        )

    def write_bronze(self, batch: RawBatch) -> WriteResult:
        path = self.layout.partition_path("bronze.krx_daily_raw", batch.unit.logical_date)
        if not batch.rows:
            return WriteResult(path=path, rows=0, skipped=True, reason="no KRX rows")
        rows = [KrxDailyPriceRow.model_validate(row).model_dump(mode="json") for row in batch.rows]
        frame = RawBatch(batch.unit, rows).to_frame(PRICE_SCHEMA)
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

    def _market_paths(self) -> dict[str, str]:
        if self.market_paths:
            return self.market_paths
        return {"KOSPI": self.daily_path}


def normalize_krx_daily_row(row: dict[str, Any], logical_date: date) -> dict[str, Any]:
    ticker = str(value_for(row, "ISU_SRT_CD", "isu_srt_cd", "ticker", default="")).zfill(6)
    name = str(value_for(row, "ISU_ABBRV", "isu_abbrv", "name", default=ticker))
    return {
        "date": logical_date,
        "ticker": ticker,
        "isin": value_for(row, "ISU_CD", "isu_cd", "isin"),
        "name": name,
        "market": str(value_for(row, "MKT_NM", "mkt_nm", "market", "_market", default="KRX")),
        "open": parse_float(value_for(row, "TDD_OPNPRC", "open"), default=0.0),
        "high": parse_float(value_for(row, "TDD_HGPRC", "high"), default=0.0),
        "low": parse_float(value_for(row, "TDD_LWPRC", "low"), default=0.0),
        "close": parse_float(value_for(row, "TDD_CLSPRC", "close"), default=0.0),
        "volume": parse_int(value_for(row, "ACC_TRDVOL", "volume"), default=0),
        "trading_value": parse_int(
            value_for(row, "ACC_TRDVAL", "trading_value"),
            default=0,
        ),
        "market_cap": parse_int(value_for(row, "MKTCAP", "market_cap"), default=None),
        "listed_shares": parse_int(
            value_for(row, "LIST_SHRS", "listed_shares"),
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


def _tag_market(row: dict[str, Any], market: str) -> dict[str, Any]:
    tagged = dict(row)
    tagged.setdefault("_market", market)
    return tagged
