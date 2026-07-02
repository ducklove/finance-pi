from __future__ import annotations

import csv
import html
import io
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from glob import glob
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import polars as pl

from finance_pi.http import SourceApiError
from finance_pi.sources.parsing import parse_float, parse_int

PUBLIC_NPS_PAGE_URL = "https://www.data.go.kr/data/3070507/fileData.do"
PUBLIC_NPS_FALLBACK_CSV_URL = (
    "https://www.data.go.kr/cmm/cmm/fileDownload.do"
    "?atchFileId=FILE_000000003558824&fileDetailSn=1&insertDataPrcus=N"
)
PUBLIC_NPS_SOURCE_DATE = date(2024, 12, 31)
FNGUIDE_URL = "https://comp.fnguide.com/SVO/WooriRenewal/Inst_Data.asp?strInstCD=49530"

_PUBLIC_DATASET_RE = re.compile(r"국민연금공단_국내주식 투자정보_(\d{8})")
_PUBLIC_CSV_URL_RE = re.compile(r'"contentUrl"\s*:\s*"([^"]+fileDownload\.do[^"]+)"')
_ROW_RE = re.compile(r"<tr[^>]*>.*?</tr>", re.IGNORECASE | re.DOTALL)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")

NPS_NAME_ALIASES: dict[str, str] = {
    "삼성전자": "005930",
    "SK하이닉스": "000660",
    "LG에너지솔루션": "373220",
    "삼성바이오로직스": "207940",
    "현대차": "005380",
    "기아": "000270",
    "NAVER": "035420",
    "셀트리온": "068270",
    "현대모비스": "012330",
    "POSCO홀딩스": "005490",
    "HD현대중공업": "329180",
    "HD한국조선해양": "009540",
    "삼성물산": "028260",
    "LG화학": "051910",
    "삼성생명": "032830",
    "한화에어로스페이스": "012450",
    "삼성SDI": "006400",
    "카카오": "035720",
    "크래프톤": "259960",
    "삼성화재": "000810",
    "두산에너빌리티": "034020",
    "기업은행": "024110",
    "삼성전기": "009150",
    "삼성에스디에스": "018260",
    "삼성중공업": "010140",
    "SK텔레콤": "017670",
    "LG전자": "066570",
    "한미반도체": "042700",
    "HD현대미포": "010620",
    "SK바이오팜": "326030",
    "LS ELECTRIC": "010120",
    "현대차2우B": "005387",
    "삼성전자우": "005935",
    "휠라홀딩스": "081660",
    "HD현대인프라코어": "042670",
    "아모레G": "002790",
    "HD현대건설기계": "267270",
    "DGB금융지주": "139130",
    "삼성화재우": "000815",
    "TKG휴켐스": "069260",
    "DI동일": "001530",
    "KCC글라스": "344820",
    "현대차우": "005385",
    "LG전자우": "066575",
    "LG화학우": "051915",
    "아모레퍼시픽우": "090435",
    "LG생활건강우": "051905",
    "미래에셋증권2우B": "00680K",
    "CJ제일제당 우": "097955",
    "금호석유우": "011785",
    "유나이티드제약": "033270",
    "CJ4우(전환)": "00104K",
    "현대차3우B": "005389",
    "삼성전기우": "009155",
    "신세계 I&C": "035510",
    "KB금융": "105560",
    "신한지주": "055550",
    "하나금융지주": "086790",
    "우리금융지주": "316140",
    "메리츠금융지주": "138040",
    "KT&G": "033780",
    "HMM": "011200",
    "LG": "003550",
    "SK": "034730",
    "LS": "006260",
    "GS": "078930",
    "CJ": "001040",
    "KT": "030200",
    "S-Oil": "010950",
}


@dataclass(frozen=True)
class PricePoint:
    logical_date: date
    close: float
    previous_close: float | None


@dataclass(frozen=True)
class NpsHoldingsClient:
    data_root: Path
    user_agent: str = "Mozilla/5.0 finance-pi/0.1"
    discover_public_csv: bool = False
    public_csv_url: str | None = None
    public_source_date: date | None = None

    def fetch_holdings(self, snapshot_date: date) -> list[dict[str, Any]]:
        rows = self.fetch_public_holdings()
        if not rows:
            rows = self.fetch_fnguide_holdings()
        rows = self.resolve_stock_codes(rows)
        return self.enrich_with_prices(rows, snapshot_date)

    def fetch_public_holdings(self) -> list[dict[str, Any]]:
        payload, source_date = self._download_public_csv()
        text = _decode_csv(payload)
        reader = csv.DictReader(io.StringIO(text))
        rows: list[dict[str, Any]] = []
        for row in reader:
            rank = parse_int(row.get("번호"), default=None)
            name = str(row.get("종목명") or "").strip()
            amount_eok = parse_float(row.get("평가액(억 원)"), default=None)
            weight_pct = parse_float(row.get("자산군 내 비중(퍼센트)"), default=None)
            ownership_pct = parse_float(row.get("지분율(퍼센트)"), default=None)
            if not rank or not name or amount_eok is None:
                continue
            rows.append(
                {
                    "rank": rank,
                    "name": name,
                    "shares": 0,
                    "ownership_pct": ownership_pct or 0.0,
                    "source": "data.go.kr",
                    "source_date": source_date,
                    "source_market_value": round(amount_eok * 100_000_000),
                    "source_weight_pct": weight_pct,
                }
            )
        return rows

    def fetch_fnguide_holdings(self) -> list[dict[str, Any]]:
        payload = _download_url(FNGUIDE_URL, self.user_agent, referer=None, timeout=30)
        text = payload.decode("utf-8", "replace")
        rows: list[dict[str, Any]] = []
        for raw_row in _ROW_RE.findall(text):
            cells = [_clean_cell(cell) for cell in _TD_RE.findall(raw_row)]
            if len(cells) < 7:
                continue
            rank = parse_int(cells[0], default=None)
            if rank is None:
                continue
            source_date = _parse_source_date(cells[6].replace(".", "-"))
            rows.append(
                {
                    "rank": rank,
                    "name": cells[1],
                    "shares": parse_int(cells[2], default=0) or 0,
                    "ownership_pct": parse_float(cells[5], default=0.0) or 0.0,
                    "source": "fnguide",
                    "source_date": source_date,
                    "source_market_value": None,
                    "source_weight_pct": None,
                }
            )
        return rows

    def resolve_stock_codes(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        exact, normalized = self._local_name_maps()
        resolved: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            name = str(item.get("name") or item.get("stock_name") or "").strip()
            code = (
                NPS_NAME_ALIASES.get(name)
                or exact.get(name)
                or exact.get(name.upper())
                or normalized.get(_normalize_lookup_name(name))
            )
            item["stock_code"] = _normalize_ticker(code or item.get("stock_code") or "")
            item["stock_name"] = name
            resolved.append(item)
        return resolved

    def enrich_with_prices(
        self,
        rows: list[dict[str, Any]],
        snapshot_date: date,
    ) -> list[dict[str, Any]]:
        tickers = [str(row.get("stock_code") or "") for row in rows if row.get("stock_code")]
        snapshot_prices = self._close_lookup(tickers, snapshot_date, lookback_days=10)
        source_dates = {
            row["source_date"]
            for row in rows
            if isinstance(row.get("source_date"), date) and row.get("source_market_value")
        }
        source_prices_by_date = {
            source_date: self._close_lookup(tickers, source_date, lookback_days=20)
            for source_date in source_dates
        }

        enriched: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            code = str(item.get("stock_code") or "")
            shares = parse_int(item.get("shares"), default=0) or 0
            source_date = item.get("source_date")
            source_market_value = parse_float(item.get("source_market_value"), default=None)
            if (
                code
                and not shares
                and source_market_value
                and isinstance(source_date, date)
                and (source_price := source_prices_by_date[source_date].get(code))
            ):
                shares = max(1, round(source_market_value / source_price.close))
                item["shares_source"] = "public_value_div_year_end_close"

            price_point = snapshot_prices.get(code)
            price = price_point.close if price_point else None
            previous_close = price_point.previous_close if price_point else None
            if price is not None and shares:
                market_value = round(price * shares)
            elif source_market_value is not None and not code:
                market_value = source_market_value
            else:
                market_value = None
            item.update(
                {
                    "date": snapshot_date,
                    "shares": shares,
                    "price": price,
                    "market_value": market_value,
                    "change_pct": (price / previous_close - 1) * 100
                    if price is not None and previous_close
                    else None,
                    "price_date": price_point.logical_date if price_point else None,
                    "is_exact_price": price_point.logical_date == snapshot_date
                    if price_point
                    else False,
                    "stock_name": item.get("stock_name") or item.get("name") or "",
                    "source": item.get("source") or "nps",
                }
            )
            enriched.append(_nps_output_row(item))
        return enriched

    def legacy_dates(self, sqlite_path: Path, since: date, until: date) -> list[date]:
        if not sqlite_path.exists():
            raise SourceApiError("nps", f"legacy SQLite DB not found: {sqlite_path}")
        with sqlite3.connect(sqlite_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT date
                FROM nps_holdings
                WHERE date BETWEEN ? AND ?
                ORDER BY date
                """,
                (since.isoformat(), until.isoformat()),
            ).fetchall()
        return [date.fromisoformat(row[0]) for row in rows]

    def fetch_legacy_sqlite_holdings(
        self,
        sqlite_path: Path,
        logical_date: date,
    ) -> list[dict[str, Any]]:
        if not sqlite_path.exists():
            raise SourceApiError("nps", f"legacy SQLite DB not found: {sqlite_path}")
        with sqlite3.connect(sqlite_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT date, stock_code, stock_name, shares, ownership_pct,
                       price, market_value, change_pct
                FROM nps_holdings
                WHERE date = ?
                ORDER BY market_value DESC NULLS LAST, stock_code
                """,
                (logical_date.isoformat(),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for rank, row in enumerate(rows, start=1):
            item = dict(row)
            item.update(
                {
                    "date": logical_date,
                    "stock_code": _normalize_ticker(item.get("stock_code")),
                    "rank": rank,
                    "source": "value-invest-sqlite",
                    "source_date": logical_date,
                    "source_market_value": None,
                    "source_weight_pct": None,
                    "shares_source": "legacy",
                    "price_date": logical_date if item.get("price") is not None else None,
                    "is_exact_price": item.get("price") is not None,
                }
            )
            result.append(_nps_output_row(item))
        return result

    def _download_public_csv(self) -> tuple[bytes, date]:
        env_url = os.getenv("NPS_PUBLIC_DATA_CSV_URL", "").strip()
        if self.public_csv_url:
            csv_url = self.public_csv_url
            source_date = self.public_source_date or PUBLIC_NPS_SOURCE_DATE
        elif env_url:
            csv_url = env_url
            source_date = _parse_source_date(
                os.getenv("NPS_PUBLIC_DATA_SOURCE_DATE", "").strip()
            ) or PUBLIC_NPS_SOURCE_DATE
        elif self.discover_public_csv or _truthy_env("NPS_PUBLIC_DATA_DISCOVER"):
            csv_url, source_date = self._public_csv_url_from_page()
        else:
            csv_url = PUBLIC_NPS_FALLBACK_CSV_URL
            source_date = PUBLIC_NPS_SOURCE_DATE

        return (
            _download_url(
                csv_url,
                self.user_agent,
                referer=PUBLIC_NPS_PAGE_URL,
                timeout=20,
            ),
            source_date,
        )

    def _public_csv_url_from_page(self) -> tuple[str, date]:
        payload = _download_url(PUBLIC_NPS_PAGE_URL, self.user_agent, referer=None, timeout=20)
        text = payload.decode("utf-8", "replace")
        match = _PUBLIC_CSV_URL_RE.search(text)
        csv_url = match.group(1).replace("&amp;", "&") if match else PUBLIC_NPS_FALLBACK_CSV_URL
        return csv_url, _source_date_from_text(text)

    def _local_name_maps(self) -> tuple[dict[str, str], dict[str, str]]:
        exact: dict[str, str] = {}
        normalized: dict[str, str] = {}
        for frame in self._identity_frames():
            for row in frame.iter_rows(named=True):
                name = str(row.get("name") or row.get("corp_name") or "").strip()
                code = _normalize_ticker(row.get("ticker") or row.get("stock_code"))
                if not name or not code:
                    continue
                exact.setdefault(name, code)
                exact.setdefault(name.upper(), code)
                normalized.setdefault(_normalize_lookup_name(name), code)
        return exact, normalized

    def _identity_frames(self) -> list[pl.DataFrame]:
        frames: list[pl.DataFrame] = []
        master = _read_parquet_optional(self.data_root / "gold/security_master.parquet")
        if master is not None and {"ticker", "name"} <= set(master.columns):
            frames.append(master.select("ticker", "name"))

        company = _read_parquet_optional(
            self.data_root / "bronze/dart_company/snapshot_dt=*/part.parquet"
        )
        if company is not None and {"stock_code", "corp_name"} <= set(company.columns):
            frames.append(company.select("stock_code", "corp_name").drop_nulls("stock_code"))
        return frames

    def _close_lookup(
        self,
        tickers: list[str],
        target_date: date,
        *,
        lookback_days: int,
    ) -> dict[str, PricePoint]:
        ticker_set = {
            ticker
            for ticker in (_normalize_ticker(ticker) for ticker in tickers)
            if ticker
        }
        if not ticker_set:
            return {}
        gold_files, silver_files = _price_files(self.data_root, target_date, lookback_days)
        files = gold_files or silver_files
        if not files:
            return {}
        frame = pl.read_parquet([path.as_posix() for path in files], hive_partitioning=True)
        if gold_files and "security_id" in frame.columns:
            frame = frame.with_columns(
                pl.col("security_id").cast(pl.String).str.strip_prefix("S").alias("ticker"),
                pl.col("close_adj").alias("close"),
            )
        elif "ticker" in frame.columns:
            frame = frame.with_columns(pl.col("ticker").cast(pl.String))
        else:
            return {}
        rows = (
            frame.with_columns(
                pl.col("date").cast(pl.Date, strict=False),
                pl.col("ticker").map_elements(_normalize_ticker, return_dtype=pl.String),
                pl.col("close").cast(pl.Float64, strict=False),
            )
            .filter(pl.col("ticker").is_in(ticker_set) & pl.col("close").is_not_null())
            .select("ticker", "date", "close")
            .sort("ticker", "date")
        )
        points: dict[str, PricePoint] = {}
        for ticker, group in rows.group_by("ticker", maintain_order=True):
            ticker_value = str(ticker[0] if isinstance(ticker, tuple) else ticker)
            ordered = group.sort("date").iter_rows(named=True)
            history = [row for row in ordered if row["date"] <= target_date]
            if not history:
                continue
            latest = history[-1]
            previous = history[-2]["close"] if len(history) >= 2 else None
            points[ticker_value] = PricePoint(
                logical_date=latest["date"],
                close=float(latest["close"]),
                previous_close=float(previous) if previous is not None else None,
            )
        return points


def _nps_output_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": item.get("date"),
        "stock_code": _normalize_ticker(item.get("stock_code")),
        "stock_name": str(item.get("stock_name") or item.get("name") or "").strip(),
        "shares": parse_int(item.get("shares"), default=0) or 0,
        "ownership_pct": parse_float(item.get("ownership_pct"), default=0.0) or 0.0,
        "price": parse_float(item.get("price"), default=None),
        "market_value": parse_float(item.get("market_value"), default=None),
        "change_pct": parse_float(item.get("change_pct"), default=None),
        "rank": parse_int(item.get("rank"), default=None),
        "source": item.get("source"),
        "source_date": item.get("source_date"),
        "source_market_value": parse_float(item.get("source_market_value"), default=None),
        "source_weight_pct": parse_float(item.get("source_weight_pct"), default=None),
        "shares_source": item.get("shares_source"),
        "price_date": item.get("price_date"),
        "is_exact_price": bool(item.get("is_exact_price")),
    }


def _download_url(
    url: str,
    user_agent: str,
    *,
    referer: str | None,
    timeout: int,
) -> bytes:
    headers = {"User-Agent": user_agent}
    if referer:
        headers["Referer"] = referer
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read()
    except Exception as exc:  # noqa: BLE001
        raise SourceApiError("nps", f"download failed for {url}: {exc}") from exc


def _decode_csv(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", "replace")


def _source_date_from_text(text: str) -> date:
    match = _PUBLIC_DATASET_RE.search(text)
    if not match:
        return PUBLIC_NPS_SOURCE_DATE
    raw = match.group(1)
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def _parse_source_date(value: str | date | None) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return date.fromisoformat(text)
    except ValueError:
        return None


def _clean_cell(value: str) -> str:
    text = html.unescape(_TAG_RE.sub(" ", value))
    return " ".join(text.split()).strip()


def _normalize_ticker(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or not re.fullmatch(r"[0-9A-Z]{1,12}", text):
        return ""
    return text.zfill(6) if text.isdigit() and len(text) <= 6 else text


def _normalize_lookup_name(value: str) -> str:
    return re.sub(r"[\s·._()/-]+", "", value.strip().upper())


def _truthy_env(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _read_parquet_optional(pattern: Path) -> pl.DataFrame | None:
    files = sorted(glob(pattern.as_posix()))
    if not files:
        return None
    try:
        return pl.read_parquet(files, hive_partitioning=True)
    except pl.exceptions.SchemaError:
        frames = [pl.read_parquet(file, hive_partitioning=True) for file in files]
        return pl.concat(frames, how="diagonal_relaxed")


def _price_files(
    data_root: Path, target_date: date, lookback_days: int
) -> tuple[list[Path], list[Path]]:
    """Collect gold and silver price partitions separately over the lookback window.

    Callers should prefer the gold (adjusted-close) files and only fall back
    to silver (raw-close) files when no gold partitions exist at all, to
    avoid mixing adjusted and raw closes in a single read.
    """
    gold_files: list[Path] = []
    silver_files: list[Path] = []
    current = target_date - timedelta(days=max(0, lookback_days))
    while current <= target_date:
        gold_path = (
            data_root
            / "gold"
            / "daily_prices_adj"
            / f"dt={current.isoformat()}"
            / "part.parquet"
        )
        silver_path = (
            data_root
            / "silver"
            / "prices"
            / f"dt={current.isoformat()}"
            / "part.parquet"
        )
        if gold_path.exists():
            gold_files.append(gold_path)
        if silver_path.exists():
            silver_files.append(silver_path)
        current += timedelta(days=1)
    return gold_files, silver_files
