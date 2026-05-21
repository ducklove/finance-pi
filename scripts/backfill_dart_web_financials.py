from __future__ import annotations

import argparse
import html.parser
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb
import polars as pl

REPORT_CODE = "11011"
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
VIEWER_URL = "https://dart.fss.or.kr/report/viewer.do"
USER_AGENT = "finance-pi-dart-web-backfill/0.1"


@dataclass(frozen=True)
class Filing:
    rcept_dt: date
    corp_code: str
    corp_name: str | None
    stock_code: str | None
    rcept_no: str
    report_nm: str


@dataclass(frozen=True)
class Section:
    text: str
    rcp_no: str
    dcm_no: str
    ele_id: str
    offset: str
    length: str
    dtd: str


class TableParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._table_depth = 0
        self._in_row = False
        self._cell_depth = 0
        self._cell: list[str] = []
        self._row: list[str] = []
        self._rows: list[list[str]] = []
        self.tables: list[list[list[str]]] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._rows = []
        elif self._table_depth and tag == "tr":
            self._in_row = True
            self._row = []
        elif self._table_depth and tag in {"td", "th"}:
            self._cell_depth += 1
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell_depth:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._table_depth and tag in {"td", "th"} and self._cell_depth:
            text = re.sub(r"\s+", " ", "".join(self._cell)).strip()
            self._row.append(text)
            self._cell_depth -= 1
        elif self._table_depth and tag == "tr" and self._in_row:
            if any(cell for cell in self._row):
                self._rows.append(self._row)
            self._in_row = False
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1:
                self.tables.append(self._rows)
            self._table_depth -= 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--since", default="2007-01-02")
    parser.add_argument("--until", default="2015-06-23")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--progress-every", type=int, default=1)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    data_root = root / "data"
    filings = _load_filings(
        data_root,
        date.fromisoformat(args.since),
        date.fromisoformat(args.until),
    )
    if args.limit is not None:
        filings = filings[: args.limit]

    print(f"DART web financial filings: {len(filings)} from {args.since}..{args.until}")
    total_rows = 0
    total_written = 0
    processed = 0
    no_rows = 0
    errors = 0
    for index, filing in enumerate(filings, start=1):
        marker = _marker_path(data_root, filing.rcept_no)
        if marker.exists() and not args.force:
            continue
        try:
            rows = _fetch_filing_rows(filing)
        except Exception as exc:  # noqa: BLE001
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(f"error: {exc}\n", encoding="utf-8")
            errors += 1
            _print_progress(
                args.progress_every,
                index,
                len(filings),
                f"error {filing.rcept_no} {filing.corp_name}: {exc}",
            )
            continue

        if rows:
            path = (
                data_root
                / "bronze"
                / "dart_financials"
                / f"rcept_dt={filing.rcept_dt}"
                / "part.parquet"
            )
            new_frame = pl.DataFrame(rows, infer_schema_length=None)
            written = _merge_existing(path, new_frame)
            total_rows += len(rows)
            total_written += written
            _print_progress(
                args.progress_every,
                index,
                len(filings),
                f"wrote {filing.rcept_no} {filing.corp_name} rows={len(rows)} new={written}",
            )
        else:
            no_rows += 1
            _print_progress(
                args.progress_every,
                index,
                len(filings),
                f"no rows {filing.rcept_no} {filing.corp_name}",
            )
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("ok\n", encoding="utf-8")
        processed += 1
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)
    print(
        f"done processed={processed} no_rows={no_rows} errors={errors} "
        f"rows={total_rows} new_rows={total_written}"
    )


def _print_progress(progress_every: int, index: int, total: int, message: str) -> None:
    if progress_every <= 1 or index % progress_every == 0 or index == total:
        print(f"{index}/{total} {message}")


def _load_filings(data_root: Path, since: date, until: date) -> list[Filing]:
    pattern = data_root / "bronze" / "dart_filings" / "dt=*" / "part.parquet"
    con = duckdb.connect()
    rows = con.sql(
        """
        select distinct
          cast(rcept_dt as date) as rcept_dt,
          cast(corp_code as varchar) as corp_code,
          cast(corp_name as varchar) as corp_name,
          cast(stock_code as varchar) as stock_code,
          cast(rcept_no as varchar) as rcept_no,
          cast(report_nm as varchar) as report_nm
        from read_parquet($pattern, hive_partitioning=true, union_by_name=true)
        where cast(rcept_dt as date) between $since and $until
          and stock_code is not null
          and report_nm like '%사업보고서%'
        order by rcept_dt, rcept_no
        """,
        params={"pattern": str(pattern), "since": since, "until": until},
    ).fetchall()
    return [
        Filing(
            rcept_dt=row[0],
            corp_code=row[1],
            corp_name=row[2],
            stock_code=row[3],
            rcept_no=row[4],
            report_nm=row[5],
        )
        for row in rows
    ]


def _fetch_filing_rows(filing: Filing) -> list[dict[str, object]]:
    main_html = _http_text(MAIN_URL, {"rcpNo": filing.rcept_no})
    sections = _parse_sections(main_html)
    rows: list[dict[str, object]] = []
    for section in sections:
        if section.text not in {"연결재무제표", "재무제표"}:
            continue
        html = _http_text(
            VIEWER_URL,
            {
                "rcpNo": section.rcp_no,
                "dcmNo": section.dcm_no,
                "eleId": section.ele_id,
                "offset": section.offset,
                "length": section.length,
                "dtd": section.dtd,
            },
        )
        rows.extend(_financial_rows_from_viewer(filing, section, html))
    return rows


def _parse_sections(html_text: str) -> list[Section]:
    node_re = re.compile(
        r"node\d+\['text'\]\s*=\s*\"(?P<text>[^\"]+)\";.*?"
        r"node\d+\['rcpNo'\]\s*=\s*\"(?P<rcp_no>\d+)\";.*?"
        r"node\d+\['dcmNo'\]\s*=\s*\"(?P<dcm_no>\d+)\";.*?"
        r"node\d+\['eleId'\]\s*=\s*\"(?P<ele_id>\d+)\";.*?"
        r"node\d+\['offset'\]\s*=\s*\"(?P<offset>\d+)\";.*?"
        r"node\d+\['length'\]\s*=\s*\"(?P<length>\d+)\";.*?"
        r"node\d+\['dtd'\]\s*=\s*\"(?P<dtd>[^\"]+)\";",
        re.DOTALL,
    )
    sections = []
    for match in node_re.finditer(html_text):
        text = re.sub(r"^\d+\.\s*", "", match.group("text")).strip()
        sections.append(
            Section(
                text=text,
                rcp_no=match.group("rcp_no"),
                dcm_no=match.group("dcm_no"),
                ele_id=match.group("ele_id"),
                offset=match.group("offset"),
                length=match.group("length"),
                dtd=match.group("dtd"),
            )
        )
    return sections


def _financial_rows_from_viewer(
    filing: Filing,
    section: Section,
    html_text: str,
) -> list[dict[str, object]]:
    parser = TableParser()
    parser.feed(html_text)
    fiscal_year = _fiscal_year(filing.report_nm, filing.rcept_dt)
    fiscal_period_end = date(fiscal_year, 12, 31)
    unit = 1.0
    rows: list[dict[str, object]] = []
    for index, table in enumerate(parser.tables):
        if not table:
            continue
        title = " ".join(" ".join(row) for row in table[:5])
        if "단위 : 백만원" in title or "단위: 백만원" in title:
            unit = 1_000_000.0
        if index == 0 or len(table[0]) < 2:
            continue
        if table[0][0] != "" and not table[0][0].startswith("과목"):
            continue
        for row in table[1:]:
            if len(row) < 2:
                continue
            account_name = row[0].strip()
            amount = _parse_amount(row[1], unit)
            if not account_name or amount is None:
                continue
            rows.append(
                {
                    "security_id": None,
                    "corp_code": filing.corp_code,
                    "fiscal_period_end": fiscal_period_end,
                    "event_date": fiscal_period_end,
                    "rcept_dt": filing.rcept_dt,
                    "available_date": filing.rcept_dt,
                    "report_type": REPORT_CODE,
                    "account_id": account_name,
                    "account_name": account_name,
                    "amount": amount,
                    "is_consolidated": section.text == "연결재무제표",
                    "accounting_basis": table[0][1] if len(table[0]) > 1 else None,
                    "_ingested_at": datetime.now(UTC),
                    "_source": "dart_web_viewer",
                    "_source_request_hash": filing.rcept_no,
                }
            )
    return rows


def _parse_amount(value: str, unit: float) -> float | None:
    text = value.replace(",", "").replace("△", "-").strip()
    if not text or text in {"-", "－"}:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text) * unit
    except ValueError:
        return None


def _fiscal_year(report_nm: str, rcept_dt: date) -> int:
    match = re.search(r"\((\d{4})\.(\d{2})\)", report_nm)
    if match:
        return int(match.group(1))
    return rcept_dt.year - 1


def _merge_existing(path: Path, new_frame: pl.DataFrame) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = new_frame
    before_rows = 0
    if path.exists():
        existing = pl.read_parquet(path)
        before_rows = existing.height
        frame = pl.concat([existing, new_frame], how="diagonal_relaxed")
    dedup = [
        "corp_code",
        "fiscal_period_end",
        "rcept_dt",
        "report_type",
        "account_name",
        "is_consolidated",
    ]
    frame = frame.unique(subset=dedup, keep="last")
    frame.write_parquet(path, compression="zstd")
    return max(0, frame.height - before_rows)


def _marker_path(data_root: Path, rcept_no: str) -> Path:
    return data_root / "_cache" / "dart_web_financials" / f"rcept_no={rcept_no}.done"


def _http_text(url: str, params: dict[str, object]) -> str:
    full_url = f"{url}?{urlencode(params)}"
    request = Request(full_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


if __name__ == "__main__":
    main()
