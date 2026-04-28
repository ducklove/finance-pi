from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
from typing import Literal

import polars as pl

Status = Literal["PASS", "WARN", "FAIL"]


@dataclass(frozen=True)
class ReportCheck:
    name: str
    status: Status
    message: str


@dataclass(frozen=True)
class DataQualityReport:
    report_date: date
    checks: tuple[ReportCheck, ...]

    def has_failures(self) -> bool:
        return any(check.status == "FAIL" for check in self.checks)

    def to_html(self) -> str:
        rows = "\n".join(
            "<tr>"
            f"<td>{escape(check.name)}</td>"
            f"<td class='{check.status.lower()}'>{check.status}</td>"
            f"<td>{escape(check.message)}</td>"
            "</tr>"
            for check in self.checks
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>finance-pi data quality {self.report_date.isoformat()}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #1f2937; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #d1d5db; padding: 8px 10px; text-align: left; }}
    .pass {{ color: #047857; font-weight: 700; }}
    .warn {{ color: #b45309; font-weight: 700; }}
    .fail {{ color: #b91c1c; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Data Quality</h1>
  <p>Report date: {self.report_date.isoformat()}</p>
  <table>
    <thead><tr><th>Check</th><th>Status</th><th>Message</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
"""

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_html(), encoding="utf-8")
        return path


def empty_data_quality_report(report_date: date) -> DataQualityReport:
    return DataQualityReport(
        report_date=report_date,
        checks=(
            ReportCheck(
                "catalog",
                "PASS",
                "Report scaffold generated; attach dataset checks next.",
            ),
        ),
    )


def build_data_quality_report(data_root: Path, report_date: date) -> DataQualityReport:
    checks: list[ReportCheck] = []
    catalog = data_root / "catalog" / "finance_pi.duckdb"
    checks.append(
        ReportCheck(
            "catalog",
            "PASS" if catalog.exists() else "WARN",
            f"{catalog} {'exists' if catalog.exists() else 'has not been built'}",
        )
    )

    prices = _read_optional(data_root / "silver/prices/dt=*/part.parquet")
    if prices is None or prices.is_empty():
        checks.append(ReportCheck("silver_prices", "WARN", "No silver price rows yet."))
        return DataQualityReport(report_date, tuple(checks))

    day_prices = prices.filter(pl.col("date") == report_date)
    checks.append(
        ReportCheck(
            "price_rows",
            "PASS" if day_prices.height > 0 else "WARN",
            f"{day_prices.height} silver price rows for {report_date.isoformat()}",
        )
    )

    jumps = (
        prices.sort(["security_id", "date"])
        .with_columns(pl.col("close").pct_change().over("security_id").alias("return_1d"))
        .filter((pl.col("date") == report_date) & (pl.col("return_1d").abs() > 0.30))
    )
    checks.append(
        ReportCheck(
            "large_price_jumps",
            "PASS" if jumps.is_empty() else "WARN",
            f"{jumps.height} rows with absolute close-to-close return above 30%",
        )
    )

    master = _read_optional(data_root / "gold/security_master.parquet")
    if master is not None and not master.is_empty():
        unknown = master.filter(pl.col("corp_code").is_null())
        checks.append(
            ReportCheck(
                "unknown_identity",
                "PASS" if unknown.is_empty() else "WARN",
                f"{unknown.height} securities do not have DART corp_code mapping",
            )
        )

    krx_kis = _krx_kis_mismatch(prices, report_date)
    if krx_kis is not None:
        checks.append(
            ReportCheck(
                "krx_kis_close_mismatch",
                "PASS" if krx_kis == 0 else "WARN",
                f"{krx_kis} KRX/KIS close mismatches above 0.1%",
            )
        )

    return DataQualityReport(report_date, tuple(checks))


def _krx_kis_mismatch(prices: pl.DataFrame, report_date: date) -> int | None:
    day = prices.filter(
        (pl.col("date") == report_date) & pl.col("price_source").is_in(["krx", "kis"])
    )
    if day.filter(pl.col("price_source") == "kis").is_empty():
        return None
    pivot = day.pivot(index="ticker", on="price_source", values="close", aggregate_function="first")
    if "krx" not in pivot.columns or "kis" not in pivot.columns:
        return None
    return pivot.filter(((pl.col("krx") - pl.col("kis")).abs() / pl.col("krx")) > 0.001).height


def _read_optional(pattern: Path) -> pl.DataFrame | None:
    from glob import glob

    file_names = sorted(glob(pattern.as_posix()))
    files = [Path(name) for name in file_names]
    if not files:
        return None
    return pl.read_parquet([file.as_posix() for file in files], hive_partitioning=True)
