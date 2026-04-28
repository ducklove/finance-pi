from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path

import polars as pl

from finance_pi.reports.data_quality import ReportCheck


@dataclass(frozen=True)
class FraudReport:
    report_date: date
    checks: tuple[ReportCheck, ...]

    def to_html(self) -> str:
        banner = ""
        if any(check.status == "FAIL" for check in self.checks):
            banner = "<div class='banner'>FAIL: backtest result should not be trusted.</div>"
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
  <title>finance-pi fraud report {self.report_date.isoformat()}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #1f2937; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #d1d5db; padding: 8px 10px; text-align: left; }}
    .banner {{ background: #fee2e2; color: #991b1b; padding: 12px; font-weight: 700; }}
    .pass {{ color: #047857; font-weight: 700; }}
    .warn {{ color: #b45309; font-weight: 700; }}
    .fail {{ color: #b91c1c; font-weight: 700; }}
  </style>
</head>
<body>
  {banner}
  <h1>Backtest Fraud Checks</h1>
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


def empty_fraud_report(report_date: date) -> FraudReport:
    return FraudReport(
        report_date=report_date,
        checks=(
            ReportCheck(
                "signal_entry_separation",
                "PASS",
                "Default engine separates signal and entry dates.",
            ),
        ),
    )


def build_fraud_report(data_root: Path, report_date: date) -> FraudReport:
    checks: list[ReportCheck] = []
    universe = _read_optional(data_root / "gold/universe_history/dt=*/part.parquet")
    prices = _read_optional(data_root / "gold/daily_prices_adj/dt=*/part.parquet")

    if universe is None or universe.is_empty():
        checks.append(ReportCheck("universe", "WARN", "No universe_history rows yet."))
        return FraudReport(report_date, tuple(checks))

    day_universe = universe.filter(pl.col("date") == report_date)
    size = day_universe.height
    checks.append(
        ReportCheck(
            "tiny_universe",
            "PASS" if size >= 30 or size == 0 else "WARN",
            f"{size} securities in universe for {report_date.isoformat()}",
        )
    )

    spac_pre = day_universe.filter(pl.col("is_spac_pre").fill_null(False)).height
    checks.append(
        ReportCheck(
            "spac_pre_merger",
            "PASS" if spac_pre == 0 else "WARN",
            f"{spac_pre} pre-merger SPAC rows are present in universe",
        )
    )

    if prices is not None and not prices.is_empty():
        penny = prices.filter((pl.col("date") == report_date) & (pl.col("close_adj") < 1000)).height
        checks.append(
            ReportCheck(
                "penny_stocks",
                "PASS" if penny == 0 else "WARN",
                f"{penny} securities closed below 1,000 KRW",
            )
        )

    return FraudReport(report_date, tuple(checks))


def _read_optional(pattern: Path) -> pl.DataFrame | None:
    from glob import glob

    files = sorted(glob(pattern.as_posix()))
    if not files:
        return None
    return pl.read_parquet(files, hive_partitioning=True)
