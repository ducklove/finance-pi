from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape
from pathlib import Path
from typing import Literal

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
