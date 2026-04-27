from __future__ import annotations

from datetime import date

from finance_pi.config import ProjectPaths
from finance_pi.reports.data_quality import empty_data_quality_report


def main() -> None:
    paths = ProjectPaths.from_cwd()
    report_date = date.today()
    output = paths.data_root / "reports" / "data_quality" / f"{report_date.isoformat()}.html"
    empty_data_quality_report(report_date).write(output)
    print(output)


if __name__ == "__main__":
    main()
