from __future__ import annotations

from datetime import date

from finance_pi.cli.app import build_data_quality_report
from finance_pi.config import ProjectPaths


def main() -> None:
    paths = ProjectPaths.from_cwd()
    report_date = date.today()
    output = paths.data_root / "reports" / "data_quality" / f"{report_date.isoformat()}.html"
    build_data_quality_report(paths.data_root, report_date).write(output)
    print(output)


if __name__ == "__main__":
    main()
