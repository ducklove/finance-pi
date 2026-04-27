from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """Filesystem locations used by a local finance-pi workspace."""

    root: Path = Path(".")
    data_dir: Path | None = None
    report_dir: Path | None = None

    @property
    def data_root(self) -> Path:
        return self.data_dir or self.root / "data"

    @property
    def reports_root(self) -> Path:
        return self.report_dir or self.data_root / "reports"

    @property
    def catalog_path(self) -> Path:
        return self.data_root / "catalog" / "finance_pi.duckdb"

    @classmethod
    def from_cwd(cls) -> ProjectPaths:
        return cls(root=Path.cwd())
