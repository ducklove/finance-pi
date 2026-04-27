from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from pathlib import Path

import duckdb

from finance_pi.storage.datasets import DatasetSpec, dataset_registry


@dataclass(frozen=True)
class CatalogBuilder:
    """Build a DuckDB catalog containing views over Parquet datasets."""

    data_root: Path
    catalog_path: Path
    registry: dict[str, DatasetSpec] | None = None

    def build(self) -> list[str]:
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        specs = self.registry or dataset_registry
        created: list[str] = []
        with duckdb.connect(str(self.catalog_path)) as conn:
            for schema in sorted({spec.schema for spec in specs.values()}):
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for spec in specs.values():
                self._create_view(conn, spec)
                created.append(spec.name)
        return created

    def _create_view(self, conn: duckdb.DuckDBPyConnection, spec: DatasetSpec) -> None:
        glob_path = spec.glob_path(self.data_root)
        sql_path = _duckdb_path(glob_path)
        if _glob_has_files(glob_path):
            hive = "true" if spec.hive_partitioning else "false"
            select_sql = (
                f"SELECT * FROM read_parquet('{sql_path}', hive_partitioning = {hive}, "
                "union_by_name = true)"
            )
        else:
            select_sql = spec.empty_select_sql
        conn.execute(f"CREATE OR REPLACE VIEW {spec.name} AS {select_sql}")


def _duckdb_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def _glob_has_files(glob_path: Path) -> bool:
    return any(Path(match).is_file() for match in glob(glob_path.as_posix()))
