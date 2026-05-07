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
            schemas = {spec.schema for spec in specs.values()} | {"analytics", "metadata"}
            for schema in sorted(schemas):
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for spec in specs.values():
                self._create_view(conn, spec)
                created.append(spec.name)
            self._create_metadata(conn, specs)
            self._create_analytics_views(conn)
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

    def _create_metadata(
        self,
        conn: duckdb.DuckDBPyConnection,
        specs: dict[str, DatasetSpec],
    ) -> None:
        rows = []
        for spec in sorted(specs.values(), key=lambda item: item.name):
            glob_path = spec.glob_path(self.data_root)
            rows.append(
                "SELECT "
                f"'{_sql_string(spec.name)}' AS dataset, "
                f"'{_sql_string(spec.layer)}' AS layer, "
                f"'{_sql_string(spec.relative_glob)}' AS relative_glob, "
                f"{str(spec.hive_partitioning).lower()} AS hive_partitioning, "
                f"{str(_glob_has_files(glob_path)).lower()} AS has_files"
            )
        select_sql = "\nUNION ALL\n".join(rows) if rows else "SELECT NULL WHERE FALSE"
        conn.execute(f"CREATE OR REPLACE TABLE metadata.datasets AS {select_sql}")

    def _create_analytics_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE OR REPLACE VIEW analytics.daily_prices AS
            SELECT
                p.date,
                p.security_id,
                p.listing_id,
                sm.ticker,
                sm.name,
                sm.market,
                sm.share_class,
                sm.security_type,
                p.open_adj,
                p.high_adj,
                p.low_adj,
                p.close_adj,
                p.return_1d,
                p.volume,
                p.trading_value,
                p.market_cap,
                p.listed_shares,
                p.is_halted,
                p.is_designated,
                p.is_liquidation_window
            FROM gold.daily_prices_adj AS p
            LEFT JOIN gold.security_master AS sm
                ON p.security_id = sm.security_id
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analytics.daily_market_caps AS
            SELECT
                date,
                security_id,
                listing_id,
                ticker,
                name,
                market,
                rank,
                close,
                trading_value,
                volume,
                market_cap,
                listed_shares,
                market_cap_source,
                is_estimated
            FROM gold.daily_market_caps
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analytics.universe AS
            SELECT
                u.date,
                u.security_id,
                u.listing_id,
                sm.ticker,
                sm.name,
                u.market,
                u.is_active,
                u.share_class,
                u.security_type,
                u.is_spac_pre,
                u.is_halted,
                u.is_designated,
                u.is_liquidation_window
            FROM gold.universe_history AS u
            LEFT JOIN gold.security_master AS sm
                ON u.security_id = sm.security_id
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analytics.securities AS
            SELECT * FROM gold.security_master
            """
        )


def _duckdb_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _glob_has_files(glob_path: Path) -> bool:
    return any(Path(match).is_file() for match in glob(glob_path.as_posix()))
