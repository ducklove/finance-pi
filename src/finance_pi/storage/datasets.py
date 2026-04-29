from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    layer: str
    relative_glob: str
    empty_select_sql: str
    hive_partitioning: bool = True

    @property
    def schema(self) -> str:
        return self.name.split(".", maxsplit=1)[0]

    @property
    def table(self) -> str:
        return self.name.split(".", maxsplit=1)[1]

    def glob_path(self, data_root: Path) -> Path:
        return data_root / self.relative_glob


def _empty_price_raw() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS isin,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS BIGINT) AS volume,
        CAST(NULL AS BIGINT) AS trading_value,
        CAST(NULL AS BIGINT) AS market_cap,
        CAST(NULL AS BIGINT) AS listed_shares,
        CAST(NULL AS TIMESTAMP) AS _ingested_at,
        CAST(NULL AS VARCHAR) AS _source,
        CAST(NULL AS VARCHAR) AS _source_request_hash
    WHERE FALSE
    """


def _empty_prices_silver() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS BIGINT) AS volume,
        CAST(NULL AS BIGINT) AS trading_value,
        CAST(NULL AS BIGINT) AS market_cap,
        CAST(NULL AS BIGINT) AS listed_shares,
        CAST(NULL AS VARCHAR) AS price_source,
        CAST(NULL AS BOOLEAN) AS is_halted,
        CAST(NULL AS BOOLEAN) AS is_designated,
        CAST(NULL AS BOOLEAN) AS is_liquidation_window
    WHERE FALSE
    """


def _empty_daily_prices_adj() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS DOUBLE) AS open_adj,
        CAST(NULL AS DOUBLE) AS high_adj,
        CAST(NULL AS DOUBLE) AS low_adj,
        CAST(NULL AS DOUBLE) AS close_adj,
        CAST(NULL AS DOUBLE) AS return_1d,
        CAST(NULL AS BIGINT) AS volume,
        CAST(NULL AS BIGINT) AS trading_value,
        CAST(NULL AS BIGINT) AS market_cap,
        CAST(NULL AS BIGINT) AS listed_shares,
        CAST(NULL AS BOOLEAN) AS is_halted,
        CAST(NULL AS BOOLEAN) AS is_designated,
        CAST(NULL AS BOOLEAN) AS is_liquidation_window
    WHERE FALSE
    """


def _empty_filings() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS rcept_dt,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS VARCHAR) AS corp_name,
        CAST(NULL AS VARCHAR) AS stock_code,
        CAST(NULL AS VARCHAR) AS rcept_no,
        CAST(NULL AS VARCHAR) AS report_nm,
        CAST(NULL AS VARCHAR) AS rm,
        CAST(NULL AS TIMESTAMP) AS _ingested_at,
        CAST(NULL AS VARCHAR) AS _source,
        CAST(NULL AS VARCHAR) AS _source_request_hash
    WHERE FALSE
    """


def _empty_financials() -> str:
    return """
    SELECT
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS DATE) AS fiscal_period_end,
        CAST(NULL AS DATE) AS event_date,
        CAST(NULL AS DATE) AS rcept_dt,
        CAST(NULL AS DATE) AS available_date,
        CAST(NULL AS VARCHAR) AS report_type,
        CAST(NULL AS VARCHAR) AS account_id,
        CAST(NULL AS VARCHAR) AS account_name,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS BOOLEAN) AS is_consolidated,
        CAST(NULL AS VARCHAR) AS accounting_basis
    WHERE FALSE
    """


def _empty_company() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS snapshot_dt,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS VARCHAR) AS corp_name,
        CAST(NULL AS VARCHAR) AS stock_code,
        CAST(NULL AS VARCHAR) AS modify_date,
        CAST(NULL AS TIMESTAMP) AS _ingested_at,
        CAST(NULL AS VARCHAR) AS _source,
        CAST(NULL AS VARCHAR) AS _source_request_hash
    WHERE FALSE
    """


def _empty_naver_summary() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS snapshot_dt,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS BIGINT) AS close,
        CAST(NULL AS BIGINT) AS change_abs,
        CAST(NULL AS DOUBLE) AS change_rate_pct,
        CAST(NULL AS BIGINT) AS par_value,
        CAST(NULL AS BIGINT) AS market_cap,
        CAST(NULL AS BIGINT) AS listed_shares,
        CAST(NULL AS DOUBLE) AS foreign_ownership_pct,
        CAST(NULL AS BIGINT) AS volume,
        CAST(NULL AS DOUBLE) AS per,
        CAST(NULL AS DOUBLE) AS roe,
        CAST(NULL AS TIMESTAMP) AS _ingested_at,
        CAST(NULL AS VARCHAR) AS _source,
        CAST(NULL AS VARCHAR) AS _source_request_hash
    WHERE FALSE
    """


def _empty_security_master() -> str:
    return """
    SELECT
        CAST(NULL AS VARCHAR) AS issuer_id,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS VARCHAR) AS isin,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS VARCHAR) AS share_class,
        CAST(NULL AS VARCHAR) AS security_type,
        CAST(NULL AS DATE) AS listed_date,
        CAST(NULL AS DATE) AS delisted_date,
        CAST(NULL AS VARCHAR) AS delisting_reason
    WHERE FALSE
    """


def _empty_universe_history() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS BOOLEAN) AS is_active,
        CAST(NULL AS VARCHAR) AS share_class,
        CAST(NULL AS VARCHAR) AS security_type,
        CAST(NULL AS BOOLEAN) AS is_spac_pre,
        CAST(NULL AS BOOLEAN) AS is_halted,
        CAST(NULL AS BOOLEAN) AS is_designated,
        CAST(NULL AS BOOLEAN) AS is_liquidation_window
    WHERE FALSE
    """


dataset_registry: dict[str, DatasetSpec] = {
    "bronze.krx_daily_raw": DatasetSpec(
        "bronze.krx_daily_raw",
        "bronze",
        "bronze/krx_daily/dt=*/part.parquet",
        _empty_price_raw(),
    ),
    "bronze.kis_daily_raw": DatasetSpec(
        "bronze.kis_daily_raw",
        "bronze",
        "bronze/kis_daily/dt=*/part.parquet",
        _empty_price_raw(),
    ),
    "bronze.naver_daily_raw": DatasetSpec(
        "bronze.naver_daily_raw",
        "bronze",
        "bronze/naver_daily/request_dt=*/chunk=*/part.parquet",
        _empty_price_raw(),
    ),
    "bronze.naver_summary_raw": DatasetSpec(
        "bronze.naver_summary_raw",
        "bronze",
        "bronze/naver_summary/dt=*/part.parquet",
        _empty_naver_summary(),
    ),
    "bronze.dart_filings_raw": DatasetSpec(
        "bronze.dart_filings_raw",
        "bronze",
        "bronze/dart_filings/dt=*/part.parquet",
        _empty_filings(),
    ),
    "bronze.dart_financials_raw": DatasetSpec(
        "bronze.dart_financials_raw",
        "bronze",
        "bronze/dart_financials/rcept_dt=*/part.parquet",
        _empty_financials(),
    ),
    "bronze.dart_company_raw": DatasetSpec(
        "bronze.dart_company_raw",
        "bronze",
        "bronze/dart_company/snapshot_dt=*/part.parquet",
        _empty_company(),
    ),
    "bronze.pre2010_raw": DatasetSpec(
        "bronze.pre2010_raw",
        "bronze",
        "bronze/pre2010/source=*/dt=*/part.parquet",
        _empty_price_raw(),
    ),
    "silver.prices": DatasetSpec(
        "silver.prices",
        "silver",
        "silver/prices/dt=*/part.parquet",
        _empty_prices_silver(),
    ),
    "silver.financials": DatasetSpec(
        "silver.financials",
        "silver",
        "silver/financials/fiscal_year=*/part.parquet",
        _empty_financials(),
    ),
    "silver.filings": DatasetSpec(
        "silver.filings",
        "silver",
        "silver/filings/dt=*/part.parquet",
        _empty_filings(),
    ),
    "silver.corporate_actions": DatasetSpec(
        "silver.corporate_actions",
        "silver",
        "silver/corporate_actions/dt=*/part.parquet",
        """
        SELECT
            CAST(NULL AS DATE) AS effective_date,
            CAST(NULL AS VARCHAR) AS security_id,
            CAST(NULL AS VARCHAR) AS action_type,
            CAST(NULL AS DOUBLE) AS adjustment_factor,
            CAST(NULL AS VARCHAR) AS source_rcept_no
        WHERE FALSE
        """,
    ),
    "silver.security_identity": DatasetSpec(
        "silver.security_identity",
        "silver",
        "silver/security_identity/part.parquet",
        _empty_security_master(),
        hive_partitioning=False,
    ),
    "gold.daily_prices_adj": DatasetSpec(
        "gold.daily_prices_adj",
        "gold",
        "gold/daily_prices_adj/dt=*/part.parquet",
        _empty_daily_prices_adj(),
    ),
    "gold.fundamentals_pit": DatasetSpec(
        "gold.fundamentals_pit",
        "gold",
        "gold/fundamentals_pit/dt=*/part.parquet",
        _empty_financials().replace(
            "rcept_dt,",
            "rcept_dt,\n        CAST(NULL AS DATE) AS as_of_date,",
        ),
    ),
    "gold.universe_history": DatasetSpec(
        "gold.universe_history",
        "gold",
        "gold/universe_history/dt=*/part.parquet",
        _empty_universe_history(),
    ),
    "gold.security_master": DatasetSpec(
        "gold.security_master",
        "gold",
        "gold/security_master.parquet",
        _empty_security_master(),
        hive_partitioning=False,
    ),
}
