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
        CAST(NULL AS VARCHAR) AS price_basis,
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
        CAST(NULL AS VARCHAR) AS accounting_basis,
        CAST(NULL AS BOOLEAN) AS is_backfilled
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


def _empty_nps_holdings_raw() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS stock_code,
        CAST(NULL AS VARCHAR) AS stock_name,
        CAST(NULL AS BIGINT) AS shares,
        CAST(NULL AS DOUBLE) AS ownership_pct,
        CAST(NULL AS DOUBLE) AS price,
        CAST(NULL AS DOUBLE) AS market_value,
        CAST(NULL AS DOUBLE) AS change_pct,
        CAST(NULL AS BIGINT) AS rank,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS DATE) AS source_date,
        CAST(NULL AS DOUBLE) AS source_market_value,
        CAST(NULL AS DOUBLE) AS source_weight_pct,
        CAST(NULL AS VARCHAR) AS shares_source,
        CAST(NULL AS DATE) AS price_date,
        CAST(NULL AS BOOLEAN) AS is_exact_price,
        CAST(NULL AS TIMESTAMP) AS _ingested_at,
        CAST(NULL AS VARCHAR) AS _source,
        CAST(NULL AS VARCHAR) AS _source_request_hash
    WHERE FALSE
    """


def _empty_nps_holdings_silver() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS stock_code,
        CAST(NULL AS VARCHAR) AS stock_name,
        CAST(NULL AS BIGINT) AS shares,
        CAST(NULL AS DOUBLE) AS ownership_pct,
        CAST(NULL AS DOUBLE) AS price,
        CAST(NULL AS DOUBLE) AS market_value,
        CAST(NULL AS DOUBLE) AS change_pct,
        CAST(NULL AS BIGINT) AS source_rank,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS DATE) AS source_date,
        CAST(NULL AS DOUBLE) AS source_market_value,
        CAST(NULL AS DOUBLE) AS source_weight_pct,
        CAST(NULL AS VARCHAR) AS shares_source,
        CAST(NULL AS DATE) AS price_date,
        CAST(NULL AS BOOLEAN) AS is_exact_price
    WHERE FALSE
    """


def _empty_nps_universe() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS DATE) AS as_of,
        CAST(NULL AS BIGINT) AS rank,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS stock_code,
        CAST(NULL AS VARCHAR) AS stock_name,
        CAST(NULL AS BIGINT) AS shares,
        CAST(NULL AS DOUBLE) AS ownership_pct,
        CAST(NULL AS DOUBLE) AS price,
        CAST(NULL AS DOUBLE) AS market_value,
        CAST(NULL AS DOUBLE) AS change_pct,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS DATE) AS source_date
    WHERE FALSE
    """


def _empty_marcap_raw() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS Date,
        CAST(NULL AS BIGINT) AS Rank,
        CAST(NULL AS VARCHAR) AS Code,
        CAST(NULL AS VARCHAR) AS Name,
        CAST(NULL AS BIGINT) AS Open,
        CAST(NULL AS BIGINT) AS High,
        CAST(NULL AS BIGINT) AS Low,
        CAST(NULL AS BIGINT) AS Close,
        CAST(NULL AS BIGINT) AS Volume,
        CAST(NULL AS BIGINT) AS Amount,
        CAST(NULL AS BIGINT) AS Changes,
        CAST(NULL AS VARCHAR) AS ChangeCode,
        CAST(NULL AS DOUBLE) AS ChagesRatio,
        CAST(NULL AS BIGINT) AS Marcap,
        CAST(NULL AS BIGINT) AS Stocks,
        CAST(NULL AS VARCHAR) AS MarketId,
        CAST(NULL AS VARCHAR) AS Market,
        CAST(NULL AS VARCHAR) AS Dept
    WHERE FALSE
    """


def _empty_market_caps() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS listing_id,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS market,
        CAST(NULL AS BIGINT) AS rank,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS BIGINT) AS trading_value,
        CAST(NULL AS BIGINT) AS volume,
        CAST(NULL AS BIGINT) AS market_cap,
        CAST(NULL AS BIGINT) AS listed_shares,
        CAST(NULL AS VARCHAR) AS market_cap_source,
        CAST(NULL AS BOOLEAN) AS is_estimated
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


def _empty_cpi() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS country,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS frequency,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS index_base,
        CAST(NULL AS DOUBLE) AS yoy_pct,
        CAST(NULL AS DOUBLE) AS mom_pct,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
    WHERE FALSE
    """


def _empty_rates() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS country,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS frequency,
        CAST(NULL AS VARCHAR) AS tenor,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS unit,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
    WHERE FALSE
    """


def _empty_indices() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS country,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS frequency,
        CAST(NULL AS VARCHAR) AS category,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS currency,
        CAST(NULL AS DOUBLE) AS return_1d,
        CAST(NULL AS DOUBLE) AS return_1m,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
    WHERE FALSE
    """


def _empty_commodities() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS commodity,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS unit,
        CAST(NULL AS VARCHAR) AS currency,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
    WHERE FALSE
    """


def _empty_fx() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS base_currency,
        CAST(NULL AS VARCHAR) AS quote_currency,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
    WHERE FALSE
    """


def _empty_dividends() -> str:
    return """
    SELECT
        CAST(NULL AS INTEGER) AS fiscal_year,
        CAST(NULL AS DATE) AS fiscal_period_end,
        CAST(NULL AS DATE) AS rcept_dt,
        CAST(NULL AS DATE) AS available_date,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS VARCHAR) AS corp_name,
        CAST(NULL AS VARCHAR) AS security_id,
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS VARCHAR) AS share_class,
        CAST(NULL AS VARCHAR) AS stock_kind,
        CAST(NULL AS DOUBLE) AS cash_dividend_per_share,
        CAST(NULL AS DOUBLE) AS stock_dividend_per_share,
        CAST(NULL AS DOUBLE) AS cash_dividend_yield_pct,
        CAST(NULL AS VARCHAR) AS currency,
        CAST(NULL AS VARCHAR) AS source_rcept_no,
        CAST(NULL AS VARCHAR) AS report_type,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS BOOLEAN) AS is_estimated
    WHERE FALSE
    """


def _empty_share_counts() -> str:
    return """
    SELECT
        CAST(NULL AS INTEGER) AS fiscal_year,
        CAST(NULL AS DATE) AS fiscal_period_end,
        CAST(NULL AS DATE) AS rcept_dt,
        CAST(NULL AS DATE) AS available_date,
        CAST(NULL AS VARCHAR) AS corp_code,
        CAST(NULL AS VARCHAR) AS corp_name,
        CAST(NULL AS VARCHAR) AS share_class,
        CAST(NULL AS VARCHAR) AS stock_kind,
        CAST(NULL AS DOUBLE) AS authorized_shares,
        CAST(NULL AS DOUBLE) AS cumulative_issued_shares,
        CAST(NULL AS DOUBLE) AS cumulative_decreased_shares,
        CAST(NULL AS DOUBLE) AS capital_reduction_shares,
        CAST(NULL AS DOUBLE) AS profit_retirement_shares,
        CAST(NULL AS DOUBLE) AS redemption_shares,
        CAST(NULL AS DOUBLE) AS other_decrease_shares,
        CAST(NULL AS DOUBLE) AS issued_shares,
        CAST(NULL AS DOUBLE) AS treasury_shares,
        CAST(NULL AS DOUBLE) AS outstanding_shares,
        CAST(NULL AS VARCHAR) AS source_rcept_no,
        CAST(NULL AS VARCHAR) AS report_type,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS BOOLEAN) AS is_estimated
    WHERE FALSE
    """


def _empty_identity_review() -> str:
    return """
    SELECT
        CAST(NULL AS VARCHAR) AS ticker,
        CAST(NULL AS DATE) AS gap_start,
        CAST(NULL AS DATE) AS gap_end,
        CAST(NULL AS VARCHAR) AS name_before,
        CAST(NULL AS VARCHAR) AS name_after,
        CAST(NULL AS BOOLEAN) AS name_changed
    WHERE FALSE
    """


def _empty_economic_indicators() -> str:
    return """
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS country,
        CAST(NULL AS VARCHAR) AS series_id,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS category,
        CAST(NULL AS VARCHAR) AS frequency,
        CAST(NULL AS DOUBLE) AS value,
        CAST(NULL AS VARCHAR) AS unit,
        CAST(NULL AS VARCHAR) AS source,
        CAST(NULL AS TIMESTAMP) AS updated_at
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
    "bronze.nps_holdings_raw": DatasetSpec(
        "bronze.nps_holdings_raw",
        "bronze",
        "bronze/nps_holdings/dt=*/part.parquet",
        _empty_nps_holdings_raw(),
    ),
    "bronze.marcap_raw": DatasetSpec(
        "bronze.marcap_raw",
        "bronze",
        "bronze/marcap/year=*/part.parquet",
        _empty_marcap_raw(),
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
    "silver.market_caps": DatasetSpec(
        "silver.market_caps",
        "silver",
        "silver/market_caps/dt=*/part.parquet",
        _empty_market_caps(),
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
            CAST(NULL AS VARCHAR) AS source_rcept_no,
            CAST(NULL AS VARCHAR) AS source,
            CAST(NULL AS VARCHAR) AS confidence
        WHERE FALSE
        """,
    ),
    "silver.dividends": DatasetSpec(
        "silver.dividends",
        "silver",
        "silver/dividends/fiscal_year=*/part.parquet",
        _empty_dividends(),
    ),
    "silver.share_counts": DatasetSpec(
        "silver.share_counts",
        "silver",
        "silver/share_counts/fiscal_year=*/part.parquet",
        _empty_share_counts(),
    ),
    "silver.nps_holdings": DatasetSpec(
        "silver.nps_holdings",
        "silver",
        "silver/nps_holdings/dt=*/part.parquet",
        _empty_nps_holdings_silver(),
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
    "gold.daily_market_caps": DatasetSpec(
        "gold.daily_market_caps",
        "gold",
        "gold/daily_market_caps/dt=*/part.parquet",
        _empty_market_caps(),
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
    "gold.nps_universe": DatasetSpec(
        "gold.nps_universe",
        "gold",
        "gold/nps_universe/dt=*/part.parquet",
        _empty_nps_universe(),
    ),
    "gold.security_master": DatasetSpec(
        "gold.security_master",
        "gold",
        "gold/security_master.parquet",
        _empty_security_master(),
        hive_partitioning=False,
    ),
    "gold.identity_review": DatasetSpec(
        "gold.identity_review",
        "gold",
        "gold/identity_review.parquet",
        _empty_identity_review(),
        hive_partitioning=False,
    ),
    "macro.cpi": DatasetSpec(
        "macro.cpi",
        "macro",
        "macro/cpi/part.parquet",
        _empty_cpi(),
        hive_partitioning=False,
    ),
    "macro.rates": DatasetSpec(
        "macro.rates",
        "macro",
        "macro/rates/part.parquet",
        _empty_rates(),
        hive_partitioning=False,
    ),
    "macro.indices": DatasetSpec(
        "macro.indices",
        "macro",
        "macro/indices/part.parquet",
        _empty_indices(),
        hive_partitioning=False,
    ),
    "macro.commodities": DatasetSpec(
        "macro.commodities",
        "macro",
        "macro/commodities/part.parquet",
        _empty_commodities(),
        hive_partitioning=False,
    ),
    "macro.fx": DatasetSpec(
        "macro.fx",
        "macro",
        "macro/fx/part.parquet",
        _empty_fx(),
        hive_partitioning=False,
    ),
    "macro.economic_indicators": DatasetSpec(
        "macro.economic_indicators",
        "macro",
        "macro/economic_indicators/part.parquet",
        _empty_economic_indicators(),
        hive_partitioning=False,
    ),
}
