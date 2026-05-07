from __future__ import annotations

from datetime import date

import duckdb
import polars as pl
import pytest

from finance_pi.storage import CatalogBuilder, DataLakeLayout, ParquetDatasetWriter


def test_parquet_writer_is_append_only_by_default(tmp_path) -> None:
    path = tmp_path / "part.parquet"
    writer = ParquetDatasetWriter()
    writer.write(pl.DataFrame({"ticker": ["005930"]}), path)

    with pytest.raises(FileExistsError):
        writer.write(pl.DataFrame({"ticker": ["000660"]}), path)


def test_catalog_builds_empty_views(tmp_path) -> None:
    data_root = tmp_path / "data"
    catalog_path = data_root / "catalog" / "finance_pi.duckdb"
    DataLakeLayout(data_root).ensure_base_dirs()
    created = CatalogBuilder(data_root, catalog_path).build()

    assert "gold.daily_prices_adj" in created
    with duckdb.connect(str(catalog_path)) as conn:
        result = conn.execute("SELECT count(*) FROM gold.daily_prices_adj").fetchone()
        analytics_result = conn.execute("SELECT count(*) FROM analytics.daily_prices").fetchone()
        datasets = conn.execute(
            "SELECT dataset FROM metadata.datasets WHERE dataset = 'gold.daily_market_caps'"
        ).fetchone()
        macro_result = conn.execute("SELECT count(*) FROM macro.cpi").fetchone()
    assert result == (0,)
    assert analytics_result == (0,)
    assert datasets == ("gold.daily_market_caps",)
    assert macro_result == (0,)


def test_layout_partition_paths(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    path = layout.partition_path("bronze.krx_daily_raw", date(2024, 1, 2))
    assert path.as_posix().endswith("bronze/krx_daily/dt=2024-01-02/part.parquet")
    naver = layout.partition_path("bronze.naver_summary_raw", date(2024, 1, 2))
    assert naver.as_posix().endswith("bronze/naver_summary/dt=2024-01-02/part.parquet")
    assert layout.singleton_path("macro.cpi").as_posix().endswith("macro/cpi/part.parquet")
    assert layout.singleton_path("macro.rates").as_posix().endswith("macro/rates/part.parquet")
    assert layout.singleton_path("macro.indices").as_posix().endswith("macro/indices/part.parquet")
    assert layout.singleton_path("macro.commodities").as_posix().endswith(
        "macro/commodities/part.parquet"
    )
    assert layout.singleton_path("macro.fx").as_posix().endswith("macro/fx/part.parquet")
    assert layout.singleton_path("macro.economic_indicators").as_posix().endswith(
        "macro/economic_indicators/part.parquet"
    )
