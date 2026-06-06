from __future__ import annotations

import sqlite3
from datetime import date

import polars as pl

import finance_pi.sources.nps.client as nps_client_module
from finance_pi.admin.server import AdminState
from finance_pi.cli import app as cli_app
from finance_pi.sources.nps.adapter import NpsHoldingsAdapter
from finance_pi.sources.nps.client import NpsHoldingsClient
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter
from finance_pi.transforms import build_nps_holdings_silver, build_nps_universe


def test_nps_public_csv_parser_uses_official_value_fields(tmp_path, monkeypatch) -> None:
    payload = (
        "번호,종목명,평가액(억 원),자산군 내 비중(퍼센트),지분율(퍼센트)\n"
        "1,삼성전자,123.4,1.2,7.5\n"
    ).encode("utf-8-sig")
    monkeypatch.setattr(
        nps_client_module,
        "_download_url",
        lambda *_args, **_kwargs: payload,
    )

    rows = NpsHoldingsClient(tmp_path).fetch_public_holdings()

    assert rows == [
        {
            "rank": 1,
            "name": "삼성전자",
            "shares": 0,
            "ownership_pct": 7.5,
            "source": "data.go.kr",
            "source_date": date(2024, 12, 31),
            "source_market_value": 12_340_000_000,
            "source_weight_pct": 1.2,
        }
    ]


def test_nps_adapter_writes_bronze_partition(tmp_path) -> None:
    class FakeNpsClient:
        def fetch_holdings(self, snapshot_date: date):
            return [
                {
                    "date": snapshot_date,
                    "stock_code": "00680K",
                    "stock_name": "미래에셋증권2우B",
                    "shares": 10,
                    "ownership_pct": 1.5,
                    "price": 1000.0,
                    "market_value": 10_000.0,
                    "change_pct": 0.5,
                    "rank": 1,
                    "source": "fixture",
                    "source_date": snapshot_date,
                    "source_market_value": None,
                    "source_weight_pct": None,
                    "shares_source": "fixture",
                    "price_date": snapshot_date,
                    "is_exact_price": True,
                }
            ]

    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    adapter = NpsHoldingsAdapter(layout, ParquetDatasetWriter(), FakeNpsClient())

    unit = next(iter(adapter.list_pending(date(2025, 1, 2), date(2025, 1, 2))))
    result = adapter.write_bronze(adapter.fetch(unit))
    frame = pl.read_parquet(result.path)

    assert result.rows == 1
    assert frame["stock_code"].to_list() == ["00680K"]


def test_nps_builders_create_ranked_universe_with_stable_ids(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    target = date(2025, 1, 2)
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": target,
                    "stock_code": "00680K",
                    "stock_name": "미래에셋증권2우B",
                    "shares": 10,
                    "ownership_pct": 1.5,
                    "price": 1000.0,
                    "market_value": 10_000.0,
                    "change_pct": 0.5,
                    "rank": 2,
                    "source": "fixture",
                    "source_date": target,
                    "source_market_value": None,
                    "source_weight_pct": None,
                    "shares_source": "fixture",
                    "price_date": target,
                    "is_exact_price": True,
                },
                {
                    "date": target,
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "shares": 100,
                    "ownership_pct": 7.5,
                    "price": 70000.0,
                    "market_value": 7_000_000.0,
                    "change_pct": 1.0,
                    "rank": 1,
                    "source": "fixture",
                    "source_date": target,
                    "source_market_value": None,
                    "source_weight_pct": None,
                    "shares_source": "fixture",
                    "price_date": target,
                    "is_exact_price": True,
                },
            ]
        ),
        layout.partition_path("bronze.nps_holdings_raw", target),
    )
    (tmp_path / "gold").mkdir(exist_ok=True)
    pl.DataFrame(
        [
            {
                "ticker": "005930",
                "security_id": "S005930",
                "listing_id": "L005930",
                "name": "삼성전자",
            }
        ]
    ).write_parquet(tmp_path / "gold" / "security_master.parquet")

    assert build_nps_holdings_silver(tmp_path)[0].rows == 2
    assert build_nps_universe(tmp_path)[0].rows == 2

    silver = pl.read_parquet(tmp_path / "silver/nps_holdings/dt=2025-01-02/part.parquet")
    gold = pl.read_parquet(tmp_path / "gold/nps_universe/dt=2025-01-02/part.parquet")
    assert silver.filter(pl.col("stock_code") == "00680K").select("security_id").item() == "S00680K"
    assert gold.sort("rank")["stock_code"].to_list() == ["005930", "00680K"]


def test_admin_nps_universe_returns_latest_snapshot_on_or_before_date(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 12, 31),
                    "as_of": date(2024, 12, 31),
                    "rank": 1,
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "shares": 100,
                    "ownership_pct": 7.5,
                    "price": 70000.0,
                    "market_value": 7_000_000.0,
                    "change_pct": 1.0,
                    "source": "fixture",
                    "source_date": date(2024, 12, 31),
                }
            ]
        ),
        layout.partition_path("gold.nps_universe", date(2024, 12, 31)),
    )

    payload = AdminState(tmp_path).nps_universe({"date": ["2025-01-01"], "top": ["1"]})

    assert payload["date"] == "2025-01-01"
    assert payload["as_of"] == "2024-12-31"
    assert payload["universe"][0]["stock_code"] == "005930"


def test_nps_shadow_compare_passes_legacy_sqlite_rows(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    target = date(2025, 1, 2)
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                {
                    "date": target,
                    "as_of": target,
                    "rank": 1,
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "shares": 100,
                    "ownership_pct": 7.5,
                    "price": 70000.0,
                    "market_value": 7_000_000.0,
                    "change_pct": 1.0,
                    "source": "fixture",
                    "source_date": target,
                }
            ]
        ),
        layout.partition_path("gold.nps_universe", target),
    )
    sqlite_path = tmp_path / "cache.db"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(
            """
            CREATE TABLE nps_holdings (
                date TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                shares INTEGER NOT NULL,
                ownership_pct REAL NOT NULL DEFAULT 0,
                price REAL,
                market_value REAL,
                change_pct REAL,
                PRIMARY KEY (date, stock_code)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO nps_holdings
            (date, stock_code, stock_name, shares, ownership_pct, price, market_value, change_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (target.isoformat(), "005930", "삼성전자", 100, 7.5, 70000.0, 7_000_000.0, 1.0),
        )

    as_of, finance_rows = cli_app._nps_universe_shadow_rows(data_root, target, 100)
    legacy_rows = cli_app._legacy_nps_shadow_rows(sqlite_path, target, 100)
    result = cli_app._compare_nps_shadow(finance_rows, legacy_rows, tolerance=1.0)

    assert as_of == target
    assert result["status"] == "pass"
