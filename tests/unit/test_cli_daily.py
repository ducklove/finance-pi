from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from finance_pi.cli import app as cli_app
from finance_pi.cli.app import (
    _admin_health_ok,
    _backfill_years,
    _catchup_dates,
    _daily_complete,
    _latest_gold_price_date,
    _latest_price_universe_tickers,
    _run_daily_builds,
    _run_daily_ingest,
    _validated_backfill_paths,
    _write_backfill_marker,
    _write_daily_marker,
    _yearly_backfill_status,
)
from finance_pi.config import ProjectPaths
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def test_catchup_dates_start_after_latest_gold_price(tmp_path) -> None:
    partition = tmp_path / "gold" / "daily_prices_adj" / "dt=2026-04-28"
    partition.mkdir(parents=True)
    (partition / "part.parquet").write_bytes(b"placeholder")

    assert _latest_gold_price_date(tmp_path) == date(2026, 4, 28)
    assert _catchup_dates(tmp_path, None, date(2026, 4, 30)) == (
        date(2026, 4, 29),
        date(2026, 4, 30),
    )


def test_admin_health_ok_validates_status_payload(monkeypatch) -> None:
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self) -> bytes:
            return b'{"status":"ok"}'

    monkeypatch.setattr(cli_app, "urlopen", lambda request, timeout: Response())

    assert _admin_health_ok("http://127.0.0.1:8400/api/health", 1.0)


def test_admin_health_ok_returns_false_on_error(monkeypatch) -> None:
    def raise_error(request, timeout):
        raise TimeoutError("timed out")

    monkeypatch.setattr(cli_app, "urlopen", raise_error)

    assert not _admin_health_ok("http://127.0.0.1:8400/api/health", 1.0)


def test_catchup_dates_use_explicit_since_and_skip_weekend(tmp_path) -> None:
    assert _catchup_dates(tmp_path, date(2026, 5, 1), date(2026, 5, 4)) == (
        date(2026, 5, 1),
        date(2026, 5, 4),
    )


def test_catchup_dates_start_after_daily_marker_for_no_price_day(tmp_path) -> None:
    partition = tmp_path / "gold" / "daily_prices_adj" / "dt=2026-04-30"
    partition.mkdir(parents=True)
    (partition / "part.parquet").write_bytes(b"placeholder")
    _write_daily_marker(
        tmp_path,
        date(2026, 5, 1),
        {
            "report_date": "2026-05-01",
            "price_date": "2026-05-01",
            "failures": [],
            "gold_price_partition": False,
        },
    )

    assert _catchup_dates(tmp_path, None, date(2026, 5, 4)) == (date(2026, 5, 4),)
    assert _daily_complete(tmp_path, date(2026, 5, 1))
    assert not _daily_complete(tmp_path, date(2026, 5, 4))


def test_daily_ingest_internal_calls_pass_concrete_defaults(tmp_path, monkeypatch) -> None:
    calls: dict[str, tuple[object, ...]] = {}

    def record(name: str):
        def inner(*args: object) -> None:
            calls[name] = args

        return inner

    monkeypatch.setattr(cli_app, "ingest_dart_company", record("dart_company"))
    monkeypatch.setattr(cli_app, "ingest_naver_summary", record("naver_summary"))
    monkeypatch.setattr(cli_app, "ingest_kis_universe", record("kis_universe"))
    monkeypatch.setattr(cli_app, "ingest_dart_filings", record("dart_filings"))
    monkeypatch.setattr(cli_app, "ingest_dart_financials_bulk", record("dart_financials_bulk"))
    def record_macro(*args: object) -> list[str]:
        calls["macro"] = args
        return []

    monkeypatch.setattr(cli_app, "_ingest_macro", record_macro)

    failures = _run_daily_ingest(
        ProjectPaths(tmp_path),
        SimpleNamespace(has_opendart=True, has_kis=True),
        date(2026, 4, 30),
    )

    assert failures == []
    assert calls["kis_universe"] == (
        "2026-04-30",
        "2026-04-30",
        tmp_path,
        None,
        1,
        0.05,
        50,
    )
    assert calls["dart_filings"] == ("2026-04-29", "2026-04-30", tmp_path, 7)
    assert calls["macro"][:3] == (ProjectPaths(tmp_path), date(1990, 1, 1), date(2026, 4, 30))


def test_fred_rows_use_api_key_json_response(monkeypatch) -> None:
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self) -> bytes:
            return (
                b'{"observations":['
                b'{"date":"2026-03-01","value":"320.0"},'
                b'{"date":"2026-04-01","value":"322.0"}'
                b"]}"
            )

    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        return Response()

    monkeypatch.setattr(cli_app, "urlopen", fake_urlopen)

    rows = cli_app._fetch_fred_rows(
        {
            "series_id": "US_CPI_ALL",
            "fred_id": "CPIAUCSL",
            "country": "US",
            "name": "US CPI",
            "frequency": "M",
            "index_base": "1982-84=100",
        },
        date(2026, 3, 1),
        date(2026, 4, 30),
        "cpi",
        "secret",
    )

    assert "api.stlouisfed.org/fred/series/observations" in seen["url"]
    assert "api_key=secret" in seen["url"]
    assert [row["value"] for row in rows] == [320.0, 322.0]


def test_daily_marker_uses_report_date_not_price_date(tmp_path, monkeypatch) -> None:
    class Report:
        def write(self, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("ok", encoding="utf-8")

    class Catalog:
        def __init__(self, data_root, catalog_path):
            self.data_root = data_root
            self.catalog_path = catalog_path

        def build(self):
            self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
            self.catalog_path.write_text("", encoding="utf-8")
            return []

    monkeypatch.setattr(cli_app, "_run_daily_builds", lambda *args: [])
    monkeypatch.setattr(cli_app, "CatalogBuilder", Catalog)
    monkeypatch.setattr(cli_app, "build_data_quality_report", lambda *args: Report())
    monkeypatch.setattr(cli_app, "build_fraud_report", lambda *args: Report())
    DataLakeLayout(tmp_path / "data").ensure_base_dirs()

    cli_app.run_daily(tmp_path, "2026-05-07", False, False, False)

    assert (tmp_path / "data" / "_state" / "daily" / "2026-05-07.json").exists()
    assert not (tmp_path / "data" / "_state" / "daily" / "2026-05-06.json").exists()


def test_macro_table_write_merges_by_date_and_series(tmp_path) -> None:
    data_root = tmp_path / "data"
    DataLakeLayout(data_root).ensure_base_dirs()

    cli_app._write_macro_table(
        data_root,
        "fx",
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "series_id": "USD_KRW",
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "value": 1320.0,
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 3),
                    "series_id": "USD_KRW",
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "value": 9999.0,
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 3),
                    "series_id": "USD_JPY",
                    "base_currency": "USD",
                    "quote_currency": "JPY",
                    "value": 145.0,
                    "source": "test",
                    "updated_at": None,
                }
            ]
        ),
    )
    cli_app._write_macro_table(
        data_root,
        "fx",
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "series_id": "USD_KRW",
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "value": 1321.0,
                    "source": "test",
                    "updated_at": None,
                },
                {
                    "date": date(2024, 1, 4),
                    "series_id": "USD_KRW",
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "value": 1322.0,
                    "source": "test",
                    "updated_at": None,
                },
            ]
        ),
    )

    frame = pl.read_parquet(data_root / "macro" / "fx" / "part.parquet")

    assert frame.height == 3
    assert frame.filter(pl.col("date") == date(2024, 1, 2)).select("value").item() == 1321.0
    assert (
        frame.filter(pl.col("series_id") == "USD_KRW")
        .sort("date")
        .select("date")
        .to_series()
        .to_list()
    ) == [
        date(2024, 1, 2),
        date(2024, 1, 4),
    ]
    assert frame.filter(pl.col("series_id") == "USD_JPY").select("value").item() == 145.0


def test_macro_ingest_start_backfills_empty_tables(tmp_path) -> None:
    data_root = tmp_path / "data"
    DataLakeLayout(data_root).ensure_base_dirs()

    assert cli_app._macro_ingest_start(data_root, date(2026, 5, 7)) == date(1990, 1, 1)


def test_macro_ingest_start_rereads_trailing_window(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    for dataset, extra in [
        (
            "macro.cpi",
            {
                "country": "KR",
                "name": "Korea CPI",
                "frequency": "M",
                "index_base": "2020=100",
                "yoy_pct": None,
                "mom_pct": None,
            },
        ),
        (
            "macro.rates",
            {
                "country": "KR",
                "name": "Base rate",
                "frequency": "D",
                "tenor": "overnight",
                "unit": "percent",
            },
        ),
        (
            "macro.indices",
            {
                "country": "KR",
                "name": "KOSPI",
                "frequency": "D",
                "category": "equity_index",
                "currency": "KRW",
                "return_1d": None,
                "return_1m": None,
            },
        ),
        (
            "macro.commodities",
            {
                "name": "Gold",
                "commodity": "gold",
                "unit": "troy_oz",
                "currency": "USD",
            },
        ),
        (
            "macro.fx",
            {
                "base_currency": "USD",
                "quote_currency": "KRW",
            },
        ),
        (
            "macro.economic_indicators",
            {
                "country": "US",
                "name": "US Unemployment Rate",
                "category": "labor",
                "frequency": "M",
                "unit": "percent",
            },
        ),
    ]:
        writer.write(
            pl.DataFrame(
                [
                    {
                        "date": date(2026, 4, 30),
                        "series_id": dataset,
                        "value": 1.0,
                        "source": "test",
                        "updated_at": None,
                        **extra,
                    }
                ]
            ),
            layout.singleton_path(dataset),
        )

    assert cli_app._macro_ingest_start(data_root, date(2026, 5, 7)) == date(2025, 2, 4)


def test_daily_builds_only_materialize_target_price_date(tmp_path) -> None:
    layout = DataLakeLayout(tmp_path)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "ticker": "005930",
                    "isin": None,
                    "name": "Samsung",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "price_source": "kis",
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                    "security_id": "S005930",
                    "listing_id": "L005930",
                }
            ]
        ),
        layout.partition_path("silver.prices", date(2024, 1, 2)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 2),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 100.0,
                    "high_adj": 100.0,
                    "low_adj": 100.0,
                    "close_adj": 100.0,
                    "return_1d": None,
                    "volume": 10,
                    "trading_value": 1000,
                    "market_cap": 1_000_000,
                    "listed_shares": 10_000,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2024, 1, 2)),
    )
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2024, 1, 3),
                    "ticker": "005930",
                    "isin": None,
                    "name": "Samsung",
                    "market": "KOSPI",
                    "open": 100.0,
                    "high": 120.0,
                    "low": 95.0,
                    "close": 110.0,
                    "volume": 20,
                    "trading_value": 2200,
                    "market_cap": 1_100_000,
                    "listed_shares": 10_000,
                }
            ]
        ),
        layout.partition_path("bronze.kis_daily_raw", date(2024, 1, 3)),
    )

    summaries = _run_daily_builds(tmp_path, False, date(2024, 1, 3))

    assert {summary.dataset for summary in summaries} >= {
        "silver.prices",
        "gold.security_master",
        "gold.universe_history",
        "gold.daily_prices_adj",
    }
    assert not (tmp_path / "silver" / "prices" / "dt=2024-01-04" / "part.parquet").exists()
    gold = pl.read_parquet(
        tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-03" / "part.parquet"
    )
    assert gold.select("return_1d").item() == pytest.approx(0.1)
    assert (tmp_path / "gold" / "daily_prices_adj" / "dt=2024-01-02" / "part.parquet").exists()


def test_price_universe_prefers_naver_summary_and_keeps_preferred_codes(tmp_path) -> None:
    paths = ProjectPaths(tmp_path)
    layout = DataLakeLayout(paths.data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "snapshot_dt": date(2026, 4, 30),
                    "ticker": "005930",
                    "name": "Samsung",
                    "market": "KOSPI",
                    "close": 100,
                    "change_abs": 0,
                    "change_rate_pct": 0.0,
                    "par_value": 100,
                    "market_cap": 1,
                    "listed_shares": 1,
                    "foreign_ownership_pct": 0.0,
                    "volume": 1,
                    "per": 1.0,
                    "roe": 1.0,
                },
                {
                    "snapshot_dt": date(2026, 4, 30),
                    "ticker": "005935",
                    "name": "Samsung Preferred",
                    "market": "KOSPI",
                    "close": 100,
                    "change_abs": 0,
                    "change_rate_pct": 0.0,
                    "par_value": 100,
                    "market_cap": 1,
                    "listed_shares": 1,
                    "foreign_ownership_pct": 0.0,
                    "volume": 1,
                    "per": 1.0,
                    "roe": 1.0,
                },
                {
                    "snapshot_dt": date(2026, 4, 30),
                    "ticker": "12345k",
                    "name": "Alpha Preferred",
                    "market": "KOSPI",
                    "close": 100,
                    "change_abs": 0,
                    "change_rate_pct": 0.0,
                    "par_value": 100,
                    "market_cap": 1,
                    "listed_shares": 1,
                    "foreign_ownership_pct": 0.0,
                    "volume": 1,
                    "per": 1.0,
                    "roe": 1.0,
                },
            ]
        ),
        layout.partition_path("bronze.naver_summary_raw", date(2026, 4, 30)),
    )

    tickers = _latest_price_universe_tickers(paths)

    assert tickers == ("005930", "005935", "12345K")


def test_backfill_status_uses_markers_and_price_partitions(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    writer = ParquetDatasetWriter()
    writer.write(
        pl.DataFrame(
            [
                {
                    "date": date(2023, 1, 2),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 1.0,
                    "high_adj": 1.0,
                    "low_adj": 1.0,
                    "close_adj": 1.0,
                    "return_1d": 0.0,
                    "volume": 1,
                    "trading_value": 1,
                    "market_cap": 1,
                    "listed_shares": 1,
                    "is_halted": False,
                    "is_designated": False,
                    "is_liquidation_window": False,
                }
            ]
        ),
        layout.partition_path("gold.daily_prices_adj", date(2023, 1, 2)),
    )
    _write_backfill_marker(
        data_root,
        2023,
        {"since": "2023-01-01", "until": "2023-12-31", "failures": []},
    )

    assert _backfill_years(2023, 2021) == (2023, 2022, 2021)
    status = _yearly_backfill_status(data_root, 2023, 2022)

    assert status[0]["status"] == "complete"
    assert status[0]["price_days"] == 1
    assert status[0]["coverage"] == "2023-01-02..2023-01-02"
    assert status[1]["status"] == "missing"


def test_backfill_root_must_be_workspace_root(tmp_path) -> None:
    workspace = tmp_path / "finance-pi"
    marker_dir = workspace / "data" / "_state" / "backfill" / "yearly"
    (workspace / "src" / "finance_pi").mkdir(parents=True)
    marker_dir.mkdir(parents=True)
    (workspace / "pyproject.toml").write_text("[project]\nname='finance-pi'\n", encoding="utf-8")

    assert _validated_backfill_paths(workspace).root == workspace
    with pytest.raises(Exception) as exc_info:
        _validated_backfill_paths(marker_dir)

    assert "workspace root" in str(exc_info.value)
    assert str(workspace) in str(exc_info.value)
