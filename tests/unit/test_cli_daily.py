from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl

from finance_pi.cli import app as cli_app
from finance_pi.cli.app import (
    _backfill_years,
    _catchup_dates,
    _latest_gold_price_date,
    _run_daily_ingest,
    _write_backfill_marker,
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


def test_catchup_dates_use_explicit_since_and_skip_weekend(tmp_path) -> None:
    assert _catchup_dates(tmp_path, date(2026, 5, 1), date(2026, 5, 4)) == (
        date(2026, 5, 1),
        date(2026, 5, 4),
    )


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
