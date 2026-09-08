from datetime import UTC, date, datetime
from types import SimpleNamespace

import polars as pl
import pytest
import typer

from finance_pi.cli import app as cli
from finance_pi.config import ProjectPaths


def write_prices(root, day, count=100):
    path = root / "gold" / "daily_prices_adj" / f"dt={day}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"date": [day] * count}).write_parquet(path)


def test_holiday_failure_marker_does_not_block_new_dates(tmp_path):
    write_prices(tmp_path, date(2026, 8, 28))
    cli._record_daily_marker(tmp_path, date(2026, 7, 17), date(2026, 7, 17), ["no prices"])
    assert cli._catchup_dates(tmp_path, None, date(2026, 9, 1)) == (
        date(2026, 8, 31),
        date(2026, 9, 1),
    )


def test_quality_gate_detects_small_transform_loss(tmp_path):
    day = date(2026, 9, 8)
    for dataset, count in [("silver.prices", 100), ("gold.daily_prices_adj", 99)]:
        path = cli.DataLakeLayout(tmp_path).partition_path(dataset, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"security_id": [f"S{i:06}" for i in range(count)]}).write_parquet(path)
    assert cli._daily_price_quality_failures(tmp_path, day) == [
        "Gold prices missing 1 Silver securities for 2026-09-08"
    ]


@pytest.mark.parametrize("failure", [typer.Exit(1), RuntimeError("catalog unavailable")])
def test_catchup_keeps_failed_date_and_publishes_later_dates(tmp_path, monkeypatch, failure):
    calls = []
    root = tmp_path / "data"

    def daily(workspace, day, *args):
        calls.append(day)
        parsed = date.fromisoformat(day)
        if day == "2026-08-31":
            raise failure
        write_prices(root, parsed)
        cli._record_daily_marker(root, parsed, parsed, [])

    monkeypatch.setattr(cli, "run_daily", daily)
    with pytest.raises(typer.Exit) as exc:
        cli.catchup_daily(tmp_path, "2026-08-31", "2026-09-01", True, True, False)
    assert exc.value.exit_code == 1
    assert calls == ["2026-08-31", "2026-09-01"]
    assert cli._catchup_dates(root, None, date(2026, 9, 1)) == (date(2026, 8, 31),)
    assert cli._daily_complete(root, date(2026, 9, 1))


def test_historical_prices_use_naver_and_reuse_validated_gold(tmp_path, monkeypatch):
    paths = ProjectPaths(tmp_path)
    calls = []
    monkeypatch.setattr(cli, "_latest_closed_price_date", lambda: date(2026, 9, 8))
    monkeypatch.setattr(cli, "ingest_naver_daily", lambda *args: calls.append(args))
    monkeypatch.setattr(cli, "ingest_kis_universe", lambda *args: pytest.fail("historical KIS"))
    settings = SimpleNamespace(has_kis=True)
    assert cli._ingest_daily_prices(paths, settings, date(2026, 8, 31)) == ([], [])
    assert calls[0][:2] == ("2026-08-31", "2026-08-31")
    write_prices(paths.data_root, date(2026, 8, 31))
    assert cli._ingest_daily_prices(paths, settings, date(2026, 8, 31)) == ([], [])
    assert len(calls) == 1


@pytest.mark.parametrize("kis_rows", [0, 94, 95])
def test_kis_missing_or_partial_prices_fall_back_to_naver(tmp_path, monkeypatch, kis_rows):
    paths = ProjectPaths(tmp_path)
    day = date(2026, 9, 8)
    write_prices(paths.data_root, date(2026, 9, 7))
    monkeypatch.setattr(cli, "_latest_closed_price_date", lambda: day)
    calls = []

    def kis(*args):
        path = paths.data_root / "bronze" / "kis_daily" / f"dt={day}" / "part.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame({"date": [day] * kis_rows}).write_parquet(path)

    monkeypatch.setattr(cli, "ingest_kis_universe", kis)
    monkeypatch.setattr(cli, "ingest_naver_daily", lambda *args: calls.append(args))
    failures, warnings = cli._ingest_daily_prices(
        paths,
        SimpleNamespace(has_kis=True, kis_daily_sleep_seconds=0, kis_daily_ticker_batch_size=50),
        day,
    )
    assert failures == []
    assert bool(calls) == (kis_rows < 95)
    assert bool(warnings) == (kis_rows < 95)


def test_source_failure_still_publishes_prices_and_remains_failed(tmp_path, monkeypatch):
    root = tmp_path / "data"
    day = date(2026, 9, 8)
    monkeypatch.setattr(cli, "_run_daily_ingest", lambda *args: (["DART unavailable"], []))

    def builds(*args):
        write_prices(root, day)
        return []

    report = SimpleNamespace(write=lambda path: None)
    monkeypatch.setattr(cli, "_run_daily_builds", builds)
    monkeypatch.setattr(cli, "CatalogBuilder", lambda *args: SimpleNamespace(build=lambda: []))
    monkeypatch.setattr(cli, "build_data_quality_report", lambda *args: report)
    monkeypatch.setattr(cli, "build_fraud_report", lambda *args: report)
    monkeypatch.setattr(cli, "_safe_build_scorecard", lambda *args: None)
    monkeypatch.setattr(cli, "_notify_daily_webhook", lambda *args: None)
    with pytest.raises(typer.Exit):
        cli.run_daily(tmp_path, day.isoformat(), True, True, False)
    assert cli._gold_price_row_count(root, day) == 100
    assert (
        cli._read_daily_marker_status(cli._daily_marker_path(root, day)) == "complete_with_failures"
    )


@pytest.mark.parametrize("hour, expected", [(0, date(2026, 9, 7)), (16, date(2026, 9, 8))])
def test_default_catchup_cutoff_waits_for_market_close(monkeypatch, hour, expected):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=UTC):
            return cls(2026, 9, 8, hour, tzinfo=tz)

    monkeypatch.setattr(cli, "datetime", Clock)
    assert cli._latest_closed_price_date() == expected


@pytest.mark.parametrize(
    "failed_day, ready", [(date(2026, 7, 17), True), (date(2026, 8, 31), False)]
)
def test_readiness_exposes_old_failures_but_excludes_holidays(
    tmp_path, monkeypatch, failed_day, ready
):
    from finance_pi.admin import server

    state = server.AdminState(tmp_path)
    state.paths.catalog_path.parent.mkdir(parents=True)
    state.paths.catalog_path.touch()
    monkeypatch.setattr(server, "_kst_today", lambda: date(2026, 9, 8))
    monkeypatch.setattr(
        server,
        "_readiness_catalog_snapshot",
        lambda *args: (date(2026, 9, 8), 4000, len(server.dataset_registry)),
    )
    cli._record_daily_marker(state.paths.data_root, failed_day, failed_day, ["no prices"])
    result = server._readiness_payload(state)
    assert (result["status"] == "ready") == ready
    assert result["checks"]["incomplete_daily_count"] == (0 if ready else 1)
