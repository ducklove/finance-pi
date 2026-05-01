from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from finance_pi.cli import app as cli_app
from finance_pi.cli.app import _catchup_dates, _latest_gold_price_date, _run_daily_ingest
from finance_pi.config import ProjectPaths


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
