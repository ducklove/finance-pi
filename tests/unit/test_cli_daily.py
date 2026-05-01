from __future__ import annotations

from datetime import date

from finance_pi.cli.app import _catchup_dates, _latest_gold_price_date


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
