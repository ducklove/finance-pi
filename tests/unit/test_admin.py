from __future__ import annotations

from datetime import date

import polars as pl

from finance_pi.admin.server import AdminState, _ensure_docs_built, _health_payload, _job_command
from finance_pi.storage import DataLakeLayout, ParquetDatasetWriter


def test_admin_overview_reports_dataset_counts(tmp_path) -> None:
    data_root = tmp_path / "data"
    layout = DataLakeLayout(data_root)
    layout.ensure_base_dirs()
    ParquetDatasetWriter().write(
        pl.DataFrame(
            [
                {
                    "date": date(2026, 4, 28),
                    "security_id": "S005930",
                    "listing_id": "L005930",
                    "open_adj": 100.0,
                    "high_adj": 101.0,
                    "low_adj": 99.0,
                    "close_adj": 100.0,
                    "return_1d": 0.0,
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
        layout.partition_path("gold.daily_prices_adj", date(2026, 4, 28)),
    )

    overview = AdminState(tmp_path).overview()
    gold_prices = next(
        dataset for dataset in overview["datasets"] if dataset["name"] == "gold.daily_prices_adj"
    )

    assert overview["max_price_date"] == "2026-04-28"
    assert gold_prices["files"] == 1
    assert gold_prices["rows"] == 1
    assert gold_prices["status"] == "ready"


def test_admin_job_command_is_allowlisted(tmp_path) -> None:
    label, command = _job_command(
        "backtest",
        {
            "factor": "quality_roa",
            "start": "2024-01-01",
            "end": "2026-04-28",
            "top_fraction": "0.2",
        },
        tmp_path,
    )

    assert label == "Backtest quality_roa"
    assert "backtest" in command
    assert "--factor" in command
    assert "quality_roa" in command


def test_admin_docs_build_command_is_allowlisted(tmp_path) -> None:
    label, command = _job_command("docs_build", {}, tmp_path)

    assert label == "Build Docs"
    assert command[-4:] == ["docs", "build", "--root", str(tmp_path)]


def test_admin_health_is_minimal_and_token_state_is_kept(tmp_path) -> None:
    state = AdminState(tmp_path, token="secret-token")
    health = _health_payload(state)

    assert state.token == "secret-token"
    assert health["status"] == "ok"
    assert health["workspace"] == str(tmp_path.resolve())
    assert health["auth"] == "token"


def test_admin_ensure_docs_built_creates_site(tmp_path) -> None:
    (tmp_path / "README.md").write_text("# Project\n", encoding="utf-8")

    _ensure_docs_built(tmp_path)

    assert (tmp_path / "data" / "docs_site" / "index.html").exists()
    assert (tmp_path / "data" / "docs_site" / "manifest.json").exists()
