# finance-pi

`finance-pi` is a Korean equity data pipeline and point-in-time factor backtesting
platform. The implementation follows `finance-pi-architecture.md`:

- append-only Bronze Parquet for raw source responses
- normalized Silver tables with stable domain IDs
- query-ready Gold tables for factors, PIT fundamentals, universes, and backtests
- DuckDB catalogs as views over Parquet, not as the system of record
- Polars LazyFrame-first analysis APIs

## Quick Start

```bash
python -m pip install -e ".[dev]"
python -m finance_pi.cli.app init --root .
python -m finance_pi.cli.app doctor --root .
python -m finance_pi.cli.app daily --root . --no-ingest
pytest
```

The local data lake is created under `data/` and is intentionally ignored by Git.

## Runtime Commands

```bash
# Source ingest
python -m finance_pi.cli.app check-krx 2026-04-28
python -m finance_pi.cli.app ingest krx --since 2026-04-29 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-company
python -m finance_pi.cli.app ingest dart-filings --since 2026-04-28 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-financials --corp-code 00126380 --year 2025
python -m finance_pi.cli.app ingest kis --ticker 005930 --since 2026-04-01 --until 2026-04-29

# Deterministic transforms
python -m finance_pi.cli.app build all --root .
python -m finance_pi.cli.app catalog build --root .

# Research outputs
python -m finance_pi.cli.app factors list
python -m finance_pi.cli.app backtest run --factor momentum_12_1 --start 2024-01-01 --end 2024-12-31
python -m finance_pi.cli.app reports dq --report-date 2026-04-29
python -m finance_pi.cli.app reports fraud --report-date 2026-04-29
```

`daily` is a one-day incremental job. For the first server run, use `bootstrap`
with a real date range:

```bash
python -m finance_pi.cli.app bootstrap --since 2024-01-01 --until 2026-04-28 --root .
```

If KRX returns `401`, run `check-krx` and verify that the key is approved for
both KRX stock APIs in the portal: `stk_bydd_trd` for KOSPI and `ksq_bydd_trd`
for KOSDAQ. A key can exist but still lack service-level approval.

## Raspberry Pi Server

Use a 64-bit Raspberry Pi OS image and put `data/` on an external SSD/NVMe for
real backfills. See [docs/raspberry-pi.md](docs/raspberry-pi.md) for a complete
server runbook, including `.env`, smoke tests, and systemd timer setup.

The daily server entrypoint is:

```bash
python -m finance_pi.cli.app daily --root .
```

## Current Scope

This repository now includes live source adapters for KRX, OpenDART, and KIS;
Bronze to Silver/Gold transforms; identity and universe builders; PIT
fundamentals; factor registration; a monthly backtest runner; data quality and
backtest fraud reports; and Raspberry Pi systemd scheduling templates.
