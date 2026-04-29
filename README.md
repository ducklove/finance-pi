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
python -m finance_pi.cli.app check-kis 005930 2026-04-28 --root .
python -m finance_pi.cli.app daily --root . --no-ingest
pytest
```

The local data lake is created under `data/` and is intentionally ignored by Git.

## Runtime Commands

```bash
# Source diagnostics
python -m finance_pi.cli.app check-kis 005930 2026-04-28 --root .
python -m finance_pi.cli.app check-krx 2026-04-28 --root .

# Source ingest
python -m finance_pi.cli.app ingest dart-company
python -m finance_pi.cli.app ingest naver-summary --snapshot-date 2026-04-29 --root .
python -m finance_pi.cli.app ingest naver-daily --since 2024-01-01 --until 2026-04-28 --root .
python -m finance_pi.cli.app ingest kis-universe --since 2026-04-29 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-filings --since 2026-04-28 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-financials --corp-code 00126380 --year 2025
python -m finance_pi.cli.app ingest dart-financials-bulk --since 2024-01-01 --until 2026-04-28
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

The default historical price source is Naver daily chart data because KIS token
and quotation limits make multi-year universe backfills too slow. KIS remains
available with `--price-source kis` or `--price-source both` for short recent
runs and cross-checks. Naver Finance is also used as a same-day summary snapshot
to enrich price rows with KRX-like `market_cap` and `listed_shares`. KRX commands
remain available for diagnostics or future use, but the normal pipeline no
longer depends on KRX.

Required live keys are `OPENDART_API_KEY`, `KIS_APP_KEY`, and
`KIS_APP_SECRET`. Keep KIS secrets as exact single-line values in `.env`;
`doctor` and `check-kis` warn when a pasted secret appears to be wrapped.
KIS access tokens are cached under `data/_cache/kis/token.json` so `check-kis`,
`bootstrap`, and `daily` do not trip the KIS one-token-per-minute issuance limit.
Naver Finance does not require a key, but its summary page is treated as a
snapshot source and is only joined to matching price dates to avoid look-ahead.
OpenDART filings are used to schedule financial statement requests. Bulk
financial ingestion is resumable and stores statement rows by `rcept_dt`, so
`silver.financials` and `gold.fundamentals_pit` are built from the disclosure
date, not from the date the backfill happened. `bootstrap` ingests annual
financial statements by default; pass
`--financial-report-codes 11013,11012,11014,11011` when quarterly statements are
needed too.

## Raspberry Pi Server

Use a 64-bit Raspberry Pi OS image and put `data/` on an external SSD/NVMe for
real backfills. See [docs/raspberry-pi.md](docs/raspberry-pi.md) for a complete
server runbook, including `.env`, smoke tests, and systemd timer setup.

The daily server entrypoint is:

```bash
python -m finance_pi.cli.app daily --root .
```

## Current Scope

This repository now includes live source adapters for KRX, OpenDART, KIS, and
Naver Finance;
Bronze to Silver/Gold transforms; identity and universe builders; PIT
fundamentals; factor registration; a monthly backtest runner; data quality and
backtest fraud reports; and Raspberry Pi systemd scheduling templates.
