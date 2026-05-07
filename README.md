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
python -m finance_pi.cli.app admin --root . --port 8400
python -m finance_pi.cli.app check-admin http://127.0.0.1:8400
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
python -m finance_pi.cli.app build market-caps --root .
python -m finance_pi.cli.app catalog build --root .

# Research outputs
python -m finance_pi.cli.app factors list
python -m finance_pi.cli.app backtest run --factor momentum_12_1 --start 2024-01-01 --end 2024-12-31
python -m finance_pi.cli.app reports dq --report-date 2026-04-29
python -m finance_pi.cli.app reports fraud --report-date 2026-04-29
python -m finance_pi.cli.app reports all --report-date 2026-04-29
python -m finance_pi.cli.app docs build --root .
python -m finance_pi.cli.app admin --root . --port 8400
python -m finance_pi.cli.app check-admin http://127.0.0.1:8400
```

`daily` is a one-day incremental job. For the first server run, use `bootstrap`
with a real date range:

```bash
python -m finance_pi.cli.app bootstrap --since 2024-01-01 --until 2026-04-28 --root .
```

The default historical price source is Naver daily chart data because KIS token
and quotation limits make multi-year universe backfills too slow. KIS remains
available with `--price-source kis` or `--price-source both` for short recent
runs and cross-checks. Naver Finance summary is the preferred listed-ticker
universe for price collection, so preferred shares and alphanumeric short codes
are not lost when OpenDART only exposes a company's representative stock code.
The same summary snapshot enriches price rows with KRX-like `market_cap` and
`listed_shares`. KRX commands remain available for diagnostics or future use, but
the normal pipeline no longer depends on KRX.

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

If the Pi was offline or the timer missed a few sessions, run catch-up once.
This starts after the latest `gold.daily_prices_adj` date unless `--since` is
provided:

```bash
python -m finance_pi.cli.app catchup --root . --since 2026-04-29 --until 2026-04-30 --no-strict
```

Daily runs skip the large `gold.fundamentals_pit` rebuild by default. Rebuild
that cache manually when needed:

```bash
python -m finance_pi.cli.app build fundamentals-pit --root .
```

Historical backfills can be run one year at a time, newest to oldest. The
default below runs only the next missing year, so it is safe to repeat:

```bash
python -m finance_pi.cli.app backfill status --root . --start-year 2023 --end-year 2010
python -m finance_pi.cli.app backfill yearly --root . --start-year 2023 --end-year 2010 --max-years 1 --no-strict
```

Each completed year writes a marker under `data/_state/backfill/yearly/`, and
`backfill status` also checks the real `gold.daily_prices_adj` date partitions.
The web admin exposes the same yearly progress table and can queue the next
missing backfill chunk with separate controls for Naver prices, DART financials,
Silver/Gold rebuilds, and the large `gold.fundamentals_pit` rebuild.

The local web admin is:

```bash
python -m finance_pi.cli.app admin --root . --host 0.0.0.0 --port 8400
```

`admin` prints a tokenized URL. `/api/health` is open for reachability checks.
Clients from loopback or a private LAN address can use the admin without a
token; public/forwarded clients still need the token for dataset APIs, logs,
files, and job execution. To pin the token across restarts, set
`FINANCE_PI_ADMIN_TOKEN` in `.env`.
For Raspberry Pi stability, the admin overview does not scan Parquet contents by
default; it uses file and partition metadata. Set `FINANCE_PI_ADMIN_SCAN_PARQUET=1`
only when you explicitly want live row counts and are comfortable with the extra
I/O and memory pressure.

Build and publish the repository documentation as HTML:

```bash
python -m finance_pi.cli.app docs build --root .
```

The generated site is written to `data/docs_site/` and is served by the admin at
`/docs/`.

## Data Access

Build the DuckDB catalog after ingest/build jobs:

```bash
python -m finance_pi.cli.app catalog build --root .
```

The catalog exposes medallion-layer views such as `silver.prices`,
`gold.daily_prices_adj`, and `gold.daily_market_caps`. It also publishes
analysis-friendly aliases:

- `analytics.daily_prices`
- `analytics.daily_market_caps`
- `analytics.universe`
- `analytics.securities`
- `metadata.datasets`

Example:

```sql
SELECT date, ticker, name, market_cap
FROM analytics.daily_market_caps
WHERE market = 'KOSPI'
ORDER BY date DESC, market_cap DESC
LIMIT 20;
```

## Current Scope

This repository now includes live source adapters for KRX, OpenDART, KIS, and
Naver Finance;
Bronze to Silver/Gold transforms; identity and universe builders; PIT
fundamentals; factor registration; a monthly backtest runner; data quality and
backtest fraud reports; and Raspberry Pi systemd scheduling templates.
