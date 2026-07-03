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
python -m finance_pi.cli.app ingest dart-dividends --since 2024-01-01 --until 2026-04-28
python -m finance_pi.cli.app ingest dart-share-counts --since 2024-01-01 --until 2026-04-28
python -m finance_pi.cli.app ingest kis --ticker 005930 --since 2026-04-01 --until 2026-04-29
python -m finance_pi.cli.app ingest nps-holdings --root .
python -m finance_pi.cli.app ingest macro --root .

# Deterministic transforms
python -m finance_pi.cli.app build all --root .
python -m finance_pi.cli.app build market-caps --root .
python -m finance_pi.cli.app build relations --root .
python -m finance_pi.cli.app build preferred-discount --root .
python -m finance_pi.cli.app build nps --root .
python -m finance_pi.cli.app build fundamentals-pit --root . --date 2026-05-02
python -m finance_pi.cli.app catalog build --root .

# Research outputs
python -m finance_pi.cli.app factors list
python -m finance_pi.cli.app backtest run --factor momentum_12_1 --start 2024-01-01 --end 2024-12-31
python -m finance_pi.cli.app backtest run --factor value_earnings_yield --start 2021-01-01 --end 2024-12-31 --commission-bps 5 --sell-tax-bps 15
python -m finance_pi.cli.app reports dq --report-date 2026-04-29
python -m finance_pi.cli.app reports fraud --report-date 2026-04-29
python -m finance_pi.cli.app reports all --report-date 2026-04-29
python -m finance_pi.cli.app docs build --root .
python -m finance_pi.cli.app admin --root . --port 8400
python -m finance_pi.cli.app check-admin http://127.0.0.1:8400
python -m finance_pi.cli.app mcp --root .
```

`backtest run` uses a flat `--cost-bps` model by default; passing
`--commission-bps` and/or `--sell-tax-bps` switches to the Korean cost model
(commission both sides plus the sell-side securities transaction tax).

Registered factors (see `factors list` for direction metadata): momentum_12_1,
reversal_1m, momentum_52w_high, value_earnings_yield, value_book_to_price,
value_dividend_yield, quality_roa, quality_gpa, quality_accruals, lowvol_252,
preferred_discount_z, nps_flow, composite_value_quality.

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

Each daily run writes a completeness marker (`complete` /
`complete_with_failures` / `failed`) under `data/_state/daily/`, including a
gold price-quality gate that flags row-count collapses against the previous
trading day. Catch-up resumes from the latest fully complete date and re-runs
incomplete ones. When `FINANCE_PI_WEBHOOK_URL` is set, daily/catch-up runs POST
a Slack/Discord-compatible summary whenever a step fails or a dataset grades
C/F on the reliability scorecard.

Daily runs skip the large `gold.fundamentals_pit` rebuild by default. Rebuild
that cache manually when needed:

```bash
python -m finance_pi.cli.app build fundamentals-pit --root .
```

After the first full build the command is incremental: it tracks
`silver.financials` state under `data/_state/fundamentals_pit.json` and only
rebuilds as-of dates affected by new filings. Delete the marker to force a full
rebuild, or pass `--date 2026-05-02` (repeatable) to rebuild specific dates.

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
Clients from loopback or a private LAN address can read the dataset APIs, logs,
and files without a token; public/forwarded clients still need the token for
those. Job execution (`POST /api/jobs`) always requires the token unless the
client is loopback, and cross-origin requests are rejected. To pin the token
across restarts, set `FINANCE_PI_ADMIN_TOKEN` in `.env`.
For Raspberry Pi stability, the admin overview does not scan Parquet contents by
default; it uses file and partition metadata. Set `FINANCE_PI_ADMIN_SCAN_PARQUET=1`
only when you explicitly want live row counts and are comfortable with the extra
I/O and memory pressure.

The admin API exposes point-in-time data under `/api/`. The fundamentals endpoint
returns selected statement metrics plus per-share values when enough data is
available:

```bash
curl "http://127.0.0.1:8400/api/fundamentals/basic?ticker=005930&as_of=2026-05-10"
```

`/api/fundamentals/basic` includes a `per_share` object with:

- `bps`: latest balance-sheet equity divided by the issuer-level DART
  outstanding-share denominator.
- `eps_annual`: reported annual basic EPS when available, otherwise latest
  annual net income divided by the share denominator.
- `eps_ttm`: reported basic EPS when the latest row is annual. For interim rows,
  it is computed as latest cumulative interim net income plus prior-year annual
  net income minus prior-year matching interim net income, divided by the share
  denominator.
- `eps_forward`: forward EPS placeholder. It returns `value: null` and
  `available: false` until a forward earnings estimate source is added.

The share denominator uses OpenDART `stockTotqySttus` rows: `distb_stock_co` is
the denominator, `tesstk_co` is retained per share class, and
`treasury_shares_excluded` is `true`. Common and preferred rows for the same
issuer (`corp_code`) are summed. If a DART share-count row is not available for
the point-in-time cutoff, BPS falls back to issuer-level listed shares from
`gold.daily_prices_adj`; those fallback rows set `treasury_shares_excluded` to
`false`.

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
`gold.daily_prices_adj`, `gold.daily_market_caps`, `gold.fundamentals_pit`
(point-in-time statement rows), `silver.corporate_actions` (detected
splits/merges/capital reductions), `silver.security_relations` (preferred ↔
common share pairs), `gold.preferred_discount` (daily preferred-share discount
with trailing z-score), `gold.nps_holdings_delta` (NPS position changes), and
`gold.identity_review` (ticker-reuse candidates). It also publishes
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

## MCP Server

Expose the research data lake to LLM clients (Claude Code, Claude Desktop)
through the Model Context Protocol. Install the optional extra and start the
stdio server:

```bash
pip install "finance-pi[mcp]"
python -m finance_pi.cli.app mcp --root .
```

Tools: `list_datasets`, `describe_table`, `query` (single read-only
SELECT/WITH statement, row-capped), `list_factors`, `run_backtest` (compact
in-memory summary), and `get_fundamentals` (PIT annual snapshot per ticker).
The `query` tool needs the DuckDB catalog, so run `catalog build` first.

Register with Claude Code:

```bash
claude mcp add finance-pi -- python -m finance_pi.cli.app mcp --root /path/to/workspace
```

When the data lake lives on a remote server (e.g. the Raspberry Pi), register
the server-side process over SSH so queries run against the real data:

```bash
claude mcp add finance-pi -- ssh cantabile@192.168.68.84 \
  /home/cantabile/Works/finance-pi/.venv/bin/python -m finance_pi.cli.app mcp \
  --root /home/cantabile/Works/finance-pi
```

or add it to Claude Desktop's `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "finance-pi": {
      "command": "python",
      "args": ["-m", "finance_pi.cli.app", "mcp", "--root", "/path/to/workspace"]
    }
  }
}
```

Example prompts:

- "2015년 이후 PBR 0.5 미만 종목의 1년 보유 수익률 분포를 보여줘"
- "momentum_12_1 팩터를 2018-01-01부터 2020-12-31까지 백테스트하고 MDD와 비용 드래그를 알려줘"

## Current Scope

This repository includes:

- live source adapters for OpenDART, KIS, Naver Finance, NPS holdings, macro
  series (FRED/ECOS/Yahoo/CNBC), and KRX (diagnostics)
- Bronze to Silver/Gold transforms with corporate-action detection from
  listed-share jumps, raw/adjusted price-basis tracking, real delisting dates,
  and memory-bounded chunked full rebuilds sized for a Raspberry Pi
- point-in-time fundamentals with strict next-day availability, deterministic
  consolidated-statement preference, backfill flags, and incremental rebuilds
- 13 registered factors with direction metadata, cross-sectional utilities
  (winsorize/zscore/rank/neutralize), composite blending, and IC/quantile/
  turnover analytics
- a monthly backtest engine with entry-day look-ahead protection, delisting
  handling, and a Korean trading-cost model (sell-side transaction tax)
- Korea-specific research datasets: preferred-share discount series and NPS
  smart-money flow deltas
- data quality reports with a per-dataset reliability scorecard (A/C/F),
  backtest fraud checks, daily completeness markers with price-quality gates,
  and optional webhook alerts (`FINANCE_PI_WEBHOOK_URL`)
- a token-protected web admin, an MCP server for LLM clients, deployment and
  verification scripts (`ops/deploy.sh`), and Raspberry Pi systemd scheduling
