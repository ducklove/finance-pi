# finance-pi

`finance-pi` is a Korean equity data pipeline and point-in-time factor backtesting
platform. The implementation follows `finance-pi-architecture.md`:

- append-only Bronze Parquet for raw source responses
- normalized Silver tables with stable domain IDs
- query-ready Gold tables for factors, PIT fundamentals, universes, and backtests
- DuckDB catalogs as views over Parquet, not as the system of record
- Polars LazyFrame-first analysis APIs

## Quick Start

```powershell
python -m pip install -e ".[dev]"
finance-pi init
finance-pi catalog build
finance-pi factors list
pytest
```

The local data lake is created under `data/` and is intentionally ignored by Git.

## Current Scope

This repository is bootstrapped with the core package structure, storage/catalog
contracts, source adapter interfaces, security identity models, PIT helpers,
factor registry, backtest skeleton, and HTML report scaffolding. Live source
credentials and large historical backfills are expected to be added through the
adapter layer.
