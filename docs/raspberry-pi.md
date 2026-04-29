# Raspberry Pi Runbook

This is the practical server path for running `finance-pi` on a Raspberry Pi.
Use this for smoke tests today and for scheduled daily jobs as live ingest
adapters are added.

## 1. Hardware And OS

Use:

- Raspberry Pi OS 64-bit
- Python 3.11 or newer
- External SSD/NVMe mounted somewhere like `/mnt/ssd`

Avoid 32-bit Raspberry Pi OS. DuckDB and Polars are much happier on 64-bit
ARM, and historical Parquet backfills will punish SD cards.

## 2. Install

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip build-essential
sudo timedatectl set-timezone Asia/Seoul

sudo mkdir -p /opt/finance-pi
sudo chown "$USER":"$USER" /opt/finance-pi

git clone https://github.com/ducklove/finance-pi.git /opt/finance-pi
cd /opt/finance-pi

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
```

## 3. Configure Secrets

```bash
cp .env.example .env
nano .env
```

Required for the normal live pipeline:

- `OPENDART_API_KEY`
- `KIS_APP_KEY`
- `KIS_APP_SECRET`

Optional diagnostics:

- `KRX_OPENAPI_KEY`

KIS `AppKey` and `AppSecret` must be exact single-line values. If a long secret
wraps onto the next line in `.env`, KIS token issuance will fail with
`EGW00105` / invalid AppSecret. `doctor` prints line-number warnings for
malformed `.env` entries without showing secret values.

Keep `.env` on the server only. It is ignored by Git.

## 4. Put Data On SSD

If the repository is in `/opt/finance-pi` but data should live on SSD:

```bash
sudo mkdir -p /mnt/ssd/finance-pi-data
sudo chown "$USER":"$USER" /mnt/ssd/finance-pi-data
ln -s /mnt/ssd/finance-pi-data /opt/finance-pi/data
```

## 5. Smoke Test

```bash
cd /opt/finance-pi
source .venv/bin/activate

python -m finance_pi.cli.app doctor --root .
python -m finance_pi.cli.app check-kis 005930 2026-04-28 --root .
python -m ruff check .
python -m pytest
python -m finance_pi.cli.app daily --root . --no-ingest
```

Expected outputs:

- `data/catalog/finance_pi.duckdb`
- `data/reports/data_quality/YYYY-MM-DD.html`
- `data/reports/backtest_fraud/YYYY-MM-DD.html`

After `.env` is configured, run the real daily path:

```bash
python -m finance_pi.cli.app daily --root .
```

`daily` is intentionally small: it is the recurring one-day incremental job. It
will finish quickly unless the source APIs are slow. The first real server run
should be a backfill:

```bash
python -m finance_pi.cli.app bootstrap --since 2024-01-01 --until 2026-04-28 --root .
```

For a larger historical load, widen `--since`. Start with a short range or
`--ticker-limit 20` first so KIS authorization and rate limits are visible before
launching a long job:

```bash
python -m finance_pi.cli.app bootstrap --since 2026-04-28 --until 2026-04-28 --root . --ticker-limit 20
```

The default price path is:

1. OpenDART `corpCode.xml` for the current listed ticker universe.
2. Naver Finance market summary snapshot for `market_cap` and `listed_shares`.
3. KIS daily item chart per ticker for OHLCV/trading value.
4. Bronze to Silver/Gold transforms.

KRX is not required for the normal pipeline.

Naver Finance is a current snapshot source, not a historical point-in-time API.
The build only joins Naver fields to the same `snapshot_dt`/price date, so it
will enrich future daily runs without leaking today's share count into older
backtests.

Manual source commands are also available:

```bash
python -m finance_pi.cli.app check-kis 005930 2026-04-28 --root .
python -m finance_pi.cli.app ingest dart-company
python -m finance_pi.cli.app ingest naver-summary --snapshot-date 2026-04-29 --root .
python -m finance_pi.cli.app ingest kis-universe --since 2026-04-29 --until 2026-04-29 --limit 20
python -m finance_pi.cli.app ingest dart-filings --since 2026-04-28 --until 2026-04-29
python -m finance_pi.cli.app build all --root .
python -m finance_pi.cli.app catalog build --root .
```

## 6. Schedule With systemd

Copy the templates and adjust paths/user names if needed. The timer uses the
server's local timezone, so keep the server timezone set to `Asia/Seoul`.

```bash
sudo cp ops/systemd/finance-pi-daily.service /etc/systemd/system/
sudo cp ops/systemd/finance-pi-daily.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now finance-pi-daily.timer
```

Check status and logs:

```bash
systemctl status finance-pi-daily.timer
journalctl -u finance-pi-daily.service -n 100 --no-pager
```

Run once manually:

```bash
sudo systemctl start finance-pi-daily.service
```

## 7. Update Deployment

```bash
cd /opt/finance-pi
git pull
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest
```

## Current Runtime Scope

The scheduled `daily` command initializes the data lake, refreshes the OpenDART
company snapshot, ingests the Naver same-day market summary, attempts KIS
universe price ingest and OpenDART filing ingest when keys are configured,
rebuilds Silver/Gold datasets, rebuilds the DuckDB view catalog, and writes
DQ/Fraud reports.
