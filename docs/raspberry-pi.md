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

Required once live ingestion is enabled:

- `KRX_OPENAPI_KEY`
- `OPENDART_API_KEY`

Optional for KIS cross-checks:

- `KIS_BASE_URL`
- `KIS_APP_KEY`
- `KIS_APP_SECRET`

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

Manual source commands are also available:

```bash
python -m finance_pi.cli.app ingest krx --since 2026-04-29 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-company
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

The scheduled `daily` command initializes the data lake, attempts KRX and
OpenDART filing ingest when keys are configured, rebuilds Silver/Gold datasets,
rebuilds the DuckDB view catalog, and writes DQ/Fraud reports. KIS remains a
manual cross-check command because it is ticker-range oriented.
