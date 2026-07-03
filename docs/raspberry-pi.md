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

KIS allows token issuance only about once per minute. `finance-pi` caches issued
tokens in `data/_cache/kis/token.json`, so run `check-kis` once and then reuse
the cached token for `bootstrap` and `daily`.

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
python -m finance_pi.cli.app admin --root . --port 8400
python -m finance_pi.cli.app check-admin http://127.0.0.1:8400
```

Expected outputs:

- `data/catalog/finance_pi.duckdb`
- `data/reports/data_quality/YYYY-MM-DD.html`
- `data/reports/backtest_fraud/YYYY-MM-DD.html`

After `.env` is configured, run the real daily path:

```bash
python -m finance_pi.cli.app daily --root .
```

If the server missed one or more sessions, run catch-up. For example, to fill
the already-missed 2026-04-29 and 2026-04-30 sessions:

```bash
python -m finance_pi.cli.app catchup --root . --since 2026-04-29 --until 2026-04-30 --no-strict
```

Without `--since`, catch-up starts from the day after the latest
`gold.daily_prices_adj` partition. The scheduled systemd service uses catch-up
instead of a single-day run so a missed timer can fill multiple weekdays on the
next execution. Use `--no-strict` for the scheduled service because the current
calendar only skips weekends; market holidays can legitimately produce no price
rows.

Daily and catch-up runs skip the large `gold.fundamentals_pit` rebuild by
default. Rebuild that derived cache manually when you need PIT fundamentals:

```bash
python -m finance_pi.cli.app build fundamentals-pit --root .
```

## 5.1 Historical Yearly Backfill

Use yearly backfill for older history. It runs newest to oldest and writes a
completion marker after each year, so it can be stopped and resumed.

Check current progress:

```bash
python -m finance_pi.cli.app backfill status --root . --start-year 2023 --end-year 2010
```

Run the next missing year only:

```bash
python -m finance_pi.cli.app backfill yearly --root . --start-year 2023 --end-year 2010 --max-years 1 --no-strict
```

To keep walking backward, repeat the same command. To run multiple missing years
in one invocation, increase `--max-years`; pass `--max-years 0` for no limit.
The command uses Naver daily price history by default and, when OpenDART is
configured, ingests yearly filings and annual financial statements. It does not
rebuild `gold.fundamentals_pit` unless `--include-fundamentals-pit` is passed.
The admin Backfill panel shows the same yearly marker/coverage status and can
queue the next missing chunk. Prices and Silver/Gold rebuilds are always on;
DART financials, strict failure behavior, forced reruns, dry runs, and the large
fundamentals PIT rebuild are explicit controls.

Completion markers live here:

```text
data/_state/backfill/yearly/YYYY.json
```

Start the web admin on the Pi:

```bash
python -m finance_pi.cli.app admin --root . --host 0.0.0.0 --port 8400
```

The server prints a tokenized URL. Clients from loopback or a private LAN
address can use the admin without a token. Public/forwarded clients still need
the token for dataset APIs, logs, files, and job execution. If port `8400` is
already forwarded, open:

```text
http://<raspberry-pi-host>:8400/?token=<printed-token>
```

For a stable URL across restarts, put this in `.env`:

```bash
FINANCE_PI_ADMIN_TOKEN=replace-with-a-long-random-token
```

The admin overview is intentionally light on Raspberry Pi: it reports file
counts, size, and partition coverage without scanning Parquet contents. Leave
`FINANCE_PI_ADMIN_SCAN_PARQUET` unset for normal operation. Set
`FINANCE_PI_ADMIN_SCAN_PARQUET=1` only for a short diagnostic run when you need
live row counts.

Direct reachability check:

```bash
python -m finance_pi.cli.app check-admin http://127.0.0.1:8400
curl http://127.0.0.1:8400/api/health
```

`daily` is intentionally small: it is the recurring one-day incremental job. It
will finish quickly unless the source APIs are slow. The first real server run
should be a backfill:

```bash
python -m finance_pi.cli.app bootstrap --since 2024-01-01 --until 2026-04-28 --root .
```

For a larger historical load, widen `--since`. Start with a short range or
`--ticker-limit 20` first so Naver throughput and local disk writes are visible
before launching a long job:

```bash
python -m finance_pi.cli.app bootstrap --since 2026-04-28 --until 2026-04-28 --root . --ticker-limit 20
```

The default historical bootstrap price path is:

1. Naver Finance market summary for the current listed ticker universe,
   including preferred shares and alphanumeric short codes.
2. OpenDART filing list chunks, used to schedule PIT financial statement loads.
3. Naver daily chart data for historical OHLCV.
4. OpenDART annual financial statements by filing date.
5. Naver Finance market summary snapshot for same-day `market_cap` and `listed_shares`.
6. Bronze to Silver/Gold transforms.

KIS is still available for short recent runs with `--price-source kis` or
`--price-source both`, but do not use it for multi-year universe backfills. It is
too slow for that shape of workload.

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
python -m finance_pi.cli.app ingest naver-daily --since 2024-01-01 --until 2026-04-28 --root . --limit 20
python -m finance_pi.cli.app ingest kis-universe --since 2026-04-29 --until 2026-04-29 --limit 20
python -m finance_pi.cli.app ingest dart-filings --since 2026-04-28 --until 2026-04-29
python -m finance_pi.cli.app ingest dart-financials-bulk --since 2026-04-28 --until 2026-04-29
python -m finance_pi.cli.app build all --root .
python -m finance_pi.cli.app catalog build --root .
python -m finance_pi.cli.app reports all --report-date 2026-04-29 --root .
python -m finance_pi.cli.app docs build --root .
```

Published documentation is available from the admin at `/docs/` after running
`docs build`.

For quarterly DART financials, use:

```bash
python -m finance_pi.cli.app ingest dart-financials-bulk \
  --since 2024-01-01 \
  --until 2026-04-28 \
  --report-codes 11013,11012,11014,11011 \
  --root .
```

## 6. Schedule With systemd (user units)

The production server runs all finance-pi units as **systemd user units**
(`~/.config/systemd/user/`): `finance-pi-daily.timer` (scheduled catch-up),
`finance-pi-admin.service` (web admin), and
`finance-pi-admin-watchdog.timer` (per-minute HTTP health check that restarts
the admin user unit — the watchdog calls `systemctl --user restart`, which is
why the admin must stay a user unit). The daily timer fires twice per weekday
(17:30 and 20:30 KST, plus up to 10 minutes of randomized delay); the second
run is a same-evening catch-up for sources that publish late. The templates in
`ops/systemd/` install unchanged at the user level.

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd/finance-pi-daily.service ~/.config/systemd/user/
cp ops/systemd/finance-pi-daily.timer ~/.config/systemd/user/
cp ops/systemd/finance-pi-admin.service ~/.config/systemd/user/
cp ops/systemd/finance-pi-admin-watchdog.service ~/.config/systemd/user/
cp ops/systemd/finance-pi-admin-watchdog.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now finance-pi-daily.timer finance-pi-admin.service finance-pi-admin-watchdog.timer
sudo loginctl enable-linger "$USER"   # keep user units running without a login session
```

Check status and logs:

```bash
systemctl --user list-timers | grep finance
systemctl --user status finance-pi-admin.service
journalctl --user -u finance-pi-daily.service -n 100 --no-pager
```

Run once manually:

```bash
systemctl --user start finance-pi-daily.service
```

> **Do not also install these units under `/etc/systemd/system/`.** A
> system-level duplicate of `finance-pi-daily.timer` once ran the daily job
> twice per day, and a system-level admin unit fights the user unit over port
> 8400 in a restart loop. Keep everything at the user level. The timer uses the
> server's local timezone, so keep the server timezone set to `Asia/Seoul`.

## 7. Update Deployment

Use the deployment script. It pulls the branch, reinstalls dependencies, runs
`doctor` and the test suite, restarts the admin service, and verifies the
runtime (admin health, factor registry, MCP tools import):

```bash
cd ~/Works/finance-pi
bash ops/deploy.sh
```

After a release that changes price adjustment or gold semantics, run the
one-time full rebuild variant (backgrounds `build all` + `catalog build` and
prints the log path):

```bash
bash ops/deploy.sh --full-rebuild        # optionally add --pit
```

From a Windows workstation, the same flow runs over SSH (piping the script, so
it works even before this commit is on the server):

```powershell
powershell -File ops\deploy-from-windows.ps1 -SetupKey   # once: install an SSH key
powershell -File ops\deploy-from-windows.ps1             # deploy + verify
powershell -File ops\deploy-from-windows.ps1 -FullRebuild
```

If the server tree has uncommitted local changes, the script aborts with the
file list; rerun with `--stash` (wrapper: `-Stash`) to set them aside as a
labeled stash, or commit them to a branch first.

The manual equivalent remains:

```bash
cd ~/Works/finance-pi
git pull
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest
systemctl --user restart finance-pi-admin.service
```

## Current Runtime Scope

The scheduled `daily` command initializes the data lake, refreshes the OpenDART
company snapshot, ingests the Naver same-day market summary, attempts KIS
universe price ingest and OpenDART filing/financial ingest when keys are
configured, rebuilds Silver/Gold datasets (including corporate actions,
preferred-share discount, and NPS delta), rebuilds the DuckDB view catalog, and
writes DQ/Fraud reports with the dataset reliability scorecard. Each run leaves
a completeness marker (`complete` / `complete_with_failures` / `failed`) under
`data/_state/daily/`, checked by a gold price-quality gate; catch-up re-runs
incomplete dates. With `FINANCE_PI_WEBHOOK_URL` set in `.env`, failing runs or
C/F dataset grades POST a Slack/Discord-compatible alert.

The `admin` command serves a local operations dashboard with dataset status,
recent reports, backtest artifacts, and allowlisted job buttons for daily runs,
builds, catalog refreshes, historical backfills, reports, and backtests. Job
execution (`POST /api/jobs`) always requires the admin token unless the client
is loopback; read-only GET endpoints remain open to the private LAN. The full
API reference is served at `/api/docs`.
