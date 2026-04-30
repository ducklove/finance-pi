# ruff: noqa: E501
from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from glob import glob
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import polars as pl

from finance_pi.config import ProjectPaths, load_dotenv
from finance_pi.storage import dataset_registry

INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>finance-pi Admin</title>
  <link rel="stylesheet" href="/assets/admin.css">
</head>
<body>
  <div class="shell">
    <aside class="rail">
      <div class="brand">
        <span class="brand-mark">fp</span>
        <div>
          <strong>finance-pi</strong>
          <span>Admin</span>
        </div>
      </div>
      <nav>
        <a href="#overview" class="active">Overview</a>
        <a href="#datasets">Datasets</a>
        <a href="#jobs">Jobs</a>
        <a href="#docs">Docs</a>
        <a href="#research">Research</a>
      </nav>
      <div class="rail-foot">
        <span id="server-clock">--</span>
      </div>
    </aside>

    <main>
      <header class="topbar">
        <div>
          <p class="eyebrow">Local Operations</p>
          <h1>finance-pi Admin</h1>
        </div>
        <div class="top-actions">
          <button class="icon-button" id="refresh-button" title="Refresh">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 11a8 8 0 0 0-14.7-4.4L3 9m0 0V3m0 6h6M4 13a8 8 0 0 0 14.7 4.4L21 15m0 0v6m0-6h-6"/></svg>
          </button>
        </div>
      </header>

      <section class="status-grid" id="overview">
        <article class="metric">
          <span>Catalog</span>
          <strong id="catalog-status">--</strong>
          <small id="catalog-path">--</small>
        </article>
        <article class="metric">
          <span>Datasets</span>
          <strong id="dataset-count">--</strong>
          <small id="dataset-files">--</small>
        </article>
        <article class="metric">
          <span>Rows</span>
          <strong id="row-count">--</strong>
          <small>known parquet rows</small>
        </article>
        <article class="metric">
          <span>Active Jobs</span>
          <strong id="active-jobs">--</strong>
          <small id="last-refresh">--</small>
        </article>
      </section>

      <section class="command-band" aria-label="Commands">
        <button data-action="build_all">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 19h16M4 15h10M4 11h16M4 7h10"/></svg>
          Build All
        </button>
        <button data-action="catalog_build">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h16v14H4zM8 5v14M4 10h16"/></svg>
          Catalog
        </button>
        <button data-action="daily_no_ingest">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5h14v14H5zM8 9h8M8 13h5"/></svg>
          Daily Dry
        </button>
        <button data-action="daily">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg>
          Daily Live
        </button>
        <button data-action="reports">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 19V5h14v14zM8 15h8M8 11h8M8 7h4"/></svg>
          Reports
        </button>
        <button data-action="docs_build">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 4h10l4 4v12H5zM14 4v5h5M8 13h8M8 17h6"/></svg>
          Docs
        </button>
      </section>

      <section class="split">
        <div class="panel" id="datasets">
          <div class="panel-head">
            <div>
              <p class="eyebrow">Lakehouse</p>
              <h2>Dataset Health</h2>
            </div>
            <span class="pill" id="data-root">--</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Dataset</th>
                  <th>Layer</th>
                  <th>Rows</th>
                  <th>Files</th>
                  <th>Latest</th>
                  <th>Size</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody id="dataset-body"></tbody>
            </table>
          </div>
        </div>

        <div class="stack">
          <section class="panel" id="jobs">
            <div class="panel-head">
              <div>
                <p class="eyebrow">Queue</p>
                <h2>Jobs</h2>
              </div>
              <span class="pill" id="job-count">--</span>
            </div>
            <div id="jobs-list" class="jobs-list"></div>
          </section>

          <section class="panel" id="research">
            <div class="panel-head">
              <div>
                <p class="eyebrow">Research</p>
                <h2>Backtest</h2>
              </div>
            </div>
            <form id="backtest-form" class="backtest-form">
              <label>Factor<select name="factor">
                <option value="momentum_12_1">momentum_12_1</option>
                <option value="value_earnings_yield">value_earnings_yield</option>
                <option value="quality_roa">quality_roa</option>
              </select></label>
              <label>Start<input name="start" type="date" value="2024-01-01"></label>
              <label>End<input name="end" type="date"></label>
              <label>Top<input name="top_fraction" type="number" step="0.01" min="0.01" max="1" value="0.10"></label>
              <button type="submit">Run</button>
            </form>
            <div id="backtests-list" class="run-list"></div>
          </section>
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Reports</p>
            <h2>Latest Artifacts</h2>
          </div>
        </div>
        <div id="reports-list" class="artifact-grid"></div>
      </section>

      <section class="panel" id="docs">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Published</p>
            <h2>Documentation</h2>
          </div>
          <a class="pill" id="docs-open" href="/docs/" target="_blank" rel="noreferrer">Open docs</a>
        </div>
        <div id="docs-list" class="artifact-grid"></div>
      </section>
    </main>
  </div>

  <dialog id="log-dialog">
    <div class="dialog-head">
      <strong id="log-title">Job Log</strong>
      <button class="icon-button" id="close-log" title="Close">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>
      </button>
    </div>
    <pre id="job-log"></pre>
  </dialog>

  <script src="/assets/admin.js"></script>
</body>
</html>
"""

ADMIN_CSS = """
:root {
  color-scheme: light;
  --bg: #f6f4ef;
  --panel: #fffdf8;
  --ink: #1c1f24;
  --muted: #667085;
  --line: #ded8cd;
  --blue: #2454a6;
  --green: #13795b;
  --gold: #a96908;
  --red: #b42318;
  --shadow: 0 14px 34px rgba(42, 32, 20, 0.10);
}

* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  background: var(--bg);
  color: var(--ink);
  font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
button, input, select { font: inherit; }
.shell { display: grid; grid-template-columns: 236px 1fr; min-height: 100vh; }
.rail {
  padding: 22px 18px;
  border-right: 1px solid var(--line);
  background: #ece7dc;
  display: flex;
  flex-direction: column;
  gap: 28px;
}
.brand { display: flex; align-items: center; gap: 12px; }
.brand-mark {
  width: 42px; height: 42px; border-radius: 8px;
  display: grid; place-items: center;
  color: white; background: #1d3f32; font-weight: 800; letter-spacing: 0;
}
.brand strong, .brand span { display: block; }
.brand span { color: var(--muted); font-size: 12px; }
nav { display: grid; gap: 6px; }
nav a {
  color: #2c3137; text-decoration: none; padding: 10px 12px;
  border-radius: 7px; border: 1px solid transparent;
}
nav a.active, nav a:hover { background: #fffdf8; border-color: var(--line); }
.rail-foot { margin-top: auto; color: var(--muted); font-size: 12px; }
main { padding: 24px; display: grid; gap: 18px; min-width: 0; }
.topbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.eyebrow { margin: 0 0 4px; color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .08em; }
h1, h2 { margin: 0; letter-spacing: 0; }
h1 { font-size: 28px; }
h2 { font-size: 18px; }
.top-actions { display: flex; gap: 8px; }
.icon-button {
  width: 40px; height: 40px; border-radius: 8px;
  border: 1px solid var(--line); background: var(--panel);
  display: grid; place-items: center; cursor: pointer;
}
svg { width: 18px; height: 18px; fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
.status-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
.metric, .panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.metric { padding: 16px; display: grid; gap: 4px; min-width: 0; }
.metric span { color: var(--muted); }
.metric strong { font-size: 24px; }
.metric small { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.command-band {
  display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px;
}
.command-band button, .backtest-form button {
  height: 42px; border: 1px solid #1d3f32; background: #1d3f32; color: white;
  border-radius: 8px; display: inline-flex; align-items: center; justify-content: center;
  gap: 8px; cursor: pointer; font-weight: 700;
}
.command-band button:nth-child(2) { background: var(--blue); border-color: var(--blue); }
.command-band button:nth-child(3) { background: var(--gold); border-color: var(--gold); }
.command-band button:nth-child(4) { background: #6938a1; border-color: #6938a1; }
.command-band button:nth-child(5) { background: var(--green); border-color: var(--green); }
.command-band button:nth-child(6) { background: #3f5661; border-color: #3f5661; }
.split { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(340px, .85fr); gap: 18px; align-items: start; }
.stack { display: grid; gap: 18px; }
.panel { padding: 16px; min-width: 0; }
.panel-head { display: flex; justify-content: space-between; align-items: start; gap: 12px; margin-bottom: 14px; }
.pill {
  max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  padding: 5px 8px; border: 1px solid var(--line); border-radius: 999px; color: var(--muted);
  background: #f8f4eb; font-size: 12px;
}
.table-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; min-width: 760px; }
th, td { padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }
th { font-size: 12px; color: var(--muted); background: #f2eee5; position: sticky; top: 0; }
tr:last-child td { border-bottom: 0; }
.status { font-weight: 800; }
.status.ready, .status.done { color: var(--green); }
.status.empty, .status.running { color: var(--gold); }
.status.failed { color: var(--red); }
.bar { height: 6px; background: #e6ded0; border-radius: 999px; overflow: hidden; margin-top: 5px; }
.bar span { display: block; height: 100%; background: var(--blue); min-width: 2px; }
.jobs-list, .run-list { display: grid; gap: 8px; }
.job, .artifact {
  border: 1px solid var(--line); border-radius: 8px; padding: 10px;
  display: grid; gap: 6px; background: #fffaf0;
}
.job-row { display: flex; justify-content: space-between; gap: 10px; align-items: center; }
.job button {
  border: 1px solid var(--line); background: white; border-radius: 7px; padding: 6px 9px; cursor: pointer;
}
.backtest-form { display: grid; grid-template-columns: 1.2fr 1fr 1fr .7fr auto; gap: 8px; align-items: end; }
.backtest-form label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
.backtest-form input, .backtest-form select {
  height: 38px; border: 1px solid var(--line); border-radius: 7px; padding: 0 9px; background: white;
}
.artifact-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
.artifact a { color: var(--blue); font-weight: 800; text-decoration: none; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.artifact span, .job small { color: var(--muted); }
dialog {
  width: min(920px, calc(100vw - 32px)); border: 1px solid var(--line);
  border-radius: 8px; padding: 0; box-shadow: var(--shadow);
}
dialog::backdrop { background: rgba(28,31,36,.28); }
.dialog-head { display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; border-bottom: 1px solid var(--line); }
pre { margin: 0; padding: 14px; max-height: 65vh; overflow: auto; background: #111827; color: #e5e7eb; font-size: 12px; }
@media (max-width: 980px) {
  .shell { grid-template-columns: 1fr; }
  .rail { position: static; flex-direction: row; align-items: center; overflow-x: auto; }
  nav { display: flex; }
  .rail-foot { margin-top: 0; margin-left: auto; }
  .status-grid, .command-band, .split, .artifact-grid { grid-template-columns: 1fr; }
  .backtest-form { grid-template-columns: 1fr 1fr; }
  .backtest-form button { grid-column: 1 / -1; }
}
"""

ADMIN_JS = """
const state = { overview: null, selectedJob: null };
const fmt = new Intl.NumberFormat();

function qs(id) { return document.getElementById(id); }
function shortDate(value) { return value ? String(value).replace('T', ' ').slice(0, 19) : '--'; }
function bytes(value) {
  if (!value) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value; let idx = 0;
  while (size >= 1024 && idx < units.length - 1) { size /= 1024; idx++; }
  return `${size.toFixed(idx ? 1 : 0)} ${units[idx]}`;
}

async function api(path, options = {}) {
  const token = adminToken();
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['X-Admin-Token'] = token;
  const response = await fetch(path, {
    headers,
    ...options,
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function adminToken() {
  const params = new URLSearchParams(window.location.search);
  const token = params.get('token');
  if (token) {
    localStorage.setItem('financePiAdminToken', token);
    window.history.replaceState({}, document.title, window.location.pathname);
    return token;
  }
  return localStorage.getItem('financePiAdminToken') || '';
}

async function refresh() {
  const overview = await api('/api/overview');
  state.overview = overview;
  renderOverview(overview);
}

function renderOverview(data) {
  qs('server-clock').textContent = shortDate(data.generated_at);
  qs('catalog-status').textContent = data.catalog.exists ? 'Ready' : 'Missing';
  qs('catalog-path').textContent = data.catalog.path;
  qs('dataset-count').textContent = `${data.datasets.filter(d => d.files > 0).length}/${data.datasets.length}`;
  qs('dataset-files').textContent = `${fmt.format(data.datasets.reduce((n, d) => n + d.files, 0))} files`;
  qs('row-count').textContent = fmt.format(data.datasets.reduce((n, d) => n + (d.rows || 0), 0));
  qs('active-jobs').textContent = data.jobs.filter(j => j.status === 'running').length;
  qs('last-refresh').textContent = shortDate(data.generated_at);
  qs('data-root').textContent = data.data_root;
  renderDatasets(data.datasets);
  renderJobs(data.jobs);
  renderReports(data.reports);
  renderDocs(data.docs);
  renderBacktests(data.backtests);
  const maxDate = data.max_price_date || new Date().toISOString().slice(0, 10);
  document.querySelector('#backtest-form [name=end]').value = maxDate;
}

function renderDatasets(datasets) {
  const maxRows = Math.max(...datasets.map(d => d.rows || 0), 1);
  qs('dataset-body').innerHTML = datasets.map(d => `
    <tr>
      <td><strong>${d.name}</strong><div class="bar"><span style="width:${Math.max(2, (d.rows || 0) / maxRows * 100)}%"></span></div></td>
      <td>${d.layer}</td>
      <td>${fmt.format(d.rows || 0)}</td>
      <td>${fmt.format(d.files)}</td>
      <td>${d.latest_partition || '--'}</td>
      <td>${bytes(d.bytes)}</td>
      <td class="status ${d.status}">${d.status}</td>
    </tr>`).join('');
}

function renderJobs(jobs) {
  qs('job-count').textContent = `${jobs.length}`;
  qs('jobs-list').innerHTML = jobs.length ? jobs.map(job => `
    <div class="job">
      <div class="job-row">
        <strong>${job.label}</strong>
        <span class="status ${job.status}">${job.status}</span>
      </div>
      <small>${shortDate(job.started_at)}${job.ended_at ? ' - ' + shortDate(job.ended_at) : ''}</small>
      <div class="job-row">
        <small>${job.command}</small>
        <button data-log="${job.id}">Log</button>
      </div>
    </div>`).join('') : '<div class="artifact"><span>No jobs yet</span></div>';
  document.querySelectorAll('[data-log]').forEach(button => button.addEventListener('click', () => openLog(button.dataset.log)));
}

function renderReports(reports) {
  qs('reports-list').innerHTML = reports.length ? reports.map(report => `
    <div class="artifact">
      <a href="${withTokenUrl(report.url)}" target="_blank" rel="noreferrer">${report.name}</a>
      <span>${report.kind}</span>
      <span>${shortDate(report.modified_at)}</span>
    </div>`).join('') : '<div class="artifact"><span>No reports yet</span></div>';
}

function renderDocs(docs) {
  qs('docs-list').innerHTML = docs.length ? docs.map(doc => `
    <div class="artifact">
      <a href="${doc.url}" target="_blank" rel="noreferrer">${doc.title}</a>
      <span>${doc.source}</span>
      <span>${shortDate(doc.modified_at)}</span>
    </div>`).join('') : '<div class="artifact"><span>No published docs yet</span></div>';
}

function renderBacktests(runs) {
  qs('backtests-list').innerHTML = runs.length ? runs.map(run => `
    <div class="artifact">
      <a href="${withTokenUrl(run.url)}" target="_blank" rel="noreferrer">${run.name}</a>
      <span>${run.nav_rows} nav rows${run.final_nav ? ' / NAV ' + run.final_nav.toFixed(4) : ''}</span>
    </div>`).join('') : '<div class="artifact"><span>No backtests yet</span></div>';
}

function withTokenUrl(url) {
  if (!url || url === '#') return '#';
  const token = adminToken();
  if (!token) return url;
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}token=${encodeURIComponent(token)}`;
}

async function startAction(action, payload = {}) {
  await api('/api/jobs', { method: 'POST', body: JSON.stringify({ action, ...payload }) });
  await refresh();
}

async function openLog(id) {
  const data = await api(`/api/jobs/${id}/log`);
  qs('log-title').textContent = data.label;
  qs('job-log').textContent = data.log || '(empty)';
  qs('log-dialog').showModal();
}

document.querySelectorAll('[data-action]').forEach(button => {
  button.addEventListener('click', () => startAction(button.dataset.action));
});
qs('refresh-button').addEventListener('click', refresh);
qs('close-log').addEventListener('click', () => qs('log-dialog').close());
qs('backtest-form').addEventListener('submit', event => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  startAction('backtest', Object.fromEntries(form.entries()));
});
refresh();
setInterval(refresh, 5000);
"""


@dataclass
class AdminJob:
    id: str
    action: str
    label: str
    command: list[str]
    log_path: Path
    status: str = "queued"
    returncode: int | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "action": self.action,
            "label": self.label,
            "command": " ".join(self.command),
            "status": self.status,
            "returncode": self.returncode,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }


class AdminState:
    def __init__(self, root: Path, token: str | None = None) -> None:
        self.paths = ProjectPaths(root=root)
        self.token = token
        self.jobs: dict[str, AdminJob] = {}
        self.lock = threading.Lock()

    def overview(self) -> dict[str, Any]:
        data_root = self.paths.data_root
        datasets = [_dataset_stat(data_root, name, spec) for name, spec in dataset_registry.items()]
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "workspace": str(self.paths.root.resolve()),
            "data_root": str(data_root.resolve()),
            "catalog": {
                "path": str(self.paths.catalog_path),
                "exists": self.paths.catalog_path.exists(),
            },
            "datasets": datasets,
            "reports": _report_artifacts(data_root),
            "docs": _docs_artifacts(data_root),
            "backtests": _backtest_runs(data_root),
            "jobs": self.job_list(),
            "max_price_date": _latest_partition_for(data_root / "gold/daily_prices_adj/dt=*/part.parquet"),
        }

    def job_list(self) -> list[dict[str, Any]]:
        with self.lock:
            jobs = list(self.jobs.values())
        return [job.to_dict() for job in sorted(jobs, key=lambda item: item.started_at, reverse=True)]

    def start_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action", ""))
        label, command = _job_command(action, payload, self.paths.root)
        job_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        log_dir = self.paths.data_root / "admin" / "jobs"
        log_dir.mkdir(parents=True, exist_ok=True)
        job = AdminJob(
            id=job_id,
            action=action,
            label=label,
            command=command,
            log_path=log_dir / f"{job_id}.log",
        )
        with self.lock:
            self.jobs[job.id] = job
        threading.Thread(target=self._run_job, args=(job,), daemon=True).start()
        return job.to_dict()

    def job_log(self, job_id: str) -> dict[str, Any]:
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if not job.log_path.exists():
            log = ""
        else:
            log = job.log_path.read_text(encoding="utf-8", errors="replace")[-120_000:]
        result = job.to_dict()
        result["log"] = log
        return result

    def _run_job(self, job: AdminJob) -> None:
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        with self.lock:
            job.status = "running"
        try:
            with job.log_path.open("w", encoding="utf-8", errors="replace") as log:
                process = subprocess.Popen(
                    job.command,
                    cwd=self.paths.root,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                )
                if process.stdout is not None:
                    for line in process.stdout:
                        log.write(line)
                        log.flush()
                returncode = process.wait()
            with self.lock:
                job.returncode = returncode
                job.status = "done" if returncode == 0 else "failed"
                job.ended_at = datetime.now(UTC)
        except Exception as exc:  # noqa: BLE001
            job.log_path.write_text(str(exc), encoding="utf-8")
            with self.lock:
                job.status = "failed"
                job.ended_at = datetime.now(UTC)


def run_admin(
    root: Path,
    host: str = "0.0.0.0",
    port: int = 8400,
    token: str | None = None,
) -> None:
    load_dotenv(root / ".env")
    auth_token = token or os.environ.get("FINANCE_PI_ADMIN_TOKEN") or secrets.token_urlsafe(24)
    state = AdminState(root.resolve(), auth_token)
    handler = _handler_for(state)
    server = ThreadingHTTPServer((host, port), handler)
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    print(f"finance-pi admin bind: http://{host}:{port}")
    print(f"finance-pi admin url:  http://{display_host}:{port}/?token={auth_token}")
    print(f"finance-pi health:     http://{display_host}:{port}/api/health")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nadmin stopped")
    finally:
        server.server_close()


def _handler_for(state: AdminState) -> type[BaseHTTPRequestHandler]:
    class AdminHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/health":
                    self._send_json(_health_payload(state))
                elif parsed.path in {"/docs", "/docs/"}:
                    self._serve_docs("index.html")
                elif parsed.path.startswith("/docs/"):
                    self._serve_docs(parsed.path.removeprefix("/docs/"))
                elif parsed.path == "/":
                    self._send_text(INDEX_HTML, "text/html; charset=utf-8")
                elif parsed.path == "/assets/admin.css":
                    self._send_text(ADMIN_CSS, "text/css; charset=utf-8")
                elif parsed.path == "/assets/admin.js":
                    self._send_text(ADMIN_JS, "application/javascript; charset=utf-8")
                elif parsed.path == "/api/overview":
                    if not self._authorized():
                        return
                    self._send_json(state.overview())
                elif parsed.path == "/api/jobs":
                    if not self._authorized():
                        return
                    self._send_json({"jobs": state.job_list()})
                elif parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/log"):
                    if not self._authorized():
                        return
                    job_id = parsed.path.split("/")[3]
                    self._send_json(state.job_log(job_id))
                elif parsed.path == "/files":
                    if not self._authorized():
                        return
                    self._serve_file(parse_qs(parsed.query).get("path", [""])[0])
                else:
                    self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            except KeyError:
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            except Exception as exc:  # noqa: BLE001
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/api/jobs":
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                return
            if not self._authorized():
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                self._send_json(state.start_job(payload), status=HTTPStatus.CREATED)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _serve_file(self, value: str) -> None:
            path = Path(value).resolve()
            allowed_roots = [
                (state.paths.data_root / "reports").resolve(),
                (state.paths.data_root / "backtests").resolve(),
                (state.paths.data_root / "docs_site").resolve(),
            ]
            if not any(path == root or root in path.parents for root in allowed_roots):
                self._send_json({"error": "forbidden"}, status=HTTPStatus.FORBIDDEN)
                return
            if not path.exists() or not path.is_file():
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                return
            content_type = _content_type(path)
            self._send_bytes(path.read_bytes(), content_type)

        def _serve_docs(self, relative: str) -> None:
            docs_root = (state.paths.data_root / "docs_site").resolve()
            path = (docs_root / relative).resolve()
            if not (path == docs_root or docs_root in path.parents):
                self._send_json({"error": "forbidden"}, status=HTTPStatus.FORBIDDEN)
                return
            if not path.exists() or not path.is_file():
                self._send_json({"error": "docs have not been built"}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_bytes(path.read_bytes(), _content_type(path))

        def _authorized(self) -> bool:
            if state.token is None:
                return True
            header = self.headers.get("X-Admin-Token", "")
            query = parse_qs(urlparse(self.path).query).get("token", [""])[0]
            if secrets.compare_digest(header or query, state.token):
                return True
            self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return False

        def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self._send_bytes(body, "application/json; charset=utf-8", status)

        def _send_text(
            self,
            payload: str,
            content_type: str,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            self._send_bytes(payload.encode("utf-8"), content_type, status)

        def _send_bytes(
            self,
            payload: bytes,
            content_type: str,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

    return AdminHandler


def _health_payload(state: AdminState) -> dict[str, Any]:
    return {
        "status": "ok",
        "generated_at": datetime.now(UTC).isoformat(),
        "workspace": str(state.paths.root.resolve()),
        "data_root": str(state.paths.data_root.resolve()),
        "auth": "token",
    }


def _job_command(action: str, payload: dict[str, Any], root: Path) -> tuple[str, list[str]]:
    base = [sys.executable, "-m", "finance_pi.cli.app"]
    root_args = ["--root", str(root)]
    if action == "build_all":
        return "Build All", [*base, "build", "all", *root_args]
    if action == "catalog_build":
        return "Catalog Build", [*base, "catalog", "build", *root_args]
    if action == "daily_no_ingest":
        return "Daily Dry Run", [*base, "daily", *root_args, "--no-ingest"]
    if action == "daily":
        return "Daily Live", [*base, "daily", *root_args]
    if action == "reports":
        report_date = str(payload.get("report_date") or datetime.now(UTC).date().isoformat())
        return "Reports", [*base, "reports", "all", "--report-date", report_date, *root_args]
    if action == "docs_build":
        return "Build Docs", [*base, "docs", "build", *root_args]
    if action == "backtest":
        factor = _safe_choice(
            payload.get("factor"),
            {"momentum_12_1", "value_earnings_yield", "quality_roa"},
            "momentum_12_1",
        )
        start = _safe_date(payload.get("start"), "2024-01-01")
        end = _safe_date(payload.get("end"), datetime.now(UTC).date().isoformat())
        top_fraction = _safe_float(payload.get("top_fraction"), "0.10")
        return (
            f"Backtest {factor}",
            [
                *base,
                "backtest",
                "run",
                "--factor",
                factor,
                "--start",
                start,
                "--end",
                end,
                "--top-fraction",
                top_fraction,
                *root_args,
            ],
        )
    raise ValueError(f"unknown action: {action}")


def _safe_choice(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or default)
    return text if text in allowed else default


def _safe_date(value: Any, default: str) -> str:
    text = str(value or default)
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return default
    return text


def _safe_float(value: Any, default: str) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return str(min(1.0, max(0.01, number)))


def _dataset_stat(data_root: Path, name: str, spec: Any) -> dict[str, Any]:
    files = [Path(file) for file in glob(spec.glob_path(data_root).as_posix())]
    size = sum(file.stat().st_size for file in files if file.exists())
    rows = _row_count(files, spec.hive_partitioning) if files else 0
    latest = _latest_partition(files)
    return {
        "name": name,
        "layer": spec.layer,
        "files": len(files),
        "rows": rows,
        "bytes": size,
        "latest_partition": latest,
        "status": "ready" if files else "empty",
    }


def _row_count(files: list[Path], hive_partitioning: bool) -> int:
    try:
        frame = pl.scan_parquet(
            [file.as_posix() for file in files],
            hive_partitioning=hive_partitioning,
        ).select(pl.len().alias("rows"))
        return int(frame.collect().item())
    except Exception:  # noqa: BLE001
        return sum(_row_count_one(file, hive_partitioning) for file in files)


def _row_count_one(file: Path, hive_partitioning: bool) -> int:
    try:
        frame = pl.scan_parquet(
            file.as_posix(),
            hive_partitioning=hive_partitioning,
        ).select(pl.len().alias("rows"))
        return int(frame.collect().item())
    except Exception:  # noqa: BLE001
        return 0


def _latest_partition(files: list[Path]) -> str | None:
    values: list[str] = []
    for file in files:
        for part in file.parts:
            if "=" in part:
                values.append(part)
    return sorted(values)[-1] if values else None


def _latest_partition_for(pattern: Path) -> str | None:
    files = [Path(file) for file in glob(pattern.as_posix())]
    latest = _latest_partition(files)
    if latest is None or "=" not in latest:
        return None
    return latest.split("=", maxsplit=1)[1]


def _report_artifacts(data_root: Path) -> list[dict[str, Any]]:
    roots = [
        ("Data Quality", data_root / "reports" / "data_quality"),
        ("Fraud", data_root / "reports" / "backtest_fraud"),
    ]
    artifacts: list[dict[str, Any]] = []
    for kind, root in roots:
        for path in sorted(root.glob("*.html"), key=lambda item: item.stat().st_mtime, reverse=True):
            artifacts.append(
                {
                    "kind": kind,
                    "name": path.name,
                    "path": str(path),
                    "url": f"/files?path={quote(str(path.resolve()))}",
                    "modified_at": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(),
                }
            )
    return sorted(artifacts, key=lambda item: item["modified_at"], reverse=True)[:8]


def _backtest_runs(data_root: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in sorted((data_root / "backtests").glob("*"), key=_mtime, reverse=True):
        if not path.is_dir():
            continue
        nav_path = path / "nav.parquet"
        nav_rows = 0
        final_nav = None
        if nav_path.exists():
            try:
                nav = pl.read_parquet(nav_path)
                nav_rows = nav.height
                if nav_rows and "nav" in nav.columns:
                    final_nav = float(nav["nav"][-1])
            except Exception:  # noqa: BLE001
                pass
        runs.append(
            {
                "name": path.name,
                "path": str(path),
                "url": f"/files?path={quote(str(nav_path.resolve()))}" if nav_path.exists() else "#",
                "nav_rows": nav_rows,
                "final_nav": final_nav,
                "modified_at": datetime.fromtimestamp(_mtime(path), UTC).isoformat(),
            }
        )
    return runs[:8]


def _docs_artifacts(data_root: Path) -> list[dict[str, Any]]:
    manifest = data_root / "docs_site" / "manifest.json"
    if not manifest.exists():
        return []
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    pages = data.get("pages", [])
    if not isinstance(pages, list):
        return []
    artifacts: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        output = str(page.get("output", ""))
        path = data_root / "docs_site" / output
        if not path.exists():
            continue
        artifacts.append(
            {
                "title": str(page.get("title") or output),
                "source": str(page.get("source") or ""),
                "url": f"/docs/{quote(output)}",
                "modified_at": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(),
            }
        )
    return artifacts


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _content_type(path: Path) -> str:
    if path.suffix == ".html":
        return "text/html; charset=utf-8"
    if path.suffix == ".css":
        return "text/css; charset=utf-8"
    if path.suffix == ".js":
        return "application/javascript; charset=utf-8"
    if path.suffix == ".json":
        return "application/json; charset=utf-8"
    if path.suffix == ".parquet":
        return "application/octet-stream"
    return "text/plain; charset=utf-8"
