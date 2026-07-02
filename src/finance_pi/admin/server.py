# ruff: noqa: E501
from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from glob import glob
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import duckdb
import polars as pl

from finance_pi.config import ProjectPaths, load_dotenv
from finance_pi.docs_site import build_docs_site
from finance_pi.sources.parsing import share_class_from_stock_kind
from finance_pi.storage import dataset_registry

DEFAULT_MAX_REQUEST_THREADS = 16
DEFAULT_MAX_ADMIN_JOBS = 1
DEFAULT_MAX_PRICE_QUERIES = 4
DEFAULT_MAX_PRICE_TICKERS = 500
DEFAULT_MAX_PRICE_DAYS = 3700
DEFAULT_PRICE_QUERY_WAIT_SECONDS = 15.0
MAX_PRICE_CELLS = 200_000
MAX_JOBS_RETAINED = 50
MACRO_TABLE_COLUMNS = {
    "cpi": (
        "date",
        "country",
        "series_id",
        "name",
        "frequency",
        "value",
        "index_base",
        "yoy_pct",
        "mom_pct",
        "source",
        "updated_at",
    ),
    "rates": (
        "date",
        "country",
        "series_id",
        "name",
        "frequency",
        "tenor",
        "value",
        "unit",
        "source",
        "updated_at",
    ),
    "indices": (
        "date",
        "country",
        "series_id",
        "name",
        "frequency",
        "category",
        "value",
        "currency",
        "return_1d",
        "return_1m",
        "source",
        "updated_at",
    ),
    "commodities": (
        "date",
        "series_id",
        "name",
        "commodity",
        "value",
        "unit",
        "currency",
        "source",
        "updated_at",
    ),
    "fx": (
        "date",
        "series_id",
        "base_currency",
        "quote_currency",
        "value",
        "source",
        "updated_at",
    ),
    "economic_indicators": (
        "date",
        "country",
        "series_id",
        "name",
        "category",
        "frequency",
        "value",
        "unit",
        "source",
        "updated_at",
    ),
}
BASIC_FUNDAMENTAL_METRICS = {
    "revenue": ("ifrs-full_Revenue", "ifrs_Revenue"),
    "operating_profit": ("dart_OperatingIncomeLoss",),
    "net_income": (
        "ifrs-full_ProfitLossAttributableToOwnersOfParent",
        "ifrs_ProfitLossAttributableToOwnersOfParent",
        "ifrs-full_ProfitLoss",
        "ifrs_ProfitLoss",
    ),
    "assets": ("ifrs-full_Assets", "ifrs_Assets"),
    "liabilities": ("ifrs-full_Liabilities", "ifrs_Liabilities"),
    "equity": (
        "ifrs-full_EquityAttributableToOwnersOfParent",
        "ifrs_EquityAttributableToOwnersOfParent",
        "ifrs-full_Equity",
        "ifrs_Equity",
    ),
    "cash_and_equivalents": (
        "ifrs-full_CashAndCashEquivalents",
        "ifrs_CashAndCashEquivalents",
    ),
    "operating_cash_flow": (
        "ifrs-full_CashFlowsFromUsedInOperatingActivities",
        "ifrs_CashFlowsFromUsedInOperatingActivities",
    ),
    "dividends_paid": (
        "ifrs-full_DividendsPaidClassifiedAsFinancingActivities",
        "ifrs_DividendsPaidClassifiedAsFinancingActivities",
        "ifrs-full_DividendsPaid",
        "ifrs_DividendsPaid",
        "dart_AnnualDividendsPaid",
    ),
    "treasury_share_purchase": (
        "dart_AcquisitionOfTreasuryShares",
        "ifrs-full_PurchaseOfTreasuryShares",
        "ifrs_PurchaseOfTreasuryShares",
    ),
    "treasury_share_sale": (
        "ifrs-full_SaleOrIssueOfTreasuryShares",
        "dart_DispositionOfTreasuryShares",
    ),
    "treasury_share_cancellation": ("ifrs-full_CancellationOfTreasuryShares",),
}
DIRECT_EPS_METRICS = {
    "eps_basic": (
        "ifrs-full_BasicEarningsLossPerShare",
        "ifrs_BasicEarningsLossPerShare",
    ),
}
CAPITAL_ACTION_METRICS = {
    key: BASIC_FUNDAMENTAL_METRICS[key]
    for key in (
        "dividends_paid",
        "treasury_share_purchase",
        "treasury_share_sale",
        "treasury_share_cancellation",
    )
}
DAILY_PRICE_FIELDS = {
    "open": "open_adj",
    "high": "high_adj",
    "low": "low_adj",
    "close": "close_adj",
    "return_1d": "return_1d",
    "volume": "volume",
    "trading_value": "trading_value",
    "market_cap": "market_cap",
    "listed_shares": "listed_shares",
}
DEFAULT_DAILY_PRICE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
)

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
        <a href="#backfill">Backfill</a>
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
          <span>Price Coverage</span>
          <strong id="coverage-start">--</strong>
          <small id="coverage-end">--</small>
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

      <section class="panel" id="backfill">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Historical Data</p>
            <h2>Yearly Backfill</h2>
          </div>
          <span class="pill" id="backfill-summary">--</span>
        </div>
        <form id="backfill-form" class="backfill-form">
          <label>Start Year<input name="start_year" type="number" min="1900" max="2100" value="2023"></label>
          <label>End Year<input name="end_year" type="number" min="1900" max="2100" value="1990"></label>
          <label>Chunks<input name="max_years" type="number" min="0" max="50" value="1"></label>
          <label class="check"><input name="include_prices" type="checkbox" checked disabled> Naver prices</label>
          <label class="check"><input name="include_financials" type="checkbox" checked> DART financials</label>
          <label class="check"><input name="include_builds" type="checkbox" checked disabled> Silver/Gold builds</label>
          <label class="check"><input name="include_fundamentals_pit" type="checkbox"> Fundamentals PIT</label>
          <label class="check"><input name="no_strict" type="checkbox" checked> Continue on source errors</label>
          <label class="check"><input name="force" type="checkbox"> Force completed years</label>
          <button type="submit">Run Next</button>
          <button type="button" id="backfill-dry-run">Dry Run</button>
        </form>
        <div class="table-wrap backfill-status">
          <table>
            <thead>
              <tr>
                <th>Year</th>
                <th>Status</th>
                <th>Price Days</th>
                <th>Rows</th>
                <th>Coverage</th>
                <th>Marker</th>
              </tr>
            </thead>
            <tbody id="backfill-body"></tbody>
          </table>
        </div>
      </section>

      <section class="split">
        <div class="panel" id="datasets">
          <div class="panel-head">
          <div>
            <p class="eyebrow">Lakehouse</p>
            <h2>Dataset Health</h2>
          </div>
          <div class="panel-tools">
            <input class="filter-input" id="dataset-filter" type="search" placeholder="Filter datasets" aria-label="Filter datasets">
            <span class="pill" id="data-root">--</span>
          </div>
        </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Dataset</th>
                  <th>Layer</th>
                  <th>Rows</th>
                  <th>Files</th>
                  <th>Coverage</th>
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
  <div class="toast" id="toast" role="status" aria-live="polite"></div>

  <script src="/assets/admin.js"></script>
</body>
</html>
"""

ADMIN_CSS = """
:root {
  color-scheme: light;
  --bg: #f4f6f8;
  --panel: #ffffff;
  --panel-soft: #f9fafb;
  --ink: #111827;
  --muted: #667085;
  --line: #d9dee7;
  --line-soft: #edf0f5;
  --rail: #151a22;
  --rail-soft: #202733;
  --blue: #2563eb;
  --green: #0f766e;
  --amber: #b45309;
  --red: #b42318;
  --violet: #6d28d9;
  --shadow: 0 16px 40px rgba(17, 24, 39, 0.08);
  --focus: 0 0 0 3px rgba(37, 99, 235, 0.18);
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  min-height: 100vh;
  background:
    linear-gradient(180deg, rgba(255,255,255,.88), rgba(244,246,248,.94) 260px),
    var(--bg);
  color: var(--ink);
  font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
button, input, select { font: inherit; }
button, a, input, select { outline-color: transparent; }
button:focus-visible, a:focus-visible, input:focus-visible, select:focus-visible { box-shadow: var(--focus); }
.shell { display: grid; grid-template-columns: 244px minmax(0, 1fr); min-height: 100vh; }
.rail {
  position: sticky;
  top: 0;
  height: 100vh;
  padding: 22px 16px;
  background: var(--rail);
  color: white;
  display: flex;
  flex-direction: column;
  gap: 26px;
}
.brand { display: flex; align-items: center; gap: 12px; padding: 0 6px; }
.brand-mark {
  width: 42px;
  height: 42px;
  border-radius: 8px;
  display: grid;
  place-items: center;
  color: white;
  background: linear-gradient(135deg, #0f766e, #2563eb);
  font-weight: 800;
  letter-spacing: 0;
  box-shadow: 0 10px 22px rgba(15, 118, 110, .28);
}
.brand strong, .brand span { display: block; }
.brand strong { font-size: 15px; }
.brand span { color: #aab3c2; font-size: 12px; }
nav { display: grid; gap: 5px; }
nav a {
  color: #d8dee8;
  text-decoration: none;
  padding: 10px 11px;
  border-radius: 7px;
  border: 1px solid transparent;
  display: flex;
  align-items: center;
  min-height: 40px;
}
nav a.active, nav a:hover {
  color: white;
  background: var(--rail-soft);
  border-color: rgba(255,255,255,.08);
}
.rail-foot {
  margin-top: auto;
  padding: 10px 11px;
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 8px;
  color: #aab3c2;
  background: rgba(255,255,255,.04);
  font-size: 12px;
}
main {
  padding: 26px;
  display: grid;
  gap: 18px;
  min-width: 0;
  max-width: 1680px;
  width: 100%;
}
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}
.eyebrow {
  margin: 0 0 4px;
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: .08em;
}
h1, h2 { margin: 0; letter-spacing: 0; }
h1 { font-size: clamp(25px, 3vw, 34px); line-height: 1.1; }
h2 { font-size: 18px; line-height: 1.22; }
.top-actions { display: flex; gap: 8px; align-items: center; }
.icon-button {
  width: 40px;
  height: 40px;
  border-radius: 8px;
  border: 1px solid var(--line);
  background: var(--panel);
  color: #344054;
  display: grid;
  place-items: center;
  cursor: pointer;
  transition: border-color .16s ease, transform .16s ease, background .16s ease;
}
.icon-button:hover { border-color: #b6c0cf; transform: translateY(-1px); }
svg {
  width: 18px;
  height: 18px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
  flex: 0 0 auto;
}
.status-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
.metric, .panel {
  background: rgba(255,255,255,.94);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.metric {
  padding: 15px;
  display: grid;
  gap: 4px;
  min-width: 0;
  position: relative;
  overflow: hidden;
}
.metric::before {
  content: "";
  position: absolute;
  inset: 0 auto 0 0;
  width: 3px;
  background: var(--blue);
}
.metric:nth-child(2)::before { background: var(--green); }
.metric:nth-child(3)::before { background: var(--violet); }
.metric:nth-child(4)::before { background: var(--amber); }
.metric span { color: var(--muted); font-size: 12px; font-weight: 700; }
.metric strong { font-size: 25px; line-height: 1.1; }
.metric small { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.command-band {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 10px;
}
.command-band button, .backtest-form button, .backfill-form button {
  min-height: 42px;
  border: 1px solid #c8d0dc;
  background: var(--panel);
  color: #1f2937;
  border-radius: 8px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  cursor: pointer;
  font-weight: 800;
  transition: transform .16s ease, border-color .16s ease, background .16s ease, color .16s ease;
}
.command-band button:hover, .backtest-form button:hover, .backfill-form button:hover {
  transform: translateY(-1px);
  border-color: #9aa6b8;
  background: #f8fafc;
}
.command-band button:first-child {
  background: #111827;
  border-color: #111827;
  color: white;
}
.command-band button:nth-child(4) {
  background: var(--green);
  border-color: var(--green);
  color: white;
}
.command-band button.is-busy, .backfill-form button.is-busy {
  opacity: .72;
  cursor: progress;
  transform: none;
}
.split {
  display: grid;
  grid-template-columns: minmax(0, 1.7fr) minmax(340px, .82fr);
  gap: 18px;
  align-items: start;
}
.stack { display: grid; gap: 18px; }
.panel { padding: 16px; min-width: 0; }
.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: start;
  gap: 12px;
  margin-bottom: 14px;
}
.panel-tools { display: flex; align-items: center; gap: 8px; min-width: 0; }
.filter-input {
  width: min(250px, 42vw);
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 0 10px;
  background: var(--panel-soft);
  color: var(--ink);
}
.pill {
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 5px 9px;
  border: 1px solid var(--line);
  border-radius: 999px;
  color: #475467;
  background: var(--panel-soft);
  font-size: 12px;
  text-decoration: none;
}
a.pill:hover { border-color: #b6c0cf; color: var(--blue); }
.table-wrap {
  overflow: auto;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: white;
}
table { width: 100%; border-collapse: collapse; min-width: 800px; }
th, td {
  padding: 11px 12px;
  border-bottom: 1px solid var(--line-soft);
  text-align: left;
  white-space: nowrap;
}
th {
  font-size: 11px;
  color: #667085;
  background: #f8fafc;
  position: sticky;
  top: 0;
  z-index: 1;
  text-transform: uppercase;
  letter-spacing: .04em;
}
tbody tr:hover { background: #f9fbff; }
tr:last-child td { border-bottom: 0; }
.status {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 800;
  border: 1px solid transparent;
}
.status.ready, .status.done {
  color: #0f766e;
  background: #ecfdf3;
  border-color: #b7e4d1;
}
.status.empty, .status.running {
  color: #b45309;
  background: #fff7ed;
  border-color: #fed7aa;
}
.status.failed {
  color: var(--red);
  background: #fef3f2;
  border-color: #fecaca;
}
.bar {
  height: 5px;
  background: #edf0f5;
  border-radius: 999px;
  overflow: hidden;
  margin-top: 6px;
  width: min(170px, 100%);
}
.bar span { display: block; height: 100%; background: linear-gradient(90deg, var(--blue), var(--green)); min-width: 2px; }
.jobs-list, .run-list { display: grid; gap: 8px; }
.job, .artifact {
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 10px;
  display: grid;
  gap: 6px;
  background: #ffffff;
}
.job:hover, .artifact:hover { border-color: #b9c3d0; }
.job-row { display: flex; justify-content: space-between; gap: 10px; align-items: center; min-width: 0; }
.job-row small { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.job button {
  border: 1px solid var(--line);
  background: #f8fafc;
  border-radius: 7px;
  padding: 6px 9px;
  cursor: pointer;
  color: #344054;
}
.job button:hover { border-color: #b6c0cf; color: var(--blue); }
.backtest-form {
  display: grid;
  grid-template-columns: 1.2fr 1fr 1fr .7fr auto;
  gap: 8px;
  align-items: end;
}
.stack .backtest-form { grid-template-columns: 1fr 1fr; }
.stack .backtest-form label:first-child { grid-column: 1 / -1; }
.stack .backtest-form button { grid-column: 1 / -1; }
.backtest-form label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 700; }
.backtest-form input, .backtest-form select {
  height: 38px;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 0 9px;
  background: white;
  color: var(--ink);
}
.backtest-form button {
  background: var(--blue);
  border-color: var(--blue);
  color: white;
  padding: 0 14px;
}
.backfill-form {
  display: grid;
  grid-template-columns: repeat(3, minmax(130px, 1fr)) repeat(2, minmax(160px, 1fr));
  gap: 10px;
  align-items: end;
  margin-bottom: 14px;
}
.backfill-form label {
  display: grid;
  gap: 5px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}
.backfill-form input[type="number"] {
  height: 38px;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 0 9px;
  background: white;
  color: var(--ink);
}
.backfill-form .check {
  min-height: 38px;
  grid-template-columns: 18px minmax(0, 1fr);
  align-items: center;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 8px 10px;
  background: #f8fafc;
  color: #344054;
}
.backfill-form input[type="checkbox"] { width: 16px; height: 16px; margin: 0; }
.backfill-form button {
  background: var(--blue);
  border-color: var(--blue);
  color: white;
  padding: 0 14px;
}
.backfill-form button[type="button"] {
  background: #ffffff;
  border-color: #c8d0dc;
  color: #1f2937;
}
.backfill-status table { min-width: 720px; }
.artifact-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
.artifact a {
  color: #1d4ed8;
  font-weight: 800;
  text-decoration: none;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.artifact a:hover { text-decoration: underline; }
.artifact span, .job small { color: var(--muted); font-size: 12px; }
.toast {
  position: fixed;
  right: 18px;
  bottom: 18px;
  z-index: 10;
  max-width: min(420px, calc(100vw - 32px));
  padding: 12px 14px;
  border-radius: 8px;
  border: 1px solid var(--line);
  background: #111827;
  color: white;
  box-shadow: var(--shadow);
  opacity: 0;
  transform: translateY(8px);
  pointer-events: none;
  transition: opacity .16s ease, transform .16s ease;
}
.toast.show { opacity: 1; transform: translateY(0); }
dialog {
  width: min(920px, calc(100vw - 32px));
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 0;
  box-shadow: 0 28px 80px rgba(17,24,39,.22);
}
dialog::backdrop { background: rgba(17,24,39,.34); }
.dialog-head { display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; border-bottom: 1px solid var(--line); }
pre { margin: 0; padding: 14px; max-height: 65vh; overflow: auto; background: #0f172a; color: #e5e7eb; font-size: 12px; }
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition: none !important; }
}
@media (max-width: 1120px) {
  .status-grid, .artifact-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .command-band { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .backfill-form { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .split { grid-template-columns: 1fr; }
}
@media (max-width: 760px) {
  .shell { grid-template-columns: 1fr; }
  .rail {
    position: static;
    height: auto;
    flex-direction: row;
    align-items: center;
    overflow-x: auto;
    gap: 14px;
    padding: 14px;
  }
  .brand { min-width: 172px; }
  nav { display: flex; }
  nav a { white-space: nowrap; }
  .rail-foot { display: none; }
  main { padding: 16px; }
  .topbar { align-items: flex-start; }
  .status-grid, .command-band, .artifact-grid { grid-template-columns: 1fr; }
  .panel-head { flex-direction: column; }
  .panel-tools, .filter-input { width: 100%; }
  .backtest-form, .backfill-form { grid-template-columns: 1fr; }
}
"""

ADMIN_JS = """
const state = { overview: null, selectedJob: null, datasetFilter: '' };
const fmt = new Intl.NumberFormat();

function qs(id) { return document.getElementById(id); }
function html(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[char]);
}
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
  qs('coverage-start').textContent = data.price_coverage.start || '--';
  qs('coverage-end').textContent = data.price_coverage.end
    ? `to ${data.price_coverage.end}`
    : 'no price data yet';
  qs('active-jobs').textContent = data.jobs.filter(j => j.status === 'running').length;
  qs('last-refresh').textContent = shortDate(data.generated_at);
  qs('data-root').textContent = data.data_root;
  renderDatasets(data.datasets);
  renderJobs(data.jobs);
  renderReports(data.reports);
  renderDocs(data.docs);
  renderBacktests(data.backtests);
  renderBackfill(data.backfill);
  const maxDate = data.max_price_date || new Date().toISOString().slice(0, 10);
  document.querySelector('#backtest-form [name=end]').value = maxDate;
}

function renderDatasets(datasets) {
  const maxRows = Math.max(...datasets.map(d => d.rows || 0), 1);
  const needle = state.datasetFilter.trim().toLowerCase();
  const visible = needle
    ? datasets.filter(d => `${d.name} ${d.layer} ${d.latest_partition || ''}`.toLowerCase().includes(needle))
    : datasets;
  if (!visible.length) {
    qs('dataset-body').innerHTML = '<tr><td colspan="7">No matching datasets</td></tr>';
    return;
  }
  qs('dataset-body').innerHTML = visible.map(d => `
    <tr>
      <td><strong>${html(d.name)}</strong>${rowBar(d.rows, maxRows)}</td>
      <td>${html(d.layer)}</td>
      <td>${d.rows == null ? '--' : fmt.format(d.rows)}</td>
      <td>${fmt.format(d.files)}</td>
      <td>${html(coverageLabel(d))}</td>
      <td>${bytes(d.bytes)}</td>
      <td><span class="status ${html(d.status)}">${html(d.status)}</span></td>
    </tr>`).join('');
}

function rowBar(rows, maxRows) {
  if (rows == null) return '';
  return `<div class="bar"><span style="width:${Math.max(2, (rows || 0) / maxRows * 100)}%"></span></div>`;
}

function coverageLabel(dataset) {
  if (!dataset.coverage_start) return '--';
  if (dataset.coverage_start === dataset.coverage_end) return dataset.coverage_start;
  return `${dataset.coverage_start} - ${dataset.coverage_end}`;
}

function renderJobs(jobs) {
  qs('job-count').textContent = `${jobs.length}`;
  qs('jobs-list').innerHTML = jobs.length ? jobs.map(job => `
    <div class="job">
      <div class="job-row">
        <strong>${html(job.label)}</strong>
        <span class="status ${html(job.status)}">${html(job.status)}</span>
      </div>
      <small>${shortDate(job.started_at)}${job.ended_at ? ' - ' + shortDate(job.ended_at) : ''}</small>
      <div class="job-row">
        <small>${html(job.command)}</small>
        <button data-log="${html(job.id)}">Log</button>
      </div>
    </div>`).join('') : '<div class="artifact"><span>No jobs yet</span></div>';
  document.querySelectorAll('[data-log]').forEach(button => button.addEventListener('click', () => openLog(button.dataset.log)));
}

function renderReports(reports) {
  qs('reports-list').innerHTML = reports.length ? reports.map(report => `
    <div class="artifact">
      <a href="${html(withTokenUrl(report.url))}" target="_blank" rel="noreferrer">${html(report.name)}</a>
      <span>${html(report.kind)}</span>
      <span>${shortDate(report.modified_at)}</span>
    </div>`).join('') : '<div class="artifact"><span>No reports yet</span></div>';
}

function renderDocs(docs) {
  qs('docs-list').innerHTML = docs.length ? docs.map(doc => `
    <div class="artifact">
      <a href="${html(doc.url)}" target="_blank" rel="noreferrer">${html(doc.title)}</a>
      <span>${html(doc.source)}</span>
      <span>${shortDate(doc.modified_at)}</span>
    </div>`).join('') : '<div class="artifact"><span>No published docs yet</span></div>';
}

function renderBacktests(runs) {
  qs('backtests-list').innerHTML = runs.length ? runs.map(run => `
    <div class="artifact">
      <a href="${html(withTokenUrl(run.url))}" target="_blank" rel="noreferrer">${html(run.name)}</a>
      <span>${run.nav_rows} nav rows${run.final_nav ? ' / NAV ' + run.final_nav.toFixed(4) : ''}</span>
    </div>`).join('') : '<div class="artifact"><span>No backtests yet</span></div>';
}

function renderBackfill(backfill) {
  const items = (backfill && backfill.years) || [];
  const complete = items.filter(item => String(item.status || '').startsWith('complete')).length;
  qs('backfill-summary').textContent = items.length ? `${complete}/${items.length} years complete` : 'no status';
  qs('backfill-body').innerHTML = items.length ? items.map(item => `
    <tr>
      <td><strong>${html(item.year)}</strong></td>
      <td><span class="status ${statusClass(item.status)}">${html(item.status)}</span></td>
      <td>${fmt.format(item.price_days || 0)}</td>
      <td>${item.rows == null ? '--' : fmt.format(item.rows)}</td>
      <td>${html(item.coverage || '--')}</td>
      <td>${html(item.marker || '-')}</td>
    </tr>`).join('') : '<tr><td colspan="6">No backfill status yet</td></tr>';
}

function statusClass(status) {
  const text = String(status || '');
  if (text === 'complete') return 'ready';
  if (text === 'complete_with_failures') return 'running';
  if (text === 'partial') return 'running';
  if (text === 'missing') return 'empty';
  return text || 'empty';
}

function withTokenUrl(url) {
  if (!url || url === '#') return '#';
  const token = adminToken();
  if (!token) return url;
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}token=${encodeURIComponent(token)}`;
}

function showToast(message) {
  const toast = qs('toast');
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => toast.classList.remove('show'), 2800);
}

async function startAction(action, payload = {}, sourceButton = null) {
  if (sourceButton) {
    sourceButton.disabled = true;
    sourceButton.classList.add('is-busy');
  }
  try {
    const job = await api('/api/jobs', { method: 'POST', body: JSON.stringify({ action, ...payload }) });
    showToast(`${job.label} queued`);
    await refresh();
  } catch (error) {
    showToast(error.message || 'Action failed');
  } finally {
    if (sourceButton) {
      sourceButton.disabled = false;
      sourceButton.classList.remove('is-busy');
    }
  }
}

async function openLog(id) {
  const data = await api(`/api/jobs/${id}/log`);
  qs('log-title').textContent = data.label;
  qs('job-log').textContent = data.log || '(empty)';
  qs('log-dialog').showModal();
}

document.querySelectorAll('[data-action]').forEach(button => {
  button.addEventListener('click', () => startAction(button.dataset.action, {}, button));
});
document.querySelectorAll('nav a').forEach(link => {
  link.addEventListener('click', () => {
    document.querySelectorAll('nav a').forEach(item => item.classList.remove('active'));
    link.classList.add('active');
  });
});
qs('dataset-filter').addEventListener('input', event => {
  state.datasetFilter = event.currentTarget.value;
  if (state.overview) renderDatasets(state.overview.datasets);
});
qs('refresh-button').addEventListener('click', refresh);
qs('close-log').addEventListener('click', () => qs('log-dialog').close());
qs('backtest-form').addEventListener('submit', event => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  startAction('backtest', Object.fromEntries(form.entries()), event.currentTarget.querySelector('button'));
});
qs('backfill-form').addEventListener('submit', event => {
  event.preventDefault();
  startAction('backfill_yearly', backfillPayload(event.currentTarget, false), event.currentTarget.querySelector('button[type=submit]'));
});
qs('backfill-dry-run').addEventListener('click', event => {
  startAction('backfill_yearly', backfillPayload(qs('backfill-form'), true), event.currentTarget);
});

function backfillPayload(form, dryRun) {
  return {
    start_year: form.elements.start_year.value,
    end_year: form.elements.end_year.value,
    max_years: form.elements.max_years.value,
    include_financials: form.elements.include_financials.checked,
    include_fundamentals_pit: form.elements.include_fundamentals_pit.checked,
    no_strict: form.elements.no_strict.checked,
    force: form.elements.force.checked,
    dry_run: dryRun,
  };
}
refresh();
setInterval(refresh, 30000);
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
    overview_cache_seconds = 120.0
    price_cache_seconds = 60.0
    realtime_cache_seconds = 15.0
    quotes_cache_seconds = 15.0
    price_cache_max_entries = 512

    def __init__(self, root: Path, token: str | None = None) -> None:
        self.paths = ProjectPaths(root=root)
        self.token = token
        self.jobs: dict[str, AdminJob] = {}
        self.lock = threading.Lock()
        self._overview_cache: tuple[float, dict[str, Any]] | None = None
        self._price_cache: dict[
            tuple[Any, ...],
            tuple[float, dict[str, list[dict[str, Any]]]],
        ] = {}
        self._realtime_cache: dict[tuple[str | None, str | None], tuple[float, dict[str, Any]]] = {}
        self._quotes_cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]]]] = {}
        self._search_cache: dict[
            tuple[tuple[str, ...], int],
            tuple[float, dict[str, list[dict[str, Any]]]],
        ] = {}
        self._job_slots = threading.BoundedSemaphore(_admin_max_jobs())
        self._price_query_slots = threading.BoundedSemaphore(_admin_max_price_queries())

    def overview(self) -> dict[str, Any]:
        now = time.monotonic()
        with self.lock:
            if self._overview_cache is not None:
                cached_at, cached = self._overview_cache
                if now - cached_at < self.overview_cache_seconds:
                    return cached

        data_root = self.paths.data_root
        datasets = [_dataset_stat(data_root, name, spec) for name, spec in dataset_registry.items()]
        overview = {
            "generated_at": datetime.now(UTC).isoformat(),
            "workspace": str(self.paths.root.resolve()),
            "data_root": str(data_root.resolve()),
            "catalog": {
                "path": str(self.paths.catalog_path),
                "exists": self.paths.catalog_path.exists(),
            },
            "datasets": datasets,
            "price_coverage": _price_coverage(datasets),
            "reports": _report_artifacts(data_root),
            "docs": _docs_artifacts(data_root),
            "backtests": _backtest_runs(data_root),
            "backfill": _backfill_overview(data_root),
            "jobs": self.job_list(),
            "max_price_date": _latest_price_date_from_datasets(datasets),
        }
        with self.lock:
            self._overview_cache = (now, overview)
        return overview

    def job_list(self) -> list[dict[str, Any]]:
        with self.lock:
            jobs = list(self.jobs.values())
        return [job.to_dict() for job in sorted(jobs, key=lambda item: item.started_at, reverse=True)]

    def close_prices(self, params: dict[str, list[str]]) -> dict[str, Any]:
        tickers = _ticker_params(params)
        since = _date_param(params, "since")
        until = _date_param(params, "until")
        _validate_price_request(tickers, since, until)

        if len(tickers) > 1:
            prices = self._run_price_query(tickers, since, until, ("close",))
            return {
                "tickers": tickers,
                "since": since.isoformat(),
                "until": until.isoformat(),
                "count": sum(len(rows) for rows in prices.values()),
                "prices": prices,
            }

        ticker = tickers[0]
        rows = self._run_price_query([ticker], since, until, ("close",))[ticker]
        return {
            "ticker": ticker,
            "since": since.isoformat(),
            "until": until.isoformat(),
            "count": len(rows),
            "prices": rows,
        }

    def daily_prices(self, params: dict[str, list[str]]) -> dict[str, Any]:
        tickers = _ticker_params(params)
        since = _date_param(params, "since")
        until = _date_param(params, "until")
        _validate_price_request(tickers, since, until)
        fields = _daily_price_fields(params)
        prices = self._run_price_query(tickers, since, until, fields)
        return {
            "tickers": tickers,
            "since": since.isoformat(),
            "until": until.isoformat(),
            "fields": list(fields),
            "count": sum(len(rows) for rows in prices.values()),
            "prices": prices,
        }

    def quotes(self, params: dict[str, list[str]]) -> dict[str, Any]:
        symbols = _symbol_params(params)
        if len(symbols) > _admin_max_price_tickers():
            raise ValueError(f"too many symbols; max is {_admin_max_price_tickers()}")
        quotes = self._run_quotes_query(symbols)
        return {
            "symbols": symbols,
            "generated_at": datetime.now(UTC).isoformat(),
            "count": len(quotes),
            "quotes": quotes,
        }

    def security_search(self, params: dict[str, list[str]]) -> dict[str, Any]:
        queries = _search_query_params(params)
        limit = _optional_int_param(params, "limit") or 10
        if limit < 1 or limit > 50:
            raise ValueError("limit must be between 1 and 50")
        results = self._run_search_query(queries, limit)
        return {
            "queries": queries,
            "limit": limit,
            "count": sum(len(rows) for rows in results.values()),
            "results": results,
        }

    def _run_quotes_query(self, symbols: list[str]) -> list[dict[str, Any]]:
        cache_key = tuple(symbols)
        now = time.monotonic()
        with self.lock:
            cached_item = self._quotes_cache.get(cache_key)
            if cached_item is not None:
                cached_at, cached = cached_item
                if now - cached_at < self.quotes_cache_seconds:
                    return deepcopy(cached)

        self._acquire_data_query_slot("quotes", symbols, None, None)
        try:
            result = _query_quotes(self.paths, symbols)
        finally:
            self._price_query_slots.release()
        with self.lock:
            self._quotes_cache[cache_key] = (time.monotonic(), deepcopy(result))
            if len(self._quotes_cache) > 128:
                oldest_key = min(self._quotes_cache, key=lambda key: self._quotes_cache[key][0])
                self._quotes_cache.pop(oldest_key, None)
        return result

    def _run_search_query(self, queries: list[str], limit: int) -> dict[str, list[dict[str, Any]]]:
        cache_key = (tuple(queries), limit)
        now = time.monotonic()
        with self.lock:
            cached_item = self._search_cache.get(cache_key)
            if cached_item is not None:
                cached_at, cached = cached_item
                if now - cached_at < self.quotes_cache_seconds:
                    return deepcopy(cached)

        self._acquire_data_query_slot("security_search", queries, None, None)
        try:
            result = _search_securities(self.paths, queries, limit)
        finally:
            self._price_query_slots.release()
        with self.lock:
            self._search_cache[cache_key] = (time.monotonic(), deepcopy(result))
            if len(self._search_cache) > 128:
                oldest_key = min(self._search_cache, key=lambda key: self._search_cache[key][0])
                self._search_cache.pop(oldest_key, None)
        return result

    def nps_universe(self, params: dict[str, list[str]]) -> dict[str, Any]:
        requested_date = _optional_date_param(params, "date") or date.today()
        top = _optional_int_param(params, "top") or 100
        if top < 1 or top > 1000:
            raise ValueError("top must be between 1 and 1000")
        as_of, rows = self._run_nps_universe_query(requested_date, top)
        return {
            "date": requested_date.isoformat(),
            "as_of": as_of.isoformat() if as_of else None,
            "top": top,
            "count": len(rows),
            "universe": rows,
        }

    def basic_fundamentals(self, params: dict[str, list[str]]) -> dict[str, Any]:
        tickers = _ticker_params(params)
        as_of = _optional_date_param(params, "as_of") or date.today()
        fiscal_year = _optional_int_param(params, "fiscal_year")
        if len(tickers) > _admin_max_price_tickers():
            raise ValueError(f"too many tickers; max is {_admin_max_price_tickers()}")

        fundamentals = self._run_fundamental_query(tickers, as_of, fiscal_year)
        return {
            "tickers": tickers,
            "as_of": as_of.isoformat(),
            "fiscal_year": fiscal_year,
            "metrics": list(BASIC_FUNDAMENTAL_METRICS.keys()),
            "count": sum(1 for row in fundamentals.values() if row["metrics"]),
            "fundamentals": fundamentals,
        }

    def capital_actions(self, params: dict[str, list[str]]) -> dict[str, Any]:
        tickers = _ticker_params(params)
        as_of = _optional_date_param(params, "as_of") or date.today()
        start_year = _optional_int_param(params, "start_year")
        end_year = _optional_int_param(params, "end_year")
        if len(tickers) > _admin_max_price_tickers():
            raise ValueError(f"too many tickers; max is {_admin_max_price_tickers()}")
        if start_year is not None and end_year is not None and end_year < start_year:
            raise ValueError("end_year must be on or after start_year")

        actions = self._run_capital_actions_query(tickers, as_of, start_year, end_year)
        return {
            "tickers": tickers,
            "as_of": as_of.isoformat(),
            "start_year": start_year,
            "end_year": end_year,
            "metrics": list(CAPITAL_ACTION_METRICS.keys()),
            "count": sum(len(rows) for rows in actions.values()),
            "capital_actions": actions,
        }

    def dividends(self, params: dict[str, list[str]]) -> dict[str, Any]:
        tickers = _ticker_params(params)
        as_of = _optional_date_param(params, "as_of") or date.today()
        start_year = _optional_int_param(params, "start_year")
        end_year = _optional_int_param(params, "end_year")
        if len(tickers) > _admin_max_price_tickers():
            raise ValueError(f"too many tickers; max is {_admin_max_price_tickers()}")
        if start_year is not None and end_year is not None and end_year < start_year:
            raise ValueError("end_year must be on or after start_year")
        dividends = self._run_dividend_query(tickers, as_of, start_year, end_year)
        return {
            "tickers": tickers,
            "as_of": as_of.isoformat(),
            "start_year": start_year,
            "end_year": end_year,
            "count": sum(len(rows) for rows in dividends.values()),
            "dividends": dividends,
        }

    def screener(self, params: dict[str, list[str]]) -> dict[str, Any]:
        # 전 유니버스 스냅샷 — ticker 파라미터가 필요 없다(다른 endpoints 와 다름).
        # 최신 거래일 기준 KOSPI/KOSDAQ 전 종목에서 가치 지표를 계산한다.
        as_of = _optional_date_param(params, "as_of") or date.today()
        return self._run_screener_query(as_of)

    def cpi(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("cpi", params)

    def rates(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("rates", params)

    def indices(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("indices", params)

    def daily_indices(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("indices", params)

    def commodities(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("commodities", params)

    def fx(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("fx", params)

    def economic_indicators(self, params: dict[str, list[str]]) -> dict[str, Any]:
        return self._macro_payload("economic_indicators", params)

    def realtime_indicators(self, params: dict[str, list[str]]) -> dict[str, Any]:
        category = _optional_text_param(params, "category")
        series_id = _optional_text_param(params, "series_id")
        if category is not None and category not in {"indices", "rates", "commodities", "fx"}:
            raise ValueError("category must be one of: indices, rates, commodities, fx")
        cache_key = (category, series_id)
        now = time.monotonic()
        with self.lock:
            cached_item = self._realtime_cache.get(cache_key)
            if cached_item is not None:
                cached_at, cached = cached_item
                if now - cached_at < self.realtime_cache_seconds:
                    return deepcopy(cached)

        payload = _query_realtime_indicators(category, series_id)
        with self.lock:
            self._realtime_cache[cache_key] = (time.monotonic(), deepcopy(payload))
            if len(self._realtime_cache) > 128:
                oldest_key = min(self._realtime_cache, key=lambda key: self._realtime_cache[key][0])
                self._realtime_cache.pop(oldest_key, None)
        return payload

    def _macro_payload(self, table: str, params: dict[str, list[str]]) -> dict[str, Any]:
        since = _optional_date_param(params, "since")
        until = _optional_date_param(params, "until")
        if since is not None and until is not None and until < since:
            raise ValueError("until must be on or after since")
        country = _optional_text_param(params, "country")
        series_id = _optional_text_param(params, "series_id")
        filters = {
            "country": country,
            "series_id": series_id,
            "commodity": _optional_text_param(params, "commodity"),
            "base_currency": _optional_text_param(params, "base_currency"),
            "quote_currency": _optional_text_param(params, "quote_currency"),
            "category": _optional_text_param(params, "category"),
            "frequency": _optional_text_param(params, "frequency"),
        }
        rows = self._run_macro_query(table, since, until, filters)
        return {
            "since": since.isoformat() if since else None,
            "until": until.isoformat() if until else None,
            **{key: value for key, value in filters.items() if value is not None},
            "count": len(rows),
            table: rows,
        }

    def _run_price_query(
        self,
        tickers: list[str],
        since: date,
        until: date,
        fields: tuple[str, ...],
    ) -> dict[str, list[dict[str, Any]]]:
        cache_key = (tuple(tickers), since, until, fields)
        now = time.monotonic()
        with self.lock:
            cached_item = self._price_cache.get(cache_key)
            if cached_item is not None:
                cached_at, cached = cached_item
                if now - cached_at < self.price_cache_seconds:
                    return deepcopy(cached)

        self._acquire_data_query_slot("price", tickers, since, until)
        try:
            result = _query_daily_prices_batch(self.paths, tickers, since, until, fields)
        finally:
            self._price_query_slots.release()
        with self.lock:
            self._price_cache[cache_key] = (time.monotonic(), deepcopy(result))
            if len(self._price_cache) > self.price_cache_max_entries:
                oldest_key = min(self._price_cache, key=lambda key: self._price_cache[key][0])
                self._price_cache.pop(oldest_key, None)
        return result

    def _run_fundamental_query(
        self,
        tickers: list[str],
        as_of: date,
        fiscal_year: int | None,
    ) -> dict[str, dict[str, Any]]:
        self._acquire_data_query_slot("fundamentals", tickers, as_of, fiscal_year)
        try:
            return _query_basic_fundamentals_batch(self.paths, tickers, as_of, fiscal_year)
        finally:
            self._price_query_slots.release()

    def _run_capital_actions_query(
        self,
        tickers: list[str],
        as_of: date,
        start_year: int | None,
        end_year: int | None,
    ) -> dict[str, list[dict[str, Any]]]:
        self._acquire_data_query_slot("capital_actions", tickers, start_year, end_year)
        try:
            return _query_capital_actions_batch(self.paths, tickers, as_of, start_year, end_year)
        finally:
            self._price_query_slots.release()

    def _run_dividend_query(
        self,
        tickers: list[str],
        as_of: date,
        start_year: int | None,
        end_year: int | None,
    ) -> dict[str, list[dict[str, Any]]]:
        self._acquire_data_query_slot("dividends", tickers, start_year, end_year)
        try:
            return _query_dividends_batch(self.paths, tickers, as_of, start_year, end_year)
        finally:
            self._price_query_slots.release()

    def _run_screener_query(self, as_of: date) -> dict[str, Any]:
        self._acquire_data_query_slot("screener", [], as_of, None)
        try:
            return _query_screener_batch(self.paths, as_of)
        finally:
            self._price_query_slots.release()

    def _run_macro_query(
        self,
        table: str,
        since: date | None,
        until: date | None,
        filters: dict[str, str | None],
    ) -> list[dict[str, Any]]:
        self._acquire_data_query_slot("macro", [], since, until)
        try:
            return _query_macro_table(self.paths, table, since, until, filters)
        finally:
            self._price_query_slots.release()

    def _run_nps_universe_query(
        self,
        requested_date: date,
        top: int,
    ) -> tuple[date | None, list[dict[str, Any]]]:
        self._acquire_data_query_slot("nps_universe", [], requested_date, top)
        try:
            return _query_nps_universe(self.paths, requested_date, top)
        finally:
            self._price_query_slots.release()

    def _acquire_data_query_slot(
        self,
        query_type: str,
        tickers: list[str],
        start: Any,
        end: Any,
    ) -> None:
        wait_seconds = _admin_price_query_wait_seconds()
        if self._price_query_slots.acquire(timeout=wait_seconds):
            return
        print(
            "finance-pi admin 503: data query queue timeout "
            f"type={query_type} wait_seconds={wait_seconds} "
            f"tickers={len(tickers)} start={start} end={end}",
            file=sys.stderr,
        )
        raise AdminServiceBusy("data query queue timeout")

    def start_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action", ""))
        label, command = _job_command(action, payload, self.paths.root)
        if not self._job_slots.acquire(blocking=False):
            raise AdminServiceBusy("admin job already running")
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
            self._overview_cache = None
            self._evict_old_jobs()
        try:
            threading.Thread(target=self._run_job, args=(job,), daemon=True).start()
        except Exception:
            self._job_slots.release()
            raise
        return job.to_dict()

    def _evict_old_jobs(self) -> None:
        """Caller must hold self.lock. Never evicts queued/running jobs."""
        completed = sorted(
            (job for job in self.jobs.values() if job.status in {"done", "failed"}),
            key=lambda item: item.started_at,
        )
        overflow = len(self.jobs) - MAX_JOBS_RETAINED
        for job in completed:
            if overflow <= 0:
                break
            del self.jobs[job.id]
            with suppress(OSError):
                job.log_path.unlink(missing_ok=True)
            overflow -= 1

    def job_log(self, job_id: str) -> dict[str, Any]:
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        log = "" if not job.log_path.exists() else _tail_file(job.log_path, 120_000)
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
                self._evict_old_jobs()
        except Exception as exc:  # noqa: BLE001
            with job.log_path.open("a", encoding="utf-8", errors="replace") as log:
                log.write(str(exc))
            with self.lock:
                job.returncode = -1
                job.status = "failed"
                job.ended_at = datetime.now(UTC)
                self._evict_old_jobs()
        finally:
            self._job_slots.release()


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        max_request_threads: int = DEFAULT_MAX_REQUEST_THREADS,
    ) -> None:
        super().__init__(server_address, handler_class)
        self._request_slots = threading.BoundedSemaphore(max(1, max_request_threads))

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            self._reject_overloaded_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()

    def _reject_overloaded_request(self, request: Any) -> None:
        try:
            body = b'{"error":"server overloaded"}'
            request.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Content-Type: application/json; charset=utf-8\r\n"
                b"Cache-Control: no-store\r\n"
                b"Connection: close\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                + body
            )
        finally:
            self.shutdown_request(request)


class AdminServiceBusy(RuntimeError):
    pass


def run_admin(
    root: Path,
    host: str = "0.0.0.0",
    port: int = 8400,
    token: str | None = None,
) -> None:
    # host defaults to 0.0.0.0 so the admin is reachable from other devices on the
    # LAN. Read-only GET endpoints trust any private/link-local peer (convenience
    # for trusted home networks); state-changing POST endpoints require a token
    # unless the peer is strictly loopback, and are also guarded against CSRF via
    # Origin/Sec-Fetch-Site checks (see _authorized/_csrf_safe below).
    load_dotenv(root / ".env")
    auth_token = token or os.environ.get("FINANCE_PI_ADMIN_TOKEN") or secrets.token_urlsafe(24)
    _ensure_docs_built(root)
    state = AdminState(root.resolve(), auth_token)
    handler = _handler_for(state)
    max_threads = _admin_max_request_threads()
    server = BoundedThreadingHTTPServer((host, port), handler, max_threads)
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    print(f"finance-pi admin bind: http://{host}:{port}")
    print(f"finance-pi admin url:  http://{display_host}:{port}/?token={auth_token}")
    print(f"finance-pi local:      http://{display_host}:{port}/ (LAN clients bypass token)")
    print(f"finance-pi health:     http://{display_host}:{port}/api/health")
    print(f"finance-pi threads:    {max_threads}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nadmin stopped")
    finally:
        server.server_close()


def _handler_for(state: AdminState) -> type[BaseHTTPRequestHandler]:
    class AdminHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self._request_started_at = time.monotonic()
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/health":
                    self._send_json(_health_payload(state))
                elif parsed.path == "/api/docs":
                    self._send_json(_api_docs_payload(state))
                elif parsed.path in {"/doc", "/doc/"}:
                    self._redirect("/docs/")
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
                elif parsed.path == "/api/prices/close":
                    if not self._authorized():
                        return
                    self._send_json(state.close_prices(parse_qs(parsed.query)))
                elif parsed.path == "/api/prices/daily":
                    if not self._authorized():
                        return
                    self._send_json(state.daily_prices(parse_qs(parsed.query)))
                elif parsed.path == "/api/quotes":
                    if not self._authorized():
                        return
                    self._send_json(state.quotes(parse_qs(parsed.query)))
                elif parsed.path == "/api/securities/search":
                    if not self._authorized():
                        return
                    self._send_json(state.security_search(parse_qs(parsed.query)))
                elif parsed.path == "/api/universe/nps":
                    if not self._authorized():
                        return
                    self._send_json(state.nps_universe(parse_qs(parsed.query)))
                elif parsed.path == "/api/fundamentals/basic":
                    if not self._authorized():
                        return
                    self._send_json(state.basic_fundamentals(parse_qs(parsed.query)))
                elif parsed.path == "/api/fundamentals/capital-actions":
                    if not self._authorized():
                        return
                    self._send_json(state.capital_actions(parse_qs(parsed.query)))
                elif parsed.path == "/api/fundamentals/dividends":
                    if not self._authorized():
                        return
                    self._send_json(state.dividends(parse_qs(parsed.query)))
                elif parsed.path == "/api/fundamentals/screener":
                    if not self._authorized():
                        return
                    self._send_json(state.screener(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/cpi":
                    if not self._authorized():
                        return
                    self._send_json(state.cpi(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/rates":
                    if not self._authorized():
                        return
                    self._send_json(state.rates(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/indices":
                    if not self._authorized():
                        return
                    self._send_json(state.indices(parse_qs(parsed.query)))
                elif parsed.path == "/api/daily-indices":
                    if not self._authorized():
                        return
                    self._send_json(state.daily_indices(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/commodities":
                    if not self._authorized():
                        return
                    self._send_json(state.commodities(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/fx":
                    if not self._authorized():
                        return
                    self._send_json(state.fx(parse_qs(parsed.query)))
                elif parsed.path == "/api/macro/economic-indicators":
                    if not self._authorized():
                        return
                    self._send_json(state.economic_indicators(parse_qs(parsed.query)))
                elif parsed.path == "/api/realtime/indicators":
                    if not self._authorized():
                        return
                    self._send_json(state.realtime_indicators(parse_qs(parsed.query)))
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
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            except AdminServiceBusy as exc:
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except (BrokenPipeError, ConnectionResetError):
                self._log_client_disconnect()
            except Exception as exc:  # noqa: BLE001
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        def do_POST(self) -> None:  # noqa: N802
            self._request_started_at = time.monotonic()
            if self.path != "/api/jobs":
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                return
            if not self._csrf_safe():
                self._send_json({"error": "forbidden"}, status=HTTPStatus.FORBIDDEN)
                return
            if not self._authorized(require_loopback=True):
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                self._send_json(state.start_job(payload), status=HTTPStatus.CREATED)
            except (BrokenPipeError, ConnectionResetError):
                self._log_client_disconnect()
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

        def _authorized(self, require_loopback: bool = False) -> bool:
            # Read-only GET endpoints trust any private-LAN peer (see
            # _is_local_admin_client). State-changing requests (require_loopback=True,
            # currently POST /api/jobs) only bypass the token for strictly loopback
            # peers (127.0.0.0/8, ::1) so another device on the LAN cannot trigger
            # jobs without a token.
            client_ip = self.client_address[0]
            if require_loopback:
                if _is_loopback_client(client_ip):
                    return True
            elif _is_local_admin_client(client_ip):
                return True
            if state.token is None:
                return True
            header = self.headers.get("X-Admin-Token", "")
            query = parse_qs(urlparse(self.path).query).get("token", [""])[0]
            if secrets.compare_digest(header or query, state.token):
                return True
            self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return False

        def _csrf_safe(self) -> bool:
            host = self.headers.get("Host", "")
            origin = self.headers.get("Origin")
            if origin and not _origin_matches_host(origin, host):
                return False
            sec_fetch_site = self.headers.get("Sec-Fetch-Site")
            return sec_fetch_site is None or sec_fetch_site in {"same-origin", "none"}

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

        def _redirect(self, location: str) -> None:
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", location)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

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
            self._log_access(status, len(payload))

        def _log_access(self, status: HTTPStatus, size: int) -> None:
            duration_ms = int((time.monotonic() - self._request_started_at) * 1000)
            print(
                "finance-pi admin access: "
                f"client={self.client_address[0]} method={self.command} "
                f"status={int(status)} duration_ms={duration_ms} bytes={size} "
                f"path={_redact_admin_request_target(self.path)}",
                file=sys.stderr,
            )

        def _log_client_disconnect(self) -> None:
            duration_ms = int((time.monotonic() - self._request_started_at) * 1000)
            print(
                "finance-pi admin client disconnected: "
                f"client={self.client_address[0]} method={self.command} "
                f"duration_ms={duration_ms} "
                f"path={_redact_admin_request_target(self.path)}",
                file=sys.stderr,
            )

    return AdminHandler


def _ensure_docs_built(root: Path) -> None:
    build_docs_site(root)


def _is_local_admin_client(host: str) -> bool:
    try:
        address = ip_address(host)
    except ValueError:
        return False
    if getattr(address, "ipv4_mapped", None) is not None:
        address = address.ipv4_mapped
    return address.is_loopback or address.is_private or address.is_link_local


def _is_loopback_client(host: str) -> bool:
    try:
        address = ip_address(host)
    except ValueError:
        return False
    if getattr(address, "ipv4_mapped", None) is not None:
        address = address.ipv4_mapped
    return address.is_loopback


def _origin_matches_host(origin: str, host: str) -> bool:
    origin_host = urlparse(origin).netloc
    return origin_host.lower() == host.lower()


def _redact_admin_request_target(target: str) -> str:
    return re.sub(r"([?&]token=)[^&]+", r"\1<redacted>", target)


def _health_payload(state: AdminState) -> dict[str, Any]:
    # Unauthenticated endpoint: no absolute filesystem paths in the response.
    return {
        "status": "ok",
        "generated_at": datetime.now(UTC).isoformat(),
        "workspace": state.paths.root.resolve().name,
        "data_root_exists": state.paths.data_root.resolve().exists(),
        "auth": "local-or-token",
    }


def _api_docs_payload(state: AdminState) -> dict[str, Any]:
    base_url = "/api"
    # Unauthenticated endpoint: no absolute filesystem paths in the response.
    return {
        "service": "finance-pi admin API",
        "generated_at": datetime.now(UTC).isoformat(),
        "workspace": state.paths.root.resolve().name,
        "auth": {
            "local_network": "LAN clients are allowed without a token",
            "token": "Use X-Admin-Token or token query parameter outside local networks",
        },
        "limits": {
            "max_request_threads": _admin_max_request_threads(),
            "max_admin_jobs": _admin_max_jobs(),
            "max_price_queries": _admin_max_price_queries(),
            "price_query_wait_seconds": _admin_price_query_wait_seconds(),
            "max_price_tickers": _admin_max_price_tickers(),
            "max_price_days": _admin_max_price_days(),
            "max_price_cells": MAX_PRICE_CELLS,
        },
        "endpoints": {
            "health": {
                "method": "GET",
                "path": f"{base_url}/health",
                "description": "Lightweight service health check.",
            },
            "close_prices": {
                "method": "GET",
                "path": f"{base_url}/prices/close",
                "description": (
                    "Adjusted close prices. Supports one ticker or batched tickers. "
                    f"tickers x days must not exceed {MAX_PRICE_CELLS}."
                ),
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "since": "YYYY-MM-DD inclusive.",
                    "until": "YYYY-MM-DD inclusive.",
                },
                "examples": [
                    f"{base_url}/prices/close?ticker=005930&since=2026-04-29&until=2026-04-30",
                    f"{base_url}/prices/close?tickers=005930,000660&since=2026-04-29&until=2026-04-30",
                ],
            },
            "daily_prices": {
                "method": "GET",
                "path": f"{base_url}/prices/daily",
                "description": (
                    "Daily adjusted OHLCV rows with optional market fields. "
                    f"tickers x days must not exceed {MAX_PRICE_CELLS}."
                ),
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "since": "YYYY-MM-DD inclusive.",
                    "until": "YYYY-MM-DD inclusive.",
                    "fields": "Optional comma-separated subset. Defaults to open,high,low,close,volume,trading_value.",
                },
                "fields": {
                    "default": list(DEFAULT_DAILY_PRICE_FIELDS),
                    "available": list(DAILY_PRICE_FIELDS.keys()),
                },
                "examples": [
                    f"{base_url}/prices/daily?tickers=005930,000660&since=2026-04-29&until=2026-04-30",
                    f"{base_url}/prices/daily?ticker=005930&since=2026-04-29&until=2026-04-30&fields=close,volume",
                ],
            },
            "quotes": {
                "method": "GET",
                "path": f"{base_url}/quotes",
                "description": "Domestic and overseas quote snapshots. Korean numeric tickers use local Naver/gold data; other symbols use CNBC quotes.",
                "query": {
                    "symbol": "Single ticker/symbol, e.g. 005930 or AAPL.",
                    "symbols": "Comma-separated tickers/symbols, e.g. 005930,AAPL,.SPX.",
                },
                "examples": [
                    f"{base_url}/quotes?symbols=005930,000660",
                    f"{base_url}/quotes?symbols=AAPL,MSFT,.SPX,US10Y",
                ],
            },
            "security_search": {
                "method": "GET",
                "path": f"{base_url}/securities/search",
                "description": "Batch search local Korean securities by ticker/name and validate overseas CNBC symbols.",
                "query": {
                    "q": "Search query. Can be repeated or comma-separated.",
                    "queries": "Alternative comma-separated batch query parameter.",
                    "limit": "Optional max results per query, 1..50. Defaults to 10.",
                },
                "examples": [
                    f"{base_url}/securities/search?q=삼성전자&q=000660",
                    f"{base_url}/securities/search?queries=AAPL,MSFT,.SPX&limit=5",
                ],
            },
            "nps_universe": {
                "method": "GET",
                "path": f"{base_url}/universe/nps",
                "description": "Point-in-time NPS holdings universe ranked by market value.",
                "query": {
                    "date": "Optional requested date as YYYY-MM-DD. Uses the latest NPS snapshot on or before this date.",
                    "top": "Optional max rows, 1..1000. Defaults to 100.",
                },
                "examples": [
                    f"{base_url}/universe/nps?date=2024-12-31&top=100",
                    f"{base_url}/universe/nps?date=2026-04-30&top=50",
                ],
            },
            "basic_fundamentals": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/basic",
                "description": "Latest available annual basic financial metrics by ticker, with per-share BPS/EPS values when DART share-denominator data is available.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "fiscal_year": "Optional fiscal year filter.",
                },
                "metrics": list(BASIC_FUNDAMENTAL_METRICS.keys()),
                "per_share": {
                    "object": "Returned per ticker as fundamentals.<ticker>.per_share when denominator data is available.",
                    "fields": {
                        "bps": "Latest balance-sheet equity divided by issuer-level DART outstanding shares when available.",
                        "eps_annual": "Latest annual net income divided by issuer-level DART outstanding shares when available.",
                        "eps_ttm": "Trailing-twelve-month net income divided by issuer-level DART outstanding shares when available. Annual latest rows equal eps_annual; interim latest rows use latest cumulative + prior annual - prior matching interim.",
                        "eps_forward": "Forward EPS placeholder. value is null and available is false until a forward earnings estimate source is added.",
                    },
                    "denominator": {
                        "share_basis": "dart_distributed_shares",
                        "issuer_key": "corp_code",
                        "preferred_shares": "Included by summing DART common/preferred rows for the same issuer.",
                        "treasury_shares": "DART stock total quantity rows are preferred; when available, shares are distb_stock_co and treasury_shares_excluded is true.",
                    },
                },
                "examples": [
                    f"{base_url}/fundamentals/basic?tickers=005930,000660",
                    f"{base_url}/fundamentals/basic?ticker=005930&as_of=2026-04-30&fiscal_year=2025",
                ],
            },
            "capital_actions": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/capital-actions",
                "description": "Annual dividend and treasury-share related financial rows by ticker.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 5930.",
                    "tickers": "Comma-separated tickers, e.g. 005930,000660.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "start_year": "Optional first fiscal year.",
                    "end_year": "Optional last fiscal year.",
                },
                "metrics": list(CAPITAL_ACTION_METRICS.keys()),
                "examples": [
                    f"{base_url}/fundamentals/capital-actions?tickers=005930,000660&start_year=2024&end_year=2025",
                    f"{base_url}/fundamentals/capital-actions?ticker=005930&as_of=2026-04-30",
                ],
            },
            "dividends": {
                "method": "GET",
                "path": f"{base_url}/fundamentals/dividends",
                "description": "Per-share dividend rows by listed security, including common/preferred stock distinctions when OpenDART can map them unambiguously.",
                "query": {
                    "ticker": "Single ticker, e.g. 005930 or 005935.",
                    "tickers": "Comma-separated tickers, e.g. 005930,005935.",
                    "as_of": "Optional YYYY-MM-DD point-in-time cutoff. Defaults to today.",
                    "start_year": "Optional first fiscal year.",
                    "end_year": "Optional last fiscal year.",
                },
                "examples": [
                    f"{base_url}/fundamentals/dividends?tickers=005930,005935&start_year=2022&end_year=2024",
                    f"{base_url}/fundamentals/dividends?ticker=005935&as_of=2026-04-30",
                ],
            },
            "cpi": {
                "method": "GET",
                "path": f"{base_url}/macro/cpi",
                "description": "Consumer price index observations from macro.cpi.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional CPI series id.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["cpi"]),
                "examples": [
                    f"{base_url}/macro/cpi?country=KR&since=2024-01-01",
                    f"{base_url}/macro/cpi?series_id=KOR_CPI_ALL&since=2024-01-01&until=2024-12-31",
                ],
            },
            "rates": {
                "method": "GET",
                "path": f"{base_url}/macro/rates",
                "description": "Interest-rate observations from macro.rates.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional rate series id.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["rates"]),
                "examples": [
                    f"{base_url}/macro/rates?country=KR&since=2024-01-01",
                    f"{base_url}/macro/rates?series_id=KOR_BASE_RATE",
                ],
            },
            "indices": {
                "method": "GET",
                "path": f"{base_url}/macro/indices",
                "description": "Market and macro index observations from macro.indices.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. KR or US.",
                    "series_id": "Optional index series id.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["indices"]),
                "examples": [
                    f"{base_url}/macro/indices?country=KR&since=2024-01-01",
                    f"{base_url}/macro/indices?series_id=KOSPI",
                ],
            },
            "daily_indices": {
                "method": "GET",
                "path": f"{base_url}/daily-indices",
                "description": "Alias for daily market index observations from macro.indices.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. JP or US.",
                    "series_id": "Optional index series id, e.g. SP500, NASDAQ, DOW_JONES, NIKKEI_225, HANG_SENG, SSE_COMPOSITE.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["indices"]),
                "examples": [
                    f"{base_url}/daily-indices?series_id=SP500&since=2024-01-01",
                    f"{base_url}/daily-indices?country=JP&since=2024-01-01",
                ],
            },
            "realtime_indicators": {
                "method": "GET",
                "path": f"{base_url}/realtime/indicators",
                "description": "Live CNBC quote snapshots for configured indices, rates, commodities, and FX indicators. Responses are cached briefly in-process.",
                "query": {
                    "category": "Optional group: indices, rates, commodities, or fx.",
                    "series_id": "Optional configured series id, e.g. SP500, USD_KRW, US_TREASURY_10Y.",
                },
                "examples": [
                    f"{base_url}/realtime/indicators",
                    f"{base_url}/realtime/indicators?category=indices",
                    f"{base_url}/realtime/indicators?series_id=USD_KRW",
                ],
            },
            "commodities": {
                "method": "GET",
                "path": f"{base_url}/macro/commodities",
                "description": "Commodity observations such as gold and silver from macro.commodities.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "series_id": "Optional commodity series id.",
                    "commodity": "Optional commodity code, e.g. gold or silver.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["commodities"]),
                "examples": [
                    f"{base_url}/macro/commodities?commodity=gold&since=2024-01-01",
                    f"{base_url}/macro/commodities?series_id=GOLD_USD_OZ",
                ],
            },
            "fx": {
                "method": "GET",
                "path": f"{base_url}/macro/fx",
                "description": "Foreign-exchange observations from macro.fx.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "series_id": "Optional FX series id.",
                    "base_currency": "Optional base currency, e.g. USD.",
                    "quote_currency": "Optional quote currency, e.g. KRW.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["fx"]),
                "examples": [
                    f"{base_url}/macro/fx?base_currency=USD&quote_currency=KRW&since=2024-01-01",
                    f"{base_url}/macro/fx?series_id=USD_KRW",
                ],
            },
            "economic_indicators": {
                "method": "GET",
                "path": f"{base_url}/macro/economic-indicators",
                "description": "General FRED macroeconomic observations from macro.economic_indicators.",
                "query": {
                    "since": "Optional YYYY-MM-DD inclusive.",
                    "until": "Optional YYYY-MM-DD inclusive.",
                    "country": "Optional country code, e.g. US.",
                    "series_id": "Optional FRED series id, e.g. UNRATE or VIXCLS.",
                    "category": "Optional category, e.g. labor, growth, risk, credit, inflation.",
                    "frequency": "Optional frequency code, e.g. D, W, M, Q.",
                },
                "columns": list(MACRO_TABLE_COLUMNS["economic_indicators"]),
                "examples": [
                    f"{base_url}/macro/economic-indicators?category=labor&since=2024-01-01",
                    f"{base_url}/macro/economic-indicators?series_id=VIXCLS&since=2024-01-01",
                ],
            },
        },
    }


def _admin_max_request_threads() -> int:
    return _positive_int_env("FINANCE_PI_ADMIN_MAX_THREADS", DEFAULT_MAX_REQUEST_THREADS)


def _admin_max_jobs() -> int:
    return _positive_int_env("FINANCE_PI_ADMIN_MAX_JOBS", DEFAULT_MAX_ADMIN_JOBS)


def _admin_max_price_queries() -> int:
    return _positive_int_env("FINANCE_PI_ADMIN_MAX_PRICE_QUERIES", DEFAULT_MAX_PRICE_QUERIES)


def _admin_price_query_wait_seconds() -> float:
    return _nonnegative_float_env(
        "FINANCE_PI_ADMIN_PRICE_QUERY_WAIT_SECONDS",
        DEFAULT_PRICE_QUERY_WAIT_SECONDS,
    )


def _admin_max_price_tickers() -> int:
    return _positive_int_env("FINANCE_PI_ADMIN_MAX_PRICE_TICKERS", DEFAULT_MAX_PRICE_TICKERS)


def _admin_max_price_days() -> int:
    return _positive_int_env("FINANCE_PI_ADMIN_MAX_PRICE_DAYS", DEFAULT_MAX_PRICE_DAYS)


def _positive_int_env(name: str, default: int) -> int:
    value = os.environ.get(name, "")
    if not value:
        return default
    try:
        return max(1, int(value))
    except ValueError:
        return default


def _nonnegative_float_env(name: str, default: float) -> float:
    value = os.environ.get(name, "")
    if not value:
        return default
    try:
        return max(0.0, float(value))
    except ValueError:
        return default


def _single_param(params: dict[str, list[str]], name: str) -> str:
    value = params.get(name, [""])[0].strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _date_param(params: dict[str, list[str]], name: str) -> date:
    value = _single_param(params, name)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD") from exc


def _optional_date_param(params: dict[str, list[str]], name: str) -> date | None:
    value = params.get(name, [""])[0].strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD") from exc


def _optional_int_param(params: dict[str, list[str]], name: str) -> int | None:
    value = params.get(name, [""])[0].strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _optional_text_param(params: dict[str, list[str]], name: str) -> str | None:
    value = params.get(name, [""])[0].strip()
    return value or None


def _normalize_ticker_param(value: str) -> str:
    ticker = value.strip().upper()
    if not re.fullmatch(r"[0-9A-Z]{1,12}", ticker):
        raise ValueError("ticker must contain only letters and digits")
    if ticker.isdigit() and len(ticker) <= 6:
        return ticker.zfill(6)
    return ticker


def _ticker_params(params: dict[str, list[str]]) -> list[str]:
    values = params.get("tickers")
    if values is None:
        values = params.get("ticker")
    if not values:
        raise ValueError("ticker is required")

    tickers: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                tickers.append(_normalize_ticker_param(part))
    if not tickers:
        raise ValueError("ticker is required")
    return list(dict.fromkeys(tickers))


def _symbol_params(params: dict[str, list[str]]) -> list[str]:
    values = params.get("symbols") or params.get("symbol")
    if not values:
        raise ValueError("symbol is required")
    symbols: list[str] = []
    for value in values:
        for part in value.split(","):
            symbol = part.strip()
            if symbol:
                symbols.append(_normalize_symbol_param(symbol))
    if not symbols:
        raise ValueError("symbol is required")
    return list(dict.fromkeys(symbols))


def _normalize_symbol_param(value: str) -> str:
    symbol = value.strip()
    if not symbol:
        raise ValueError("symbol is required")
    if symbol.isdigit() and len(symbol) <= 6:
        return symbol.zfill(6)
    return symbol.upper()


def _search_query_params(params: dict[str, list[str]]) -> list[str]:
    values = params.get("queries") or params.get("q") or params.get("query")
    if not values:
        raise ValueError("query is required")
    queries: list[str] = []
    for value in values:
        for part in value.split(","):
            query = part.strip()
            if query:
                queries.append(query)
    if not queries:
        raise ValueError("query is required")
    return list(dict.fromkeys(queries))


def _daily_price_fields(params: dict[str, list[str]]) -> tuple[str, ...]:
    values = params.get("fields")
    if not values:
        return DEFAULT_DAILY_PRICE_FIELDS

    fields: list[str] = []
    for value in values:
        for part in value.split(","):
            field_name = part.strip()
            if not field_name:
                continue
            if field_name not in DAILY_PRICE_FIELDS:
                raise ValueError(f"unknown daily price field: {field_name}")
            fields.append(field_name)
    return tuple(dict.fromkeys(fields)) or DEFAULT_DAILY_PRICE_FIELDS


def _validate_price_request(tickers: list[str], since: date, until: date) -> None:
    if until < since:
        raise ValueError("until must be on or after since")
    if len(tickers) > _admin_max_price_tickers():
        raise ValueError(f"too many tickers; max is {_admin_max_price_tickers()}")
    days = (until - since).days + 1
    if days > _admin_max_price_days():
        raise ValueError(f"date range is too large; max days is {_admin_max_price_days()}")
    if len(tickers) * days > MAX_PRICE_CELLS:
        raise ValueError(
            f"tickers x days is too large; max is {MAX_PRICE_CELLS} "
            f"(requested {len(tickers)} tickers x {days} days)"
        )


def _query_nps_universe(
    paths: ProjectPaths,
    requested_date: date,
    top: int,
) -> tuple[date | None, list[dict[str, Any]]]:
    partition = _latest_nps_universe_partition(paths.data_root, requested_date)
    if partition is not None:
        as_of, path = partition
        frame = (
            pl.read_parquet(path, hive_partitioning=True)
            .sort("rank")
            .head(top)
            .select(
                "rank",
                "stock_code",
                "stock_name",
                "security_id",
                "listing_id",
                "shares",
                "ownership_pct",
                "price",
                "market_value",
                "change_pct",
                "source",
                "source_date",
            )
        )
        return as_of, [_nps_universe_row_dict(row) for row in frame.iter_rows(named=True)]

    if not paths.catalog_path.exists():
        return None, []

    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        rows = conn.execute(
            """
            WITH chosen AS (
                SELECT max(date) AS as_of
                FROM gold.nps_universe
                WHERE date <= ?
            )
            SELECT rank, stock_code, stock_name, security_id, listing_id,
                   shares, ownership_pct, price, market_value, change_pct,
                   source, source_date, date
            FROM gold.nps_universe
            WHERE date = (SELECT as_of FROM chosen)
            ORDER BY rank
            LIMIT ?
            """,
            (requested_date, top),
        ).fetchall()
    if not rows:
        return None, []
    as_of = rows[0][12]
    return _coerce_date(as_of), [
        _nps_universe_row_dict(
            {
                "rank": row[0],
                "stock_code": row[1],
                "stock_name": row[2],
                "security_id": row[3],
                "listing_id": row[4],
                "shares": row[5],
                "ownership_pct": row[6],
                "price": row[7],
                "market_value": row[8],
                "change_pct": row[9],
                "source": row[10],
                "source_date": row[11],
            }
        )
        for row in rows
    ]


def _latest_nps_universe_partition(
    data_root: Path,
    requested_date: date,
) -> tuple[date, Path] | None:
    candidates: list[tuple[date, Path]] = []
    for path in data_root.glob("gold/nps_universe/dt=*/part.parquet"):
        partition_date = _partition_date(path, "dt")
        if partition_date is not None and partition_date <= requested_date:
            candidates.append((partition_date, path))
    return max(candidates, key=lambda item: item[0]) if candidates else None


def _nps_universe_row_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": row.get("rank"),
        "stock_code": row.get("stock_code"),
        "stock_name": row.get("stock_name"),
        "security_id": row.get("security_id"),
        "listing_id": row.get("listing_id"),
        "shares": row.get("shares"),
        "ownership_pct": row.get("ownership_pct"),
        "price": row.get("price"),
        "market_value": row.get("market_value"),
        "change_pct": row.get("change_pct"),
        "source": row.get("source"),
        "source_date": _format_date_like(row.get("source_date")),
    }


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    with suppress(ValueError):
        return date.fromisoformat(str(value))
    return None


def _format_date_like(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _query_daily_prices_batch(
    paths: ProjectPaths,
    tickers: list[str],
    since: date,
    until: date,
    fields: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    prices = {ticker: [] for ticker in tickers}
    if not tickers:
        return prices

    select_fields = ", ".join(
        f"{source_column} AS {field_name}"
        for field_name, source_column in _daily_price_field_columns(fields)
    )
    price_files = _daily_price_partition_files(paths.data_root, since, until)
    if price_files:
        rows = _query_daily_price_partition_files(price_files, tickers, since, until, fields)
        _append_daily_price_rows(prices, rows, fields)
        return prices

    if paths.catalog_path.exists():
        with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT ticker, date, {select_fields}
                FROM analytics.daily_prices
                WHERE ticker IN ({_sql_placeholders(tickers)})
                  AND date BETWEEN ? AND ?
                ORDER BY ticker, date
                """,
                [*tickers, since, until],
            ).fetchall()
        _append_daily_price_rows(prices, rows, fields)
        return prices

    price_glob = paths.data_root / "gold/daily_prices_adj/dt=*/part.parquet"
    master_path = paths.data_root / "gold/security_master.parquet"
    if not glob(price_glob.as_posix()):
        return prices

    sql_price_path = _duckdb_path(price_glob)
    security_ids = [f"S{ticker}" for ticker in tickers]
    if master_path.exists():
        sql_master_path = _duckdb_path(master_path)
        sql = f"""
            WITH prices AS (
                SELECT *
                FROM read_parquet('{sql_price_path}', hive_partitioning = true, union_by_name = true)
            ),
            master AS (
                SELECT *
                FROM read_parquet('{sql_master_path}', union_by_name = true)
            )
            SELECT COALESCE(sm.ticker, regexp_replace(p.security_id, '^S', '')) AS ticker,
                   p.date,
                   {select_fields}
            FROM prices AS p
            LEFT JOIN master AS sm
                ON p.security_id = sm.security_id
            WHERE (sm.ticker IN ({_sql_placeholders(tickers)})
                   OR p.security_id IN ({_sql_placeholders(security_ids)}))
              AND p.date BETWEEN ? AND ?
            ORDER BY sm.ticker, p.security_id, p.date
        """
        params: list[Any] = [*tickers, *security_ids, since, until]
    else:
        sql = f"""
            SELECT regexp_replace(security_id, '^S', '') AS ticker,
                   date,
                   {select_fields}
            FROM read_parquet('{sql_price_path}', hive_partitioning = true, union_by_name = true)
            WHERE security_id IN ({_sql_placeholders(security_ids)})
              AND date BETWEEN ? AND ?
            ORDER BY security_id, date
        """
        params = [*security_ids, since, until]

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(sql, params).fetchall()
    _append_daily_price_rows(prices, rows, fields)
    return prices


def _query_quotes(paths: ProjectPaths, symbols: list[str]) -> list[dict[str, Any]]:
    domestic_symbols = [symbol for symbol in symbols if _is_korean_ticker(symbol)]
    overseas_symbols = [symbol for symbol in symbols if symbol not in domestic_symbols]
    domestic_quotes = _query_domestic_quotes(paths, domestic_symbols)
    overseas_quotes = _query_cnbc_symbol_quotes(overseas_symbols)
    by_symbol = {
        quote["symbol"]: quote
        for quote in [*domestic_quotes, *overseas_quotes]
    }
    return [by_symbol[symbol] for symbol in symbols if symbol in by_symbol]


def _is_korean_ticker(symbol: str) -> bool:
    return symbol.isdigit() and len(symbol) == 6


def _query_domestic_quotes(paths: ProjectPaths, symbols: list[str]) -> list[dict[str, Any]]:
    if not symbols:
        return []
    latest_summary = _query_latest_naver_summary_quotes(paths, symbols)
    missing = [symbol for symbol in symbols if symbol not in latest_summary]
    latest_gold = _query_latest_gold_quotes(paths, missing)
    by_symbol = {**latest_gold, **latest_summary}
    return [by_symbol[symbol] for symbol in symbols if symbol in by_symbol]


def _query_latest_naver_summary_quotes(
    paths: ProjectPaths,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    files = sorted(paths.data_root.glob("bronze/naver_summary/dt=*/part.parquet"))
    if not files:
        return {}
    latest = files[-1]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"""
            SELECT
                ticker,
                name,
                market,
                snapshot_dt,
                close,
                change_abs,
                change_rate_pct,
                volume,
                market_cap
            FROM read_parquet('{_duckdb_path(latest)}', union_by_name = true)
            WHERE ticker IN ({_sql_placeholders(symbols)})
            ORDER BY ticker
            """,
            symbols,
        ).fetchall()
    return {
        row[0]: {
            "symbol": row[0],
            "name": row[1],
            "market": row[2],
            "country": "KR",
            "asset_type": "equity",
            "price": row[4],
            "change": row[5],
            "change_pct": row[6],
            "volume": row[7],
            "trading_value": None,
            "market_cap": row[8],
            "as_of": _iso_or_none(row[3]),
            "source": "naver_summary",
        }
        for row in rows
    }


def _query_latest_gold_quotes(
    paths: ProjectPaths,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    price_files = sorted(paths.data_root.glob("gold/daily_prices_adj/dt=*/part.parquet"))
    if not price_files:
        return {}
    master_path = paths.data_root / "gold/security_master.parquet"
    latest = price_files[-1]
    security_ids = [f"S{symbol}" for symbol in symbols]
    if master_path.exists():
        sql = f"""
            WITH prices AS (
                SELECT *
                FROM read_parquet('{_duckdb_path(latest)}', hive_partitioning = true, union_by_name = true)
            ),
            master AS (
                SELECT *
                FROM read_parquet('{_duckdb_path(master_path)}', union_by_name = true)
            )
            SELECT
                COALESCE(sm.ticker, regexp_replace(p.security_id, '^S', '')) AS ticker,
                sm.name,
                sm.market,
                p.date,
                p.close_adj,
                p.return_1d,
                p.volume,
                p.trading_value,
                p.market_cap
            FROM prices AS p
            LEFT JOIN master AS sm
                ON p.security_id = sm.security_id
            WHERE sm.ticker IN ({_sql_placeholders(symbols)})
               OR p.security_id IN ({_sql_placeholders(security_ids)})
        """
        params: list[Any] = [*symbols, *security_ids]
    else:
        sql = f"""
            SELECT
                regexp_replace(security_id, '^S', '') AS ticker,
                NULL AS name,
                NULL AS market,
                date,
                close_adj,
                return_1d,
                volume,
                trading_value,
                market_cap
            FROM read_parquet('{_duckdb_path(latest)}', hive_partitioning = true, union_by_name = true)
            WHERE security_id IN ({_sql_placeholders(security_ids)})
        """
        params = security_ids
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(sql, params).fetchall()
    return {
        row[0]: {
            "symbol": row[0],
            "name": row[1],
            "market": row[2],
            "country": "KR",
            "asset_type": "equity",
            "price": row[4],
            "change": None,
            "change_pct": row[5] * 100 if row[5] is not None else None,
            "volume": row[6],
            "trading_value": row[7],
            "market_cap": row[8],
            "as_of": _iso_or_none(row[3]),
            "source": "gold.daily_prices_adj",
        }
        for row in rows
    }


def _query_cnbc_symbol_quotes(symbols: list[str]) -> list[dict[str, Any]]:
    if not symbols:
        return []
    from finance_pi.cli import app as cli_app

    quotes = cli_app._fetch_cnbc_quotes(symbols)
    rows = []
    for symbol in symbols:
        quote_item = quotes.get(symbol)
        if quote_item is None:
            continue
        rows.append(_cnbc_quote_payload(symbol, quote_item))
    return rows


def _cnbc_quote_payload(symbol: str, quote_item: dict[str, Any]) -> dict[str, Any]:
    last = _safe_quote_float(quote_item.get("last"))
    previous = _safe_quote_float(
        quote_item.get("previous_day_closing") or quote_item.get("prev_prev_closing")
    )
    change = _safe_quote_float(quote_item.get("change"))
    change_pct = _safe_quote_float(quote_item.get("change_pct"))
    if change is None and last is not None and previous is not None:
        change = last - previous
    if change_pct is None and last is not None and previous not in (None, 0):
        change_pct = (last / previous - 1.0) * 100.0
    return {
        "symbol": symbol,
        "name": quote_item.get("name") or quote_item.get("shortName"),
        "market": quote_item.get("exchange"),
        "country": quote_item.get("countryCode"),
        "asset_type": _lower_or_none(quote_item.get("assetType")),
        "price": last,
        "change": change,
        "change_pct": change_pct,
        "volume": _safe_quote_float(quote_item.get("volume")),
        "trading_value": None,
        "market_cap": _safe_quote_float(quote_item.get("marketCap")),
        "currency": quote_item.get("currencyCode"),
        "as_of": quote_item.get("last_time") or quote_item.get("reg_last_time"),
        "source": "cnbc",
    }


def _search_securities(
    paths: ProjectPaths,
    queries: list[str],
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    local = _search_local_securities(paths, queries, limit)
    symbol_queries = [_normalize_symbol_param(query) for query in queries if _looks_like_symbol(query)]
    cnbc_quotes = {row["symbol"]: row for row in _query_cnbc_symbol_quotes(symbol_queries)}
    result: dict[str, list[dict[str, Any]]] = {}
    for query in queries:
        rows = list(local.get(query, []))
        normalized = _normalize_symbol_param(query) if _looks_like_symbol(query) else None
        if normalized in cnbc_quotes and not any(row.get("symbol") == normalized for row in rows):
            rows.append(
                {
                    "symbol": normalized,
                    "name": cnbc_quotes[normalized].get("name"),
                    "market": cnbc_quotes[normalized].get("market"),
                    "country": cnbc_quotes[normalized].get("country"),
                    "asset_type": cnbc_quotes[normalized].get("asset_type"),
                    "source": "cnbc",
                }
            )
        result[query] = rows[:limit]
    return result


def _looks_like_symbol(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9.@=^_-]{1,24}", value.strip()))


def _search_local_securities(
    paths: ProjectPaths,
    queries: list[str],
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    rows = _local_security_search_rows(paths)
    result: dict[str, list[dict[str, Any]]] = {query: [] for query in queries}
    for query in queries:
        normalized_query = query.lower()
        normalized_ticker = query.zfill(6) if query.isdigit() and len(query) <= 6 else query.upper()
        matches = []
        for row in rows:
            ticker = str(row.get("ticker") or "")
            name = str(row.get("name") or "")
            if ticker == normalized_ticker or normalized_query in name.lower():
                matches.append(row)
            if len(matches) >= limit:
                break
        result[query] = matches
    return result


def _local_security_search_rows(paths: ProjectPaths) -> list[dict[str, Any]]:
    frames = []
    master_path = paths.data_root / "gold/security_master.parquet"
    if master_path.exists():
        frames.append(
            pl.read_parquet(master_path)
            .select(["ticker", "name", "market", "security_type", "share_class"])
            .with_columns(
                pl.lit("KR").alias("country"),
                pl.lit("security_master").alias("source"),
            )
        )
    naver_files = sorted(paths.data_root.glob("bronze/naver_summary/dt=*/part.parquet"))
    if naver_files:
        frames.append(
            pl.read_parquet(naver_files[-1])
            .select(["ticker", "name", "market"])
            .with_columns(
                pl.lit("equity").alias("security_type"),
                pl.lit(None).alias("share_class"),
                pl.lit("KR").alias("country"),
                pl.lit("naver_summary").alias("source"),
            )
        )
    if not frames:
        return []
    frame = pl.concat(frames, how="diagonal_relaxed").unique(subset=["ticker"], keep="first")
    return [
        {
            "symbol": row["ticker"],
            "ticker": row["ticker"],
            "name": row["name"],
            "market": row["market"],
            "country": row["country"],
            "asset_type": row["security_type"],
            "share_class": row["share_class"],
            "source": row["source"],
        }
        for row in frame.sort("ticker").to_dicts()
    ]


def _daily_price_partition_files(data_root: Path, since: date, until: date) -> list[Path]:
    files: list[Path] = []
    current = since
    while current <= until:
        path = data_root / "gold" / "daily_prices_adj" / f"dt={current.isoformat()}" / "part.parquet"
        if path.exists():
            files.append(path)
        current += timedelta(days=1)
    return files


def _query_daily_price_partition_files(
    price_files: list[Path],
    tickers: list[str],
    since: date,
    until: date,
    fields: tuple[str, ...],
) -> list[tuple[Any, ...]]:
    security_ids = [f"S{ticker}" for ticker in tickers]
    frame = (
        pl.scan_parquet(
            [path.as_posix() for path in price_files],
            hive_partitioning=True,
        )
        .filter(
            pl.col("security_id").is_in(security_ids)
            & (pl.col("date").cast(pl.Date, strict=False) >= since)
            & (pl.col("date").cast(pl.Date, strict=False) <= until)
        )
        .select(
            pl.col("security_id").str.replace(r"^S", "").alias("ticker"),
            pl.col("date"),
            *[
                pl.col(source_column).alias(field_name)
                for field_name, source_column in _daily_price_field_columns(fields)
            ],
        )
        .sort("ticker", "date")
        .collect()
    )
    return list(frame.iter_rows())


def _daily_price_field_columns(fields: tuple[str, ...]) -> list[tuple[str, str]]:
    return [(field_name, DAILY_PRICE_FIELDS[field_name]) for field_name in fields]


def _append_daily_price_rows(
    prices: dict[str, list[dict[str, Any]]],
    rows: list[tuple[Any, ...]],
    fields: tuple[str, ...],
) -> None:
    for row in rows:
        ticker = row[0]
        logical_date = row[1]
        if ticker in prices:
            prices[ticker].append(
                {
                    "date": logical_date.isoformat(),
                    **{field_name: row[index + 2] for index, field_name in enumerate(fields)},
                }
            )


def _query_basic_fundamentals_batch(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
    fiscal_year: int | None,
) -> dict[str, dict[str, Any]]:
    result = {
        ticker: {
            "ticker": ticker,
            "as_of": as_of.isoformat(),
            "fiscal_year": fiscal_year,
            "metrics": {},
        }
        for ticker in tickers
    }
    if not tickers:
        return result

    rows = _query_fundamental_rows(
        paths,
        tickers,
        _basic_fundamental_account_ids(BASIC_FUNDAMENTAL_METRICS),
        as_of,
        fiscal_year,
        fiscal_year,
    )

    candidates: dict[str, dict[str, list[dict[str, Any]]]] = {
        ticker: {metric: [] for metric in BASIC_FUNDAMENTAL_METRICS} for ticker in tickers
    }
    for row in rows:
        ticker = row["ticker"]
        metric = _basic_fundamental_metric(row["account_id"])
        if ticker in candidates and metric is not None:
            candidates[ticker][metric].append(row)

    for ticker, metrics in candidates.items():
        selected: dict[str, dict[str, Any]] = {}
        selected_years: list[int] = []
        selected_dates: list[date] = []
        for metric, rows_for_metric in metrics.items():
            row = _select_basic_fundamental_row(metric, rows_for_metric)
            if row is None:
                continue
            selected[metric] = _fundamental_metric_payload(row)
            if row["fiscal_year"] is not None:
                selected_years.append(row["fiscal_year"])
            if row["available_date"] is not None:
                selected_dates.append(row["available_date"])
        _sanitize_selected_equity_metric(selected)
        result[ticker]["metrics"] = selected
        if fiscal_year is None and selected_years:
            result[ticker]["fiscal_year"] = max(selected_years)
        if selected_dates:
            result[ticker]["available_date"] = max(selected_dates).isoformat()
    _attach_per_share_metrics(paths, result, as_of)
    return result


def _sanitize_selected_equity_metric(selected: dict[str, dict[str, Any]]) -> None:
    assets = selected.get("assets")
    liabilities = selected.get("liabilities")
    if assets is None or liabilities is None:
        return
    assets_amount = assets.get("amount")
    liabilities_amount = liabilities.get("amount")
    if not isinstance(assets_amount, int | float) or not isinstance(liabilities_amount, int | float):
        return
    derived_amount = assets_amount - liabilities_amount
    if derived_amount <= 0:
        return

    equity = selected.get("equity")
    if equity is None:
        selected["equity"] = _derived_equity_payload(assets, liabilities, derived_amount)
        return

    account_id = str(equity.get("account_id") or "")
    if "EquityAttributableToOwnersOfParent" in account_id:
        return

    equity_amount = equity.get("amount")
    if not isinstance(equity_amount, int | float):
        selected["equity"] = _derived_equity_payload(assets, liabilities, derived_amount)
        return
    relative_gap = abs(equity_amount - derived_amount) / max(abs(derived_amount), 1.0)
    if relative_gap > 0.05:
        selected["equity"] = _derived_equity_payload(assets, liabilities, derived_amount, equity)


def _derived_equity_payload(
    assets: dict[str, Any],
    liabilities: dict[str, Any],
    amount: float,
    replaced: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "amount": amount,
        "account_id": "derived_AssetsMinusLiabilities",
        "account_name": "assets minus liabilities",
        "fiscal_year": assets.get("fiscal_year"),
        "fiscal_period_end": assets.get("fiscal_period_end"),
        "available_date": _max_present(values=(assets.get("available_date"), liabilities.get("available_date"))),
        "report_type": assets.get("report_type"),
        "is_consolidated": assets.get("is_consolidated"),
        "derived_from": ["assets", "liabilities"],
        "quality_note": "equity derived from assets minus liabilities",
    }
    if replaced is not None:
        payload["replaced_account_id"] = replaced.get("account_id")
        payload["replaced_account_name"] = replaced.get("account_name")
        payload["replaced_amount"] = replaced.get("amount")
    return payload


def _max_present(values: tuple[Any, ...]) -> Any:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _query_capital_actions_batch(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    rows = _query_fundamental_rows(
        paths,
        tickers,
        _basic_fundamental_account_ids(CAPITAL_ACTION_METRICS),
        as_of,
        start_year,
        end_year,
    )
    by_key: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
    for row in rows:
        metric = _basic_fundamental_metric(row["account_id"], CAPITAL_ACTION_METRICS)
        if metric is None or row["fiscal_year"] is None:
            continue
        by_key.setdefault((row["ticker"], row["fiscal_year"], metric), []).append(row)

    grouped: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for (ticker, fiscal_year, metric), metric_rows in by_key.items():
        row = _select_basic_fundamental_row(metric, metric_rows, CAPITAL_ACTION_METRICS)
        if row is None:
            continue
        grouped.setdefault((ticker, fiscal_year), {})[metric] = _fundamental_metric_payload(row)

    for (ticker, fiscal_year), metrics in sorted(grouped.items()):
        if ticker in result:
            result[ticker].append({"fiscal_year": fiscal_year, "metrics": metrics})
    return result


def _attach_per_share_metrics(
    paths: ProjectPaths,
    fundamentals: dict[str, dict[str, Any]],
    as_of: date,
) -> None:
    cutoffs: dict[str, date] = {}
    for ticker, payload in fundamentals.items():
        metrics = payload.get("metrics", {})
        dates = [
            _parse_iso_date(metric.get("fiscal_period_end"))
            for metric_name, metric in metrics.items()
            if metric_name in {"net_income", "equity"}
        ]
        dates = [value for value in dates if value is not None]
        if dates:
            cutoffs[ticker] = max(dates)
    if not cutoffs:
        return

    denominators = _query_share_denominators(paths, cutoffs, as_of)
    eps_numerators = _query_eps_numerators(paths, list(cutoffs), as_of)
    direct_eps = _query_direct_eps_metrics(paths, list(cutoffs), as_of)
    for ticker, payload in fundamentals.items():
        metrics = payload.get("metrics", {})
        denominator = denominators.get(ticker)
        per_share: dict[str, Any] = {}
        eps = eps_numerators.get(ticker, {})
        direct = direct_eps.get(ticker, {})
        if denominator and denominator["shares"]:
            if "equity" in metrics:
                per_share["bps"] = _per_share_payload(
                    metrics["equity"],
                    denominator,
                    "equity",
                )
            if eps.get("annual") is not None:
                per_share["eps_annual"] = _per_share_payload(
                    eps["annual"],
                    denominator,
                    "net_income_annual",
                )
            if eps.get("ttm") is not None:
                per_share["eps_ttm"] = _per_share_payload(
                    eps["ttm"],
                    denominator,
                    "net_income_ttm",
                )
            else:
                per_share["eps_ttm"] = _unavailable_per_share_payload(
                    denominator,
                    "net_income_ttm",
                    eps.get("ttm_reason", "ttm source rows unavailable"),
                )
            per_share["eps_forward"] = _unavailable_per_share_payload(
                denominator,
                "net_income_forward",
                "forward earnings estimates are not available in the local dataset",
            )
        elif "equity" in metrics:
            per_share["bps"] = _unavailable_denominator_per_share_payload(
                metrics["equity"],
                "equity",
                "share denominator is not available as of the equity fiscal period",
            )
        if direct.get("annual") is not None:
            per_share["eps_annual"] = _direct_per_share_payload(
                direct["annual"],
                "basic_eps_annual",
            )
        if direct.get("ttm") is not None:
            per_share["eps_ttm"] = _direct_per_share_payload(
                direct["ttm"],
                "basic_eps_ttm",
            )
        if per_share:
            payload["per_share"] = per_share


def _per_share_payload(
    metric: dict[str, Any],
    denominator: dict[str, Any],
    numerator_metric: str,
) -> dict[str, Any]:
    shares = denominator["shares"]
    value = metric["amount"] / shares if shares else None
    return {
        "value": value,
        "numerator_metric": numerator_metric,
        "numerator_amount": metric["amount"],
        "shares": shares,
        "share_date": denominator["share_date"],
        "share_basis": denominator["share_basis"],
        "includes_preferred": denominator["includes_preferred"],
        "treasury_shares_excluded": denominator["treasury_shares_excluded"],
        "components": denominator["components"],
    }


def _unavailable_per_share_payload(
    denominator: dict[str, Any],
    numerator_metric: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "value": None,
        "numerator_metric": numerator_metric,
        "numerator_amount": None,
        "shares": denominator["shares"],
        "share_date": denominator["share_date"],
        "share_basis": denominator["share_basis"],
        "includes_preferred": denominator["includes_preferred"],
        "treasury_shares_excluded": denominator["treasury_shares_excluded"],
        "components": denominator["components"],
        "available": False,
        "reason": reason,
    }


def _unavailable_denominator_per_share_payload(
    metric: dict[str, Any],
    numerator_metric: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "value": None,
        "numerator_metric": numerator_metric,
        "numerator_amount": metric["amount"],
        "shares": None,
        "share_date": None,
        "share_basis": None,
        "includes_preferred": None,
        "treasury_shares_excluded": None,
        "components": [],
        "available": False,
        "reason": reason,
    }


def _direct_per_share_payload(metric: dict[str, Any], numerator_metric: str) -> dict[str, Any]:
    return {
        "value": metric["amount"],
        "numerator_metric": numerator_metric,
        "numerator_amount": metric["amount"],
        "shares": None,
        "share_date": metric["fiscal_period_end"],
        "share_basis": "reported_basic_eps",
        "includes_preferred": None,
        "treasury_shares_excluded": True,
        "components": [],
        "source_account_id": metric["account_id"],
        "source_account_name": metric["account_name"],
        "available": True,
    }


def _query_direct_eps_metrics(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    rows = _query_fundamental_rows(
        paths,
        tickers,
        _basic_fundamental_account_ids(DIRECT_EPS_METRICS),
        as_of,
        None,
        None,
    )
    by_ticker: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    for row in rows:
        if row["ticker"] in by_ticker:
            by_ticker[row["ticker"]].append(row)

    result: dict[str, dict[str, Any]] = {}
    for ticker, ticker_rows in by_ticker.items():
        annual = _select_direct_eps_annual_row(ticker_rows)
        latest = _select_basic_fundamental_row("eps_basic", ticker_rows, DIRECT_EPS_METRICS)
        ttm = annual if latest is not None and latest["report_type"] == "11011" else None
        result[ticker] = {
            "annual": _eps_metric_payload(annual, "reported_annual") if annual else None,
            "ttm": _eps_metric_payload(ttm, "reported_ttm") if ttm else None,
        }
    return result


def _select_direct_eps_annual_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    annual_rows = [row for row in rows if row["report_type"] == "11011"]
    return _select_basic_fundamental_row("eps_basic", annual_rows, DIRECT_EPS_METRICS)


def _query_eps_numerators(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    rows = _query_fundamental_rows(
        paths,
        tickers,
        _basic_fundamental_account_ids({"net_income": BASIC_FUNDAMENTAL_METRICS["net_income"]}),
        as_of,
        None,
        None,
    )
    by_ticker: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    for row in rows:
        if row["ticker"] in by_ticker:
            by_ticker[row["ticker"]].append(row)

    result: dict[str, dict[str, Any]] = {}
    for ticker, ticker_rows in by_ticker.items():
        annual = _select_eps_annual_row(ticker_rows)
        ttm, reason = _select_eps_ttm_row(ticker_rows)
        result[ticker] = {
            "annual": _eps_metric_payload(annual, "annual") if annual else None,
            "ttm": _eps_metric_payload(ttm, "ttm") if ttm else None,
            "ttm_reason": reason,
        }
    return result


def _select_eps_annual_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    annual_rows = [row for row in rows if row["report_type"] == "11011"]
    return _select_basic_fundamental_row("net_income", annual_rows)


def _select_eps_ttm_row(rows: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str | None]:
    latest = _select_basic_fundamental_row("net_income", rows)
    if latest is None:
        return None, "net income rows unavailable"
    if latest["report_type"] == "11011":
        return latest, None

    period_end = latest["fiscal_period_end"]
    fiscal_year = latest["fiscal_year"]
    if not isinstance(period_end, date) or fiscal_year is None:
        return None, "latest net income row lacks fiscal period metadata"

    prior_annual = _select_matching_eps_row(rows, fiscal_year - 1, 12, "11011")
    prior_same_period = _select_matching_eps_row(
        rows,
        fiscal_year - 1,
        period_end.month,
        latest["report_type"],
    )
    if prior_annual is None or prior_same_period is None:
        return None, "prior-year annual or matching interim row unavailable"

    ttm = dict(latest)
    ttm["amount"] = latest["amount"] + prior_annual["amount"] - prior_same_period["amount"]
    ttm["account_name"] = "TTM net income"
    return ttm, None


def _select_matching_eps_row(
    rows: list[dict[str, Any]],
    fiscal_year: int,
    fiscal_period_month: int,
    report_type: str,
) -> dict[str, Any] | None:
    matches = [
        row
        for row in rows
        if row["fiscal_year"] == fiscal_year
        and row["report_type"] == report_type
        and isinstance(row["fiscal_period_end"], date)
        and row["fiscal_period_end"].month == fiscal_period_month
    ]
    return _select_basic_fundamental_row("net_income", matches)


def _eps_metric_payload(row: dict[str, Any], basis: str) -> dict[str, Any]:
    payload = _fundamental_metric_payload(row)
    payload["basis"] = basis
    return payload


def _query_share_denominators(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    if not cutoffs:
        return {}
    return _query_dart_share_denominators(paths, cutoffs, as_of)


def _query_dart_share_denominators(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
    as_of: date,
) -> dict[str, dict[str, Any]]:
    ticker_to_corp = _query_ticker_corp_codes(paths, list(cutoffs))
    if not ticker_to_corp:
        return {}
    result = _query_dart_share_denominators_dataset(paths, cutoffs, as_of, ticker_to_corp)
    missing = {ticker: cutoff for ticker, cutoff in cutoffs.items() if ticker not in result}
    if not missing:
        return result
    rows_by_corp = _load_cached_dart_stock_total_rows(paths.data_root)
    for ticker, cutoff in missing.items():
        corp_code = ticker_to_corp.get(ticker)
        if not corp_code:
            continue
        candidates = [
            row
            for row in rows_by_corp.get(corp_code, [])
            if row.get("stock_date") is not None and row["stock_date"] <= cutoff
        ]
        denominator = _dart_share_denominator_from_rows(candidates)
        if denominator is not None:
            result[ticker] = denominator
    return result


def _query_dart_share_denominators_dataset(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
    as_of: date,
    ticker_to_corp: dict[str, str],
) -> dict[str, dict[str, Any]]:
    if paths.catalog_path.exists():
        return _query_dart_share_denominators_dataset_catalog(paths, cutoffs, as_of, ticker_to_corp)
    return _query_dart_share_denominators_dataset_parquet(paths, cutoffs, as_of, ticker_to_corp)


def _query_dart_share_denominators_dataset_catalog(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
    as_of: date,
    ticker_to_corp: dict[str, str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        for ticker, cutoff in cutoffs.items():
            corp_code = ticker_to_corp.get(ticker)
            if not corp_code:
                continue
            rows = conn.execute(
                """
                WITH latest_period AS (
                    SELECT max(fiscal_period_end) AS fiscal_period_end
                    FROM silver.share_counts
                    WHERE corp_code = ?
                      AND fiscal_period_end <= ?
                      AND available_date <= ?
                )
                SELECT
                    stock_kind,
                    share_class,
                    fiscal_period_end,
                    issued_shares,
                    treasury_shares,
                    outstanding_shares,
                    source_rcept_no
                FROM silver.share_counts
                WHERE corp_code = ?
                  AND fiscal_period_end = (SELECT fiscal_period_end FROM latest_period)
                  AND available_date <= ?
                """,
                [corp_code, cutoff, as_of, corp_code, as_of],
            ).fetchall()
            denominator = _dart_share_denominator_from_dataset_rows(rows)
            if denominator is not None:
                result[ticker] = denominator
    return result


def _query_dart_share_denominators_dataset_parquet(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
    as_of: date,
    ticker_to_corp: dict[str, str],
) -> dict[str, dict[str, Any]]:
    share_glob = paths.data_root / "silver/share_counts/fiscal_year=*/part.parquet"
    if not glob(share_glob.as_posix()):
        return {}
    result: dict[str, dict[str, Any]] = {}
    with duckdb.connect(":memory:") as conn:
        for ticker, cutoff in cutoffs.items():
            corp_code = ticker_to_corp.get(ticker)
            if not corp_code:
                continue
            rows = conn.execute(
                f"""
                WITH share_counts AS (
                    SELECT *
                    FROM read_parquet('{_duckdb_path(share_glob)}', hive_partitioning = true, union_by_name = true)
                ),
                latest_period AS (
                    SELECT max(fiscal_period_end) AS fiscal_period_end
                    FROM share_counts
                    WHERE corp_code = ?
                      AND fiscal_period_end <= ?
                      AND available_date <= ?
                )
                SELECT
                    stock_kind,
                    share_class,
                    fiscal_period_end,
                    issued_shares,
                    treasury_shares,
                    outstanding_shares,
                    source_rcept_no
                FROM share_counts
                WHERE corp_code = ?
                  AND fiscal_period_end = (SELECT fiscal_period_end FROM latest_period)
                  AND available_date <= ?
                """,
                [corp_code, cutoff, as_of, corp_code, as_of],
            ).fetchall()
            denominator = _dart_share_denominator_from_dataset_rows(rows)
            if denominator is not None:
                result[ticker] = denominator
    return result


def _query_ticker_corp_codes(paths: ProjectPaths, tickers: list[str]) -> dict[str, str]:
    if not tickers:
        return {}
    result: dict[str, str] = {}
    if paths.catalog_path.exists():
        with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
            for ticker, corp_code in conn.execute(
                """
                SELECT ticker, corp_code
                FROM analytics.securities
                WHERE ticker IN (SELECT unnest(?))
                  AND corp_code IS NOT NULL
                """,
                [tickers],
            ).fetchall():
                result[str(ticker).zfill(6)] = str(corp_code)
        return result

    master_path = paths.data_root / "gold/security_master.parquet"
    if not master_path.exists():
        return result
    with duckdb.connect(":memory:") as conn:
        for ticker, corp_code in conn.execute(
            f"""
            SELECT ticker, corp_code
            FROM read_parquet('{_duckdb_path(master_path)}', union_by_name = true)
            WHERE ticker IN (SELECT unnest(?))
              AND corp_code IS NOT NULL
            """,
            [tickers],
        ).fetchall():
            result[str(ticker).zfill(6)] = str(corp_code)
    return result


def _load_cached_dart_stock_total_rows(data_root: Path) -> dict[str, list[dict[str, Any]]]:
    cache_glob = data_root / "_cache/opendart_listed_investments_*/stockTotqySttus_*_*.json"
    rows_by_corp: dict[str, list[dict[str, Any]]] = {}
    for path in glob(cache_glob.as_posix()):
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for row in payload.get("list") or []:
            if not isinstance(row, dict):
                continue
            corp_code = str(row.get("corp_code") or "")
            if not corp_code:
                continue
            out = dict(row)
            out["stock_date"] = _parse_iso_date(row.get("stlm_dt"))
            rows_by_corp.setdefault(corp_code, []).append(out)
    return rows_by_corp


def _dart_share_denominator_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    dated_rows = [row for row in rows if row.get("stock_date") is not None]
    if not dated_rows:
        return None
    latest_date = max(row["stock_date"] for row in dated_rows)
    latest_rows = [row for row in dated_rows if row["stock_date"] == latest_date]
    components: list[dict[str, Any]] = []
    for row in latest_rows:
        stock_kind = str(row.get("se") or "")
        if stock_kind in {"합계", "비고"}:
            continue
        outstanding = _int_or_none(row.get("distb_stock_co"))
        if outstanding is None or outstanding <= 0:
            continue
        issued = _int_or_none(row.get("istc_totqy"))
        treasury = _int_or_none(row.get("tesstk_co"))
        components.append(
            {
                "stock_kind": stock_kind,
                "share_class": share_class_from_stock_kind(stock_kind),
                "share_date": latest_date.isoformat(),
                "issued_shares": issued,
                "treasury_shares": treasury,
                "outstanding_shares": outstanding,
                "source_rcept_no": row.get("rcept_no"),
                "source": "opendart.stockTotqySttus",
            }
        )
    shares = sum(row["outstanding_shares"] or 0 for row in components)
    if shares <= 0:
        return None
    return {
        "shares": shares,
        "share_date": latest_date.isoformat(),
        "share_basis": "dart_distributed_shares",
        "includes_preferred": any(row["share_class"] == "preferred" for row in components),
        "treasury_shares_excluded": True,
        "components": components,
    }


def _dart_share_denominator_from_dataset_rows(rows: list[tuple[Any, ...]]) -> dict[str, Any] | None:
    components: list[dict[str, Any]] = []
    for row in rows:
        if row[0] in {"합계", "비고"}:
            continue
        outstanding = row[5]
        if outstanding is None or outstanding <= 0:
            continue
        components.append(
            {
                "stock_kind": row[0],
                "share_class": row[1],
                "share_date": _iso_or_none(row[2]),
                "issued_shares": row[3],
                "treasury_shares": row[4],
                "outstanding_shares": outstanding,
                "source_rcept_no": row[6],
                "source": "opendart.stockTotqySttus",
            }
        )
    shares = sum(row["outstanding_shares"] or 0 for row in components)
    if shares <= 0:
        return None
    share_dates = [row["share_date"] for row in components if row["share_date"]]
    return {
        "shares": shares,
        "share_date": max(share_dates) if share_dates else None,
        "share_basis": "dart_distributed_shares",
        "includes_preferred": any(row["share_class"] == "preferred" for row in components),
        "treasury_shares_excluded": True,
        "components": components,
    }


def _query_share_denominators_catalog(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        for ticker, cutoff in cutoffs.items():
            rows = conn.execute(
                """
                WITH requested AS (
                    SELECT COALESCE(corp_code, security_id) AS issuer_key
                    FROM analytics.securities
                    WHERE ticker = ?
                    LIMIT 1
                ),
                issuer_securities AS (
                    SELECT
                        sm.security_id,
                        sm.ticker,
                        sm.share_class,
                        sm.security_type
                    FROM analytics.securities AS sm
                    JOIN requested AS r
                        ON COALESCE(sm.corp_code, sm.security_id) = r.issuer_key
                    WHERE sm.security_type = 'equity'
                ),
                ranked AS (
                    SELECT
                        s.ticker,
                        s.security_id,
                        s.share_class,
                        p.date,
                        p.listed_shares,
                        row_number() OVER (
                            PARTITION BY s.security_id
                            ORDER BY p.date DESC
                        ) AS rn
                    FROM issuer_securities AS s
                    JOIN analytics.daily_prices AS p
                        ON p.security_id = s.security_id
                    WHERE p.date <= ?
                      AND p.listed_shares IS NOT NULL
                      AND p.listed_shares > 0
                )
                SELECT ticker, security_id, share_class, date, listed_shares
                FROM ranked
                WHERE rn = 1
                ORDER BY ticker
                """,
                [ticker, cutoff],
            ).fetchall()
            denominator = _share_denominator_from_rows(rows)
            if denominator is not None:
                result[ticker] = denominator
    return result


def _query_share_denominators_parquet(
    paths: ProjectPaths,
    cutoffs: dict[str, date],
) -> dict[str, dict[str, Any]]:
    master_path = paths.data_root / "gold/security_master.parquet"
    price_glob = paths.data_root / "gold/daily_prices_adj/dt=*/part.parquet"
    if not master_path.exists() or not glob(price_glob.as_posix()):
        return {}
    result: dict[str, dict[str, Any]] = {}
    with duckdb.connect(":memory:") as conn:
        for ticker, cutoff in cutoffs.items():
            rows = conn.execute(
                f"""
                WITH master AS (
                    SELECT *
                    FROM read_parquet('{_duckdb_path(master_path)}', union_by_name = true)
                ),
                requested AS (
                    SELECT COALESCE(corp_code, security_id) AS issuer_key
                    FROM master
                    WHERE ticker = ?
                    LIMIT 1
                ),
                issuer_securities AS (
                    SELECT
                        sm.security_id,
                        sm.ticker,
                        sm.share_class,
                        sm.security_type
                    FROM master AS sm
                    JOIN requested AS r
                        ON COALESCE(sm.corp_code, sm.security_id) = r.issuer_key
                    WHERE sm.security_type = 'equity'
                ),
                prices AS (
                    SELECT *
                    FROM read_parquet('{_duckdb_path(price_glob)}', hive_partitioning = true, union_by_name = true)
                ),
                ranked AS (
                    SELECT
                        s.ticker,
                        s.security_id,
                        s.share_class,
                        p.date,
                        p.listed_shares,
                        row_number() OVER (
                            PARTITION BY s.security_id
                            ORDER BY p.date DESC
                        ) AS rn
                    FROM issuer_securities AS s
                    JOIN prices AS p
                        ON p.security_id = s.security_id
                    WHERE p.date <= ?
                      AND p.listed_shares IS NOT NULL
                      AND p.listed_shares > 0
                )
                SELECT ticker, security_id, share_class, date, listed_shares
                FROM ranked
                WHERE rn = 1
                ORDER BY ticker
                """,
                [ticker, cutoff],
            ).fetchall()
            denominator = _share_denominator_from_rows(rows)
            if denominator is not None:
                result[ticker] = denominator
    return result


def _share_denominator_from_rows(rows: list[tuple[Any, ...]]) -> dict[str, Any] | None:
    components = [
        {
            "ticker": row[0],
            "security_id": row[1],
            "share_class": row[2],
            "share_date": _iso_or_none(row[3]),
            "listed_shares": row[4],
        }
        for row in rows
    ]
    shares = sum(row["listed_shares"] or 0 for row in components)
    if shares <= 0:
        return None
    share_dates = [row["share_date"] for row in components if row["share_date"]]
    return {
        "shares": shares,
        "share_date": max(share_dates) if share_dates else None,
        "share_basis": "issuer_listed_shares_sum",
        "includes_preferred": any(row["share_class"] == "preferred" for row in components),
        "treasury_shares_excluded": False,
        "components": components,
    }


def _query_dividends_batch(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    if not tickers:
        return result
    rows = (
        _query_dividend_rows_catalog(paths, tickers, as_of, start_year, end_year)
        if paths.catalog_path.exists()
        else _query_dividend_rows_parquet(paths, tickers, as_of, start_year, end_year)
    )
    for row in rows:
        ticker = row.get("ticker")
        if ticker in result:
            result[ticker].append(row)
    return result


def _dividend_year_filter(
    start_year: int | None,
    end_year: int | None,
) -> tuple[str, list[int]]:
    clauses: list[str] = []
    params: list[int] = []
    if start_year is not None:
        clauses.append("AND d.fiscal_year >= ?")
        params.append(start_year)
    if end_year is not None:
        clauses.append("AND d.fiscal_year <= ?")
        params.append(end_year)
    return "\n".join(clauses), params


def _query_dividend_rows_catalog(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    year_filter, year_params = _dividend_year_filter(start_year, end_year)
    params: list[Any] = [*tickers, as_of, *year_params]
    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT
                d.fiscal_year,
                d.fiscal_period_end,
                d.rcept_dt,
                d.available_date,
                d.corp_code,
                d.corp_name,
                d.security_id,
                d.ticker,
                d.share_class,
                d.stock_kind,
                d.cash_dividend_per_share,
                d.stock_dividend_per_share,
                d.cash_dividend_yield_pct,
                d.currency,
                d.source_rcept_no,
                d.report_type,
                d.source,
                d.is_estimated
            FROM silver.dividends AS d
            WHERE d.ticker IN ({_sql_placeholders(tickers)})
              AND d.available_date <= ?
              {year_filter}
            ORDER BY d.ticker, d.fiscal_year, d.share_class, d.stock_kind
            """,
            params,
        ).fetchall()
    return [_dividend_row_dict(row) for row in rows]


def _query_dividend_rows_parquet(
    paths: ProjectPaths,
    tickers: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    dividends_glob = paths.data_root / "silver/dividends/fiscal_year=*/part.parquet"
    if not glob(dividends_glob.as_posix()):
        return []
    year_filter, year_params = _dividend_year_filter(start_year, end_year)
    params: list[Any] = [*tickers, as_of, *year_params]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"""
            SELECT
                d.fiscal_year,
                d.fiscal_period_end,
                d.rcept_dt,
                d.available_date,
                d.corp_code,
                d.corp_name,
                d.security_id,
                d.ticker,
                d.share_class,
                d.stock_kind,
                d.cash_dividend_per_share,
                d.stock_dividend_per_share,
                d.cash_dividend_yield_pct,
                d.currency,
                d.source_rcept_no,
                d.report_type,
                d.source,
                d.is_estimated
            FROM read_parquet('{_duckdb_path(dividends_glob)}', hive_partitioning = true, union_by_name = true) AS d
            WHERE d.ticker IN ({_sql_placeholders(tickers)})
              AND d.available_date <= ?
              {year_filter}
            ORDER BY d.ticker, d.fiscal_year, d.share_class, d.stock_kind
            """,
            params,
        ).fetchall()
    return [_dividend_row_dict(row) for row in rows]


def _dividend_row_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "fiscal_year": row[0],
        "fiscal_period_end": _iso_or_none(row[1]),
        "rcept_dt": _iso_or_none(row[2]),
        "available_date": _iso_or_none(row[3]),
        "corp_code": row[4],
        "corp_name": row[5],
        "security_id": row[6],
        "ticker": row[7],
        "share_class": row[8],
        "stock_kind": row[9],
        "cash_dividend_per_share": row[10],
        "stock_dividend_per_share": row[11],
        "cash_dividend_yield_pct": row[12],
        "currency": row[13],
        "source_rcept_no": row[14],
        "report_type": row[15],
        "source": row[16],
        "is_estimated": row[17],
    }


# --- Screener endpoint -----------------------------------------------------
# 가치 스크리너용 전 유니버스 스냅샷. 최신 거래일의 KOSPI/KOSDAQ 전 종목에서
# PER/PBR/ROE/배당수익률/영업이익률/부채비율/시가총액을 한 번에 계산해 반환한다.
# value-invest 허브가 라이브로 호출하며, finance-pi 자체에는 결과를 저장하지 않는다.

# 스크리너에서 pivot 할 재무 계정. BASIC_FUNDAMENTAL_METRICS 의 부분집합 —
# ROE/영업이익률/부채비율/PBR 계산에 필요한 다섯 가지(liabilities 가 부채비율용).
_SCREENER_FUNDAMENTAL_METRICS = ("revenue", "operating_profit", "net_income", "equity", "liabilities")


def _screener_account_ids() -> list[str]:
    return _basic_fundamental_account_ids(
        {m: BASIC_FUNDAMENTAL_METRICS[m] for m in _SCREENER_FUNDAMENTAL_METRICS}
    )


def _screener_metric_for_account(account_id: str) -> str | None:
    for metric in _SCREENER_FUNDAMENTAL_METRICS:
        if account_id in BASIC_FUNDAMENTAL_METRICS[metric]:
            return metric
    return None


def _query_screener_batch(
    paths: ProjectPaths,
    as_of: date,
) -> dict[str, Any]:
    """전 KOSPI/KOSDAQ 유니버스의 가치 지표 스냅샷.

    catalog 가 있으면 DuckDB 뷰(analytics.daily_prices + silver.*)로 한 번에
    조인하고, 없으면 parquet 직접 scan 으로 폴백한다. 유니버스는 최신 거래일의
    analytics.daily_prices 에서 market IN (KOSPI, KOSDAQ) 이고 SPAC/리츠가 아닌
    종목으로 정한다.
    """
    account_ids = _screener_account_ids()
    if paths.catalog_path.exists():
        rows = _query_screener_rows_catalog(paths, account_ids, as_of)
    else:
        rows = _query_screener_rows_parquet(paths, account_ids, as_of)
    snapshot_date = as_of.isoformat()
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(_build_screener_row(row, snapshot_date))
    return {
        "as_of": snapshot_date,
        "count": len(results),
        "rows": results,
    }


def _build_screener_row(row: dict[str, Any], as_of_text: str) -> dict[str, Any]:
    """쿼리 결과 한 행(이미 pivot 된 정규화 필드)을 스크리너 응답으로 다듬는다.

    파생 지표를 우선순위와 함께 계산한다:
      - per: naver 원본값 우선, 없으면 market_cap / net_income
      - roe: naver 원본값 우선, 없으면 net_income / equity * 100
      - pbr: market_cap / equity (equity 없으면 null)
      - operating_margin: operating_profit / revenue * 100
      - debt_ratio: liabilities / equity * 100 (liability 는 별도 조인 없이
        equity 와 함께 fundamentals_pit 에서 가져올 수도 있지만, 스크리너의
        주 관심사는 아니므로 row 에 있을 때만 채운다)
    """
    close = row.get("close")
    market_cap = row.get("market_cap")
    equity = row.get("equity")
    net_income = row.get("net_income")
    operating_profit = row.get("operating_profit")
    revenue = row.get("revenue")

    per_naver = row.get("per_naver")
    roe_naver = row.get("roe_naver")

    per = per_naver
    if per is None and market_cap and net_income and net_income != 0:
        per = market_cap / net_income

    roe = roe_naver
    if roe is None and net_income is not None and equity and equity != 0:
        roe = net_income / equity * 100

    pbr = None
    if market_cap and equity and equity != 0:
        pbr = market_cap / equity

    operating_margin = None
    if operating_profit is not None and revenue and revenue != 0:
        operating_margin = operating_profit / revenue * 100

    liabilities = row.get("liabilities")
    debt_ratio = None
    if liabilities is not None and equity and equity != 0:
        debt_ratio = liabilities / equity * 100

    return {
        "ticker": row["ticker"],
        "name": row.get("name"),
        "market": row.get("market"),
        "close": close,
        "per": _round_or_none(per),
        "pbr": _round_or_none(pbr),
        "roe": _round_or_none(roe),
        "market_cap": market_cap,
        "dividend_yield": row.get("dividend_yield"),
        "operating_margin": _round_or_none(operating_margin),
        "debt_ratio": _round_or_none(debt_ratio),
        "revenue": revenue,
        "operating_profit": operating_profit,
        "net_income": net_income,
        "equity": equity,
        "as_of": as_of_text,
    }


def _round_or_none(value: Any, ndigits: int = 4) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return None


def _query_screener_rows_catalog(
    paths: ProjectPaths,
    account_ids: list[str],
    as_of: date,
) -> list[dict[str, Any]]:
    """catalog 경로: analytics.daily_prices + silver.financials/dividends + naver.

    전체를 하나의 DuckDB 쿼리로 조인한다. 유니버스(최신 거래일 daily_prices)를
    베이스로 LEFT JOIN 하므로, 재무/배당 데이터가 없는 종목도 시세·PER(naver)
    는 노출된다.
    """
    params: list[Any] = [as_of, *account_ids, as_of, as_of]
    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            WITH latest_day AS (
                SELECT MAX(date) AS d FROM analytics.daily_prices
                WHERE date <= ?
            ),
            prices AS (
                SELECT
                    dp.ticker,
                    dp.name,
                    dp.market,
                    dp.security_type,
                    dp.close_adj AS close,
                    dp.market_cap
                FROM analytics.daily_prices AS dp, latest_day
                WHERE dp.date = latest_day.d
                  AND dp.market IN ('KOSPI', 'KOSDAQ')
                  AND dp.security_type IS DISTINCT FROM 'spac_pre'
                  AND dp.ticker IS NOT NULL
                  AND dp.name NOT LIKE '%리츠%'
                  AND dp.name NOT ILIKE '%reit%'
            ),
            fin AS (
                SELECT
                    sm.ticker,
                    f.account_id,
                    f.amount
                FROM silver.financials AS f
                JOIN analytics.securities AS sm
                    ON f.security_id = sm.security_id
                WHERE f.account_id IN ({_sql_placeholders(account_ids)})
                  AND f.available_date <= ?
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY sm.ticker, f.account_id
                    ORDER BY f.available_date DESC, f.fiscal_period_end DESC
                ) = 1
            ),
            fin_pivot AS (
                SELECT ticker,
                       MAX(CASE WHEN account_id LIKE '%Revenue%' THEN amount END) AS revenue,
                       MAX(CASE WHEN account_id = 'dart_OperatingIncomeLoss' THEN amount END) AS operating_profit,
                       MAX(CASE WHEN account_id LIKE '%ProfitLoss%' THEN amount END) AS net_income,
                       MAX(CASE WHEN account_id LIKE '%Equity%' THEN amount END) AS equity,
                       MAX(CASE WHEN account_id LIKE '%Liabilities%' THEN amount END) AS liabilities
                FROM fin
                GROUP BY ticker
            ),
            div AS (
                SELECT ticker, MAX(cash_dividend_yield_pct) AS dividend_yield
                FROM silver.dividends
                WHERE available_date <= ?
                GROUP BY ticker
            ),
            naver AS (
                SELECT ticker, per, roe
                FROM (
                    SELECT ticker, per, roe,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY snapshot_dt DESC) AS rn
                    FROM bronze.naver_summary_raw
                ) WHERE rn = 1
            )
            SELECT
                p.ticker,
                p.name,
                p.market,
                p.close,
                p.market_cap,
                fp.revenue,
                fp.operating_profit,
                fp.net_income,
                fp.equity,
                fp.liabilities,
                div.dividend_yield,
                nv.per AS per_naver,
                nv.roe AS roe_naver
            FROM prices AS p
            LEFT JOIN fin_pivot AS fp ON p.ticker = fp.ticker
            LEFT JOIN div ON p.ticker = div.ticker
            LEFT JOIN naver AS nv ON p.ticker = nv.ticker
            ORDER BY p.ticker
            """,
            params,
        ).fetchall()
        columns = [d[0] for d in conn.description] if conn.description else []
    return [dict(zip(columns, r, strict=False)) for r in rows]


def _query_screener_rows_parquet(
    paths: ProjectPaths,
    account_ids: list[str],
    as_of: date,
) -> list[dict[str, Any]]:
    """catalog 미구축 시 parquet 직접 scan 폴백.

    daily_prices_adj + security_master + silver/financials + silver/dividends +
    bronze/naver_summary(최신 파티션) 를 메모리 DuckDB 에서 조인한다.
    데이터가 전혀 없으면 빈 리스트.
    """
    price_glob = paths.data_root / "gold/daily_prices_adj/dt=*/part.parquet"
    master_path = paths.data_root / "gold/security_master.parquet"
    fin_glob = paths.data_root / "silver/financials/fiscal_year=*/part.parquet"
    div_glob = paths.data_root / "silver/dividends/fiscal_year=*/part.parquet"
    if not glob(price_glob.as_posix()) or not master_path.exists():
        return []

    sql_price = _duckdb_path(price_glob)
    sql_master = _duckdb_path(master_path)

    has_fin = glob(fin_glob.as_posix())
    has_div = glob(div_glob.as_posix())
    sql_fin = _duckdb_path(fin_glob)
    sql_div = _duckdb_path(div_glob)

    # naver_summary 최신 파티션.
    naver_files = sorted(paths.data_root.glob("bronze/naver_summary/dt=*/part.parquet"))
    sql_naver = _duckdb_path(naver_files[-1]) if naver_files else None
    naver_cte = ""
    naver_join = ""
    naver_cols = "NULL AS per_naver, NULL AS roe_naver"
    if sql_naver:
        naver_cte = f"""
            , naver AS (
                SELECT ticker, per, roe
                FROM (
                    SELECT ticker, per, roe,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY snapshot_dt DESC) AS rn
                    FROM read_parquet('{sql_naver}', union_by_name = true)
                ) WHERE rn = 1
            )
        """
        naver_join = "LEFT JOIN naver AS nv ON p.ticker = nv.ticker"
        naver_cols = "nv.per AS per_naver, nv.roe AS roe_naver"

    # financials / dividends 가 없으면 빈 CTE 로 대체 — read_parquet(glob) 가
    # 매칭 파일이 없을 때 IO 에러를 뱉기 때문에 미리 체크한다.
    if has_fin:
        fin_cte = f"""
            , fin AS (
                SELECT
                    sm.ticker,
                    f.account_id,
                    f.amount
                FROM read_parquet('{sql_fin}', hive_partitioning = true, union_by_name = true) AS f
                LEFT JOIN read_parquet('{sql_master}', union_by_name = true) AS sm
                    ON f.security_id = sm.security_id
                WHERE f.account_id IN ({_sql_placeholders(account_ids)})
                  AND f.available_date <= ?
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY sm.ticker, f.account_id
                    ORDER BY f.available_date DESC, f.fiscal_period_end DESC
                ) = 1
            )
            , fin_pivot AS (
                SELECT ticker,
                       MAX(CASE WHEN account_id LIKE '%Revenue%' THEN amount END) AS revenue,
                       MAX(CASE WHEN account_id = 'dart_OperatingIncomeLoss' THEN amount END) AS operating_profit,
                       MAX(CASE WHEN account_id LIKE '%ProfitLoss%' THEN amount END) AS net_income,
                       MAX(CASE WHEN account_id LIKE '%Equity%' THEN amount END) AS equity,
                       MAX(CASE WHEN account_id LIKE '%Liabilities%' THEN amount END) AS liabilities
                FROM fin
                GROUP BY ticker
            )
        """
        fin_params: list[Any] = [*account_ids, as_of]
    else:
        fin_cte = """
            , fin_pivot AS (
                SELECT CAST(NULL AS VARCHAR) AS ticker,
                       CAST(NULL AS DOUBLE) AS revenue,
                       CAST(NULL AS DOUBLE) AS operating_profit,
                       CAST(NULL AS DOUBLE) AS net_income,
                       CAST(NULL AS DOUBLE) AS equity,
                       CAST(NULL AS DOUBLE) AS liabilities
                WHERE FALSE
            )
        """
        fin_params = []

    if has_div:
        div_cte = f"""
            , div AS (
                SELECT ticker, MAX(cash_dividend_yield_pct) AS dividend_yield
                FROM read_parquet('{sql_div}', hive_partitioning = true, union_by_name = true)
                WHERE available_date <= ?
                GROUP BY ticker
            )
        """
        div_params: list[Any] = [as_of]
    else:
        div_cte = """
            , div AS (
                SELECT CAST(NULL AS VARCHAR) AS ticker,
                       CAST(NULL AS DOUBLE) AS dividend_yield
                WHERE FALSE
            )
        """
        div_params = []

    params: list[Any] = [as_of, *fin_params, *div_params]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"""
            WITH prices_raw AS (
                SELECT
                    p.security_id,
                    sm.ticker,
                    sm.name,
                    sm.market,
                    sm.security_type,
                    p.close_adj AS close,
                    p.market_cap,
                    p.date
                FROM read_parquet('{sql_price}', hive_partitioning = true, union_by_name = true) AS p
                LEFT JOIN read_parquet('{sql_master}', union_by_name = true) AS sm
                    ON p.security_id = sm.security_id
                WHERE p.date <= ?
                  AND sm.market IN ('KOSPI', 'KOSDAQ')
                  AND sm.security_type IS DISTINCT FROM 'spac_pre'
                  AND sm.ticker IS NOT NULL
                  AND sm.name NOT LIKE '%리츠%'
                  AND sm.name NOT ILIKE '%reit%'
            ),
            latest_per_ticker AS (
                SELECT security_id, ticker, name, market, security_type, close, market_cap
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY date DESC) AS rn
                    FROM prices_raw
                ) WHERE rn = 1
            ),
            prices AS (
                SELECT ticker, name, market, security_type, close, market_cap
                FROM latest_per_ticker
            )
            {fin_cte}
            {div_cte}
            {naver_cte}
            SELECT
                p.ticker,
                p.name,
                p.market,
                p.close,
                p.market_cap,
                fp.revenue,
                fp.operating_profit,
                fp.net_income,
                fp.equity,
                fp.liabilities,
                div.dividend_yield,
                {naver_cols}
            FROM prices AS p
            LEFT JOIN fin_pivot AS fp ON p.ticker = fp.ticker
            LEFT JOIN div ON p.ticker = div.ticker
            {naver_join}
            ORDER BY p.ticker
            """,
            params,
        ).fetchall()
        columns = [d[0] for d in conn.description] if conn.description else []
    return [dict(zip(columns, r, strict=False)) for r in rows]


def _query_fundamental_rows(
    paths: ProjectPaths,
    tickers: list[str],
    account_ids: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    if not tickers or not account_ids:
        return []
    if paths.catalog_path.exists():
        return _query_fundamental_rows_catalog(
            paths,
            tickers,
            account_ids,
            as_of,
            start_year,
            end_year,
        )
    return _query_fundamental_rows_parquet(
        paths,
        tickers,
        account_ids,
        as_of,
        start_year,
        end_year,
    )


def _fundamental_year_filter(start_year: int | None, end_year: int | None) -> tuple[str, list[int]]:
    clauses: list[str] = []
    params: list[int] = []
    if start_year is not None:
        clauses.append("AND f.fiscal_year >= ?")
        params.append(start_year)
    if end_year is not None:
        clauses.append("AND f.fiscal_year <= ?")
        params.append(end_year)
    return "\n".join(clauses), params


def _query_fundamental_rows_catalog(
    paths: ProjectPaths,
    tickers: list[str],
    account_ids: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    year_filter, year_params = _fundamental_year_filter(start_year, end_year)
    params: list[Any] = [*tickers, *account_ids, as_of, *year_params]
    with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT
                sm.ticker,
                f.security_id,
                f.corp_code,
                f.fiscal_year,
                f.fiscal_period_end,
                f.available_date,
                f.report_type,
                f.account_id,
                f.account_name,
                f.amount,
                f.is_consolidated
            FROM silver.financials AS f
            JOIN analytics.securities AS sm
                ON f.security_id = sm.security_id
            WHERE sm.ticker IN ({_sql_placeholders(tickers)})
              AND f.account_id IN ({_sql_placeholders(account_ids)})
              AND f.available_date <= ?
              {year_filter}
            """,
            params,
        ).fetchall()
    return [_fundamental_row_dict(row) for row in rows]


def _query_fundamental_rows_parquet(
    paths: ProjectPaths,
    tickers: list[str],
    account_ids: list[str],
    as_of: date,
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    financial_glob = paths.data_root / "silver/financials/fiscal_year=*/part.parquet"
    master_path = paths.data_root / "gold/security_master.parquet"
    if not glob(financial_glob.as_posix()) or not master_path.exists():
        return []

    year_filter, year_params = _fundamental_year_filter(start_year, end_year)
    sql_financial_path = _duckdb_path(financial_glob)
    sql_master_path = _duckdb_path(master_path)
    params = [*tickers, *account_ids, as_of, *year_params]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"""
            WITH financials AS (
                SELECT *
                FROM read_parquet('{sql_financial_path}', hive_partitioning = true, union_by_name = true)
            ),
            master AS (
                SELECT *
                FROM read_parquet('{sql_master_path}', union_by_name = true)
            )
            SELECT
                sm.ticker,
                f.security_id,
                f.corp_code,
                f.fiscal_year,
                f.fiscal_period_end,
                f.available_date,
                f.report_type,
                f.account_id,
                f.account_name,
                f.amount,
                f.is_consolidated
            FROM financials AS f
            JOIN master AS sm
                ON f.security_id = sm.security_id
            WHERE sm.ticker IN ({_sql_placeholders(tickers)})
              AND f.account_id IN ({_sql_placeholders(account_ids)})
              AND f.available_date <= ?
              {year_filter}
            """,
            params,
        ).fetchall()
    return [_fundamental_row_dict(row) for row in rows]


def _fundamental_row_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "ticker": row[0],
        "security_id": row[1],
        "corp_code": row[2],
        "fiscal_year": row[3],
        "fiscal_period_end": row[4],
        "available_date": row[5],
        "report_type": row[6],
        "account_id": row[7],
        "account_name": row[8],
        "amount": row[9],
        "is_consolidated": row[10],
    }


def _fundamental_metric_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "amount": row["amount"],
        "account_id": row["account_id"],
        "account_name": row["account_name"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_period_end": _iso_or_none(row["fiscal_period_end"]),
        "available_date": _iso_or_none(row["available_date"]),
        "report_type": row["report_type"],
        "is_consolidated": row["is_consolidated"],
    }


def _basic_fundamental_account_ids(metrics: dict[str, tuple[str, ...]]) -> list[str]:
    return list(dict.fromkeys(account for accounts in metrics.values() for account in accounts))


def _basic_fundamental_metric(
    account_id: str,
    metrics: dict[str, tuple[str, ...]] = BASIC_FUNDAMENTAL_METRICS,
) -> str | None:
    for metric, accounts in metrics.items():
        if account_id in accounts:
            return metric
    return None


def _select_basic_fundamental_row(
    metric: str,
    rows: list[dict[str, Any]],
    metrics: dict[str, tuple[str, ...]] = BASIC_FUNDAMENTAL_METRICS,
) -> dict[str, Any] | None:
    if not rows:
        return None
    rank = {account_id: index for index, account_id in enumerate(metrics[metric])}
    return sorted(
        rows,
        key=lambda row: (
            row["fiscal_year"] or 0,
            row["available_date"] or date.min,
            1 if row["is_consolidated"] else 0,
            -rank.get(row["account_id"], 999),
        ),
        reverse=True,
    )[0]


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _safe_quote_float(value: Any) -> float | None:
    if value in (None, "", "N/A", "--"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _lower_or_none(value: Any) -> str | None:
    return str(value).lower() if value is not None else None


def _parse_iso_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, "", " ", "-"):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def _query_macro_table(
    paths: ProjectPaths,
    table: str,
    since: date | None,
    until: date | None,
    filters: dict[str, str | None],
) -> list[dict[str, Any]]:
    if table not in MACRO_TABLE_COLUMNS:
        raise ValueError(f"unknown macro table: {table}")
    columns = MACRO_TABLE_COLUMNS[table]
    where_sql, params = _macro_where_clause(columns, since, until, filters)
    select_sql = ", ".join(columns)
    if paths.catalog_path.exists():
        with duckdb.connect(str(paths.catalog_path), read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT {select_sql}
                FROM macro.{table}
                {where_sql}
                ORDER BY date, series_id
                """,
                params,
            ).fetchall()
        return [_macro_row_dict(columns, row) for row in rows]

    parquet_path = paths.data_root / "macro" / table / "part.parquet"
    if not parquet_path.exists():
        return []
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"""
            SELECT {select_sql}
            FROM read_parquet('{_duckdb_path(parquet_path)}', union_by_name = true)
            {where_sql}
            ORDER BY date, series_id
            """,
            params,
        ).fetchall()
    return [_macro_row_dict(columns, row) for row in rows]


def _query_realtime_indicators(
    category: str | None,
    series_id: str | None,
) -> dict[str, Any]:
    from finance_pi.cli import app as cli_app

    series_by_category = {
        "indices": cli_app.CNBC_INDEX_SERIES,
        "rates": cli_app.CNBC_RATE_SERIES,
        "commodities": cli_app.CNBC_COMMODITY_SERIES,
        "fx": cli_app.CNBC_FX_SERIES,
    }
    selected_categories = [category] if category is not None else list(series_by_category)
    selected_series = {
        key: [
            item
            for item in series_by_category[key]
            if series_id is None or item["series_id"] == series_id
        ]
        for key in selected_categories
    }
    derived_fx = [
        item
        for item in cli_app.CNBC_DERIVED_FX_SERIES
        if (category in (None, "fx")) and (series_id is None or item["series_id"] == series_id)
    ]
    symbols = [
        item["cnbc_symbol"]
        for items in selected_series.values()
        for item in items
    ]
    symbols.extend(item["numerator_symbol"] for item in derived_fx)
    symbols.extend(item["denominator_symbol"] for item in derived_fx)
    symbols = sorted(set(symbols))
    quotes = cli_app._fetch_cnbc_quotes(symbols)
    today = datetime.now(UTC).date()
    since = today - timedelta(days=7)
    until = today + timedelta(days=1)
    indicators = {
        "indices": cli_app._cnbc_index_rows(selected_series.get("indices", []), quotes, since, until),
        "rates": cli_app._cnbc_rate_rows(selected_series.get("rates", []), quotes, since, until),
        "commodities": cli_app._cnbc_commodity_rows(
            selected_series.get("commodities", []),
            quotes,
            since,
            until,
        ),
        "fx": [
            *cli_app._cnbc_fx_rows(selected_series.get("fx", []), quotes, since, until),
            *cli_app._cnbc_derived_fx_rows(derived_fx, quotes, since, until),
        ],
    }
    if series_id is not None:
        indicators = {
            key: [row for row in rows if row.get("series_id") == series_id]
            for key, rows in indicators.items()
        }
    if category is not None:
        indicators = {category: indicators[category]}
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "cnbc",
        "cache_ttl_seconds": AdminState.realtime_cache_seconds,
        "category": category,
        "series_id": series_id,
        "count": sum(len(rows) for rows in indicators.values()),
        "indicators": indicators,
    }


def _macro_where_clause(
    columns: tuple[str, ...],
    since: date | None,
    until: date | None,
    filters: dict[str, str | None],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if since is not None:
        clauses.append("date >= ?")
        params.append(since)
    if until is not None:
        clauses.append("date <= ?")
        params.append(until)
    for key, value in filters.items():
        if value is not None and key in columns:
            clauses.append(f"{key} = ?")
            params.append(value)
    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def _macro_row_dict(columns: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        column: value.isoformat() if hasattr(value, "isoformat") else value
        for column, value in zip(columns, row, strict=True)
    }


def _sql_placeholders(values: list[str]) -> str:
    return ", ".join("?" for _ in values)


def _duckdb_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


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
    if action == "backfill_yearly":
        start_year = _safe_int(payload.get("start_year"), 2023, 1900, 2100)
        end_year = _safe_int(payload.get("end_year"), 1990, 1900, 2100)
        max_years = _safe_int(payload.get("max_years"), 1, 0, 50)
        command = [
            *base,
            "backfill",
            "yearly",
            *root_args,
            "--start-year",
            str(start_year),
            "--end-year",
            str(end_year),
            "--max-years",
            str(max_years),
        ]
        if not _safe_bool(payload.get("include_financials"), True):
            command.append("--skip-financials")
        if _safe_bool(payload.get("include_fundamentals_pit"), False):
            command.append("--include-fundamentals-pit")
        if _safe_bool(payload.get("no_strict"), True):
            command.append("--no-strict")
        if _safe_bool(payload.get("force"), False):
            command.append("--force")
        if _safe_bool(payload.get("dry_run"), False):
            command.append("--dry-run")
        return f"Backfill {start_year}..{end_year}", command
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


def _safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return min(maximum, max(minimum, number))


def _safe_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _safe_float(value: Any, default: str) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return str(min(1.0, max(0.01, number)))


def _dataset_stat(data_root: Path, name: str, spec: Any) -> dict[str, Any]:
    file_stats = _dataset_file_stats(data_root, spec)
    files = [path for path, _size in file_stats]
    size = sum(file_size for _path, file_size in file_stats)
    rows = _row_count(files, spec.hive_partitioning) if files and _scan_parquet_enabled() else None
    latest = _latest_partition(files)
    coverage = _coverage_for(files, spec.hive_partitioning) if files else {}
    return {
        "name": name,
        "layer": spec.layer,
        "files": len(files),
        "rows": rows,
        "bytes": size,
        "latest_partition": latest,
        **coverage,
        "status": "ready" if files else "empty",
    }


def _latest_price_date_from_datasets(datasets: list[dict[str, Any]]) -> str | None:
    dataset = next((item for item in datasets if item["name"] == "gold.daily_prices_adj"), None)
    latest = dataset.get("latest_partition") if dataset else None
    if not isinstance(latest, str) or "=" not in latest:
        return None
    return latest.split("=", maxsplit=1)[1]


def _dataset_file_stats(data_root: Path, spec: Any) -> list[tuple[Path, int]]:
    relative_glob = getattr(spec, "relative_glob", "")
    files = _partitioned_part_file_stats(data_root, relative_glob)
    if files is not None:
        return files
    result: list[tuple[Path, int]] = []
    for file in glob(spec.glob_path(data_root).as_posix()):
        path = Path(file)
        try:
            result.append((path, path.stat().st_size))
        except OSError:
            continue
    return result


def _partitioned_part_file_stats(data_root: Path, relative_glob: str) -> list[tuple[Path, int]] | None:
    parts = relative_glob.split("/")
    if not parts or parts[-1] != "part.parquet" or not any("*" in part for part in parts):
        return None

    results: list[tuple[Path, int]] = []

    def visit(directory: Path, index: int) -> None:
        if index == len(parts) - 1:
            path = directory / parts[index]
            with suppress(OSError):
                results.append((path, path.stat().st_size))
            return

        part = parts[index]
        if "*" not in part:
            visit(directory / part, index + 1)
            return

        prefix, suffix = part.split("*", maxsplit=1)
        try:
            entries = list(os.scandir(directory))
        except OSError:
            return
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            if entry.name.startswith(prefix) and entry.name.endswith(suffix):
                visit(Path(entry.path), index + 1)

    visit(data_root, 0)
    return results


def _coverage_for(files: list[Path], hive_partitioning: bool) -> dict[str, str | None]:
    columns = ("date", "dt", "rcept_dt", "snapshot_dt", "request_dt", "fiscal_year")
    partitions = _partition_ranges(files)
    for column in columns:
        values = partitions.get(column)
        if values:
            return {
                "coverage_field": column,
                "coverage_start": values[0],
                "coverage_end": values[-1],
            }
    if _scan_parquet_enabled():
        try:
            scan = pl.scan_parquet(
                [file.as_posix() for file in files],
                hive_partitioning=hive_partitioning,
            )
            schema = scan.collect_schema()
            for column in columns:
                if column not in schema.names():
                    continue
                row = (
                    scan.select(
                        pl.col(column).min().alias("coverage_start"),
                        pl.col(column).max().alias("coverage_end"),
                    )
                    .collect()
                    .row(0)
                )
                return {
                    "coverage_field": column,
                    "coverage_start": _format_coverage_value(row[0]),
                    "coverage_end": _format_coverage_value(row[1]),
                }
        except Exception:  # noqa: BLE001
            pass
    return {"coverage_field": None, "coverage_start": None, "coverage_end": None}


def _partition_ranges(files: list[Path]) -> dict[str, list[str]]:
    ranges: dict[str, set[str]] = {}
    for file in files:
        for part in file.parts:
            if "=" not in part:
                continue
            key, value = part.split("=", maxsplit=1)
            ranges.setdefault(key, set()).add(value)
    return {key: sorted(values) for key, values in ranges.items()}


def _format_coverage_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _price_coverage(datasets: list[dict[str, Any]]) -> dict[str, str | None]:
    for name in ("gold.daily_prices_adj", "silver.prices", "bronze.naver_daily"):
        dataset = next((item for item in datasets if item["name"] == name), None)
        if dataset and dataset.get("coverage_start"):
            return {
                "dataset": name,
                "start": dataset.get("coverage_start"),
                "end": dataset.get("coverage_end"),
            }
    return {"dataset": None, "start": None, "end": None}


def _backfill_overview(
    data_root: Path,
    start_year: int = 2023,
    end_year: int = 1990,
) -> dict[str, Any]:
    years = tuple(range(max(start_year, end_year), min(start_year, end_year) - 1, -1))
    price_file_stats = _partitioned_part_file_stats(
        data_root,
        "gold/daily_prices_adj/dt=*/part.parquet",
    )
    price_files = [path for path, _size in price_file_stats or []]
    dates_by_year: dict[int, list[str]] = {year: [] for year in years}
    for file in price_files:
        partition_date = _partition_date(file, "dt")
        if partition_date is None or partition_date.year not in dates_by_year:
            continue
        dates_by_year[partition_date.year].append(partition_date.isoformat())

    rows_by_year = _yearly_price_rows(price_files) if _scan_parquet_enabled() else {}
    rows: list[dict[str, Any]] = []
    for year in years:
        dates = sorted(set(dates_by_year[year]))
        marker = _backfill_marker_path(data_root, year)
        marker_status = _read_backfill_marker_status(marker)
        if marker_status:
            status = marker_status
        elif dates:
            status = "partial"
        else:
            status = "missing"
        rows.append(
            {
                "year": year,
                "status": status,
                "price_days": len(dates),
                "rows": rows_by_year.get(year),
                "coverage": "--" if not dates else f"{dates[0]}..{dates[-1]}",
                "marker": marker.name if marker.exists() else "-",
            }
        )
    return {"start_year": start_year, "end_year": end_year, "years": rows}


def _yearly_price_rows(files: list[Path]) -> dict[int, int]:
    if not files:
        return {}
    try:
        scan = pl.scan_parquet(
            [file.as_posix() for file in files],
            hive_partitioning=True,
        )
        names = scan.collect_schema().names()
        if "date" in names:
            year_expr = pl.col("date").cast(pl.Date, strict=False).dt.year()
        elif "dt" in names:
            year_expr = pl.col("dt").cast(pl.Date, strict=False).dt.year()
        else:
            raise ValueError("no date column")
        frame = (
            scan.with_columns(year_expr.alias("_year"))
            .group_by("_year")
            .agg(pl.len().alias("rows"))
            .collect()
        )
        return {
            int(row["_year"]): int(row["rows"])
            for row in frame.iter_rows(named=True)
            if row["_year"] is not None
        }
    except Exception:  # noqa: BLE001
        rows: dict[int, int] = {}
        for file in files:
            partition_date = _partition_date(file, "dt")
            if partition_date is None:
                continue
            rows[partition_date.year] = rows.get(partition_date.year, 0) + _row_count_one(
                file,
                True,
            )
        return rows


def _partition_date(path: Path, key: str) -> date | None:
    prefix = f"{key}="
    for part in path.parts:
        if not part.startswith(prefix):
            continue
        try:
            return datetime.strptime(part.removeprefix(prefix), "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _backfill_marker_path(data_root: Path, year: int) -> Path:
    return data_root / "_state" / "backfill" / "yearly" / f"{year}.json"


def _read_backfill_marker_status(marker: Path) -> str | None:
    if not marker.exists():
        return None
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "marker_invalid"
    status = data.get("status")
    return str(status) if status else "complete"


def _scan_parquet_enabled() -> bool:
    return os.environ.get("FINANCE_PI_ADMIN_SCAN_PARQUET", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
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


def _tail_file(path: Path, max_bytes: int) -> str:
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > max_bytes:
            handle.seek(size - max_bytes)
        data = handle.read()
    return data.decode("utf-8", errors="replace")


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
