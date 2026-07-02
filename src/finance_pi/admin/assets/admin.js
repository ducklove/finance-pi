
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
