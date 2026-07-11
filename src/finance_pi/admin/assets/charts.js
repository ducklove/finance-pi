// Overview SVG 차트 — /api/overview 데이터를 외부 라이브러리 없이 자체 SVG로 그린다.
// 색은 admin.css 디자인 토큰(--blue/--green/--amber/--red/--violet)을 getComputedStyle로
// 읽는다. 테마 토글 시 토큰 값이 바뀌므로 admin.js가 rerenderOverviewCharts()를 호출해
// 재렌더해야 색이 함께 갱신된다. admin.js보다 먼저 로드되는 독립 스크립트라
// 헬퍼를 자체 정의한다(전역 오염 방지를 위해 chart* 접두사 사용).

const chartsState = { overview: null };
const chartFmt = new Intl.NumberFormat();

function chartEscape(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[char]);
}

function chartToken(name, fallback) {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

function chartTextColor() { return chartToken('--muted', '#667085'); }
function chartGridColor() { return chartToken('--line', '#d9dee7'); }
function chartTrackColor() { return chartToken('--bar-bg', '#edf0f5'); }

// 레이어 → 토큰 매핑. 새 레이어가 생겨도 fallback(--muted 계열)으로 동작한다.
const CHART_LAYER_TOKENS = {
  bronze: ['--amber', '#b45309'],
  silver: ['--blue', '#2563eb'],
  gold: ['--green', '#0f766e'],
  macro: ['--violet', '#6d28d9'],
};

function chartLayerColor(layer) {
  const spec = CHART_LAYER_TOKENS[String(layer || '').toLowerCase()];
  return spec ? chartToken(spec[0], spec[1]) : chartTextColor();
}

function chartTrim(text, max) {
  const value = String(text ?? '');
  return value.length > max ? `${value.slice(0, max - 1)}…` : value;
}

function chartBytesLabel(value) {
  if (!value) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value; let idx = 0;
  while (size >= 1024 && idx < units.length - 1) { size /= 1024; idx++; }
  return `${size.toFixed(idx ? 1 : 0)} ${units[idx]}`;
}

function chartDurationLabel(seconds) {
  const total = Math.max(0, Math.round(seconds));
  if (total < 60) return `${total}초`;
  if (total < 3600) return `${Math.floor(total / 60)}분 ${total % 60}초`;
  return `${Math.floor(total / 3600)}시간 ${Math.floor((total % 3600) / 60)}분`;
}

function chartEmpty(message) {
  return `<div class="chart-empty">${chartEscape(message)}</div>`;
}

// 데이터셋별 용량 수평 막대 — 상위 N개, 레이어별 색 구분.
// 막대 길이는 bytes 기준(항상 존재), 행 수는 값 라벨에 병기(스캔 비활성 시 생략).
function chartDatasetSvg(datasets) {
  const items = (datasets || [])
    .filter(d => d && (d.bytes || 0) > 0)
    .sort((a, b) => (b.bytes || 0) - (a.bytes || 0))
    .slice(0, 8);
  if (!items.length) return chartEmpty('수집된 데이터셋이 없습니다');

  const maxBytes = Math.max(1, ...items.map(d => d.bytes || 0));
  const rowH = 26;
  const top = 30;
  const labelX = 168;
  const barX = 176;
  const barMax = 268;
  const height = top + items.length * rowH + 8;
  const text = chartTextColor();

  const layers = [...new Set(items.map(d => String(d.layer || '')))];
  let legendX = 0;
  const legend = layers.map(layer => {
    const piece = `
      <circle cx="${legendX}" cy="-4" r="4" fill="${chartLayerColor(layer)}"/>
      <text x="${legendX + 8}" y="0" fill="${text}" font-size="11">${chartEscape(layer)}</text>`;
    legendX += 8 + layer.length * 7 + 22;
    return piece;
  }).join('');

  const rows = items.map((d, idx) => {
    const y = top + idx * rowH;
    const barW = Math.max(3, (d.bytes || 0) / maxBytes * barMax);
    const value = d.rows == null
      ? chartBytesLabel(d.bytes)
      : `${chartBytesLabel(d.bytes)} · ${chartFmt.format(d.rows)}행`;
    return `
      <text x="${labelX}" y="${y + 13}" fill="${text}" font-size="11" text-anchor="end">${chartEscape(chartTrim(d.name, 24))}</text>
      <rect x="${barX}" y="${y + 4}" width="${barMax}" height="12" rx="3" fill="${chartTrackColor()}"/>
      <rect x="${barX}" y="${y + 4}" width="${barW.toFixed(1)}" height="12" rx="3" fill="${chartLayerColor(d.layer)}"/>
      <text x="${barX + barMax + 8}" y="${y + 13}" fill="${text}" font-size="11">${chartEscape(value)}</text>`;
  }).join('');

  return `
    <svg viewBox="0 0 560 ${height}" role="img"
      aria-label="데이터셋 용량 상위 ${items.length}개 수평 막대 차트" xmlns="http://www.w3.org/2000/svg">
      <g transform="translate(8,16)" font-size="11">${legend}</g>
      ${rows}
    </svg>`;
}

// 연도별 백필 진행 — 시세 일수 세로 막대, 상태별 색 구분.
function chartBackfillSvg(backfill) {
  const years = ((backfill && backfill.years) || [])
    .slice()
    .sort((a, b) => (a.year || 0) - (b.year || 0));
  if (!years.length) return chartEmpty('백필 상태 정보가 없습니다');
  const maxDays = Math.max(...years.map(y => y.price_days || 0));
  if (maxDays <= 0) return chartEmpty('백필된 시세 데이터가 없습니다');

  const text = chartTextColor();
  const grid = chartGridColor();
  const x0 = 40;
  const plotW = 508;
  const y0 = 170;
  const plotH = 140;
  const barW = Math.max(3, Math.min(24, plotW / years.length - 3));
  const step = plotW / years.length;

  const statusColor = status => {
    const value = String(status || '');
    if (value === 'complete') return chartToken('--green', '#0f766e');
    if (value === 'complete_with_failures' || value === 'partial') return chartToken('--amber', '#b45309');
    if (value === 'missing') return chartToken('--red', '#b42318');
    return chartTextColor();
  };

  const labelEvery = Math.max(1, Math.ceil(years.length / 6));
  const bars = years.map((item, idx) => {
    const x = x0 + idx * step + (step - barW) / 2;
    const h = (item.price_days || 0) / maxDays * plotH;
    const bar = h > 0
      ? `<rect x="${x.toFixed(1)}" y="${(y0 - h).toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" rx="2" fill="${statusColor(item.status)}"/>`
      : `<rect x="${x.toFixed(1)}" y="${y0 - 2}" width="${barW.toFixed(1)}" height="2" fill="${chartTrackColor()}"/>`;
    const label = (idx === 0 || idx === years.length - 1 || idx % labelEvery === 0)
      ? `<text x="${(x + barW / 2).toFixed(1)}" y="${y0 + 16}" fill="${text}" font-size="10" text-anchor="middle">${chartEscape(item.year)}</text>`
      : '';
    return bar + label;
  }).join('');

  const complete = years.filter(y => String(y.status || '').startsWith('complete')).length;
  const partial = years.filter(y => y.status === 'partial').length;
  const legend = `완료 ${complete} · 부분 ${partial} · 미수집 ${years.length - complete - partial}`;

  return `
    <svg viewBox="0 0 560 196" role="img"
      aria-label="연도별 백필 시세 일수 막대 차트" xmlns="http://www.w3.org/2000/svg">
      <line x1="${x0}" y1="18" x2="${x0}" y2="${y0}" stroke="${grid}" stroke-width="1"/>
      <line x1="${x0}" y1="${y0}" x2="${x0 + plotW}" y2="${y0}" stroke="${grid}" stroke-width="1"/>
      <line x1="${x0}" y1="${y0 - plotH / 2}" x2="${x0 + plotW}" y2="${y0 - plotH / 2}" stroke="${grid}" stroke-width="1" stroke-dasharray="3 5"/>
      <text x="${x0 - 6}" y="${y0 - plotH + 4}" fill="${text}" font-size="10" text-anchor="end">${chartFmt.format(maxDays)}일</text>
      <text x="${x0 - 6}" y="${y0 + 3}" fill="${text}" font-size="10" text-anchor="end">0</text>
      <text x="${x0 + plotW}" y="14" fill="${text}" font-size="11" text-anchor="end">${chartEscape(legend)}</text>
      ${bars}
    </svg>`;
}

// 최근 완료된 작업 소요 시간 수평 막대 — 상태별 색 구분(완료/실패).
function chartJobDurationSvg(jobs) {
  const finished = (jobs || [])
    .filter(job => job && job.started_at && job.ended_at)
    .slice(0, 10)
    .reverse();
  const items = finished
    .map(job => ({
      label: job.label || job.action || job.id,
      status: String(job.status || ''),
      seconds: (new Date(job.ended_at) - new Date(job.started_at)) / 1000,
    }))
    .filter(item => Number.isFinite(item.seconds) && item.seconds >= 0);
  if (!items.length) return chartEmpty('완료된 작업이 없습니다');

  const maxSeconds = Math.max(1, ...items.map(item => item.seconds));
  const rowH = 26;
  const top = 8;
  const labelX = 168;
  const barX = 176;
  const barMax = 262;
  const height = top + items.length * rowH + 8;
  const text = chartTextColor();

  const statusColor = status => {
    if (status === 'failed') return chartToken('--red', '#b42318');
    if (status === 'done') return chartToken('--green', '#0f766e');
    return chartToken('--amber', '#b45309');
  };

  const rows = items.map((item, idx) => {
    const y = top + idx * rowH;
    const barW = Math.max(3, item.seconds / maxSeconds * barMax);
    return `
      <text x="${labelX}" y="${y + 13}" fill="${text}" font-size="11" text-anchor="end">${chartEscape(chartTrim(item.label, 24))}</text>
      <rect x="${barX}" y="${y + 4}" width="${barMax}" height="12" rx="3" fill="${chartTrackColor()}"/>
      <rect x="${barX}" y="${y + 4}" width="${barW.toFixed(1)}" height="12" rx="3" fill="${statusColor(item.status)}"/>
      <text x="${barX + barMax + 8}" y="${y + 13}" fill="${text}" font-size="11">${chartEscape(chartDurationLabel(item.seconds))}</text>`;
  }).join('');

  return `
    <svg viewBox="0 0 560 ${height}" role="img"
      aria-label="최근 완료 작업 ${items.length}건 소요 시간 막대 차트" xmlns="http://www.w3.org/2000/svg">
      ${rows}
    </svg>`;
}

// admin.js renderOverview()가 매 갱신마다 호출한다. overview 없이 호출하면
// (테마 토글 재렌더) 마지막 데이터로 다시 그린다.
function renderOverviewCharts(overview) {
  if (overview) chartsState.overview = overview;
  const data = chartsState.overview;
  const targets = [
    ['chart-datasets', data ? chartDatasetSvg(data.datasets) : chartEmpty('데이터 대기 중')],
    ['chart-backfill', data ? chartBackfillSvg(data.backfill) : chartEmpty('데이터 대기 중')],
    ['chart-jobs', data ? chartJobDurationSvg(data.jobs) : chartEmpty('데이터 대기 중')],
  ];
  for (const [id, markup] of targets) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = markup;
  }
}

// 테마 토글 등 색 토큰이 바뀐 뒤 다시 그릴 때 사용.
function rerenderOverviewCharts() {
  renderOverviewCharts(null);
}

renderOverviewCharts(null);
