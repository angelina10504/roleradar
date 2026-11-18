'use strict';

// Runs the dashboard's own inline script, unmodified, against a stub DOM and
// canned API responses, and reads back what it put on the page.
//
// This is not a browser. There is no layout, no CSS and no real Chart.js; the
// stubs record what the script asked for. What it does prove: the script
// loads, every element it reaches for exists in the HTML, each panel lands in
// the right one of its states, and one failing endpoint does not take the
// others down. What it cannot prove: that the page looks right.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const STATIC = path.join(__dirname, '..', '..', 'api', 'static');
const HTML = fs.readFileSync(path.join(STATIC, 'index.html'), 'utf8');
const PIPELINE = require(path.join(STATIC, 'pipeline.js'));

const INLINE = HTML.match(/<script>\s*("use strict";[\s\S]*?)<\/script>/)[1];
const IDS = new Set([...HTML.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]));
// A state box whose previous sibling is a chart area, as in the real markup.
const AFTER_PLOT = new Set(
  [...HTML.matchAll(/<canvas id="c-[^"]+"><\/canvas><\/div>\s*<div class="state" id="([^"]+)">/g)].map(m => m[1]));

function element(id, classes) {
  const set = new Set((classes || '').split(' ').filter(Boolean));
  const el = {
    id, dataset: {}, style: {}, innerHTML: '', textContent: '', title: '', value: '30',
    get className() { return [...set].join(' '); },
    set className(v) { set.clear(); String(v).split(' ').filter(Boolean).forEach(c => set.add(c)); },
    classList: {
      add: c => set.add(c), remove: c => set.delete(c), contains: c => set.has(c),
      toggle: (c, on) => (on ? set.add(c) : set.delete(c)),
    },
    addEventListener() {}, setAttribute() {},
    querySelector: () => element(id + ' child'),
    previousElementSibling: null, parentElement: null,
  };
  return el;
}

function page(routes) {
  const elements = new Map();
  const missing = [];
  const charts = [];
  const requested = [];

  const get = id => {
    if (!IDS.has(id)) missing.push(id);
    if (!elements.has(id)) {
      const el = element(id);
      if (AFTER_PLOT.has(id)) el.previousElementSibling = element(id + ' plot', 'plot');
      el.parentElement = element(id + ' parent');
      elements.set(id, el);
    }
    return elements.get(id);
  };

  class Chart {
    constructor(canvas, config) { this.canvas = canvas; this.config = config; charts.push(this); }
    destroy() { this.destroyed = true; }
  }

  const context = {
    window: { RoleRadarPipeline: PIPELINE, matchMedia: () => ({ addEventListener() {} }) },
    document: {
      documentElement: {}, getElementById: get,
      querySelectorAll: () => [],
    },
    getComputedStyle: () => ({ getPropertyValue: () => ' #123456 ' }),
    Chart, console, setTimeout, clearTimeout, Promise, Error, Object, Math, Number, String, Date, isNaN,
    fetch: async url => {
      requested.push(url);
      const key = url.split('?')[0];
      const route = routes[key];
      if (route === undefined) throw new Error('connection refused');
      const [code, body] = Array.isArray(route) ? route : [200, route];
      return { ok: code >= 200 && code < 300, status: code, statusText: 'x', json: async () => body };
    },
  };
  vm.createContext(context);
  vm.runInContext(INLINE, context, { filename: 'index.html <script>' });

  return {
    el: id => elements.get(id) || element(id),
    kind: panel => (elements.get('s-' + panel) || element('x')).dataset.kind || '',
    stateText: panel => (elements.get('s-' + panel) || element('x')).innerHTML,
    settle: () => context.loadAll(true),
    missing, charts, requested,
    liveCharts: () => charts.filter(c => !c.destroyed).map(c => c.canvas.id),
  };
}

// ---- canned responses ------------------------------------------------------

const RUN = {
  run_id: '3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b', n8n_execution_id: null, status: 'completed',
  is_stale: false, started_at: '2026-11-12T00:30:00.412000+00:00',
  completed_at: '2026-11-12T00:30:09.127000+00:00', duration_seconds: 8.715, age_seconds: 5400,
  fetched_count: 13, processed_count: 5, accepted_count: 4, rejected_count: 1,
  accepted_rows: 4, rejected_rows: 1, rows_match_counts: true, error: null, error_count: 0,
};
const FAILED_RUN = { ...RUN, status: 'failed', accepted_count: null, rejected_count: null,
  processed_count: null, fetched_count: null, rows_match_counts: null,
  error: { category: 'upstream_llm', message: null }, error_count: 1 };

const STATUS = {
  pipeline_status: 'degraded', reasons: ['recent_failures'],
  last_attempted_run: RUN, last_successful_run: RUN, last_failed_run: FAILED_RUN,
  running: { count: 0, stale_count: 0, runs: [] },
  recent_failures: { window_hours: 24, failed_runs: 1, errors: 1, uncorrelated_errors: 0,
    by_category: { upstream_llm: 1 }, errors_since_last_successful_run: 0, last_error_at: null },
  freshness: { evidence: 'explicit', expected_interval_hours: 24, grace_minutes: 60,
    last_successful_run_completed_at: RUN.completed_at, seconds_since_last_successful_run: 5391,
    overdue: false, last_legacy_data_at: null, seconds_since_last_legacy_data: null },
};

const LEGACY_RUN = {
  run_started_at: '2026-09-03T15:03:56.860000', run_ended_at: '2026-09-03T15:03:56.865000',
  ingested: 5, validated: 4, rejected: 1, success_rate: 0.8, success_rate_delta: 0.0,
  success_rate_rolling3: 0.8, cumulative_ingested: 26, cumulative_validated: 20,
  status: 'degraded', run_source: 'legacy', run_id: null,
};

const QUALITY = {
  '/api/health': { database: 'up', checked_at: '2026-11-12T02:00:00' },
  '/api/runs': { row_count: 1, runs: [LEGACY_RUN] },
  '/api/summary': { run_count: 1, totals: { ingested: 5, validated: 4, rejected: 1, success_rate: 0.8 },
    last_run: { run_started_at: LEGACY_RUN.run_started_at, ingested: 5, validated: 4, rejected: 1,
      success_rate: 0.8, status: 'degraded' },
    last_successful_run_at: LEGACY_RUN.run_started_at, seconds_since_last_successful_run: 600 },
  '/api/error-classes': { row_count: 1, total_rejected_rows: 1, total_code_occurrences: 1,
    classes: [{ code: 'ENUM', category: 'schema', rows: 1, occurrences: 1, pct_of_rejected_rows: 1 }] },
  '/api/model-accuracy': { row_count: 1, by_version: [{ model: 'm', extraction_version: '1.0.0',
    processed: 5, mismatches: 0, mismatch_rate: 0 }], by_run: [{ run_started_at: LEGACY_RUN.run_started_at,
    processed: 5, mismatches: 0, mismatch_rate: 0 }] },
};

const PIPELINE_OK = {
  '/api/pipeline-status': STATUS,
  '/api/pipeline-runs': { row_count: 2, total: 2, runs: [RUN, FAILED_RUN] },
  '/api/pipeline-errors': { row_count: 1, total: 1, errors: [{ id: 1,
    occurred_at: '2026-11-11T00:30:04.900000+00:00', failed_node: 'Claude Extract',
    run_id: FAILED_RUN.run_id, category: 'upstream_llm', workflow_name: null,
    execution_id: null, message: null }] },
};

const OUTDATED = [503, { error: 'database_unavailable', reason: 'schema_outdated',
  detail: 'the database schema is older than this API expects' }];
const DOWN = [503, { error: 'database_unavailable', reason: 'unreachable',
  detail: 'the database could not be reached' }];

// ---- tests -----------------------------------------------------------------

test('every element the script reaches for exists in the page', async () => {
  for (const routes of [{ ...QUALITY, ...PIPELINE_OK }, {}]) {
    const p = page(routes);
    await p.settle();
    assert.deepEqual(p.missing, []);
  }
});

test('the page loads pipeline.js before its own script', () => {
  const at = HTML.indexOf('<script src="pipeline.js"></script>');
  assert.ok(at > 0 && at < HTML.indexOf('<script>\n"use strict";'));
});

test('with data, the pipeline sections render and no panel is in a state', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK });
  await p.settle();

  for (const panel of ['pstatus', 'pruns', 'duration', 'perrors', 'volume', 'rate', 'runs', 'errors', 'model']) {
    assert.equal(p.kind(panel), '', `${panel} is showing a state`);
  }
  assert.match(p.el('p-badge').innerHTML, /class="phealth degraded"/);
  assert.match(p.el('p-reasons').innerHTML, /recent_failures/);
  assert.equal((p.el('p-tiles').innerHTML.match(/class="tile"/g) || []).length, 5);
  assert.match(p.el('p-tiles').innerHTML, /Anthropic request/);
  assert.equal(p.el('pstatus-body').classList.contains('hidden'), false);
  assert.equal((p.el('pruns-table-wrap').innerHTML.match(/<tr>/g) || []).length, 3);
  assert.match(p.el('pcat-table-wrap').innerHTML, /upstream_llm/);
  assert.match(p.el('perrors-table-wrap').innerHTML, /Claude Extract/);
  assert.ok(p.liveCharts().includes('c-duration'));
});

test('the dashboard asks for the three pipeline endpoints', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK });
  await p.settle();
  for (const path of ['/api/pipeline-status', '/api/pipeline-runs', '/api/pipeline-errors']) {
    assert.ok(p.requested.some(u => u.split('?')[0] === path), path);
  }
  assert.ok(p.requested.every(u => !/token|key|secret/i.test(u)));
});

test('the old panels still render historical data, with the data-quality words', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK });
  await p.settle();
  const table = p.el('runs-table-wrap').innerHTML;
  assert.match(table, /<th>Data quality<\/th>/);
  assert.match(table, /class="status degraded"/);
  assert.doesNotMatch(table, /phealth|rstate/);
  assert.equal(p.el('t-total').textContent, '5');
  assert.deepEqual(['c-volume', 'c-rate', 'c-errors', 'c-model'].filter(c => !p.liveCharts().includes(c)), []);
});

test('a warehouse with only legacy rows shows the old panels and honest empties', async () => {
  const p = page({
    ...QUALITY,
    '/api/pipeline-status': { ...STATUS, pipeline_status: 'failed',
      reasons: ['legacy_data_only', 'data_overdue'], last_attempted_run: null,
      last_successful_run: null, last_failed_run: null,
      recent_failures: { ...STATUS.recent_failures, by_category: {} },
      freshness: { ...STATUS.freshness, evidence: 'legacy', seconds_since_last_successful_run: null,
        overdue: true, seconds_since_last_legacy_data: 5313468 } },
    '/api/pipeline-runs': { row_count: 0, total: 0, runs: [] },
    '/api/pipeline-errors': { row_count: 0, total: 0, errors: [] },
  });
  await p.settle();

  assert.match(p.el('p-badge').innerHTML, /class="phealth failed"/);
  assert.match(p.el('p-tiles').innerHTML, /Not tracked/);
  assert.equal(p.kind('pruns'), 'empty');
  assert.match(p.stateText('pruns'), /No tracked execution yet/);
  assert.equal(p.kind('duration'), 'empty');
  assert.equal(p.kind('perrors'), 'empty');
  assert.match(p.stateText('perrors'), /not that none happened/);
  assert.equal(p.el('pruns-table-wrap').innerHTML, '');
  assert.ok(!p.liveCharts().includes('c-duration'));
  // The historical panels are untouched by any of that.
  assert.equal(p.kind('runs'), '');
  assert.match(p.el('runs-table-wrap').innerHTML, /class="status degraded"/);
});

test('no data anywhere: every panel says empty and nothing is drawn', async () => {
  const p = page({
    '/api/health': QUALITY['/api/health'],
    '/api/runs': { row_count: 0, runs: [] },
    '/api/summary': { run_count: 0, totals: {}, last_run: {} },
    '/api/error-classes': { row_count: 0, classes: [] },
    '/api/model-accuracy': { row_count: 0, by_version: [], by_run: [] },
    '/api/pipeline-status': { pipeline_status: 'no_data', reasons: ['never_executed'],
      last_attempted_run: null, last_successful_run: null, last_failed_run: null,
      running: { count: 0, stale_count: 0, runs: [] },
      recent_failures: { by_category: {} }, freshness: { evidence: 'none', overdue: true } },
    '/api/pipeline-runs': { row_count: 0, total: 0, runs: [] },
    '/api/pipeline-errors': { row_count: 0, total: 0, errors: [] },
  });
  await p.settle();

  assert.match(p.el('p-badge').innerHTML, /class="phealth no_data"/);
  assert.doesNotMatch(p.el('p-tiles').innerHTML, /\d/);
  for (const panel of ['pruns', 'duration', 'perrors', 'volume', 'rate', 'runs', 'errors', 'model']) {
    assert.equal(p.kind(panel), 'empty', panel);
  }
  assert.deepEqual(p.liveCharts(), []);
  assert.equal(p.el('t-total').textContent, '—');
});

test('an unmigrated database: pipeline panels say not set up, old panels are errors', async () => {
  const p = page({
    '/api/health': QUALITY['/api/health'],
    '/api/runs': OUTDATED, '/api/summary': OUTDATED, '/api/model-accuracy': OUTDATED,
    '/api/error-classes': QUALITY['/api/error-classes'],
    '/api/pipeline-status': OUTDATED, '/api/pipeline-runs': OUTDATED, '/api/pipeline-errors': OUTDATED,
  });
  await p.settle();

  for (const panel of ['pstatus', 'pruns', 'duration', 'perrors']) {
    assert.equal(p.kind(panel), 'empty', panel);
    assert.match(p.stateText(panel), /not set up in this database yet/);
  }
  assert.equal(p.el('pstatus-body').classList.contains('hidden'), true);
  assert.equal(p.kind('runs'), 'error');
  assert.match(p.stateText('runs'), /schema is older than this API expects/);
  assert.equal(p.kind('errors'), '');
});

test('pipeline endpoints down, quality endpoints up: one half fails, the other renders', async () => {
  const p = page({ ...QUALITY, '/api/pipeline-status': DOWN, '/api/pipeline-runs': DOWN,
                   '/api/pipeline-errors': DOWN });
  await p.settle();

  for (const panel of ['pstatus', 'pruns', 'duration', 'perrors']) {
    assert.equal(p.kind(panel), 'error', panel);
    assert.match(p.stateText(panel), /could not be reached/);
  }
  assert.equal(p.el('pstatus-body').classList.contains('hidden'), true);
  assert.equal(p.el('pruns-table-wrap').innerHTML, '');
  assert.equal(p.kind('runs'), '');
  assert.equal(p.el('t-total').textContent, '5');
});

test('one pipeline endpoint failing does not blank the other two', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK, '/api/pipeline-errors': DOWN });
  await p.settle();
  assert.equal(p.kind('perrors'), 'error');
  assert.equal(p.kind('pstatus'), '');
  assert.equal(p.kind('pruns'), '');
  assert.match(p.el('p-badge').innerHTML, /phealth degraded/);
});

test('the network being unreachable is an error on every panel, never an empty chart', async () => {
  const p = page({});
  await p.settle();
  for (const panel of ['pstatus', 'pruns', 'duration', 'perrors', 'volume', 'rate', 'runs', 'errors', 'model']) {
    assert.equal(p.kind(panel), 'error', panel);
  }
  assert.deepEqual(p.liveCharts(), []);
});

test('a status the page does not know is shown as unknown', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK,
                   '/api/pipeline-status': { ...STATUS, pipeline_status: 'splendid', reasons: [] } });
  await p.settle();
  assert.match(p.el('p-badge').innerHTML, /class="phealth unknown"/);
  assert.doesNotMatch(p.el('p-badge').innerHTML, /Healthy/);
});

test('an error message is escaped before it reaches the page', async () => {
  const p = page({ ...QUALITY, ...PIPELINE_OK,
    '/api/pipeline-runs': [503, { error: 'x', reason: 'unreachable', detail: '<img src=x onerror=1>' }] });
  await p.settle();
  assert.doesNotMatch(p.stateText('pruns'), /<img/);
});
