'use strict';

// The pure view functions behind the pipeline-health sections of the
// dashboard. No browser and no dependencies: api/static/pipeline.js is plain
// functions from an API response to text or HTML.
//
// Fixtures are hand-written responses in the shape tests/test_api.py pins.

const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../../api/static/pipeline.js');

const UTC = iso => P.fmtTime(iso, 'en-GB', 'UTC');

const run = (over = {}) => ({
  run_id: '3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b', n8n_execution_id: null,
  status: 'completed', is_stale: false,
  started_at: '2026-11-12T00:30:00.412000+00:00',
  completed_at: '2026-11-12T00:30:09.127000+00:00',
  duration_seconds: 8.715, age_seconds: 5400,
  fetched_count: 13, processed_count: 5, accepted_count: 4, rejected_count: 1,
  accepted_rows: 4, rejected_rows: 1, rows_match_counts: true,
  error: null, error_count: 0, ...over,
});

const status = (over = {}) => ({
  pipeline_status: 'healthy', reasons: [],
  last_attempted_run: run(), last_successful_run: run(), last_failed_run: null,
  running: { count: 0, stale_count: 0, runs: [] },
  recent_failures: { window_hours: 24, failed_runs: 0, errors: 0, uncorrelated_errors: 0,
                     by_category: {}, errors_since_last_successful_run: 0, last_error_at: null },
  freshness: { evidence: 'explicit', expected_interval_hours: 24, grace_minutes: 60,
               last_successful_run_completed_at: '2026-11-12T00:30:09.127000+00:00',
               seconds_since_last_successful_run: 5391, overdue: false,
               last_legacy_data_at: null, seconds_since_last_legacy_data: null },
  ...over,
});

const NO_DATA = status({
  pipeline_status: 'no_data', reasons: ['never_executed'],
  last_attempted_run: null, last_successful_run: null,
  freshness: { evidence: 'none', expected_interval_hours: 24, grace_minutes: 60,
               last_successful_run_completed_at: null, seconds_since_last_successful_run: null,
               overdue: true, last_legacy_data_at: null, seconds_since_last_legacy_data: null },
});

const tile = (s, key) => P.tiles(s, UTC).find(t => t.key === key);

// -- health status mapping ---------------------------------------------------

test('each of the four pipeline statuses has its own label, glyph and class', () => {
  const seen = ['healthy', 'degraded', 'failed', 'no_data'].map(code => {
    const v = P.healthView(status({ pipeline_status: code }));
    assert.equal(v.code, code);
    assert.match(P.healthBadgeHTML(v), new RegExp(`class="phealth ${code}"`));
    return v;
  });
  assert.equal(new Set(seen.map(v => v.label)).size, 4);
  assert.equal(new Set(seen.map(v => v.glyph)).size, 4);
  assert.deepEqual(seen.map(v => v.label), ['Healthy', 'Degraded', 'Failed', 'No data']);
});

test('an unknown or missing status is shown as unknown, never as healthy', () => {
  for (const s of [status({ pipeline_status: 'splendid' }), {}, null, undefined, { pipeline_status: null },
                   status({ pipeline_status: 'constructor' }), status({ pipeline_status: 'ok' })]) {
    const v = P.healthView(s);
    assert.equal(v.code, 'unknown');
    assert.equal(v.label, 'Unknown');
    assert.doesNotMatch(P.healthBadgeHTML(v), /healthy/);
  }
});

test('the health badge never uses the data-quality class or words', () => {
  for (const code of ['healthy', 'degraded', 'failed', 'no_data']) {
    const html = P.healthBadgeHTML(P.healthView(status({ pipeline_status: code })));
    assert.doesNotMatch(html, /class="status/);
    assert.doesNotMatch(html, />ok</);
  }
});

test('every reason the API can send has a sentence', () => {
  const codes = ['never_executed', 'legacy_data_only', 'data_overdue', 'last_run_failed',
    'last_run_abandoned', 'error_since_last_completed_run', 'no_completed_run',
    'completed_run_overdue', 'stale_running_run', 'recent_failures', 'rows_do_not_match_counts'];
  const v = P.healthView(status({ pipeline_status: 'failed', reasons: codes }));
  assert.equal(v.reasons.length, codes.length);
  for (const r of v.reasons) assert.notEqual(r.text, r.code, `${r.code} has no text`);
});

test('a reason this page does not know is shown as its code, escaped', () => {
  const v = P.healthView(status({ pipeline_status: 'failed', reasons: ['<img src=x onerror=alert(1)>'] }));
  const html = P.reasonsHTML(v);
  assert.doesNotMatch(html, /<img/);
  assert.match(html, /&lt;img/);
});

test('no reasons renders nothing, not an empty list', () => {
  assert.equal(P.reasonsHTML(P.healthView(status())), '');
});

// -- tiles -------------------------------------------------------------------

test('a healthy status fills all five tiles from the response', () => {
  const tiles = P.tiles(status(), UTC);
  assert.deepEqual(tiles.map(t => t.key), ['attempted', 'successful', 'failed', 'freshness', 'observations']);
  assert.match(tile(status(), 'attempted').value, /12 Nov/);
  assert.match(tile(status(), 'attempted').note, /completed/);
  assert.match(tile(status(), 'successful').note, /took 8\.7s/);
  assert.equal(tile(status(), 'freshness').value, 'On schedule');
  assert.equal(tile(status(), 'observations').value, '4 + 1');
  assert.match(tile(status(), 'observations').note, /validator said 4 \+ 1 of 13 fetched/);
  assert.ok(tiles.every(t => t.warn === false));
});

test('no data shows dashes and says why, with no number and no success', () => {
  const tiles = P.tiles(NO_DATA, UTC);
  for (const t of tiles) {
    assert.equal(t.value, '—', `${t.key} shows a value`);
    assert.doesNotMatch(t.note, /\d/, `${t.key} note has a number`);
    assert.doesNotMatch(t.note, /completed ·|On schedule|healthy/i);
  }
  assert.equal(tile(NO_DATA, 'attempted').note, 'no tracked run yet');
  assert.equal(tile(NO_DATA, 'failed').note, 'no failed run recorded');
});

test('a completed run with zero records is shown as zero, not as a dash', () => {
  const zero = run({ fetched_count: 0, processed_count: 0, accepted_count: 0, rejected_count: 0,
                     accepted_rows: 0, rejected_rows: 0 });
  const t = tile(status({ last_successful_run: zero, last_attempted_run: zero }), 'observations');
  assert.equal(t.value, '0 + 0');
  assert.match(t.note, /validator said 0 \+ 0 of 0 fetched/);
});

test('legacy-only data says freshness is not tracked instead of borrowing a run', () => {
  const legacy = status({
    pipeline_status: 'failed', reasons: ['legacy_data_only', 'data_overdue'],
    last_attempted_run: null, last_successful_run: null,
    freshness: { evidence: 'legacy', expected_interval_hours: 24, grace_minutes: 60,
                 last_successful_run_completed_at: null, seconds_since_last_successful_run: null,
                 overdue: true, last_legacy_data_at: '2026-09-11T14:02:11.345000',
                 seconds_since_last_legacy_data: 5313468 },
  });
  const t = tile(legacy, 'freshness');
  assert.equal(t.value, 'Not tracked');
  assert.match(t.note, /newest untracked row 61d/);
  assert.equal(t.warn, true);
  assert.equal(tile(legacy, 'successful').value, '—');
});

test('an overdue run is flagged', () => {
  const s = status({ pipeline_status: 'failed', reasons: ['completed_run_overdue'] });
  s.freshness.overdue = true;
  s.freshness.seconds_since_last_successful_run = 3 * 86400;
  assert.equal(tile(s, 'freshness').value, 'Overdue');
  assert.equal(tile(s, 'freshness').warn, true);
  assert.match(tile(s, 'freshness').note, /3d 0h ago/);
});

test('the last failed run shows its category in words and no message', () => {
  const failed = run({ status: 'failed', duration_seconds: 4.2, accepted_count: null,
                       error: { category: 'upstream_llm', message: null }, error_count: 1 });
  const t = tile(status({ last_failed_run: failed }), 'failed');
  assert.match(t.note, /Anthropic request/);
  assert.equal(t.warn, false);
});

test('a failed or abandoned last attempt is flagged on its own tile', () => {
  const failed = run({ status: 'failed', error: { category: 'database', message: null } });
  const stale = run({ status: 'running', is_stale: true, completed_at: null, duration_seconds: null });
  assert.equal(tile(status({ last_attempted_run: failed }), 'attempted').warn, true);
  assert.match(tile(status({ last_attempted_run: stale }), 'attempted').note, /^stale/);
  assert.equal(tile(status({ last_attempted_run: stale }), 'attempted').warn, true);
});

test('rows that disagree with the counts are flagged on the observations tile', () => {
  const off = run({ accepted_rows: 3, rows_match_counts: false });
  assert.equal(tile(status({ last_successful_run: off }), 'observations').warn, true);
  assert.equal(tile(status({ last_successful_run: off }), 'observations').value, '3 + 1');
});

test('tiles tolerate a response with whole sections missing', () => {
  for (const s of [{}, null, { pipeline_status: 'failed' }]) {
    const tiles = P.tiles(s, UTC);
    assert.equal(tiles.length, 5);
    assert.ok(tiles.every(t => t.value === '—'));
  }
});

// -- run state ---------------------------------------------------------------

test('run states are completed, failed, running and stale', () => {
  assert.equal(P.runState(run()), 'completed');
  assert.equal(P.runState(run({ status: 'failed' })), 'failed');
  assert.equal(P.runState(run({ status: 'running' })), 'running');
  assert.equal(P.runState(run({ status: 'running', is_stale: true })), 'stale');
  assert.equal(P.runState(run({ status: 'exploded' })), null);
  assert.equal(P.runState(null), null);
});

test('a run state is never drawn with the data-quality or health classes', () => {
  for (const r of [run(), run({ status: 'failed' }), run({ status: 'running', is_stale: true })]) {
    const html = P.runStateHTML(r);
    assert.match(html, /class="rstate /);
    assert.doesNotMatch(html, /class="(status|phealth)/);
  }
  assert.equal(P.runStateHTML(run({ status: 'exploded' })), '—');
});

// -- executions table --------------------------------------------------------

test('the executions table has one row per run and the nine columns', () => {
  const html = P.runsTableHTML([run(), run({ status: 'failed' })], UTC);
  assert.equal((html.match(/<tr>/g) || []).length, 3);
  assert.equal((html.match(/<th>/g) || []).length, 9);
  assert.match(html, /8\.7s/);
  assert.match(html, /<td class="num">13<\/td>/);
});

test('an empty list of runs renders nothing, so the page can show its empty state', () => {
  assert.equal(P.runsTableHTML([], UTC), '');
  assert.equal(P.runsTableHTML(null, UTC), '');
});

test('a run that never closed shows dashes for its verdicts, not zeros', () => {
  const open = run({ status: 'running', completed_at: null, duration_seconds: null,
                     fetched_count: null, processed_count: null, accepted_count: null,
                     rejected_count: null, accepted_rows: 2, rejected_rows: 0, rows_match_counts: null });
  const cells = P.runsTableHTML([open], UTC).split('<tbody>')[1];
  assert.equal((cells.match(/<td class="num">—<\/td>/g) || []).length, 5);
  assert.match(cells, /<td class="num">2 \+ 0<\/td>/);
  assert.doesNotMatch(cells, /<td class="num">0<\/td>/);
});

test('a count mismatch is marked in the table', () => {
  const html = P.runsTableHTML([run({ accepted_rows: 3, rows_match_counts: false })], UTC);
  assert.match(html, /stored counts and rows disagree/);
  assert.doesNotMatch(P.runsTableHTML([run()], UTC), /disagree/);
});

test('values from the API are escaped on the way into HTML', () => {
  const nasty = '<script>alert(1)</script>';
  const html = P.errorsTableHTML([{ occurred_at: '2026-11-12T00:30:04+00:00', category: 'database',
                                    failed_node: nasty, run_id: null }], UTC)
             + P.runsTableHTML([run({ status: nasty, error: { category: nasty, message: null } })], UTC)
             + P.errorCategoriesHTML(status({ recent_failures: { by_category: { [nasty]: 1 } } }));
  assert.doesNotMatch(html, /<script>/);
});

// -- errors ------------------------------------------------------------------

test('error categories are listed largest first with a stable tie order', () => {
  const s = status({ recent_failures: { by_category: { database: 1, upstream_llm: 3, trigger: 1 } } });
  assert.deepEqual(P.errorCategories(s).map(c => [c.category, c.count]),
    [['upstream_llm', 3], ['database', 1], ['trigger', 1]]);
  assert.match(P.errorCategoriesHTML(s), /Anthropic request/);
});

test('no recent errors renders nothing', () => {
  assert.equal(P.errorCategoriesHTML(status()), '');
  assert.equal(P.errorCategoriesHTML({}), '');
  assert.equal(P.errorsTableHTML([], UTC), '');
});

test('the public errors table shows category, node and whether a run was found', () => {
  const html = P.errorsTableHTML([
    { id: 2, occurred_at: '2026-11-12T00:30:04.900000+00:00', category: 'upstream_llm',
      failed_node: 'Claude Extract', run_id: 'abc', workflow_name: null, execution_id: null, message: null },
    { id: 1, occurred_at: '2026-11-11T00:30:04.900000+00:00', category: 'made_up',
      failed_node: null, run_id: null, workflow_name: null, execution_id: null, message: null },
  ], UTC);
  assert.match(html, /Anthropic request/);
  assert.match(html, /Claude Extract/);
  assert.match(html, /<td>yes<\/td>/);
  assert.match(html, /<td>no<\/td>/);
  assert.match(html, /Unclassified/);
  assert.doesNotMatch(html, /null|undefined/);
});

test('the errors table has no column for a message, even if one is sent', () => {
  const html = P.errorsTableHTML([{ occurred_at: '2026-11-12T00:30:04+00:00', category: 'database',
    failed_node: 'Start Run', run_id: null, message: 'password=hunter2 rejected' }], UTC);
  assert.doesNotMatch(html, /hunter2|Message/);
});

// -- duration ----------------------------------------------------------------

test('the duration series is completed runs only, oldest first', () => {
  const series = P.durationSeries([
    run({ started_at: '2026-11-12T00:30:00+00:00', duration_seconds: 9 }),
    run({ status: 'failed', started_at: '2026-11-11T12:00:00+00:00', duration_seconds: 2 }),
    run({ status: 'running', started_at: '2026-11-11T06:00:00+00:00', duration_seconds: null }),
    run({ started_at: '2026-11-10T00:30:00+00:00', duration_seconds: 7.5 }),
  ], UTC);
  assert.deepEqual(series.seconds, [7.5, 9]);
  assert.equal(series.labels.length, 2);
  assert.match(series.labels[0], /10 Nov/);
});

test('no completed run gives an empty series, not a zero', () => {
  assert.deepEqual(P.durationSeries([run({ status: 'failed' })], UTC), { labels: [], seconds: [] });
  assert.deepEqual(P.durationSeries(null, UTC), { labels: [], seconds: [] });
});

// -- formatting --------------------------------------------------------------

test('timestamps with an offset and naive UTC timestamps both parse', () => {
  assert.equal(P.parseTime('2026-11-12T00:30:00.412000+00:00').toISOString(), '2026-11-12T00:30:00.412Z');
  assert.equal(P.parseTime('2026-09-03T15:03:56.860000').toISOString(), '2026-09-03T15:03:56.860Z');
  assert.equal(P.parseTime('2026-11-12T06:00:00+05:30').toISOString(), '2026-11-12T00:30:00.000Z');
  assert.equal(P.parseTime('2026-11-12T00:30:00Z').toISOString(), '2026-11-12T00:30:00.000Z');
});

test('a missing or broken timestamp is a dash, never Invalid Date', () => {
  for (const bad of [null, undefined, '', 'soon', '2026-13-45T99:00:00+00:00']) {
    assert.equal(P.fmtTime(bad, 'en-GB', 'UTC'), '—');
  }
});

test('durations and ages', () => {
  assert.equal(P.fmtDuration(8.715), '8.7s');
  assert.equal(P.fmtDuration(0), '0.0s');
  assert.equal(P.fmtDuration(75), '1m 15s');
  assert.equal(P.fmtDuration(null), '—');
  assert.equal(P.fmtAge(45), '45s ago');
  assert.equal(P.fmtAge(5400), '1h 30m ago');
  assert.equal(P.fmtAge(3 * 86400 + 3600), '3d 1h ago');
  assert.equal(P.fmtAge(undefined), '—');
});

test('a missing number is a dash and zero is zero', () => {
  assert.equal(P.num(null), '—');
  assert.equal(P.num(undefined), '—');
  assert.equal(P.num(0), '0');
  assert.equal(P.num(1234), '1,234');
});
