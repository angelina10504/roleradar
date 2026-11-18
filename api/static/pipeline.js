/* RoleRadar dashboard: pipeline health.
 *
 * Everything here is a pure function from an API response to text or HTML.
 * No fetch, no DOM, no clock. index.html does the loading and puts the
 * strings on the page. That split is what lets this file be tested under
 * plain `node --test`, with no browser and no dependencies.
 *
 * Two vocabularies are kept apart on purpose:
 *
 *   pipeline health   healthy / degraded / failed / no_data   did it run
 *   run state         completed / failed / running / stale    one execution
 *
 * and neither is the data-quality status (ok / degraded / failed) that the
 * rest of the page uses for "were the rows good". "degraded" and "failed"
 * appear in two of the three, so each gets its own label, glyph and CSS
 * class, and the page says which question a badge is answering.
 *
 * Nothing is filled in. A value the API did not send is shown as a dash,
 * never as a zero and never as a success.
 */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.RoleRadarPipeline = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  const DASH = "—";

  const esc = s => String(s).replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));

  const has = v => v !== null && v !== undefined;
  const num = n => has(n) ? Number(n).toLocaleString("en-US") : DASH;

  /* ---- pipeline health ------------------------------------------------ */

  const HEALTH = {
    healthy:  { label: "Healthy",  glyph: "●", summary: "The last run that was due completed." },
    degraded: { label: "Degraded", glyph: "▲", summary: "The pipeline is running, but something needs a look." },
    failed:   { label: "Failed",   glyph: "✕", summary: "The latest news from the pipeline is bad, or there is none." },
    no_data:  { label: "No data",  glyph: "○", summary: "No run has ever been recorded." },
  };

  const REASON_TEXT = {
    never_executed: "Nothing has been recorded: no tracked run, no error, no rows.",
    legacy_data_only: "Only rows from before run tracking exist. They show something ran; they cannot show it finished.",
    data_overdue: "The newest of those rows is older than the expected run interval.",
    last_run_failed: "The most recent finished run failed.",
    last_run_abandoned: "The most recent run never finished and is past the stale threshold.",
    error_since_last_completed_run: "An error was recorded after the last completed run.",
    no_completed_run: "No run has ever completed.",
    completed_run_overdue: "The last completed run is older than the expected interval plus grace. The scheduler may be off.",
    stale_running_run: "An older run is still marked running and will never finish.",
    recent_failures: "There was a failure inside the recent window, since recovered from.",
    rows_do_not_match_counts: "The last completed run's stored counts disagree with the rows now in the tables.",
  };

  /* An unknown status is shown as unknown. It is never rounded to healthy. */
  function healthView(status) {
    const code = status && status.pipeline_status;
    const known = Object.prototype.hasOwnProperty.call(HEALTH, code);
    const h = known ? HEALTH[code] : { label: "Unknown", glyph: "?", summary: "The API returned a status this page does not know." };
    const reasons = ((status && status.reasons) || []).map(r => ({
      code: r,
      text: Object.prototype.hasOwnProperty.call(REASON_TEXT, r) ? REASON_TEXT[r] : r,
    }));
    return { code: known ? code : "unknown", label: h.label, glyph: h.glyph, summary: h.summary, reasons };
  }

  function healthBadgeHTML(view) {
    return `<span class="phealth ${esc(view.code)}"><span class="g">${view.glyph}</span>${esc(view.label)}</span>`;
  }

  function reasonsHTML(view) {
    if (!view.reasons.length) return "";
    return `<ul class="reasons">${view.reasons.map(r =>
      `<li><code>${esc(r.code)}</code> ${esc(r.text)}</li>`).join("")}</ul>`;
  }

  /* ---- formatting ----------------------------------------------------- */

  /* The pipeline endpoints send an offset (+00:00). The older endpoints send
     naive UTC. Accept both; never append a Z to something that has a zone. */
  function parseTime(iso) {
    if (!iso) return null;
    const zoned = /(Z|[+-]\d{2}:?\d{2})$/.test(iso);
    const d = new Date(zoned ? iso : iso + "Z");
    return isNaN(d.getTime()) ? null : d;
  }

  function fmtTime(iso, locale, timeZone) {
    const d = parseTime(iso);
    if (!d) return DASH;
    const opts = { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" };
    if (timeZone) opts.timeZone = timeZone;
    return d.toLocaleString(locale, opts);
  }

  /* How long a run took. Sub-minute runs keep one decimal: they are seconds
     long and "9s" against "8s" hides most of the difference. */
  function fmtDuration(seconds) {
    if (!has(seconds)) return DASH;
    const s = Math.max(0, Number(seconds));
    if (s < 60) return s.toFixed(1) + "s";
    const m = Math.floor(s / 60);
    if (m < 90) return m + "m " + Math.floor(s % 60) + "s";
    return Math.floor(m / 60) + "h " + (m % 60) + "m";
  }

  /* How long ago. Coarser, and never a decimal. */
  function fmtAge(seconds) {
    if (!has(seconds)) return DASH;
    const s = Math.max(0, Math.floor(Number(seconds)));
    if (s < 90) return s + "s ago";
    const m = Math.floor(s / 60);
    if (m < 90) return m + "m ago";
    const h = Math.floor(m / 60);
    if (h < 48) return h + "h " + (m % 60) + "m ago";
    return Math.floor(h / 24) + "d " + (h % 24) + "h ago";
  }

  /* ---- one execution -------------------------------------------------- */

  const RUN_STATE = {
    completed: { label: "completed", glyph: "✓" },
    failed:    { label: "failed",    glyph: "✕" },
    running:   { label: "running",   glyph: "…" },
    stale:     { label: "stale",     glyph: "!" },
  };

  /* A run still marked running past the threshold is shown as stale, which
     is what it is. The API's own status word is kept in the title. */
  function runState(run) {
    if (!run) return null;
    if (run.status === "running" && run.is_stale) return "stale";
    return Object.prototype.hasOwnProperty.call(RUN_STATE, run.status) ? run.status : null;
  }

  function runStateHTML(run) {
    const state = runState(run);
    if (!state) return DASH;
    const s = RUN_STATE[state];
    return `<span class="rstate ${state}" title="status: ${esc(run.status)}"><span class="g">${s.glyph}</span>${s.label}</span>`;
  }

  const ERROR_CATEGORY_TEXT = {
    upstream_source: "Remotive request",
    upstream_llm: "Anthropic request",
    database: "Database write",
    pipeline_code: "Code node",
    trigger: "Trigger",
    unknown: "Unclassified",
  };

  const categoryText = c =>
    Object.prototype.hasOwnProperty.call(ERROR_CATEGORY_TEXT, c) ? ERROR_CATEGORY_TEXT[c] : "Unclassified";

  /* ---- tiles ---------------------------------------------------------- */

  /* Each tile: { key, label, value, note, warn }. value and note are plain
     text; index.html escapes nothing further because it sets textContent. */
  function tiles(status, fmt) {
    const time = fmt || fmtTime;
    const s = status || {};
    const attempted = s.last_attempted_run || null;
    const ok = s.last_successful_run || null;
    const bad = s.last_failed_run || null;
    const fresh = s.freshness || {};

    const out = [];

    out.push(attempted
      ? { key: "attempted", label: "Last attempted run", value: time(attempted.started_at),
          note: (RUN_STATE[runState(attempted)] || { label: "unknown state" }).label + " · " + fmtAge(attempted.age_seconds),
          warn: runState(attempted) === "failed" || runState(attempted) === "stale" }
      : { key: "attempted", label: "Last attempted run", value: DASH, note: "no tracked run yet", warn: false });

    out.push(ok
      ? { key: "successful", label: "Last successful run", value: time(ok.completed_at),
          note: "took " + fmtDuration(ok.duration_seconds) + " · " + fmtAge(fresh.seconds_since_last_successful_run),
          warn: false }
      : { key: "successful", label: "Last successful run", value: DASH, note: "no run has completed", warn: false });

    out.push(bad
      ? { key: "failed", label: "Last failed run", value: time(bad.started_at),
          note: (bad.error ? categoryText(bad.error.category) : "no category recorded") + " · " + fmtAge(bad.age_seconds),
          warn: false }
      : { key: "failed", label: "Last failed run", value: DASH, note: "no failed run recorded", warn: false });

    /* Freshness is measured from the last completed run. Before any run has
       completed there is nothing to measure from, and the tile says so
       rather than showing the age of older, untracked rows as if it were. */
    if (ok && has(fresh.seconds_since_last_successful_run)) {
      out.push({
        key: "freshness", label: "Freshness",
        value: fresh.overdue ? "Overdue" : "On schedule",
        note: "last completed " + fmtAge(fresh.seconds_since_last_successful_run)
            + " · expected every " + num(fresh.expected_interval_hours) + "h",
        warn: !!fresh.overdue,
      });
    } else if (fresh.evidence === "legacy" && has(fresh.seconds_since_last_legacy_data)) {
      out.push({
        key: "freshness", label: "Freshness", value: "Not tracked",
        note: "newest untracked row " + fmtAge(fresh.seconds_since_last_legacy_data),
        warn: !!fresh.overdue,
      });
    } else {
      out.push({ key: "freshness", label: "Freshness", value: DASH, note: "nothing to measure from", warn: false });
    }

    out.push(ok
      ? { key: "observations", label: "Last successful run wrote",
          value: num(ok.accepted_rows) + " + " + num(ok.rejected_rows),
          note: "accepted + rejected rows · validator said "
              + num(ok.accepted_count) + " + " + num(ok.rejected_count)
              + " of " + num(ok.fetched_count) + " fetched",
          warn: ok.rows_match_counts === false }
      : { key: "observations", label: "Last successful run wrote", value: DASH,
          note: "no run has completed", warn: false });

    return out;
  }

  /* ---- tables --------------------------------------------------------- */

  function runsTableHTML(runs, fmt) {
    const time = fmt || fmtTime;
    if (!runs || !runs.length) return "";
    return `<table>
      <thead><tr>
        <th>Started</th><th>State</th><th>Duration</th><th>Fetched</th>
        <th>Processed</th><th>Accepted</th><th>Rejected</th><th>Rows written</th><th>Error</th>
      </tr></thead>
      <tbody>${runs.map(r => {
        const rows = num(r.accepted_rows) + " + " + num(r.rejected_rows);
        const mismatch = r.rows_match_counts === false
          ? ` <span class="rstate stale" title="stored counts and rows disagree"><span class="g">!</span></span>` : "";
        return `<tr>
          <td>${esc(time(r.started_at))}</td>
          <td>${runStateHTML(r)}</td>
          <td class="num">${esc(fmtDuration(r.duration_seconds))}</td>
          <td class="num">${num(r.fetched_count)}</td>
          <td class="num">${num(r.processed_count)}</td>
          <td class="num">${num(r.accepted_count)}</td>
          <td class="num">${num(r.rejected_count)}</td>
          <td class="num">${rows}${mismatch}</td>
          <td>${r.error ? esc(categoryText(r.error.category)) : DASH}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>`;
  }

  /* Categories inside the recent window, largest first, ties by name so the
     order does not shuffle between refreshes. */
  function errorCategories(status) {
    const by = (status && status.recent_failures && status.recent_failures.by_category) || {};
    return Object.keys(by)
      .map(k => ({ category: k, label: categoryText(k), count: by[k] }))
      .sort((a, b) => b.count - a.count || a.category.localeCompare(b.category));
  }

  function errorCategoriesHTML(status) {
    const rows = errorCategories(status);
    if (!rows.length) return "";
    return `<table>
      <thead><tr><th>Category</th><th>Errors</th></tr></thead>
      <tbody>${rows.map(r => `<tr>
        <td>${esc(r.label)} <code>${esc(r.category)}</code></td>
        <td class="num">${num(r.count)}</td>
      </tr>`).join("")}</tbody>
    </table>`;
  }

  function errorsTableHTML(errors, fmt) {
    const time = fmt || fmtTime;
    if (!errors || !errors.length) return "";
    return `<table>
      <thead><tr><th>When</th><th>Category</th><th>Node</th><th>Tied to a run</th></tr></thead>
      <tbody>${errors.map(e => `<tr>
        <td>${esc(time(e.occurred_at))}</td>
        <td>${esc(categoryText(e.category))}</td>
        <td>${has(e.failed_node) ? esc(e.failed_node) : DASH}</td>
        <td>${e.run_id ? "yes" : "no"}</td>
      </tr>`).join("")}</tbody>
    </table>`;
  }

  /* Durations of completed runs, oldest first, for the chart. Runs that did
     not complete have no duration and are left out, not plotted as zero. */
  function durationSeries(runs, fmt) {
    const time = fmt || fmtTime;
    const done = (runs || []).filter(r => r.status === "completed" && has(r.duration_seconds)).slice().reverse();
    return { labels: done.map(r => time(r.started_at)), seconds: done.map(r => Number(r.duration_seconds)) };
  }

  return {
    HEALTH, REASON_TEXT, RUN_STATE, ERROR_CATEGORY_TEXT,
    esc, num, parseTime, fmtTime, fmtDuration, fmtAge,
    healthView, healthBadgeHTML, reasonsHTML,
    runState, runStateHTML, categoryText,
    tiles, runsTableHTML, errorCategories, errorCategoriesHTML, errorsTableHTML,
    durationSeries,
  };
});
