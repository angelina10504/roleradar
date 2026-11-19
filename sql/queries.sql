-- queries.sql
--
-- Every number the API returns comes from one of these. Queries 1 to 4 are
-- the dashboard. Query 5 is a diagnostic that is not wired to anything.
-- Queries 6 to 8 are pipeline monitoring.
--
-- Aggregation happens here, not in JavaScript. The browser receives rows that
-- are already the answer.
--
-- These are the source of truth for the logic. api/main.py holds its own copy
-- as Python string constants, the same hand-pasted-duplicate problem
-- validate.js has with the n8n Code node. See README.md, "Known limitations".
--
-- ---------------------------------------------------------------------------
-- Shared preamble: what a "run" is
-- ---------------------------------------------------------------------------
--
-- A run is one n8n execution. The design intent was that every row in a run
-- shares an ingested_at, so grouping would be GROUP BY ingested_at.
--
-- It does not hold. The Validator Code node calls new Date().toISOString()
-- inside its per-item loop, so each item reads the clock separately. Five items
-- usually land on the same millisecond and sometimes do not, and one straddled
-- millisecond splits a run in two, which turns a 4/5 success rate into a 3/3
-- and a 1/2. Exact-equality grouping is not safe on this data.
--
-- So runs are recovered by gap, not by equality: order every row from both
-- tables by time, and start a new run wherever the gap from the previous row
-- exceeds RUN_GAP. A run takes seconds; the schedule is daily. Anything in
-- between is noise. This is also why the ROWS UNBOUNDED PRECEDING running sum
-- below is doing real work rather than decoration: it converts the per-row
-- "is this a boundary" flag into a run number.
--
-- That is how rows written before run tracking are grouped, and it is still
-- how they are grouped. Those rows have run_id NULL. They are never given a
-- run_id after the fact: a guessed run recorded as a fact is worse than an
-- honest inference made on read.
--
-- Rows written by the run-aware workflow carry the run_id of their
-- pipeline_runs row, and for them there is nothing to infer. Their run is
-- their run_id. So every run-scoped query below has two arms:
--
--   legacy    run_id IS NULL      grouped by gap, exactly as before
--   explicit  run_id IS NOT NULL  grouped by run_id
--
-- Only legacy rows take part in the LAG. An explicit run that lands within
-- RUN_GAP of a legacy one is therefore never merged into it, and two explicit
-- runs seconds apart stay two runs.
--
-- run_key is the grouping key: 'legacy:<n>' or 'run:<uuid>'. The prefix is the
-- point. An inferred run can never be read as a real UUID. The API returns
-- run_source ('legacy' or 'explicit') and run_id (the UUID, or NULL).
--
-- On a warehouse with only legacy rows these queries return what they
-- returned before the change. tests/test_api.py runs the old and new text
-- side by side to hold that.
--
-- RUN_GAP is passed as a parameter so it is visible at the call site rather
-- than buried. 2 minutes is the default.
--
-- The two ingested_at columns have different types: timestamp in
-- jobs_extracted, timestamptz in dead_letter_log. The validator writes the same
-- ISO-8601 Z string to both and one side drops the offset. Every query below
-- normalises the timestamptz side with AT TIME ZONE 'UTC' so both are naive
-- UTC. If rows were ever written under a non-UTC session timezone the two sides
-- are offset from each other and runs will not pair up -- query 5 exists to
-- check that against real rows.


-- ===========================================================================
-- 1. GET /api/runs
-- ===========================================================================
-- Serves three things at once, from one result set, so they cannot disagree:
--   - ingestion volume over time      (ingested, validated, rejected)
--   - extraction success rate over time (success_rate, success_rate_rolling3)
--   - the recent runs table           (all of it, plus status)
--
-- Window functions here, all four load-bearing:
--   LAG   -> gap detection, to find run boundaries
--   SUM   -> running sum of boundaries into a run number, and a true
--            cumulative ingested total for the volume chart
--   LAG   -> success rate vs the previous run, period over period
--   AVG   -> 3-run rolling success rate. At 5 rows per run one rejection is a
--            20-point swing, so the raw line is unreadable on this data volume.
--
-- The limit is applied AFTER the windows, via a descending ROW_NUMBER, so that
-- cumulative_ingested stays a true lifetime total and the delta on the oldest
-- visible run still refers to the run before it rather than to nothing.
--
-- Parameters: %(run_gap)s interval, %(limit)s integer.
-- Ordered ascending. Empty table returns zero rows, which the API reports as
-- row_count 0 rather than as a chart of zeros.

WITH events AS (
    SELECT ingested_at                    AS ts, run_id, 1 AS validated, 0 AS rejected
    FROM public.jobs_extracted
    UNION ALL
    SELECT ingested_at AT TIME ZONE 'UTC' AS ts, run_id, 0 AS validated, 1 AS rejected
    FROM public.dead_letter_log
),
marked AS (
    SELECT
        ts, validated, rejected,
        CASE
            WHEN LAG(ts) OVER (ORDER BY ts) IS NULL                      THEN 1
            WHEN ts - LAG(ts) OVER (ORDER BY ts) > %(run_gap)s::interval THEN 1
            ELSE 0
        END AS is_run_start
    FROM events
    WHERE run_id IS NULL
),
numbered AS (
    SELECT
        ts, validated, rejected,
        'legacy:' || SUM(is_run_start) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING)
            AS run_key,
        NULL::uuid AS run_id,
        'legacy'   AS run_source
    FROM marked
    UNION ALL
    SELECT
        ts, validated, rejected,
        'run:' || run_id::text AS run_key,
        run_id,
        'explicit' AS run_source
    FROM events
    WHERE run_id IS NOT NULL
)
,
per_run AS (
    SELECT
        run_key, run_id, run_source,
        MIN(ts) AS run_started_at,
        MAX(ts) AS run_ended_at,
        COUNT(*) AS ingested,
        SUM(validated) AS validated,
        SUM(rejected) AS rejected
    FROM numbered
    GROUP BY run_key, run_id, run_source
),
scored AS (
    SELECT
        run_key, run_id, run_source,
        run_started_at, run_ended_at, ingested, validated, rejected,
        validated::numeric / NULLIF(ingested, 0) AS success_rate
    FROM per_run
),
windowed AS (
    SELECT
        run_key, run_id, run_source,
        run_started_at,
        run_ended_at,
        ingested,
        validated,
        rejected,
        ROUND(success_rate, 4) AS success_rate,
        ROUND(success_rate - LAG(success_rate) OVER (ORDER BY run_started_at, run_key), 4)
            AS success_rate_delta,
        ROUND(AVG(success_rate) OVER (
            ORDER BY run_started_at, run_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 4) AS success_rate_rolling3,
        (SUM(ingested) OVER (ORDER BY run_started_at, run_key ROWS UNBOUNDED PRECEDING))::bigint
            AS cumulative_ingested,
        (SUM(validated) OVER (ORDER BY run_started_at, run_key ROWS UNBOUNDED PRECEDING))::bigint
            AS cumulative_validated,
        CASE
            WHEN rejected = 0  THEN 'ok'
            WHEN validated = 0 THEN 'failed'
            ELSE 'degraded'
        END AS status,
        ROW_NUMBER() OVER (ORDER BY run_started_at DESC, run_key DESC) AS recency
    FROM scored
)
SELECT
    run_started_at, run_ended_at, ingested, validated, rejected,
    success_rate, success_rate_delta, success_rate_rolling3,
    cumulative_ingested, cumulative_validated, status,
    run_source, run_id
FROM windowed
WHERE recency <= %(limit)s
ORDER BY run_started_at, run_key;


-- ===========================================================================
-- 2. GET /api/summary
-- ===========================================================================
-- Serves the summary strip:
--   total records            -> total_ingested / total_validated
--   current success rate     -> last_run_success_rate, headline
--                               overall_success_rate, sub-label
--   rejected in last run     -> last_run_rejected
--   time since last good run -> seconds_since_last_successful_run
--
-- "Successful" means the run wrote at least one row to jobs_extracted, i.e.
-- validated > 0. Not rejected = 0. At 5 rows per run a fully clean run is not
-- guaranteed to be common, and a tile that reads "never" on a working pipeline
-- teaches you to ignore it.
--
-- The aggregate has no GROUP BY, so it returns exactly one row even against
-- empty tables. Every value in that row is then 0 or NULL, and the API reports
-- run_count 0 so the strip can say "no runs recorded" instead of showing a
-- confident 0%.
--
-- Note what this cannot see: a run that died before either Postgres node wrote
-- anything leaves no rows, so it is not a run here at all. This query measures
-- data quality, not pipeline liveness. seconds_since_last_successful_run going
-- stale is the only signal on this dashboard that the pipeline stopped.
--
-- Parameters: %(run_gap)s interval.

WITH events AS (
    SELECT ingested_at                    AS ts, run_id, 1 AS validated, 0 AS rejected
    FROM public.jobs_extracted
    UNION ALL
    SELECT ingested_at AT TIME ZONE 'UTC' AS ts, run_id, 0 AS validated, 1 AS rejected
    FROM public.dead_letter_log
),
marked AS (
    SELECT
        ts, validated, rejected,
        CASE
            WHEN LAG(ts) OVER (ORDER BY ts) IS NULL                      THEN 1
            WHEN ts - LAG(ts) OVER (ORDER BY ts) > %(run_gap)s::interval THEN 1
            ELSE 0
        END AS is_run_start
    FROM events
    WHERE run_id IS NULL
),
numbered AS (
    SELECT
        ts, validated, rejected,
        'legacy:' || SUM(is_run_start) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING)
            AS run_key,
        NULL::uuid AS run_id,
        'legacy'   AS run_source
    FROM marked
    UNION ALL
    SELECT
        ts, validated, rejected,
        'run:' || run_id::text AS run_key,
        run_id,
        'explicit' AS run_source
    FROM events
    WHERE run_id IS NOT NULL
)
,
per_run AS (
    SELECT
        run_key,
        MIN(ts) AS run_started_at,
        COUNT(*) AS ingested,
        SUM(validated) AS validated,
        SUM(rejected) AS rejected
    FROM numbered
    GROUP BY run_key
),
last_run AS (
    SELECT * FROM per_run
    ORDER BY run_started_at DESC, run_key DESC
    LIMIT 1
),
agg AS (
    SELECT
        COUNT(*)                    AS run_count,
        COALESCE(SUM(ingested),  0)::bigint AS total_ingested,
        COALESCE(SUM(validated), 0)::bigint AS total_validated,
        COALESCE(SUM(rejected),  0)::bigint AS total_rejected,
        MAX(run_started_at)         AS last_run_started_at,
        MAX(run_started_at) FILTER (WHERE validated > 0)
                                    AS last_successful_run_at
    FROM per_run
)
SELECT
    a.run_count,
    a.total_ingested,
    a.total_validated,
    a.total_rejected,
    ROUND(a.total_validated::numeric / NULLIF(a.total_ingested, 0), 4)
        AS overall_success_rate,
    a.last_run_started_at,
    l.ingested  AS last_run_ingested,
    l.validated AS last_run_validated,
    l.rejected  AS last_run_rejected,
    ROUND(l.validated::numeric / NULLIF(l.ingested, 0), 4)
        AS last_run_success_rate,
    CASE
        WHEN l.run_started_at IS NULL THEN NULL
        WHEN l.rejected  = 0 THEN 'ok'
        WHEN l.validated = 0 THEN 'failed'
        ELSE 'degraded'
    END AS last_run_status,
    a.last_successful_run_at,
    EXTRACT(EPOCH FROM (
        (now() AT TIME ZONE 'UTC') - a.last_successful_run_at
    ))::bigint AS seconds_since_last_successful_run
FROM agg a
LEFT JOIN last_run l ON true;


-- ===========================================================================
-- 3. GET /api/error-classes
-- ===========================================================================
-- Categories, and what each one says went wrong:
--   model_disagreement  the model's mentions_n8n disagreed with the regex
--   envelope            the response had no tool_use block
--   schema              a field the model produced has the wrong type or value
--   salary_logic        the salary fields contradict each other
--   provenance          a field Remotive should have supplied is missing
--   validator_internal  validate() threw. A bug in this project's code.
--
-- validator_internal is matched first. Without its own branch the code
-- VALIDATOR_EXCEPTION would fall through to 'schema' and be counted as a model
-- mistake. No stored row carries the code, so adding the branch moved nothing.
--
-- Serves the error-class bar chart: rejection reasons by category.
--
-- Counts two different things and returns both, because they are not the same
-- number and the difference is the interesting part:
--
--   rows        how many dead-lettered postings carry this code at all.
--               COUNT(DISTINCT id). A row with three bad enums counts once.
--   occurrences how many entries in errors_json carry this code.
--               COUNT(*). That same row counts three times.
--
-- The chart plots rows by default: "how many postings did this break" is the
-- triage question, and occurrences lets one badly-mangled row dominate the
-- chart. Both ship in the payload and the page has a toggle.
--
-- Categorisation reads errors_json, not error_codes. error_codes is deduped to
-- distinct codes per row and drops the field name, and MISSING is emitted for
-- both a missing source_job_id and a missing normalized_title. Those are not
-- the same failure -- CLAUDE.md is explicit that a provenance fault is a
-- pipeline fault and must not inflate the model error rate -- and only the
-- field tells them apart.
--
-- errors_json is double-encoded: the validator stringifies it before insert, so
-- it lands in the jsonb column as a JSON string rather than an array. The CASE
-- unwraps that, and falls through for any row that was written as a proper
-- array, so the query works across both encodings rather than assuming today's.
--
-- A code that appears under two categories returns two rows. That is
-- deliberate, not a grouping bug: provenance MISSING and schema MISSING are
-- different failures that happen to share a code.
--
-- No parameters. Ordered most-broken first.

WITH exploded AS (
    SELECT
        d.id                AS dead_letter_id,
        e ->> 'field'       AS field,
        e ->> 'code'        AS code
    FROM public.dead_letter_log d
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(d.errors_json) = 'string'
                THEN (d.errors_json #>> '{}')::jsonb
            ELSE d.errors_json
        END
    ) AS e
),
classified AS (
    SELECT
        dead_letter_id,
        code,
        CASE
            WHEN code = 'VALIDATOR_EXCEPTION' THEN 'validator_internal'
            WHEN code = 'N8N_MISMATCH'                            THEN 'model_disagreement'
            WHEN code = 'TOOL_USE_MISSING'                        THEN 'envelope'
            WHEN code IN ('SALARY_ORDER', 'SALARY_PARTIAL',
                          'SALARY_PERIOD_MISMATCH', 'SALARY_SPAN_IMPLAUSIBLE')
                                                                  THEN 'salary_logic'
            WHEN field IN ('source', 'source_job_id', 'source_url',
                           'raw_title', 'raw_company', 'description_clean')
                                                                  THEN 'provenance'
            ELSE 'schema'
        END AS category
    FROM exploded
),
totals AS (
    SELECT COUNT(*) AS total_rejected_rows FROM public.dead_letter_log
)
SELECT
    c.code,
    c.category,
    COUNT(DISTINCT c.dead_letter_id)    AS rows,
    COUNT(*)                            AS occurrences,
    ROUND(
        COUNT(DISTINCT c.dead_letter_id)::numeric
        / NULLIF((SELECT total_rejected_rows FROM totals), 0)
    , 4) AS pct_of_rejected_rows,
    -- Both denominators travel with the rows, so the page never has to sum a
    -- column to label its own axis. SUM over COUNT is an aggregate inside a
    -- window: the window runs after grouping, so this totals the buckets.
    (SELECT total_rejected_rows FROM totals) AS total_rejected_rows,
    SUM(COUNT(*)) OVER ()::bigint            AS total_code_occurrences
FROM classified c
GROUP BY c.code, c.category
ORDER BY rows DESC, occurrences DESC, c.code;


-- ===========================================================================
-- 4. GET /api/model-accuracy
-- ===========================================================================
-- Serves the model-disagreement panel. Not part of the core dashboard spec;
-- it is here because it is the metric CLAUDE.md decision (c) exists to make
-- countable, and both boolean columns were stored specifically so this could
-- be a query rather than a number computed once at ingestion.
--
-- A mismatch is a dead_letter_log row carrying N8N_MISMATCH: the model's
-- mentions_n8n disagreed with /\bn8n\b/i run over the description.
--
-- The denominator is everything the model processed -- jobs_extracted plus
-- dead_letter_log -- not just the rejected rows. Over rejections alone, a run
-- with a single rejection reads as a 100% model error rate.
--
-- This is a floor, not an exact rate. The validator only runs the cross-check
-- when mentions_n8n passed its own boolean type check, so a row that failed
-- schema validation and would also have disagreed does not always carry the
-- code. Undercounts, never overcounts.
--
-- 4a: by model and extraction_version. Rows stay comparable across prompt and
-- schema changes, which is what those two provenance columns are for.
-- No parameters.

WITH processed AS (
    SELECT
        model,
        extraction_version,
        false AS mismatch
    FROM public.jobs_extracted
    UNION ALL
    SELECT
        model,
        extraction_version,
        error_codes @> '["N8N_MISMATCH"]'::jsonb AS mismatch
    FROM public.dead_letter_log
)
SELECT
    COALESCE(model, '(unrecorded)')              AS model,
    COALESCE(extraction_version, '(unrecorded)') AS extraction_version,
    COUNT(*)                                     AS processed,
    COUNT(*) FILTER (WHERE mismatch)             AS mismatches,
    ROUND(
        COUNT(*) FILTER (WHERE mismatch)::numeric / NULLIF(COUNT(*), 0)
    , 4) AS mismatch_rate
FROM processed
GROUP BY 1, 2
ORDER BY 1, 2;


-- 4b: the same rate per run, for the trend line. Same sessionisation as query
-- 1, carrying a mismatch flag through instead of a validated/rejected split.
-- Parameters: %(run_gap)s interval.

WITH events AS (
    SELECT ingested_at AS ts, run_id, false AS mismatch
    FROM public.jobs_extracted
    UNION ALL
    SELECT ingested_at AT TIME ZONE 'UTC' AS ts, run_id,
           error_codes @> '["N8N_MISMATCH"]'::jsonb AS mismatch
    FROM public.dead_letter_log
),
marked AS (
    SELECT
        ts, mismatch,
        CASE
            WHEN LAG(ts) OVER (ORDER BY ts) IS NULL                      THEN 1
            WHEN ts - LAG(ts) OVER (ORDER BY ts) > %(run_gap)s::interval THEN 1
            ELSE 0
        END AS is_run_start
    FROM events
    WHERE run_id IS NULL
),
numbered AS (
    SELECT
        ts, mismatch,
        'legacy:' || SUM(is_run_start) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING)
            AS run_key
    FROM marked
    UNION ALL
    SELECT ts, mismatch, 'run:' || run_id::text AS run_key
    FROM events
    WHERE run_id IS NOT NULL
)
SELECT
    MIN(ts)                          AS run_started_at,
    COUNT(*)                         AS processed,
    COUNT(*) FILTER (WHERE mismatch) AS mismatches,
    ROUND(COUNT(*) FILTER (WHERE mismatch)::numeric / NULLIF(COUNT(*), 0), 4)
        AS mismatch_rate
FROM numbered
GROUP BY run_key
ORDER BY run_started_at, run_key;


-- ===========================================================================
-- 6. GET /api/pipeline-runs
-- ===========================================================================
-- One row per pipeline_runs row, newest first. This is the only place a run
-- that wrote nothing can be seen: one that fetched zero postings, or one that
-- died before either insert.
--
-- Two kinds of number, deliberately side by side:
--   *_count  what the workflow recorded when it closed the run. The
--            validator's verdicts. NULL on a run that never closed.
--   *_rows   counted from the tables on every read. What is actually there.
-- A replayed insert is skipped by the unique index from migration 0004, so
-- neither can be inflated. When they differ the difference is the finding.
--
-- is_stale: still 'running' after %(stale)s. A run takes seconds.
--
-- error_message and n8n_execution_id are returned raw by this query. The API
-- returns neither to the public, and redacts and shortens the message for an
-- operator. Do not paste a result of this query anywhere public.
--
-- Parameters: %(stale)s interval, %(status)s text or NULL, %(limit)s, %(offset)s.

SELECT
    r.run_id::text      AS run_id,
    r.n8n_execution_id,
    r.status,
    (r.status = 'running'
        AND r.started_at < now() - %(stale)s::interval) AS is_stale,
    r.started_at,
    r.completed_at,
    ROUND(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))::numeric, 3)
        AS duration_seconds,
    EXTRACT(EPOCH FROM (now() - r.started_at))::bigint AS age_seconds,
    r.fetched_count,
    r.processed_count,
    r.accepted_count,
    r.rejected_count,
    (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id)
        AS accepted_rows,
    (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id)
        AS rejected_rows,
    r.error_category,
    r.error_message,
    (SELECT count(*) FROM public.pipeline_errors e WHERE e.run_id = r.run_id)
        AS error_count

FROM public.pipeline_runs r
WHERE (%(status)s::text IS NULL OR r.status = %(status)s::text)
ORDER BY r.started_at DESC, r.run_id
LIMIT %(limit)s OFFSET %(offset)s;

-- The total for the same filter, for pagination.
SELECT count(*) AS total
FROM public.pipeline_runs r
WHERE (%(status)s::text IS NULL OR r.status = %(status)s::text);


-- ===========================================================================
-- 7. GET /api/pipeline-errors
-- ===========================================================================
-- Infra failures written by the Error Trigger workflow through
-- record_pipeline_error() (migration 0009). Newest first. run_id is NULL when
-- the failure could not be tied to a run, which is normal for a failure
-- before the run row was written.
--
-- The API returns error_message, workflow_name and execution_id only to a
-- caller holding the operator token. Publicly they are null.
--
-- Parameters: %(run_id)s uuid or NULL, %(limit)s, %(offset)s.

SELECT
    e.id,
    e.occurred_at,
    e.workflow_name,
    e.failed_node,
    e.execution_id,
    e.run_id::text AS run_id,
    e.error_category,
    e.error_message
FROM public.pipeline_errors e
WHERE (%(run_id)s::uuid IS NULL OR e.run_id = %(run_id)s::uuid)
ORDER BY e.occurred_at DESC, e.id DESC
LIMIT %(limit)s OFFSET %(offset)s;

SELECT count(*) AS total
FROM public.pipeline_errors e
WHERE (%(run_id)s::uuid IS NULL OR e.run_id = %(run_id)s::uuid);


-- ===========================================================================
-- 8. GET /api/pipeline-status
-- ===========================================================================
-- Two statements. The status word itself is decided in api/main.py by
-- pipeline_health(), from what these return; the rules are in the README
-- under "Pipeline health".
--
-- 8a: the runs the decision reads, tagged by role.
--   last_attempt    most recently started, whatever its state
--   last_settled    most recent run that is over: closed, or open so long it
--                   is dead. A run in progress settles nothing.
--   last_completed  most recent run that closed as 'completed'
--   last_failed     most recent run that closed as 'failed'
--   running         every open run
--
-- Parameters: %(stale)s interval.

WITH runs AS (SELECT
    r.run_id::text      AS run_id,
    r.n8n_execution_id,
    r.status,
    (r.status = 'running'
        AND r.started_at < now() - %(stale)s::interval) AS is_stale,
    r.started_at,
    r.completed_at,
    ROUND(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))::numeric, 3)
        AS duration_seconds,
    EXTRACT(EPOCH FROM (now() - r.started_at))::bigint AS age_seconds,
    r.fetched_count,
    r.processed_count,
    r.accepted_count,
    r.rejected_count,
    (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id)
        AS accepted_rows,
    (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id)
        AS rejected_rows,
    r.error_category,
    r.error_message,
    (SELECT count(*) FROM public.pipeline_errors e WHERE e.run_id = r.run_id)
        AS error_count

    FROM public.pipeline_runs r
)
(SELECT 'last_attempt' AS role, x.* FROM (
    SELECT * FROM runs ORDER BY started_at DESC, run_id LIMIT 1) x)
UNION ALL
(SELECT 'last_settled' AS role, x.* FROM (
    SELECT * FROM runs WHERE status <> 'running' OR is_stale
    ORDER BY started_at DESC, run_id LIMIT 1) x)
UNION ALL
(SELECT 'last_completed' AS role, x.* FROM (
    SELECT * FROM runs WHERE status = 'completed'
    ORDER BY completed_at DESC, run_id LIMIT 1) x)
UNION ALL
(SELECT 'last_failed' AS role, x.* FROM (
    SELECT * FROM runs WHERE status = 'failed'
    ORDER BY started_at DESC, run_id LIMIT 1) x)
UNION ALL
(SELECT 'running' AS role, x.* FROM (
    SELECT * FROM runs WHERE status = 'running'
    ORDER BY started_at DESC, run_id LIMIT 20) x);

-- 8b: the counts. Always exactly one row, including on an empty warehouse.
-- errors_since_last_completed is what separates a failure that is still the
-- latest news from one the pipeline has recovered from.
--
-- Parameters: %(stale)s interval, %(window)s interval.

WITH last_completed AS (
    SELECT MAX(completed_at) AS at
    FROM public.pipeline_runs
    WHERE status = 'completed'
),
legacy AS (
    SELECT MAX(ts) AS last_at, COUNT(*) AS n
    FROM (
        SELECT ingested_at AS ts
        FROM public.jobs_extracted WHERE run_id IS NULL
        UNION ALL
        SELECT ingested_at AT TIME ZONE 'UTC' AS ts
        FROM public.dead_letter_log WHERE run_id IS NULL
    ) rows_without_a_run
),
recent_errors AS (
    SELECT COALESCE(error_category, 'unknown') AS category, run_id
    FROM public.pipeline_errors
    WHERE occurred_at >= now() - %(window)s::interval
)
SELECT
    (SELECT count(*) FROM public.pipeline_runs)   AS run_count,
    (SELECT count(*) FROM public.pipeline_errors) AS error_count,
    (SELECT count(*) FROM public.pipeline_runs
      WHERE status = 'running')                   AS running_count,
    (SELECT count(*) FROM public.pipeline_runs
      WHERE status = 'running'
        AND started_at < now() - %(stale)s::interval) AS stale_running_count,
    (SELECT at FROM last_completed)               AS last_completed_at,
    EXTRACT(EPOCH FROM (now() - (SELECT at FROM last_completed)))::bigint
        AS seconds_since_last_completed,
    (SELECT count(*) FROM public.pipeline_errors e
      WHERE e.occurred_at > COALESCE((SELECT at FROM last_completed),
                                     '-infinity'::timestamptz))
        AS errors_since_last_completed,
    (SELECT MAX(occurred_at) FROM public.pipeline_errors) AS last_error_at,
    (SELECT count(*) FROM public.pipeline_runs
      WHERE status = 'failed'
        AND completed_at >= now() - %(window)s::interval) AS recent_failed_runs,
    (SELECT count(*) FROM recent_errors)                  AS recent_errors,
    (SELECT count(*) FROM recent_errors WHERE run_id IS NULL)
        AS recent_uncorrelated_errors,
    (SELECT jsonb_object_agg(category, n)
       FROM (SELECT category, count(*) AS n
             FROM recent_errors GROUP BY category) c) AS recent_errors_by_category,
    (SELECT n FROM legacy)       AS legacy_rows,
    (SELECT last_at FROM legacy) AS last_legacy_data_at,
    EXTRACT(EPOCH FROM (
        (now() AT TIME ZONE 'UTC') - (SELECT last_at FROM legacy)
    ))::bigint AS seconds_since_last_legacy_data;


-- ===========================================================================
-- 5. Diagnostic. Not wired to an endpoint.
-- ===========================================================================
-- Checks the assumption every query above rests on: that the two ingested_at
-- columns, after AT TIME ZONE 'UTC', describe the same instants.
--
-- jobs_extracted.ingested_at is timestamp, dead_letter_log.ingested_at is
-- timestamptz, and the validator writes the same ISO-8601 Z string to both.
-- If every row was inserted under a UTC session timezone they line up and the
-- spread within each run is milliseconds. If the session timezone was ever
-- something else, one side is shifted by the offset, the gap detection splits
-- those rows into separate runs, and every success rate on the dashboard is
-- wrong in a way that looks plausible.
--
-- Read the output this way:
--   spread_seconds in the milliseconds  -> fine, this is the per-item clock
--                                          read drift the gap logic is for
--   spread_seconds near 3600 or 19800   -> a timezone offset, not a run.
--                                          Stop and fix the data before
--                                          trusting any rate.
--   validated_rows or rejected_rows 0   -> expected. Most runs reject nothing.
--
-- Run it with:
--   docker exec -i roleradar-pg psql -U roleradar -d roleradar
--
-- Parameters: %(run_gap)s interval. Defaults to 2 minutes if run by hand with
-- the literal below substituted.

WITH events AS (
    SELECT
        'jobs_extracted'::text AS src,
        ingested_at            AS ts_normalised,
        ingested_at::text      AS ts_raw
    FROM public.jobs_extracted
    UNION ALL
    SELECT
        'dead_letter_log'::text,
        ingested_at AT TIME ZONE 'UTC',
        ingested_at::text
    FROM public.dead_letter_log
),
marked AS (
    SELECT
        src, ts_normalised, ts_raw,
        CASE
            WHEN LAG(ts_normalised) OVER (ORDER BY ts_normalised) IS NULL
                THEN 1
            WHEN ts_normalised - LAG(ts_normalised) OVER (ORDER BY ts_normalised)
                 > interval '2 minutes'
                THEN 1
            ELSE 0
        END AS is_run_start
    FROM events
),
numbered AS (
    SELECT
        src, ts_normalised, ts_raw,
        SUM(is_run_start) OVER (ORDER BY ts_normalised ROWS UNBOUNDED PRECEDING)
            AS run_seq
    FROM marked
)
SELECT
    run_seq,
    COUNT(*)                                              AS rows_in_run,
    COUNT(*) FILTER (WHERE src = 'jobs_extracted')        AS validated_rows,
    COUNT(*) FILTER (WHERE src = 'dead_letter_log')       AS rejected_rows,
    MIN(ts_normalised)                                    AS first_ts,
    MAX(ts_normalised)                                    AS last_ts,
    EXTRACT(EPOCH FROM (MAX(ts_normalised) - MIN(ts_normalised)))
                                                          AS spread_seconds,
    MIN(ts_raw) FILTER (WHERE src = 'jobs_extracted')     AS raw_validated_ts,
    MIN(ts_raw) FILTER (WHERE src = 'dead_letter_log')    AS raw_rejected_ts
FROM numbered
GROUP BY run_seq
ORDER BY run_seq;
