"""RoleRadar metrics API.

Reads the Postgres warehouse and serves the numbers behind
``static/index.html``. Aggregation happens in SQL; nothing here loops over rows
to compute a metric. The one piece of logic in Python is ``pipeline_health``,
which turns facts the database returned into a status word. It is a decision,
not a measurement, and it is a pure function so it can be tested without a
database.

The queries are duplicated from ``sql/queries.sql``, which is where the
reasoning for each one is written down. That file is the readable source of
truth; these constants are the deployed copy. The alternative was to read and
split the .sql file at import, which removes the duplication but makes the app
depend on splitting SQL on semicolons -- fragile against a semicolon inside a
string literal, and it puts a file-path dependency between the app and the repo
layout. Duplication that is documented beats a parser that is nearly right.
See README.md, "Known limitations".

No credentials in this file. Connection details come from the environment.

What leaves this file is deliberately less than what it knows. A database
error is logged here and answered with a fixed sentence. An n8n error message
is stored whole and returned to the public as a category. See README,
"Security boundaries".
"""

from __future__ import annotations

import hmac
import logging
import os
import re
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Optional
from uuid import UUID

import psycopg
from fastapi import FastAPI, Header, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

STATIC_DIR = Path(__file__).parent / "static"

# How long a gap between rows starts a new run. See README, "What a run is".
RUN_GAP_MINUTES = int(os.environ.get("RUN_GAP_MINUTES", "2"))
RUN_GAP = f"{RUN_GAP_MINUTES} minutes"

# Pipeline health thresholds. See README, "Pipeline health".
#
# How often a run is expected, and how late it may be before it counts as
# missed. The API only knows an interval. The external freshness check knows
# the actual schedule and is the stricter of the two.
EXPECTED_RUN_INTERVAL_HOURS = float(os.environ.get("EXPECTED_RUN_INTERVAL_HOURS", "24"))
RUN_GRACE_MINUTES = float(os.environ.get("RUN_GRACE_MINUTES", "60"))
# A run takes seconds. One still 'running' after this long is dead: n8n was
# killed, or the error workflow could not write.
STALE_RUN_MINUTES = float(os.environ.get("STALE_RUN_MINUTES", "15"))
# How far back "recent failures" looks.
RECENT_FAILURE_HOURS = float(os.environ.get("RECENT_FAILURE_HOURS", "24"))
# Longest error message returned to an operator, after redaction. The public
# gets no message at all.
PUBLIC_MESSAGE_CHARS = 300

# The newest migration this file's SQL depends on. /api/health reports whether
# the database has it, so "is it safe to deploy this API" is one request.
REQUIRED_SCHEMA_VERSION = "0009"

# Server-side log. Vercel captures stderr per request; under uvicorn it goes to
# the terminal. Every database failure is written here in full, minus
# credentials, because the client is no longer told what it was.
logger = logging.getLogger("roleradar.api")

# Seconds to wait for a connection before giving up and returning 503.
#
# 8s, not 3s. Neon's free tier suspends the compute when idle and takes several
# seconds to wake, so a short timeout reports "database down" for a database
# that is merely asleep -- a false alarm is worse than a slow first paint. Still
# bounded, because a dashboard that hangs forever is worse than both.
DB_TIMEOUT = float(os.environ.get("DB_TIMEOUT_SECONDS", "8"))

# Seconds for the diagnostic probe below. Short: it runs only on a failure
# path, and its answer is a message, not data.
PROBE_TIMEOUT = float(os.environ.get("DB_PROBE_TIMEOUT_SECONDS", "2"))


def connection_string() -> str:
    """Build the libpq connection string from the environment.

    DATABASE_URL wins if set. Otherwise the POSTGRES_* variables, defaulting to
    what docker-compose.yml creates -- .env only carries POSTGRES_PASSWORD, so
    the rest have to fall back to something or every developer sets six
    variables to their documented values.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "roleradar")
    user = os.environ.get("POSTGRES_USER", "roleradar")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    return (
        f"host={host} port={port} dbname={db} user={user} "
        f"password={password} connect_timeout=3"
    )


# --------------------------------------------------------------------------
# Queries. Duplicated from sql/queries.sql -- see the module docstring.
# --------------------------------------------------------------------------

# The run-grouping preamble, shared by every run-scoped query.
#
# There are two kinds of row. A row written by the run-aware workflow carries
# the run_id of its pipeline_runs row, and its run is simply that id. A row
# written before that has run_id NULL, and its run has to be inferred.
#
# Inference is by gap, as it always was. Legacy rows do not reliably share an
# ingested_at within a run, because the old Validator node read the clock once
# per item. LAG finds the boundaries; the running SUM turns them into a run
# number. Only legacy rows take part in the LAG, so an explicit run that
# happens to sit close to a legacy one in time is never merged into it.
#
# run_key is what the queries group by. It is 'legacy:<n>' or 'run:<uuid>', so
# an inferred run can never be mistaken for a real one. run_id is the real
# UUID or NULL. Nothing is backfilled.
#
# dead_letter_log.ingested_at is timestamptz and jobs_extracted's is
# timestamp, so the timestamptz side is normalised to naive UTC.
_RUNS_PREAMBLE = """
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
"""

# 1. GET /api/runs -- volume chart, success-rate chart, runs table.
# The limit is applied after the window functions via a descending ROW_NUMBER,
# so cumulative_ingested stays a lifetime total and the delta on the oldest
# visible run still refers to the run before it.
#
# A run appears here only if it wrote at least one row to either table. That
# was always the definition and it has not changed: this endpoint measures
# data quality over observed rows. A run that fetched nothing, or died before
# writing, is in /api/pipeline-runs.
#
# run_key breaks ties in every ORDER BY. Two legacy runs could not share a
# start time; a legacy run and an explicit one can.
SQL_RUNS = (
    _RUNS_PREAMBLE
    + """,
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
ORDER BY run_started_at, run_key
"""
)

# 2. GET /api/summary -- the summary strip.
# The aggregate has no GROUP BY, so it returns exactly one row even against
# empty tables, with zeros and NULLs in it. run_count is what tells the page
# whether there is anything to show.
SQL_SUMMARY = (
    _RUNS_PREAMBLE
    + """,
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
LEFT JOIN last_run l ON true
"""
)

# 3. GET /api/error-classes -- the rejection-reason bar chart.
# Categories come from errors_json rather than error_codes, because error_codes
# drops the field name and MISSING is emitted for both a missing source_job_id
# (a pipeline fault) and a missing normalized_title (a model fault).
# The six legacy rows have errors_json double-encoded, so the CASE unwraps a
# JSON string and falls through for a proper array.
#
# VALIDATOR_EXCEPTION is tested first and gets its own category. It means
# validate() threw: a bug in this project's code, not something the model or
# Remotive did. Before this branch existed the code would have fallen through
# to 'schema'. No stored row carries it, so no historical row changes category.
SQL_ERROR_CLASSES = """
WITH exploded AS (
    SELECT
        d.id          AS dead_letter_id,
        e ->> 'field' AS field,
        e ->> 'code'  AS code
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
            WHEN code = 'N8N_MISMATCH'     THEN 'model_disagreement'
            WHEN code = 'TOOL_USE_MISSING' THEN 'envelope'
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
    COUNT(DISTINCT c.dead_letter_id) AS rows,
    COUNT(*)                         AS occurrences,
    ROUND(
        COUNT(DISTINCT c.dead_letter_id)::numeric
        / NULLIF((SELECT total_rejected_rows FROM totals), 0)
    , 4) AS pct_of_rejected_rows,
    (SELECT total_rejected_rows FROM totals) AS total_rejected_rows,
    SUM(COUNT(*)) OVER ()::bigint            AS total_code_occurrences
FROM classified c
GROUP BY c.code, c.category
ORDER BY rows DESC, occurrences DESC, c.code
"""

# 4a. GET /api/model-accuracy -- disagreement rate by model and version.
# The denominator is everything the model processed, both tables. Over
# rejections alone, a run with one rejection reads as 100% model error.
SQL_MODEL_ACCURACY_BY_VERSION = """
WITH processed AS (
    SELECT model, extraction_version, false AS mismatch
    FROM public.jobs_extracted
    UNION ALL
    SELECT model, extraction_version,
           error_codes @> '["N8N_MISMATCH"]'::jsonb AS mismatch
    FROM public.dead_letter_log
)
SELECT
    COALESCE(model, '(unrecorded)')              AS model,
    COALESCE(extraction_version, '(unrecorded)') AS extraction_version,
    COUNT(*)                                     AS processed,
    COUNT(*) FILTER (WHERE mismatch)             AS mismatches,
    ROUND(COUNT(*) FILTER (WHERE mismatch)::numeric / NULLIF(COUNT(*), 0), 4)
        AS mismatch_rate
FROM processed
GROUP BY 1, 2
ORDER BY 1, 2
"""

# 4b. The same rate per run, for the trend line. Same grouping as the shared
# preamble, carrying a mismatch flag instead of the validated/rejected split.
SQL_MODEL_ACCURACY_BY_RUN = """
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
ORDER BY run_started_at, run_key
"""

# 6. GET /api/pipeline-runs -- one row per pipeline_runs row.
#
# The four *_count columns are what the workflow recorded: the validator's
# verdicts. accepted_rows and rejected_rows are counted from the tables on
# every read, so they are what is actually there. A replayed insert is skipped
# by the unique index and cannot raise either. The two sets agree on a normal
# run and the difference, when there is one, is the finding.
_RUN_COLUMNS = """
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
"""

SQL_PIPELINE_RUNS = (
    "SELECT" + _RUN_COLUMNS + """
FROM public.pipeline_runs r
WHERE (%(status)s::text IS NULL OR r.status = %(status)s::text)
ORDER BY r.started_at DESC, r.run_id
LIMIT %(limit)s OFFSET %(offset)s
"""
)

SQL_PIPELINE_RUNS_TOTAL = """
SELECT count(*) AS total
FROM public.pipeline_runs r
WHERE (%(status)s::text IS NULL OR r.status = %(status)s::text)
"""

# 7. GET /api/pipeline-errors -- infra failures, newest first.
SQL_PIPELINE_ERRORS = """
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
LIMIT %(limit)s OFFSET %(offset)s
"""

SQL_PIPELINE_ERRORS_TOTAL = """
SELECT count(*) AS total
FROM public.pipeline_errors e
WHERE (%(run_id)s::uuid IS NULL OR e.run_id = %(run_id)s::uuid)
"""

# 8a. GET /api/pipeline-status -- the runs the status is decided from.
# One result set, tagged by role, so the four cannot come from different
# moments.
#   last_attempt    the most recently started run, whatever its state
#   last_settled    the most recent run that is over: closed, or running so
#                   long it is dead. A run in progress settles nothing.
#   last_completed  the most recent run that closed as 'completed'
#   last_failed     the most recent run that closed as 'failed'
#   running         every run still open, newest first
SQL_PIPELINE_STATUS_RUNS = (
    "WITH runs AS (SELECT" + _RUN_COLUMNS + """
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
    ORDER BY started_at DESC, run_id LIMIT 20) x)
"""
)

# 8b. The counts the status is decided from. Always exactly one row.
# "Since the last completion" is what separates a failure that is still the
# latest news from one the pipeline has already recovered from.
SQL_PIPELINE_STATUS_FACTS = """
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
    ))::bigint AS seconds_since_last_legacy_data
"""


# --------------------------------------------------------------------------
# Database access
# --------------------------------------------------------------------------

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> ConnectionPool:
    """Create the pool on first use, not at import and not in a lifespan hook.

    This started as a FastAPI lifespan handler, which is the idiomatic place for
    it and works under uvicorn. It does not survive the move to Vercel: the
    serverless Python runtime does not reliably run ASGI lifespan events, so the
    pool stayed None and every request died on an assertion. Lazy
    initialisation behaves identically under uvicorn and is correct under a
    runtime that never sends a startup event.

    min_size=0 because a suspended Neon compute should not be held awake by an
    idle dashboard, and because each serverless instance would otherwise open a
    connection it may never use.
    """
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ConnectionPool(
                    conninfo=connection_string(),
                    min_size=0,
                    max_size=int(os.environ.get("DB_POOL_MAX", "2")),
                    open=True,
                    kwargs={"row_factory": dict_row},
                    configure=_read_only,
                )
    return _pool


def _read_only(conn: psycopg.Connection) -> None:
    """Every transaction this API opens is READ ONLY, enforced by Postgres.

    The API only reads. This makes that a property of the connection rather
    than of the SQL, so a bug, or a preview deployment that was handed the
    production DATABASE_URL, cannot write to the warehouse.

    psycopg sends BEGIN READ ONLY at the start of each transaction. The
    alternative, the default_transaction_read_only startup option, was
    rejected: Neon's pooled endpoint is PgBouncer in transaction mode, which
    can refuse unknown startup parameters and would break the connection.
    BEGIN READ ONLY is ordinary SQL and survives a transaction pooler.
    """
    conn.read_only = True


# Every reason a request can fail at the database, and the only sentences a
# client is ever given for it. The driver's own text names hosts, users, roles,
# tables and columns, so it is logged and never returned.
#
# The 'waking' sentence is load-bearing: the dashboard looks for it to decide
# between "waking the database" and an error. Change both or neither.
WAKING = "the database may be waking from suspend"
PUBLIC_DETAIL = {
    "waking": f"no database connection within {DB_TIMEOUT:g}s ({WAKING})",
    "pool_busy": f"no pooled connection within {DB_TIMEOUT:g}s, though a direct one succeeded",
    "unreachable": "the database could not be reached",
    "schema_outdated": "the database schema is older than this API expects",
    "query_failed": "the database could not answer this query",
}


class DatabaseUnavailable(Exception):
    """Raised when the warehouse could not be reached or the query failed.

    Carries a reason code, not the driver's message. str() of it is the fixed
    public sentence for that reason and is safe to return.
    """

    def __init__(self, reason: str):
        self.reason = reason if reason in PUBLIC_DETAIL else "query_failed"
        super().__init__(PUBLIC_DETAIL[self.reason])


def _log_database_error(reason: str, exc: BaseException) -> None:
    """The full story, for whoever reads the server log. Redacted first: a
    connection failure can quote the connection string back."""
    logger.warning(
        "database error reason=%s type=%s detail=%s",
        reason, type(exc).__name__, sanitize_message(str(exc), 1000),
    )


def run_query(sql: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute one statement and return its rows.

    Wraps every psycopg failure in DatabaseUnavailable so the endpoints have a
    single thing to catch and can answer 503 with a reason instead of a 500
    with a stack trace. The driver's message goes to the log, not the client.
    """
    try:
        with get_pool().connection(timeout=DB_TIMEOUT) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                return [_jsonable(row) for row in cur.fetchall()]
    except PoolTimeout as exc:
        raise DatabaseUnavailable(_explain_timeout()) from exc
    except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn,
            psycopg.errors.UndefinedFunction) as exc:
        # The API was deployed ahead of the migrations. Named separately
        # because the fix is different from every other failure here.
        _log_database_error("schema_outdated", exc)
        raise DatabaseUnavailable("schema_outdated") from exc
    except psycopg.OperationalError as exc:
        _log_database_error("unreachable", exc)
        raise DatabaseUnavailable("unreachable") from exc
    except psycopg.Error as exc:
        _log_database_error("query_failed", exc)
        raise DatabaseUnavailable("query_failed") from exc


def _explain_timeout() -> str:
    """Work out why the pool timed out, because the pool will not say.

    psycopg_pool retries a failing connection until its deadline, so a
    database that is refusing connections and one that is merely slow to wake
    both arrive here as PoolTimeout with the same useless message. Reporting
    both as "may be waking" would collapse the distinction the dashboard is
    built to draw -- a suspended Neon compute and a dead warehouse are
    different facts and the page shows them differently.

    So probe once, directly, outside the pool. A refused connection, a bad
    host or a rejected password all fail immediately: that is 'unreachable',
    and what exactly was refused goes to the log. Only a probe that also times
    out is genuinely ambiguous, and that is the case where 'waking' is the
    honest answer. Returns a reason code.
    """
    try:
        with psycopg.connect(connection_string(), connect_timeout=PROBE_TIMEOUT):
            # Connected on the second attempt: the pool was contended or the
            # compute woke in between, not a failure worth naming.
            return "pool_busy"
    except psycopg.OperationalError as probe:
        detail = " ".join(str(probe).split())
        timed_out = "timeout expired" in detail.lower() or "timed out" in detail.lower()
        if timed_out or not detail:
            return "waking"
        _log_database_error("unreachable", probe)
        return "unreachable"
    except Exception as probe:  # noqa: BLE001 - the probe must never raise
        _log_database_error("waking", probe)
        return "waking"


def _jsonable(row: dict) -> dict:
    """Convert psycopg types to JSON primitives.

    numeric comes back as Decimal and timestamps as datetime. Both are
    converted here rather than left to the serialiser, so the response shape
    documented in the README is the shape produced by this function.
    """
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            out[key] = float(value)
        elif isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, UUID):
            out[key] = str(value)
        else:
            out[key] = value
    return out


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _unavailable(exc: DatabaseUnavailable) -> JSONResponse:
    """The one failure response shape. 503, never an empty result set.

    An endpoint that returned {"runs": []} on a connection failure would draw
    an empty chart, and an empty chart is indistinguishable from a pipeline
    that ingested nothing. The page has to be able to tell those apart.
    """
    return JSONResponse(
        status_code=503,
        content={
            "error": "database_unavailable",
            "reason": exc.reason,
            "detail": str(exc),
            "generated_at": _now(),
        },
    )


# --------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------

app = FastAPI(
    title="RoleRadar metrics",
    description=(
        "Pipeline metrics over the RoleRadar warehouse. One query per "
        "endpoint; the queries and their reasoning are in sql/queries.sql."
    ),
    version="1.0.0",
)

# Open CORS. The dashboard is one static file designed to be dropped on an
# nginx root, which puts it on a different origin from this API. There is
# nothing to protect here: every endpoint is read-only aggregate counts over a
# public job feed, and there is no auth to steal.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _schema_version() -> Optional[str]:
    """The newest migration recorded in this database, or None if it has no
    ledger at all. Two statements, because a query cannot mention a table that
    may not exist."""
    has_ledger = run_query(
        "SELECT to_regclass('public.schema_migrations') IS NOT NULL AS present"
    )[0]["present"]
    if not has_ledger:
        return None
    return run_query("SELECT max(version) AS version FROM public.schema_migrations")[0]["version"]


@app.get("/api/health")
def health():
    """Is the warehouse reachable, and is its schema new enough for this API.

    Exists so the page can distinguish "the API is down" from "the pipeline has
    not ingested anything". Those look identical on a chart and mean opposite
    things.

    schema.compatible is the deployment check. False means this API's run
    queries will fail against this database: migrate before deploying.
    """
    try:
        run_query("SELECT 1 AS ok")
        version = _schema_version()
    except DatabaseUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={
                "database": "down",
                "reason": exc.reason,
                "detail": str(exc),
                "checked_at": _now(),
            },
        )
    return {
        "database": "up",
        "checked_at": _now(),
        "schema": {
            "version": version,
            "required": REQUIRED_SCHEMA_VERSION,
            "compatible": version is not None and version >= REQUIRED_SCHEMA_VERSION,
        },
    }


@app.get("/api/runs")
def runs(limit: int = Query(30, ge=1, le=500)):
    """Per-run ingestion volume and success rate, oldest first.

    Serves the volume chart, the success-rate chart and the runs table from one
    result set, so the three cannot disagree with each other.
    """
    try:
        rows = run_query(SQL_RUNS, {"run_gap": RUN_GAP, "limit": limit})
    except DatabaseUnavailable as exc:
        return _unavailable(exc)
    return {
        "generated_at": _now(),
        "run_gap_minutes": RUN_GAP_MINUTES,
        "limit": limit,
        "row_count": len(rows),
        "runs": rows,
    }


@app.get("/api/summary")
def summary():
    """The summary strip.

    Headline success rate is the last run's; the lifetime rate rides along as
    overall_success_rate. A successful run is one with validated > 0, not one
    with zero rejections -- see README, "Definitions".
    """
    try:
        rows = run_query(SQL_SUMMARY, {"run_gap": RUN_GAP})
    except DatabaseUnavailable as exc:
        return _unavailable(exc)

    # The aggregate always returns exactly one row, even against empty tables.
    row = rows[0] if rows else {}
    run_count = row.get("run_count") or 0

    return {
        "generated_at": _now(),
        "run_count": run_count,
        # row_count mirrors run_count so every endpoint reports emptiness the
        # same way and the page has one rule for it.
        "row_count": run_count,
        "totals": {
            "ingested": row.get("total_ingested") or 0,
            "validated": row.get("total_validated") or 0,
            "rejected": row.get("total_rejected") or 0,
            "success_rate": row.get("overall_success_rate"),
        },
        "last_run": {
            "run_started_at": row.get("last_run_started_at"),
            "ingested": row.get("last_run_ingested"),
            "validated": row.get("last_run_validated"),
            "rejected": row.get("last_run_rejected"),
            "success_rate": row.get("last_run_success_rate"),
            "status": row.get("last_run_status"),
        },
        "last_successful_run_at": row.get("last_successful_run_at"),
        "seconds_since_last_successful_run": row.get(
            "seconds_since_last_successful_run"
        ),
    }


@app.get("/api/error-classes")
def error_classes():
    """Rejection reasons, counted two ways.

    rows counts postings that carry the code; occurrences counts entries in
    errors_json. They differ whenever one row carries a code more than once,
    and both ship so the page can say which it is plotting.
    """
    try:
        rows = run_query(SQL_ERROR_CLASSES)
    except DatabaseUnavailable as exc:
        return _unavailable(exc)

    # Both totals are computed in SQL and repeated on every row; lift them off
    # the first one. When there are no rows there is nothing to reject, so the
    # denominators are genuinely zero rather than unknown.
    total_rejected_rows = rows[0]["total_rejected_rows"] if rows else 0
    total_code_occurrences = rows[0]["total_code_occurrences"] if rows else 0

    return {
        "generated_at": _now(),
        "row_count": len(rows),
        "default_metric": "rows",
        "total_rejected_rows": total_rejected_rows,
        "total_code_occurrences": total_code_occurrences,
        "classes": [
            {
                "code": r["code"],
                "category": r["category"],
                "rows": r["rows"],
                "occurrences": r["occurrences"],
                "pct_of_rejected_rows": r["pct_of_rejected_rows"],
            }
            for r in rows
        ],
    }


@app.get("/api/model-accuracy")
def model_accuracy():
    """How often the model's mentions_n8n disagreed with the regex.

    A floor, not an exact rate: the cross-check only runs when mentions_n8n
    passed its own type check, so a row that failed schema validation and would
    also have disagreed does not always carry the code.
    """
    try:
        by_version = run_query(SQL_MODEL_ACCURACY_BY_VERSION)
        by_run = run_query(SQL_MODEL_ACCURACY_BY_RUN, {"run_gap": RUN_GAP})
    except DatabaseUnavailable as exc:
        return _unavailable(exc)
    return {
        "generated_at": _now(),
        "row_count": len(by_version),
        "by_version": by_version,
        "by_run": by_run,
    }


# --------------------------------------------------------------------------
# Pipeline monitoring: runs, infra errors, health
# --------------------------------------------------------------------------

# The categories record_pipeline_error() writes. Anything else in the column,
# including NULL on rows written before the column existed, is reported as
# 'unknown' rather than passed through.
ERROR_CATEGORIES = {
    "upstream_source",   # the Remotive request failed
    "upstream_llm",      # the Anthropic request failed
    "database",          # a Postgres node failed
    "pipeline_code",     # a Code node threw
    "trigger",           # the workflow failed before an execution existed
    "unknown",
}

# Who may see what.
#
# The public gets a summary: that an error happened, when, in which node, and
# its category. The message itself is operational detail and is returned only
# to a caller who sends the operator token in X-Ops-Token.
#
# There is one token, set in OPS_TOKEN. No users, no sessions, no login page:
# the only reader of diagnostics is the owner, with curl. If OPS_TOKEN is not
# set, or is shorter than 24 characters, there is no operator mode at all and
# the messages are reachable only by querying the database.
#
# A wrong token is refused with 401 rather than quietly answered in public
# mode. Otherwise a typo looks exactly like "there are no messages".
MIN_OPS_TOKEN_CHARS = 24


class BadOpsToken(Exception):
    pass


def is_operator(presented: Optional[str]) -> bool:
    """True if the caller proved it is the operator. Raises BadOpsToken if a
    token was presented and is not the configured one."""
    if presented is None:
        return False
    configured = os.environ.get("OPS_TOKEN", "")
    if len(configured) < MIN_OPS_TOKEN_CHARS:
        raise BadOpsToken()
    if not hmac.compare_digest(presented.encode(), configured.encode()):
        raise BadOpsToken()
    return True


def _bad_token() -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"error": "invalid_ops_token", "generated_at": _now()},
        headers={"Cache-Control": "no-store"},
    )


# Even the operator's copy is redacted and shortened. An n8n error message is
# whatever the failing node chose to say: it can quote a connection string, a
# request URL or a response body, and a token in a log scrollback is still a
# leaked token. The stored message is untouched; this runs on the way out only.
_REDACTIONS = [
    # scheme://user:password@host
    (re.compile(r"\b([a-z][a-z0-9+.\-]*://)[^\s/@:]+:[^\s/@]+@", re.I), r"\1[redacted]@"),
    # Anthropic-style and similar keys
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}"), "[redacted]"),
    # Neon passwords
    (re.compile(r"\bnpg_[A-Za-z0-9]{6,}"), "[redacted]"),
    # Bearer tokens
    (re.compile(r"\b(bearer)\s+[A-Za-z0-9._~+/=\-]{8,}", re.I), r"\1 [redacted]"),
    # JSON web tokens
    (re.compile(r"\beyJ[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]*"), "[redacted]"),
    # key=value and key: value for names that hold secrets
    (
        re.compile(
            r"\b(password|passwd|pwd|secret|token|api[_\-]?key|x-api-key|authorization)"
            r"(\"?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&]+)",
            re.I,
        ),
        r"\1\2[redacted]",
    ),
    # query strings, which is where keys travel in URLs
    (re.compile(r"(https?://[^\s?#]+)\?[^\s]+", re.I), r"\1?[redacted]"),
    # any remaining long unbroken token
    (re.compile(r"\b[A-Za-z0-9_\-]{40,}\b"), "[redacted]"),
]


def sanitize_message(message: Optional[str], limit: int = PUBLIC_MESSAGE_CHARS) -> Optional[str]:
    """Redact and shorten an error message for a public response.

    The length cap does a second job. If a node ever echoes a request body
    into its error, the body here is a job description, and 300 characters is
    not enough of one to matter.
    """
    if message is None:
        return None
    text = " ".join(str(message).split())
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def _category(value: Optional[str]) -> str:
    return value if value in ERROR_CATEGORIES else "unknown"


def _shape_run(row: dict, operator: bool = False) -> dict:
    """One pipeline_runs row as the API returns it.

    The keys are the same for everyone. n8n_execution_id and error.message are
    null unless the caller is the operator.
    """
    has_error = row["error_category"] is not None or row["error_message"] is not None
    rows_match = None
    if row["status"] == "completed":
        rows_match = (
            row["accepted_rows"] == row["accepted_count"]
            and row["rejected_rows"] == row["rejected_count"]
        )
    return {
        "run_id": row["run_id"],
        "n8n_execution_id": row["n8n_execution_id"] if operator else None,
        "status": row["status"],
        "is_stale": row["is_stale"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "duration_seconds": row["duration_seconds"],
        "age_seconds": row["age_seconds"],
        "fetched_count": row["fetched_count"],
        "processed_count": row["processed_count"],
        "accepted_count": row["accepted_count"],
        "rejected_count": row["rejected_count"],
        "accepted_rows": row["accepted_rows"],
        "rejected_rows": row["rejected_rows"],
        "rows_match_counts": rows_match,
        "error": (
            {
                "category": _category(row["error_category"]),
                "message": sanitize_message(row["error_message"]) if operator else None,
            }
            if has_error
            else None
        ),
        "error_count": row["error_count"],
    }


def pipeline_health(facts: dict, last_settled: Optional[dict], last_completed: Optional[dict]) -> tuple[str, list[str]]:
    """Decide pipeline_status from facts. Pure: no clock, no database.

    This is about whether the pipeline RAN, not about how good the data was.
    A run that completed and accepted nothing is healthy here; the ok /
    degraded / failed status on /api/runs is the one that judges the data.

    healthy   the latest finished run completed, it is recent enough, and
              nothing has failed in the recent window.
    degraded  the pipeline is working but something needs a look.
    failed    the latest news is bad, or there is no recent news at all.
    no_data   nothing has ever been recorded. Not the same as a run that
              completed with zero records, which is healthy.

    Silence is failure. A scheduler that is switched off writes nothing, so
    the last completed run going stale is the only sign of it, and that is
    'failed', never 'healthy'.
    """
    limit_seconds = EXPECTED_RUN_INTERVAL_HOURS * 3600 + RUN_GRACE_MINUTES * 60
    runs = facts["run_count"] or 0
    errors = facts["error_count"] or 0
    legacy_rows = facts["legacy_rows"] or 0

    if runs == 0 and errors == 0 and legacy_rows == 0:
        return "no_data", ["never_executed"]

    # Rows exist but no run was ever tracked: everything predates run
    # tracking. Fresh rows show something ran. They cannot show it finished.
    if runs == 0 and errors == 0:
        age = facts["seconds_since_last_legacy_data"]
        if age is not None and age <= limit_seconds:
            return "degraded", ["legacy_data_only"]
        return "failed", ["legacy_data_only", "data_overdue"]

    failed: list[str] = []
    degraded: list[str] = []

    if last_settled is not None:
        if last_settled["status"] == "failed":
            failed.append("last_run_failed")
        elif last_settled["status"] == "running":
            failed.append("last_run_abandoned")

    if (facts["errors_since_last_completed"] or 0) > 0:
        failed.append("error_since_last_completed_run")

    if last_completed is None:
        failed.append("no_completed_run")
    elif (facts["seconds_since_last_completed"] or 0) > limit_seconds:
        failed.append("completed_run_overdue")
    elif last_completed["rows_match_counts"] is False:
        degraded.append("rows_do_not_match_counts")

    if (facts["stale_running_count"] or 0) > 0 and "last_run_abandoned" not in failed:
        degraded.append("stale_running_run")
    if (facts["recent_failed_runs"] or 0) > 0 or (facts["recent_errors"] or 0) > 0:
        degraded.append("recent_failures")

    if failed:
        return "failed", failed + degraded
    if degraded:
        return "degraded", degraded
    return "healthy", []


@app.get("/api/pipeline-runs")
def pipeline_runs(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    status: Optional[Literal["running", "completed", "failed"]] = Query(None),
    response: Response = None,
    x_ops_token: Optional[str] = Header(None),
):
    """Tracked runs, newest first. One row per pipeline_runs row.

    Includes runs /api/runs cannot see: ones that fetched nothing, and ones
    that died before writing. Runs from before run tracking are not here;
    they have no row to return and none is invented.
    """
    try:
        operator = is_operator(x_ops_token)
    except BadOpsToken:
        return _bad_token()
    if operator:
        response.headers["Cache-Control"] = "no-store"
    params = {
        "limit": limit,
        "offset": offset,
        "status": status,
        "stale": f"{STALE_RUN_MINUTES} minutes",
    }
    try:
        rows = run_query(SQL_PIPELINE_RUNS, params)
        total = run_query(SQL_PIPELINE_RUNS_TOTAL, {"status": status})[0]["total"]
    except DatabaseUnavailable as exc:
        return _unavailable(exc)
    return {
        "generated_at": _now(),
        "limit": limit,
        "offset": offset,
        "total": total,
        "row_count": len(rows),
        "stale_run_minutes": STALE_RUN_MINUTES,
        "detail_level": "operator" if operator else "public",
        "runs": [_shape_run(r, operator) for r in rows],
    }


@app.get("/api/pipeline-errors")
def pipeline_errors(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    run_id: Optional[UUID] = Query(None),
    response: Response = None,
    x_ops_token: Optional[str] = Header(None),
):
    """Infra failures recorded by the Error Trigger workflow, newest first.

    Publicly: when, which node, which category, which run. With the operator
    token: also the message (redacted, cut to 300 characters), the workflow
    name and the execution id. The keys are the same either way; the three
    operator fields are null in public. No stack trace is stored or returned.

    run_id is null when the failure could not be tied to a run, which is the
    normal case for a failure before the run row was written.
    """
    try:
        operator = is_operator(x_ops_token)
    except BadOpsToken:
        return _bad_token()
    if operator:
        response.headers["Cache-Control"] = "no-store"
    params = {
        "limit": limit,
        "offset": offset,
        "run_id": str(run_id) if run_id else None,
    }
    try:
        rows = run_query(SQL_PIPELINE_ERRORS, params)
        total = run_query(SQL_PIPELINE_ERRORS_TOTAL, {"run_id": params["run_id"]})[0]["total"]
    except DatabaseUnavailable as exc:
        return _unavailable(exc)
    return {
        "generated_at": _now(),
        "limit": limit,
        "offset": offset,
        "total": total,
        "row_count": len(rows),
        "detail_level": "operator" if operator else "public",
        "errors": [
            {
                "id": r["id"],
                "occurred_at": r["occurred_at"],
                "failed_node": r["failed_node"],
                "run_id": r["run_id"],
                "category": _category(r["error_category"]),
                "workflow_name": r["workflow_name"] if operator else None,
                "execution_id": r["execution_id"] if operator else None,
                "message": sanitize_message(r["error_message"]) if operator else None,
            }
            for r in rows
        ],
    }


@app.get("/api/pipeline-status")
def pipeline_status():
    """Did the pipeline run. See pipeline_health for the rules.

    Always the public view: no messages and no execution ids, whoever asks.
    This is the endpoint a monitor polls, and a monitor's logs are not a place
    for diagnostics.

    Answers 200 whenever the database answered, including when the status is
    'failed'. 503 means this API could not reach the warehouse, which says
    nothing about the pipeline. A monitor has to tell those two apart.
    """
    try:
        tagged = run_query(
            SQL_PIPELINE_STATUS_RUNS, {"stale": f"{STALE_RUN_MINUTES} minutes"}
        )
        facts = run_query(
            SQL_PIPELINE_STATUS_FACTS,
            {
                "stale": f"{STALE_RUN_MINUTES} minutes",
                "window": f"{RECENT_FAILURE_HOURS} hours",
            },
        )[0]
    except DatabaseUnavailable as exc:
        return _unavailable(exc)

    by_role: dict[str, list[dict]] = {}
    for row in tagged:
        by_role.setdefault(row["role"], []).append(_shape_run(row))

    def one(role: str) -> Optional[dict]:
        found = by_role.get(role)
        return found[0] if found else None

    last_completed = one("last_completed")
    status, reasons = pipeline_health(facts, one("last_settled"), last_completed)

    # Two stored spellings can both map to 'unknown', so add rather than assign.
    by_category: dict[str, int] = {}
    for name, count in (facts["recent_errors_by_category"] or {}).items():
        by_category[_category(name)] = by_category.get(_category(name), 0) + count

    limit_seconds = EXPECTED_RUN_INTERVAL_HOURS * 3600 + RUN_GRACE_MINUTES * 60
    since = facts["seconds_since_last_completed"]
    if last_completed is not None:
        evidence = "explicit"
    elif (facts["legacy_rows"] or 0) > 0:
        evidence = "legacy"
    else:
        evidence = "none"

    return {
        "generated_at": _now(),
        "pipeline_status": status,
        "reasons": reasons,
        "last_attempted_run": one("last_attempt"),
        "last_successful_run": last_completed,
        "last_failed_run": one("last_failed"),
        "running": {
            "count": facts["running_count"],
            "stale_count": facts["stale_running_count"],
            "runs": by_role.get("running", []),
        },
        "recent_failures": {
            "window_hours": RECENT_FAILURE_HOURS,
            "failed_runs": facts["recent_failed_runs"],
            "errors": facts["recent_errors"],
            "uncorrelated_errors": facts["recent_uncorrelated_errors"],
            "by_category": by_category,
            "errors_since_last_successful_run": facts["errors_since_last_completed"],
            "last_error_at": facts["last_error_at"],
        },
        "freshness": {
            "evidence": evidence,
            "expected_interval_hours": EXPECTED_RUN_INTERVAL_HOURS,
            "grace_minutes": RUN_GRACE_MINUTES,
            "last_successful_run_completed_at": facts["last_completed_at"],
            "seconds_since_last_successful_run": since,
            "overdue": (since is None or since > limit_seconds),
            "last_legacy_data_at": facts["last_legacy_data_at"],
            "seconds_since_last_legacy_data": facts["seconds_since_last_legacy_data"],
        },
        "totals": {
            "tracked_runs": facts["run_count"],
            "recorded_errors": facts["error_count"],
            "legacy_rows": facts["legacy_rows"],
        },
        "thresholds": {
            "stale_run_minutes": STALE_RUN_MINUTES,
            "recent_failure_hours": RECENT_FAILURE_HOURS,
        },
    }


# Serve the dashboard for local work. Mounted last so it cannot shadow /api.
# The same file works unchanged from an nginx static root; see README.
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
