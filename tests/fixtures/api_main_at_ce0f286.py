# FROZEN COPY. This is api/main.py exactly as committed at ce0f286, the version
# serving the live dashboard before the run-tracking work. Nothing below this
# header was edited.
#
# tests/test_deployment_rehearsal.py runs it against a migrated database to
# show that applying the migrations does not break the API that is already
# deployed. That is the property the release order depends on. Do not update
# this file to match the current API; that would make the test meaningless.
#
"""RoleRadar metrics API.

Reads the Postgres warehouse and serves the numbers behind
``static/index.html``. Every endpoint is one query. Aggregation happens in SQL;
nothing here loops over rows to compute a metric.

The queries are duplicated from ``sql/queries.sql``, which is where the
reasoning for each one is written down. That file is the readable source of
truth; these constants are the deployed copy. The alternative was to read and
split the .sql file at import, which removes the duplication but makes the app
depend on splitting SQL on semicolons -- fragile against a semicolon inside a
string literal, and it puts a file-path dependency between the app and the repo
layout. Duplication that is documented beats a parser that is nearly right.
See README.md, "Known limitations".

No credentials in this file. Connection details come from the environment.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, Query
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

# The sessionisation preamble, shared by every run-scoped query. Rows do not
# reliably share an ingested_at within a run, because the Validator Code node
# reads the clock once per item, so runs are recovered by gap instead of by
# equality. LAG finds the boundaries; the running SUM turns them into a run
# number. dead_letter_log.ingested_at is timestamptz and jobs_extracted's is
# timestamp, so the timestamptz side is normalised to naive UTC.
_RUNS_PREAMBLE = """
WITH events AS (
    SELECT ingested_at                    AS ts, 1 AS validated, 0 AS rejected
    FROM public.jobs_extracted
    UNION ALL
    SELECT ingested_at AT TIME ZONE 'UTC' AS ts, 0 AS validated, 1 AS rejected
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
),
numbered AS (
    SELECT
        ts, validated, rejected,
        SUM(is_run_start) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING) AS run_seq
    FROM marked
)
"""

# 1. GET /api/runs -- volume chart, success-rate chart, runs table.
# The limit is applied after the window functions via a descending ROW_NUMBER,
# so cumulative_ingested stays a lifetime total and the delta on the oldest
# visible run still refers to the run before it.
SQL_RUNS = (
    _RUNS_PREAMBLE
    + """,
per_run AS (
    SELECT
        MIN(ts) AS run_started_at,
        MAX(ts) AS run_ended_at,
        COUNT(*) AS ingested,
        SUM(validated) AS validated,
        SUM(rejected) AS rejected
    FROM numbered
    GROUP BY run_seq
),
scored AS (
    SELECT
        run_started_at, run_ended_at, ingested, validated, rejected,
        validated::numeric / NULLIF(ingested, 0) AS success_rate
    FROM per_run
),
windowed AS (
    SELECT
        run_started_at,
        run_ended_at,
        ingested,
        validated,
        rejected,
        ROUND(success_rate, 4) AS success_rate,
        ROUND(success_rate - LAG(success_rate) OVER (ORDER BY run_started_at), 4)
            AS success_rate_delta,
        ROUND(AVG(success_rate) OVER (
            ORDER BY run_started_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 4) AS success_rate_rolling3,
        (SUM(ingested) OVER (ORDER BY run_started_at ROWS UNBOUNDED PRECEDING))::bigint
            AS cumulative_ingested,
        (SUM(validated) OVER (ORDER BY run_started_at ROWS UNBOUNDED PRECEDING))::bigint
            AS cumulative_validated,
        CASE
            WHEN rejected = 0  THEN 'ok'
            WHEN validated = 0 THEN 'failed'
            ELSE 'degraded'
        END AS status,
        ROW_NUMBER() OVER (ORDER BY run_started_at DESC) AS recency
    FROM scored
)
SELECT
    run_started_at, run_ended_at, ingested, validated, rejected,
    success_rate, success_rate_delta, success_rate_rolling3,
    cumulative_ingested, cumulative_validated, status
FROM windowed
WHERE recency <= %(limit)s
ORDER BY run_started_at
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
        MIN(ts) AS run_started_at,
        COUNT(*) AS ingested,
        SUM(validated) AS validated,
        SUM(rejected) AS rejected
    FROM numbered
    GROUP BY run_seq
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
LEFT JOIN per_run l ON l.run_started_at = a.last_run_started_at
"""
)

# 3. GET /api/error-classes -- the rejection-reason bar chart.
# Categories come from errors_json rather than error_codes, because error_codes
# drops the field name and MISSING is emitted for both a missing source_job_id
# (a pipeline fault) and a missing normalized_title (a model fault).
# errors_json is double-encoded -- the validator stringifies it before insert --
# so the CASE unwraps a JSON string and falls through for a proper array.
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

# 4b. The same rate per run, for the trend line. Same sessionisation, carrying
# a mismatch flag instead of the validated/rejected split.
SQL_MODEL_ACCURACY_BY_RUN = """
WITH events AS (
    SELECT ingested_at AS ts, false AS mismatch
    FROM public.jobs_extracted
    UNION ALL
    SELECT ingested_at AT TIME ZONE 'UTC' AS ts,
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
),
numbered AS (
    SELECT
        ts, mismatch,
        SUM(is_run_start) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING) AS run_seq
    FROM marked
)
SELECT
    MIN(ts)                          AS run_started_at,
    COUNT(*)                         AS processed,
    COUNT(*) FILTER (WHERE mismatch) AS mismatches,
    ROUND(COUNT(*) FILTER (WHERE mismatch)::numeric / NULLIF(COUNT(*), 0), 4)
        AS mismatch_rate
FROM numbered
GROUP BY run_seq
ORDER BY run_started_at
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
                )
    return _pool


class DatabaseUnavailable(Exception):
    """Raised when the warehouse could not be reached or the query failed."""


def run_query(sql: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute one statement and return its rows.

    Wraps every psycopg failure in DatabaseUnavailable so the endpoints have a
    single thing to catch and can answer 503 with the real message instead of a
    500 with a stack trace.
    """
    try:
        with get_pool().connection(timeout=DB_TIMEOUT) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                return [_jsonable(row) for row in cur.fetchall()]
    except PoolTimeout as exc:
        raise DatabaseUnavailable(_explain_timeout()) from exc
    except psycopg.Error as exc:
        raise DatabaseUnavailable(str(exc).strip()) from exc


WAKING = "the database may be waking from suspend"


def _explain_timeout() -> str:
    """Work out why the pool timed out, because the pool will not say.

    psycopg_pool retries a failing connection until its deadline, so a
    database that is refusing connections and one that is merely slow to wake
    both arrive here as PoolTimeout with the same useless message. Reporting
    both as "may be waking" would collapse the distinction the dashboard is
    built to draw -- a suspended Neon compute and a dead warehouse are
    different facts and the page shows them differently.

    So probe once, directly, outside the pool. A refused connection, a bad
    host or a rejected password all fail immediately and produce a real error
    worth showing. Only a probe that also times out is genuinely ambiguous,
    and that is the case where "waking" is the honest answer.
    """
    try:
        with psycopg.connect(connection_string(), connect_timeout=PROBE_TIMEOUT):
            # Connected on the second attempt: the pool was contended or the
            # compute woke in between, not a failure worth naming.
            return f"no pooled connection within {DB_TIMEOUT:g}s, though a direct one succeeded"
    except psycopg.OperationalError as probe:
        detail = " ".join(str(probe).split())
        timed_out = "timeout expired" in detail.lower() or "timed out" in detail.lower()
        if timed_out or not detail:
            return f"no database connection within {DB_TIMEOUT:g}s ({WAKING})"
        return detail
    except Exception:
        return f"no database connection within {DB_TIMEOUT:g}s ({WAKING})"


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


@app.get("/api/health")
def health():
    """Is the warehouse reachable.

    Exists so the page can distinguish "the API is down" from "the pipeline has
    not ingested anything". Those look identical on a chart and mean opposite
    things.
    """
    try:
        run_query("SELECT 1 AS ok")
    except DatabaseUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={
                "database": "down",
                "detail": str(exc),
                "checked_at": _now(),
            },
        )
    return {"database": "up", "checked_at": _now()}


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


# Serve the dashboard for local work. Mounted last so it cannot shadow /api.
# The same file works unchanged from an nginx static root; see README.
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
