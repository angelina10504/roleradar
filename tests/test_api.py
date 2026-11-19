"""The FastAPI endpoints against a real, disposable Postgres.

Each test gets its own migrated database and points the app at it. Rows are
synthetic. Nothing here calls Remotive, Anthropic, n8n or Neon.

A "failed run" in this file is a row written by calling
record_pipeline_error() directly. It shows what the API reports when such rows
exist. It is not evidence that n8n's Error Trigger produces them.
"""

from __future__ import annotations

import sys
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from conftest import FIXTURES, REPO
from test_migrations import insert_dead, insert_job
from test_run_lifecycle import close, record, start

sys.path.insert(0, str(REPO / "api"))
sys.path.insert(0, str(FIXTURES))

import main  # noqa: E402
import migrate  # noqa: E402
import queries_at_ce0f286 as committed  # noqa: E402

RUN_KEYS = {
    "run_id", "n8n_execution_id", "status", "is_stale", "started_at", "completed_at",
    "duration_seconds", "age_seconds", "fetched_count", "processed_count",
    "accepted_count", "rejected_count", "accepted_rows", "rejected_rows",
    "rows_match_counts", "error", "error_count",
}


OPS_TOKEN = "test-operator-token-0123456789abcdef"
OPERATOR = {"X-Ops-Token": OPS_TOKEN}


def _client(conninfo, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", conninfo)
    monkeypatch.setenv("OPS_TOKEN", OPS_TOKEN)
    main._pool = None
    return TestClient(main.app)


def _shutdown():
    if main._pool is not None:
        main._pool.close()
        main._pool = None


@pytest.fixture
def api(migrated_db, monkeypatch):
    """The app over the legacy rows plus every migration."""
    yield _client(migrated_db, monkeypatch)
    _shutdown()


@pytest.fixture
def empty_db(db):
    """Migrated from nothing: every table exists and every table is empty."""
    migrate.up(db, log=lambda _line: None)
    return db


@pytest.fixture
def empty_conn(empty_db):
    with psycopg.connect(empty_db, autocommit=True) as connection:
        yield connection


@pytest.fixture
def empty_api(empty_db, monkeypatch):
    yield _client(empty_db, monkeypatch)
    _shutdown()


def completed_run(conn, execution_id="100", accepted=("a", "b"), rejected=("d",), fetched=13):
    run = start(conn, execution_id)
    for job in accepted:
        insert_job(conn, run, job)
    for job in rejected:
        insert_dead(conn, run, job)
    close(conn, run, fetched, list(accepted), list(rejected))
    return run


def backdate(conn, run_id, started, completed=None):
    """Move a run into the past. Test setup only; nothing in the pipeline does this."""
    conn.execute(
        "UPDATE public.pipeline_runs SET started_at = now() - %s::interval, "
        "completed_at = CASE WHEN completed_at IS NULL THEN NULL "
        "ELSE now() - %s::interval END WHERE run_id = %s",
        (started, completed or started, run_id),
    )


def status_of(client):
    response = client.get("/api/pipeline-status")
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------------
# /api/pipeline-runs
# ---------------------------------------------------------------------------


def test_a_successful_run_is_reported_with_counts_and_rows(empty_api, empty_conn):
    run = completed_run(empty_conn)

    body = empty_api.get("/api/pipeline-runs").json()

    assert (body["total"], body["row_count"]) == (1, 1)
    got = body["runs"][0]
    assert set(got) == RUN_KEYS
    assert got["run_id"] == str(run) and uuid.UUID(got["run_id"])
    assert got["status"] == "completed" and got["is_stale"] is False
    assert (got["fetched_count"], got["processed_count"]) == (13, 3)
    assert (got["accepted_count"], got["rejected_count"]) == (2, 1)
    assert (got["accepted_rows"], got["rejected_rows"]) == (2, 1)
    assert got["rows_match_counts"] is True
    assert got["duration_seconds"] >= 0 and got["completed_at"] > got["started_at"]
    assert got["error"] is None and got["error_count"] == 0


def test_a_failed_run_carries_a_category_publicly_and_a_message_for_the_operator(empty_api, empty_conn):
    run = start(empty_conn, "100")
    record(empty_conn, "100",
           message="connect failed postgresql://owner:npg_FAKEnotARealToken0@ep-x.neon.tech/db")

    public = empty_api.get("/api/pipeline-runs").json()
    private = empty_api.get("/api/pipeline-runs", headers=OPERATOR).json()

    got = public["runs"][0]
    assert public["detail_level"] == "public" and private["detail_level"] == "operator"
    assert got["run_id"] == str(run) and got["status"] == "failed"
    assert got["error"] == {"category": "upstream_llm", "message": None}
    assert got["n8n_execution_id"] is None
    assert got["error_count"] == 1
    assert got["accepted_count"] is None and got["rows_match_counts"] is None

    seen = private["runs"][0]
    assert set(seen) == set(got) == RUN_KEYS
    assert seen["n8n_execution_id"] == "100"
    assert seen["error"]["message"].startswith("connect failed")
    assert "npg_" not in seen["error"]["message"] and "owner:" not in seen["error"]["message"]


def test_zero_fetched_and_zero_accepted_are_completed_runs(empty_api, empty_conn):
    completed_run(empty_conn, "1", accepted=(), rejected=(), fetched=0)
    completed_run(empty_conn, "2", accepted=(), rejected=("d",), fetched=13)

    runs = {r["n8n_execution_id"]: r
            for r in empty_api.get("/api/pipeline-runs", headers=OPERATOR).json()["runs"]}

    assert runs["1"]["status"] == "completed" and runs["1"]["fetched_count"] == 0
    assert (runs["1"]["processed_count"], runs["1"]["accepted_rows"]) == (0, 0)
    assert runs["2"]["status"] == "completed"
    assert (runs["2"]["accepted_count"], runs["2"]["rejected_count"]) == (0, 1)


def test_a_partially_written_run_shows_rows_but_no_verdict(empty_api, empty_conn):
    """Two rows landed and the execution died before Complete Run."""
    run = start(empty_conn, "100")
    insert_job(empty_conn, run, "a")
    insert_job(empty_conn, run, "b")

    got = empty_api.get("/api/pipeline-runs").json()["runs"][0]

    assert got["status"] == "running" and got["completed_at"] is None
    assert got["accepted_rows"] == 2 and got["accepted_count"] is None
    assert got["rows_match_counts"] is None and got["duration_seconds"] is None
    assert got["is_stale"] is False


def test_a_run_open_past_the_threshold_is_stale(empty_api, empty_conn, monkeypatch):
    monkeypatch.setattr(main, "STALE_RUN_MINUTES", 10.0)
    fresh = start(empty_conn, "1")
    old = start(empty_conn, "2")
    backdate(empty_conn, fresh, "9 minutes")
    backdate(empty_conn, old, "11 minutes")

    runs = {r["n8n_execution_id"]: r
            for r in empty_api.get("/api/pipeline-runs", headers=OPERATOR).json()["runs"]}

    assert runs["1"]["is_stale"] is False and runs["2"]["is_stale"] is True
    assert runs["2"]["age_seconds"] >= 660


def test_replayed_rows_do_not_inflate_the_reported_counts(empty_api, empty_conn):
    run = completed_run(empty_conn, accepted=("a",), rejected=("d",))
    for _ in range(3):
        insert_job(empty_conn, run, "a", "ON CONFLICT DO NOTHING")
        insert_dead(empty_conn, run, "d", "ON CONFLICT DO NOTHING")

    got = empty_api.get("/api/pipeline-runs").json()["runs"][0]

    assert (got["accepted_count"], got["accepted_rows"]) == (1, 1)
    assert (got["rejected_count"], got["rejected_rows"]) == (1, 1)


def test_rows_that_disagree_with_the_counts_are_flagged(empty_api, empty_conn):
    run = completed_run(empty_conn, accepted=("a", "b"), rejected=())
    empty_conn.execute("DELETE FROM public.jobs_extracted WHERE run_id = %s AND source_job_id = 'b'", (run,))
    got = empty_api.get("/api/pipeline-runs").json()["runs"][0]
    assert (got["accepted_count"], got["accepted_rows"]) == (2, 1)
    assert got["rows_match_counts"] is False


def test_runs_paginate_newest_first(empty_api, empty_conn):
    for n in range(5):
        backdate(empty_conn, completed_run(empty_conn, str(n)), f"{10 - n} minutes")

    first = empty_api.get("/api/pipeline-runs?limit=2", headers=OPERATOR).json()
    second = empty_api.get("/api/pipeline-runs?limit=2&offset=2", headers=OPERATOR).json()
    beyond = empty_api.get("/api/pipeline-runs?limit=2&offset=50", headers=OPERATOR).json()

    assert [r["n8n_execution_id"] for r in first["runs"]] == ["4", "3"]
    assert [r["n8n_execution_id"] for r in second["runs"]] == ["2", "1"]
    assert first["total"] == second["total"] == beyond["total"] == 5
    assert beyond["runs"] == [] and beyond["row_count"] == 0


def test_runs_filter_by_status(empty_api, empty_conn):
    completed_run(empty_conn, "1")
    start(empty_conn, "2")
    start(empty_conn, "3")
    record(empty_conn, "3")

    def ids(status):
        body = empty_api.get(f"/api/pipeline-runs?status={status}", headers=OPERATOR).json()
        assert body["total"] == len(body["runs"])
        return [r["n8n_execution_id"] for r in body["runs"]]

    assert (ids("completed"), ids("running"), ids("failed")) == (["1"], ["2"], ["3"])


@pytest.mark.parametrize(
    "path",
    [
        "/api/pipeline-runs?limit=0",
        "/api/pipeline-runs?limit=201",
        "/api/pipeline-runs?limit=abc",
        "/api/pipeline-runs?offset=-1",
        "/api/pipeline-runs?status=done",
        "/api/pipeline-runs?status=completed'%20OR%201=1--",
        "/api/pipeline-errors?limit=0",
        "/api/pipeline-errors?limit=1000",
        "/api/pipeline-errors?offset=-5",
        "/api/pipeline-errors?run_id=not-a-uuid",
        "/api/pipeline-errors?run_id=legacy:1",
    ],
)
def test_invalid_query_parameters_are_rejected(empty_api, path):
    assert empty_api.get(path).status_code == 422


def test_legacy_rows_do_not_invent_tracked_runs(api):
    body = api.get("/api/pipeline-runs").json()
    assert body["total"] == 0 and body["runs"] == []


# ---------------------------------------------------------------------------
# /api/pipeline-errors
# ---------------------------------------------------------------------------

SECRET_MESSAGES = {
    "anthropic key": "401 invalid x-api-key: sk-ant-api03-AbCdEfGhIjKlMnOpQrStUv",
    "connection url": "could not connect postgresql://neondb_owner:hunter2pass@ep-cool.aws.neon.tech/neondb",
    "bearer token": "Authorization failed for Bearer abcdef0123456789abcdef",
    "password pair": "FATAL: auth failed password=SuperSecret99 host=db",
    "json key": '{"api_key": "kkkkkkkk12345678", "detail": "bad"}',
    "query string": "GET https://example.test/hook?token=abcd1234efgh5678&x=1 returned 500",
    "jwt": "bad token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk",
}
LEAKS = ["sk-ant", "AbCdEfGh", "hunter2pass", "neondb_owner:", "abcdef0123456789abcdef",
         "SuperSecret99", "kkkkkkkk12345678", "abcd1234efgh5678", "eyJhbGci"]


@pytest.mark.parametrize("label", sorted(SECRET_MESSAGES))
def test_secrets_are_redacted_from_public_error_messages(empty_api, empty_conn, label):
    start(empty_conn, "100")
    record(empty_conn, "100", message=SECRET_MESSAGES[label])

    bodies = [
        empty_api.get("/api/pipeline-errors").text,
        empty_api.get("/api/pipeline-runs").text,
        empty_api.get("/api/pipeline-status").text,
        empty_api.get("/api/pipeline-status", headers=OPERATOR).text,
    ]
    operator_errors = empty_api.get("/api/pipeline-errors", headers=OPERATOR).text
    operator_runs = empty_api.get("/api/pipeline-runs", headers=OPERATOR).text

    # The operator's copy is still redacted.
    for leak in LEAKS:
        assert all(leak not in body for body in bodies)
        assert leak not in operator_errors and leak not in operator_runs
    assert "[redacted]" in operator_errors
    # The public copy has no message at all, redacted or otherwise.
    assert all("[redacted]" not in body for body in bodies)
    # The stored message is the diagnostic copy and is left whole.
    assert empty_conn.execute(
        "SELECT error_message FROM public.pipeline_errors"
    ).fetchone()[0] == SECRET_MESSAGES[label]


def test_a_long_message_is_cut_so_a_description_cannot_ride_along(empty_api, empty_conn):
    description = "We are hiring a senior engineer. " * 200
    record(empty_conn, "100", message=f"Bad request - please check your parameters\n\n{description}")

    message = empty_api.get("/api/pipeline-errors", headers=OPERATOR).json()["errors"][0]["message"]
    public = empty_api.get("/api/pipeline-errors").text

    assert "hiring" not in public and "Bad request" not in public
    assert len(message) <= main.PUBLIC_MESSAGE_CHARS
    assert message.startswith("Bad request - please check your parameters")
    assert "\n" not in message


def test_ordinary_diagnostics_survive_sanitizing():
    kept = "Request failed with status code 529: Overloaded (node Claude Extract, attempt 1)"
    assert main.sanitize_message(kept) == kept
    assert main.sanitize_message(None) is None


def test_error_rows_expose_only_the_documented_fields(empty_api, empty_conn):
    run = start(empty_conn, "100")
    record(empty_conn, "100")

    body = empty_api.get("/api/pipeline-errors").json()
    private = empty_api.get("/api/pipeline-errors", headers=OPERATOR).json()

    keys = {"generated_at", "limit", "offset", "total", "row_count", "detail_level", "errors"}
    row_keys = {"id", "occurred_at", "workflow_name", "failed_node",
                "execution_id", "run_id", "category", "message"}
    assert set(body) == set(private) == keys
    row, seen = body["errors"][0], private["errors"][0]
    assert set(row) == set(seen) == row_keys
    # Public: what, when, where, which run.
    assert row["run_id"] == str(run) and row["category"] == "upstream_llm"
    assert row["failed_node"] == "Claude Extract"
    assert (row["message"], row["execution_id"], row["workflow_name"]) == (None, None, None)
    # Operator: the same row, with the three diagnostic fields filled.
    assert (seen["execution_id"], seen["workflow_name"]) == ("100", "RoleRadar")
    assert "529" in seen["message"]


def test_errors_paginate_and_filter_by_run(empty_api, empty_conn):
    run = start(empty_conn, "100")
    record(empty_conn, "100", message="one")
    record(empty_conn, "100", message="two")
    record(empty_conn, None, message="no run")

    everything = empty_api.get("/api/pipeline-errors?limit=2", headers=OPERATOR).json()
    rest = empty_api.get("/api/pipeline-errors?limit=2&offset=2").json()
    mine = empty_api.get(f"/api/pipeline-errors?run_id={run}").json()

    assert (everything["total"], everything["row_count"], rest["row_count"]) == (3, 2, 1)
    assert [e["message"] for e in everything["errors"]] == ["no run", "two"]
    assert mine["total"] == 2 and {e["run_id"] for e in mine["errors"]} == {str(run)}


def test_an_unrecognised_or_missing_category_is_reported_as_unknown(empty_api, empty_conn):
    empty_conn.execute(
        "INSERT INTO public.pipeline_errors (error_message, error_category) "
        "VALUES ('typo', 'databse'), ('older row', NULL)"
    )
    body = empty_api.get("/api/pipeline-errors").json()
    assert {e["category"] for e in body["errors"]} == {"unknown"}
    assert status_of(empty_api)["recent_failures"]["by_category"] == {"unknown": 2}


# ---------------------------------------------------------------------------
# /api/pipeline-status
# ---------------------------------------------------------------------------


def test_never_executed_is_no_data(empty_api):
    body = status_of(empty_api)
    assert body["pipeline_status"] == "no_data" and body["reasons"] == ["never_executed"]
    assert body["last_attempted_run"] is None and body["last_successful_run"] is None
    assert body["last_failed_run"] is None
    assert body["running"] == {"count": 0, "stale_count": 0, "runs": []}
    assert body["freshness"]["evidence"] == "none"


def test_a_completed_run_with_zero_accepted_records_is_not_no_data(empty_api, empty_conn):
    completed_run(empty_conn, accepted=(), rejected=(), fetched=0)
    body = status_of(empty_api)
    assert body["pipeline_status"] == "healthy" and body["reasons"] == []
    assert body["last_successful_run"]["accepted_count"] == 0
    assert body["last_successful_run"]["fetched_count"] == 0


def test_a_recent_completed_run_is_healthy(empty_api, empty_conn):
    run = completed_run(empty_conn)
    body = status_of(empty_api)
    assert body["pipeline_status"] == "healthy"
    assert body["last_successful_run"]["run_id"] == str(run)
    assert body["last_attempted_run"]["run_id"] == str(run)
    assert set(body["last_successful_run"]) == RUN_KEYS
    assert body["freshness"]["evidence"] == "explicit" and body["freshness"]["overdue"] is False
    assert body["freshness"]["seconds_since_last_successful_run"] < 60


def test_an_offline_scheduler_is_not_healthy(empty_api, empty_conn):
    """The last write succeeded. Then nothing ran for three days."""
    backdate(empty_conn, completed_run(empty_conn), "3 days")
    body = status_of(empty_api)
    assert body["pipeline_status"] == "failed"
    assert body["reasons"] == ["completed_run_overdue"]
    assert body["freshness"]["overdue"] is True
    assert body["last_successful_run"]["status"] == "completed"


def test_the_overdue_limit_is_interval_plus_grace(empty_api, empty_conn, monkeypatch):
    monkeypatch.setattr(main, "EXPECTED_RUN_INTERVAL_HOURS", 24.0)
    monkeypatch.setattr(main, "RUN_GRACE_MINUTES", 60.0)
    run = completed_run(empty_conn)
    backdate(empty_conn, run, "24 hours 50 minutes")
    assert status_of(empty_api)["pipeline_status"] == "healthy"
    backdate(empty_conn, run, "25 hours 10 minutes")
    assert status_of(empty_api)["pipeline_status"] == "failed"


def test_a_failed_last_run_is_failed(empty_api, empty_conn):
    backdate(empty_conn, completed_run(empty_conn, "1"), "2 hours")
    start(empty_conn, "2")
    record(empty_conn, "2", node="Insert rows in a table", node_type="n8n-nodes-base.postgres",
           message="relation does not exist")

    body = status_of(empty_api)

    assert body["pipeline_status"] == "failed"
    assert "last_run_failed" in body["reasons"]
    assert body["last_attempted_run"]["status"] == "failed"
    assert body["last_failed_run"]["run_id"] == body["last_attempted_run"]["run_id"]
    assert body["last_failed_run"]["error"] == {"category": "database", "message": None}
    assert body["last_successful_run"]["status"] == "completed"
    assert body["recent_failures"]["failed_runs"] == 1
    assert body["recent_failures"]["by_category"] == {"database": 1}
    assert body["recent_failures"]["errors_since_last_successful_run"] == 1


def test_a_failure_before_any_run_row_is_failed(empty_api, empty_conn):
    """Start Run could not write. There is an error and no run at all."""
    backdate(empty_conn, completed_run(empty_conn, "1"), "2 hours")
    record(empty_conn, "2", node="Start Run", node_type="n8n-nodes-base.postgres")

    body = status_of(empty_api)

    assert body["pipeline_status"] == "failed"
    assert body["reasons"][0] == "error_since_last_completed_run"
    assert body["recent_failures"]["uncorrelated_errors"] == 1
    assert body["last_attempted_run"]["status"] == "completed"
    assert body["last_failed_run"] is None


def test_only_errors_and_no_run_ever_is_failed_not_no_data(empty_api, empty_conn):
    record(empty_conn, None, node=None, node_type=None, message="trigger failed")
    body = status_of(empty_api)
    assert body["pipeline_status"] == "failed"
    assert "no_completed_run" in body["reasons"]


def test_a_run_in_progress_does_not_change_the_status(empty_api, empty_conn):
    backdate(empty_conn, completed_run(empty_conn, "1"), "2 hours")
    running = start(empty_conn, "2")

    body = status_of(empty_api)

    assert body["pipeline_status"] == "healthy"
    assert body["running"]["count"] == 1 and body["running"]["stale_count"] == 0
    assert body["running"]["runs"][0]["run_id"] == str(running)
    assert body["last_attempted_run"]["run_id"] == str(running)


def test_an_abandoned_last_run_is_failed(empty_api, empty_conn):
    backdate(empty_conn, completed_run(empty_conn, "1"), "2 hours")
    backdate(empty_conn, start(empty_conn, "2"), "40 minutes")

    body = status_of(empty_api)

    assert body["pipeline_status"] == "failed"
    assert body["reasons"] == ["last_run_abandoned"]
    assert body["running"]["stale_count"] == 1
    assert body["running"]["runs"][0]["is_stale"] is True


def test_an_old_abandoned_run_behind_a_good_one_is_degraded(empty_api, empty_conn):
    backdate(empty_conn, start(empty_conn, "1"), "3 hours")
    completed_run(empty_conn, "2")
    body = status_of(empty_api)
    assert body["pipeline_status"] == "degraded" and body["reasons"] == ["stale_running_run"]


def test_a_recovered_failure_is_degraded_until_the_window_passes(empty_api, empty_conn):
    failed = start(empty_conn, "1")
    record(empty_conn, "1")
    backdate(empty_conn, failed, "3 hours")
    empty_conn.execute("UPDATE public.pipeline_errors SET occurred_at = now() - interval '3 hours'")
    completed_run(empty_conn, "2")

    assert status_of(empty_api)["pipeline_status"] == "degraded"
    assert status_of(empty_api)["reasons"] == ["recent_failures"]

    backdate(empty_conn, failed, "30 hours")
    empty_conn.execute("UPDATE public.pipeline_errors SET occurred_at = now() - interval '30 hours'")
    assert status_of(empty_api)["pipeline_status"] == "healthy"


def test_a_completed_run_with_missing_rows_is_degraded(empty_api, empty_conn):
    run = completed_run(empty_conn, accepted=("a", "b"), rejected=())
    empty_conn.execute("DELETE FROM public.jobs_extracted WHERE run_id = %s AND source_job_id = 'b'", (run,))
    body = status_of(empty_api)
    assert body["pipeline_status"] == "degraded"
    assert body["reasons"] == ["rows_do_not_match_counts"]


def test_legacy_only_data_that_is_old_is_failed(api):
    """The live warehouse the day after migrating: rows from September, no runs."""
    body = status_of(api)
    assert body["pipeline_status"] == "failed"
    assert body["reasons"] == ["legacy_data_only", "data_overdue"]
    assert body["freshness"]["evidence"] == "legacy"
    assert body["freshness"]["last_legacy_data_at"].startswith("2026-09-11")
    assert body["totals"] == {"tracked_runs": 0, "recorded_errors": 0, "legacy_rows": 31}
    assert body["last_successful_run"] is None


def test_legacy_only_data_that_is_fresh_is_degraded_not_healthy(api, conn):
    """Fresh rows show something ran. Without a run row they cannot show it finished."""
    insert_job(conn, None, "fresh")
    body = status_of(api)
    assert body["pipeline_status"] == "degraded" and body["reasons"] == ["legacy_data_only"]


def test_legacy_rows_do_not_hide_a_tracked_failure(api, conn):
    start(conn, "100")
    record(conn, "100")
    assert status_of(api)["pipeline_status"] == "failed"


def test_an_unreachable_database_is_503_not_a_pipeline_status(monkeypatch):
    """The monitor must be able to tell a broken API from a broken pipeline."""
    monkeypatch.setattr(main, "DB_TIMEOUT", 1.0)
    monkeypatch.setattr(main, "PROBE_TIMEOUT", 1.0)
    client = _client("host=127.0.0.1 port=1 dbname=x user=x connect_timeout=1", monkeypatch)
    try:
        for path in ("/api/pipeline-status", "/api/pipeline-runs", "/api/pipeline-errors"):
            response = client.get(path)
            assert response.status_code == 503
            assert response.json()["error"] == "database_unavailable"
            assert response.json()["reason"] == "unreachable"
            assert "pipeline_status" not in response.json()
    finally:
        _shutdown()


# ---------------------------------------------------------------------------
# pipeline_health, the pure function
# ---------------------------------------------------------------------------

BASE_FACTS = {
    "run_count": 1, "error_count": 0, "legacy_rows": 0,
    "seconds_since_last_legacy_data": None, "errors_since_last_completed": 0,
    "seconds_since_last_completed": 60, "stale_running_count": 0,
    "recent_failed_runs": 0, "recent_errors": 0,
}
GOOD = {"status": "completed", "rows_match_counts": True}


def test_health_reads_no_clock_and_no_database():
    assert main.pipeline_health(dict(BASE_FACTS), GOOD, GOOD) == ("healthy", [])
    assert main.pipeline_health(dict(BASE_FACTS), GOOD, GOOD) == ("healthy", [])


def test_failed_outranks_degraded_and_keeps_both_reasons():
    facts = dict(BASE_FACTS, recent_errors=2, errors_since_last_completed=1)
    status, reasons = main.pipeline_health(facts, {"status": "failed"}, GOOD)
    assert status == "failed"
    assert reasons == ["last_run_failed", "error_since_last_completed_run", "recent_failures"]


# ---------------------------------------------------------------------------
# /api/runs: legacy and explicit runs side by side
# ---------------------------------------------------------------------------


def run_sql(conninfo, sql, params):
    with psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row) as c:
        return [main._jsonable(r) for r in c.execute(sql, params).fetchall()]


def test_legacy_only_runs_are_identical_to_the_committed_query(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_RUNS, {"run_gap": "2 minutes", "limit": 30})

    body = api.get("/api/runs").json()

    assert len(before) > 1
    assert [{k: r[k] for k in before[0]} for r in body["runs"]] == before
    assert {r["run_source"] for r in body["runs"]} == {"legacy"}
    assert {r["run_id"] for r in body["runs"]} == {None}


def test_the_runs_response_keeps_every_committed_key(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_RUNS, {"run_gap": "2 minutes", "limit": 30})
    body = api.get("/api/runs").json()
    assert set(body) == {"generated_at", "run_gap_minutes", "limit", "row_count", "runs"}
    assert set(body["runs"][0]) == set(before[0]) | {"run_source", "run_id"}


def test_the_limit_still_applies_after_the_windows(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_RUNS, {"run_gap": "2 minutes", "limit": 2})
    got = api.get("/api/runs?limit=2").json()["runs"]
    assert [{k: r[k] for k in before[0]} for r in got] == before
    assert got[-1]["cumulative_ingested"] == 31


def test_mixed_data_adds_explicit_runs_without_moving_legacy_ones(api, conn, migrated_db):
    legacy_before = api.get("/api/runs").json()["runs"]
    run = completed_run(conn, accepted=("a", "b", "c"), rejected=("d",))

    runs = api.get("/api/runs").json()["runs"]

    assert len(runs) == len(legacy_before) + 1
    legacy_after = [r for r in runs if r["run_source"] == "legacy"]
    assert legacy_after == legacy_before
    explicit = runs[-1]
    assert explicit["run_source"] == "explicit" and explicit["run_id"] == str(run)
    assert (explicit["ingested"], explicit["validated"], explicit["rejected"]) == (4, 3, 1)
    assert explicit["status"] == "degraded" and explicit["success_rate"] == 0.75
    assert explicit["cumulative_ingested"] == 35


def test_two_explicit_runs_seconds_apart_are_two_runs(empty_api, empty_conn):
    """The gap rule would have merged these. A run_id is not a guess."""
    first = completed_run(empty_conn, "1", accepted=("a",), rejected=())
    second = completed_run(empty_conn, "2", accepted=("a",), rejected=("d",))

    runs = empty_api.get("/api/runs").json()["runs"]

    assert {r["run_id"] for r in runs} == {str(first), str(second)}
    assert sorted(r["ingested"] for r in runs) == [1, 2]
    assert {r["status"] for r in runs} == {"ok", "degraded"}


def test_a_legacy_row_next_to_an_explicit_run_is_not_merged_into_it(empty_api, empty_conn):
    insert_job(empty_conn, None, "legacy-1")
    run = completed_run(empty_conn, accepted=("a",), rejected=())
    insert_job(empty_conn, None, "legacy-2")

    runs = empty_api.get("/api/runs").json()["runs"]

    by_source = {r["run_source"]: r for r in runs}
    assert len(runs) == 2
    # The two legacy rows are within the gap of each other and stay one run.
    assert by_source["legacy"]["ingested"] == 2 and by_source["legacy"]["run_id"] is None
    assert by_source["explicit"]["ingested"] == 1 and by_source["explicit"]["run_id"] == str(run)


def test_a_legacy_run_is_never_given_something_that_parses_as_a_uuid(api):
    for row in api.get("/api/runs").json()["runs"]:
        assert row["run_source"] == "legacy" and row["run_id"] is None


def test_an_explicit_run_that_wrote_nothing_is_not_in_api_runs(empty_api, empty_conn):
    """Unchanged definition: this endpoint is built from observed rows."""
    completed_run(empty_conn, accepted=(), rejected=(), fetched=0)
    assert empty_api.get("/api/runs").json()["runs"] == []
    assert empty_api.get("/api/pipeline-runs").json()["total"] == 1


def test_summary_on_legacy_data_matches_the_committed_query(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_SUMMARY, {"run_gap": "2 minutes"})[0]

    body = api.get("/api/summary").json()

    assert set(body) == {"generated_at", "run_count", "row_count", "totals", "last_run",
                         "last_successful_run_at", "seconds_since_last_successful_run"}
    assert body["run_count"] == before["run_count"]
    assert body["totals"] == {
        "ingested": before["total_ingested"], "validated": before["total_validated"],
        "rejected": before["total_rejected"], "success_rate": before["overall_success_rate"],
    }
    assert body["last_run"] == {
        "run_started_at": before["last_run_started_at"],
        "ingested": before["last_run_ingested"], "validated": before["last_run_validated"],
        "rejected": before["last_run_rejected"],
        "success_rate": before["last_run_success_rate"], "status": before["last_run_status"],
    }
    assert body["last_successful_run_at"] == before["last_successful_run_at"]


def test_summary_counts_an_explicit_run_once(api, conn):
    before = api.get("/api/summary").json()
    completed_run(conn, accepted=("a", "b"), rejected=("d",))
    after = api.get("/api/summary").json()
    assert after["run_count"] == before["run_count"] + 1
    assert after["totals"]["ingested"] == before["totals"]["ingested"] + 3
    assert (after["last_run"]["ingested"], after["last_run"]["status"]) == (3, "degraded")


def test_summary_on_an_empty_warehouse_is_still_one_zero_row(empty_api):
    body = empty_api.get("/api/summary").json()
    assert body["run_count"] == 0 and body["totals"]["ingested"] == 0
    assert body["last_run"]["status"] is None


def test_model_accuracy_by_run_on_legacy_data_matches_the_committed_query(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_MODEL_ACCURACY_BY_RUN, {"run_gap": "2 minutes"})
    assert api.get("/api/model-accuracy").json()["by_run"] == before


def test_health_keeps_its_old_keys_and_adds_the_schema_check(api):
    body = api.get("/api/health").json()
    assert set(body) == {"database", "checked_at", "schema"} and body["database"] == "up"
    assert body["schema"] == {"version": "0009", "required": "0009", "compatible": True}


# ---------------------------------------------------------------------------
# /api/error-classes
# ---------------------------------------------------------------------------


def test_historical_rejections_keep_their_categories_and_counts(api, migrated_db):
    before = run_sql(migrated_db, committed.SQL_ERROR_CLASSES, {})

    body = api.get("/api/error-classes").json()

    assert body["classes"] == [
        {k: r[k] for k in ("code", "category", "rows", "occurrences", "pct_of_rejected_rows")}
        for r in before
    ]
    assert body["total_rejected_rows"] == 6
    assert "validator_internal" not in {c["category"] for c in body["classes"]}


def test_validator_exception_is_its_own_category_not_schema(api, conn, migrated_db):
    run = start(conn, "100")
    conn.execute(
        "INSERT INTO public.dead_letter_log "
        "(source_job_id, ingested_at, error_codes, errors_json, run_id) VALUES "
        "('boom', now(), '[\"VALIDATOR_EXCEPTION\"]', "
        "'[{\"field\": \"validator\", \"code\": \"VALIDATOR_EXCEPTION\", \"message\": \"x\"}]', %s)",
        (run,),
    )

    classes = api.get("/api/error-classes").json()["classes"]
    committed_view = run_sql(migrated_db, committed.SQL_ERROR_CLASSES, {})

    mine = [c for c in classes if c["code"] == "VALIDATOR_EXCEPTION"]
    assert mine == [{"code": "VALIDATOR_EXCEPTION", "category": "validator_internal",
                     "rows": 1, "occurrences": 1, "pct_of_rejected_rows": round(1 / 7, 4)}]
    # What the committed query would have said about the same row.
    assert [r["category"] for r in committed_view if r["code"] == "VALIDATOR_EXCEPTION"] == ["schema"]
    # Every other class is counted exactly as before.
    others_now = {(c["code"], c["category"], c["rows"]) for c in classes if c["code"] != "VALIDATOR_EXCEPTION"}
    others_then = {(r["code"], r["category"], r["rows"]) for r in committed_view if r["code"] != "VALIDATOR_EXCEPTION"}
    assert others_now == others_then


# ---------------------------------------------------------------------------
# The two copies of each query
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["SQL_RUNS", "SQL_SUMMARY", "SQL_MODEL_ACCURACY_BY_RUN", "SQL_PIPELINE_RUNS",
     "SQL_PIPELINE_RUNS_TOTAL", "SQL_PIPELINE_ERRORS", "SQL_PIPELINE_ERRORS_TOTAL",
     "SQL_PIPELINE_STATUS_RUNS", "SQL_PIPELINE_STATUS_FACTS"],
)
def test_queries_sql_holds_the_text_the_api_runs(name):
    """sql/queries.sql is the readable copy. Whitespace aside, it must contain
    exactly what api/main.py executes, or the documentation is describing a
    different query."""
    readable = " ".join((REPO / "sql" / "queries.sql").read_text(encoding="utf-8").split())
    assert " ".join(getattr(main, name).split()) in readable


# ---------------------------------------------------------------------------
# Security: what a client is told when something breaks
# ---------------------------------------------------------------------------

EVERY_ENDPOINT = ["/api/runs", "/api/summary", "/api/error-classes", "/api/model-accuracy",
                  "/api/pipeline-runs", "/api/pipeline-errors", "/api/pipeline-status", "/api/health"]
SAFE_DETAILS = set(main.PUBLIC_DETAIL.values())


def test_an_unreachable_database_tells_the_client_nothing_about_itself(monkeypatch, caplog):
    """The driver's message names the host, port and user. It goes to the log."""
    monkeypatch.setattr(main, "DB_TIMEOUT", 1.0)
    monkeypatch.setattr(main, "PROBE_TIMEOUT", 1.0)
    client = _client(
        "host=127.0.0.1 port=1 dbname=secretdb user=secretuser "
        "password=TopSecretPw123 connect_timeout=1",
        monkeypatch,
    )
    caplog.set_level("WARNING", logger="roleradar.api")
    try:
        for path in EVERY_ENDPOINT:
            response = client.get(path)
            body = response.json()
            assert response.status_code == 503, path
            assert body["reason"] == "unreachable" and body["detail"] in SAFE_DETAILS
            for private in ("127.0.0.1", "secretdb", "secretuser", "TopSecretPw123", "psycopg", "port 1"):
                assert private not in response.text, (path, private)
    finally:
        _shutdown()
    # The log has the diagnosis the client was not given, minus the password.
    assert "reason=unreachable" in caplog.text and "127.0.0.1" in caplog.text
    assert "TopSecretPw123" not in caplog.text


def test_a_database_behind_the_api_is_named_as_that_and_no_more(legacy_db, monkeypatch, caplog):
    """The deployment hazard: this API against a database with no migrations."""
    client = _client(legacy_db, monkeypatch)
    caplog.set_level("WARNING", logger="roleradar.api")
    try:
        for path in ("/api/runs", "/api/summary", "/api/model-accuracy",
                     "/api/pipeline-runs", "/api/pipeline-errors", "/api/pipeline-status"):
            response = client.get(path)
            assert response.status_code == 503, path
            assert response.json()["reason"] == "schema_outdated"
            assert response.json()["detail"] == main.PUBLIC_DETAIL["schema_outdated"]
            for private in ("run_id", "pipeline_runs", "column", "relation", "LINE"):
                assert private not in response.text, (path, private)
        health = client.get("/api/health")
        assert health.status_code == 200
        assert health.json()["schema"] == {"version": None, "required": "0009", "compatible": False}
    finally:
        _shutdown()
    assert "reason=schema_outdated" in caplog.text and "run_id" in caplog.text


def test_health_reports_a_partly_migrated_database_as_incompatible(legacy_db, monkeypatch):
    migrate.up(legacy_db, to="0008", log=lambda _line: None)
    client = _client(legacy_db, monkeypatch)
    try:
        assert client.get("/api/health").json()["schema"] == {
            "version": "0008", "required": "0009", "compatible": False}
    finally:
        _shutdown()


def test_log_lines_are_redacted_before_they_are_written(caplog):
    caplog.set_level("WARNING", logger="roleradar.api")
    main._log_database_error(
        "unreachable",
        RuntimeError("connection to postgresql://owner:npg_FAKEnotARealToken0@ep-x.neon.tech/db failed"),
    )
    assert "npg_FAKEnotARealToken0" not in caplog.text and "owner:" not in caplog.text
    assert "ep-x.neon.tech" in caplog.text and "RuntimeError" in caplog.text


# ---------------------------------------------------------------------------
# Security: the operator token
# ---------------------------------------------------------------------------

DIAGNOSTIC_PATHS = ["/api/pipeline-runs", "/api/pipeline-errors"]


@pytest.mark.parametrize("path", DIAGNOSTIC_PATHS)
def test_a_wrong_token_is_refused_not_quietly_downgraded(empty_api, empty_conn, path):
    start(empty_conn, "100")
    record(empty_conn, "100", message="the secret diagnosis")
    for wrong in ("nope", OPS_TOKEN[:-1], OPS_TOKEN + "x", "", " "):
        response = empty_api.get(path, headers={"X-Ops-Token": wrong})
        assert response.status_code == 401, repr(wrong)
        assert response.json()["error"] == "invalid_ops_token"
        assert "the secret diagnosis" not in response.text and "runs" not in response.json()


@pytest.mark.parametrize("configured", [None, "", "short", "x" * 23])
def test_without_a_strong_configured_token_there_is_no_operator_mode(
        empty_api, empty_conn, monkeypatch, configured):
    """Unset, empty or guessable: nobody is the operator, whatever they send."""
    if configured is None:
        monkeypatch.delenv("OPS_TOKEN", raising=False)
    else:
        monkeypatch.setenv("OPS_TOKEN", configured)
    record(empty_conn, "100", message="the secret diagnosis")
    for sent in (configured or "", "anything", OPS_TOKEN):
        response = empty_api.get("/api/pipeline-errors", headers={"X-Ops-Token": sent})
        assert response.status_code == 401
        assert "the secret diagnosis" not in response.text
    # The public view is unaffected.
    public = empty_api.get("/api/pipeline-errors")
    assert public.status_code == 200 and public.json()["errors"][0]["message"] is None


@pytest.mark.parametrize("path", DIAGNOSTIC_PATHS)
def test_operator_responses_are_marked_not_to_be_cached(empty_api, path):
    assert empty_api.get(path, headers=OPERATOR).headers["cache-control"] == "no-store"
    assert "cache-control" not in empty_api.get(path).headers


def test_the_token_is_not_accepted_as_a_query_parameter(empty_api, empty_conn):
    """A token in a URL ends up in logs and browser history."""
    record(empty_conn, "100", message="the secret diagnosis")
    for name in ("ops_token", "x_ops_token", "X-Ops-Token", "token"):
        body = empty_api.get(f"/api/pipeline-errors?{name}={OPS_TOKEN}").json()
        assert body["detail_level"] == "public" and body["errors"][0]["message"] is None


def test_pipeline_status_is_the_public_view_even_for_the_operator(empty_api, empty_conn):
    start(empty_conn, "777")
    record(empty_conn, "777", message="the secret diagnosis")
    for headers in ({}, OPERATOR):
        text = empty_api.get("/api/pipeline-status", headers=headers).text
        assert "the secret diagnosis" not in text and '"777"' not in text


def test_public_responses_carry_no_stored_diagnostic_text(empty_api, empty_conn):
    """Everything the error workflow stores, searched for in everything public."""
    start(empty_conn, "424242")
    record(empty_conn, "424242", workflow_name="Private Workflow Name",
           message="UNIQUE-DIAGNOSTIC-MARKER at step seven")
    for path in EVERY_ENDPOINT:
        text = empty_api.get(path).text
        for private in ("UNIQUE-DIAGNOSTIC-MARKER", "Private Workflow Name", "424242"):
            assert private not in text, (path, private)


# ---------------------------------------------------------------------------
# Security: predictable shapes and bounded input
# ---------------------------------------------------------------------------

STATUS_KEYS = {"generated_at", "pipeline_status", "reasons", "last_attempted_run",
               "last_successful_run", "last_failed_run", "running", "recent_failures",
               "freshness", "totals", "thresholds"}


def shape(value):
    """The key structure of a response, ignoring the values."""
    if isinstance(value, dict):
        return {k: shape(v) for k, v in sorted(value.items())}
    return None


def test_the_status_response_has_the_same_keys_in_every_state(empty_api, empty_conn):
    empty = status_of(empty_api)
    start(empty_conn, "1")
    record(empty_conn, "1")
    completed_run(empty_conn, "2")
    busy = status_of(empty_api)

    for body in (empty, busy):
        assert set(body) == STATUS_KEYS
        for section in ("running", "freshness", "totals", "thresholds"):
            assert shape(body[section]) == shape(empty[section]), section
        assert set(body["recent_failures"]) == set(empty["recent_failures"])
        assert body["pipeline_status"] in {"healthy", "degraded", "failed", "no_data"}
    assert set(busy["last_failed_run"]) == set(busy["last_successful_run"]) == RUN_KEYS


def test_the_status_response_has_those_keys_on_legacy_only_data_too(api):
    body = status_of(api)
    assert set(body) == STATUS_KEYS
    assert set(body["freshness"]) == {
        "evidence", "expected_interval_hours", "grace_minutes",
        "last_successful_run_completed_at", "seconds_since_last_successful_run",
        "overdue", "last_legacy_data_at", "seconds_since_last_legacy_data"}


@pytest.mark.parametrize("path, keys", [
    ("/api/pipeline-runs", {"generated_at", "limit", "offset", "total", "row_count",
                            "stale_run_minutes", "detail_level", "runs"}),
    ("/api/pipeline-errors", {"generated_at", "limit", "offset", "total", "row_count",
                              "detail_level", "errors"}),
])
def test_list_endpoints_have_the_same_envelope_empty_or_not(empty_api, empty_conn, path, keys):
    before = empty_api.get(path).json()
    start(empty_conn, "1")
    record(empty_conn, "1")
    after = empty_api.get(path).json()
    assert set(before) == set(after) == keys
    assert before["row_count"] == 0 and after["row_count"] == 1


@pytest.mark.parametrize("path, cap", [
    ("/api/pipeline-runs", 200), ("/api/pipeline-errors", 200), ("/api/runs", 500)])
def test_page_size_is_capped(empty_api, path, cap):
    assert empty_api.get(f"{path}?limit={cap}").status_code == 200
    assert empty_api.get(f"{path}?limit={cap + 1}").status_code == 422
    assert empty_api.get(f"{path}?limit=99999999999999999999").status_code == 422


@pytest.mark.parametrize("path", DIAGNOSTIC_PATHS)
def test_offset_is_bounded(empty_api, path):
    assert empty_api.get(f"{path}?offset=100000").status_code == 200
    assert empty_api.get(f"{path}?offset=100001").status_code == 422


def test_no_sql_is_assembled_from_anything_but_constants():
    """Every statement run_query executes is a module constant or a literal,
    and every SQL constant is built only from other constants. Values reach
    Postgres as parameters, never as text."""
    import ast

    tree = ast.parse((REPO / "api" / "main.py").read_text(encoding="utf-8"))

    def constant_only(node):
        if isinstance(node, ast.Constant):
            return isinstance(node.value, str)
        if isinstance(node, ast.Name):
            return node.id.isupper() or node.id.startswith("_")
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return constant_only(node.left) and constant_only(node.right)
        return False  # f-strings, .format(), %, calls

    sql_constants = 0
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name.startswith("SQL_") or name in ("_RUNS_PREAMBLE", "_RUN_COLUMNS"):
                sql_constants += 1
                assert constant_only(node.value), f"{name} is not built from constants"
    assert sql_constants >= 12

    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "run_query"]
    assert len(calls) >= 12
    for call in calls:
        first = call.args[0]
        assert isinstance(first, (ast.Name, ast.Constant)), ast.dump(first)
        if isinstance(first, ast.Name):
            assert first.id.startswith("SQL_"), first.id
    # And nothing else in the file talks to a cursor directly.
    executes = [n for n in ast.walk(tree)
                if isinstance(n, ast.Attribute) and n.attr == "execute"]
    assert len(executes) == 1


def test_a_late_row_after_completion_is_visible_as_a_mismatch(empty_api, empty_conn):
    """Nothing forbids the insert. The stored verdicts then disagree with the rows."""
    run = completed_run(empty_conn, accepted=("a",), rejected=())
    insert_job(empty_conn, run, "late")

    got = empty_api.get("/api/pipeline-runs").json()["runs"][0]
    assert got["status"] == "completed"
    assert (got["accepted_count"], got["accepted_rows"]) == (1, 2)
    assert got["rows_match_counts"] is False
    body = status_of(empty_api)
    assert body["pipeline_status"] == "degraded" and body["reasons"] == ["rows_do_not_match_counts"]


# ---------------------------------------------------------------------------
# The API is a reader, and the tests cannot reach a real database
# ---------------------------------------------------------------------------


def test_the_api_cannot_write_even_if_asked_to(api, conn):
    """Every pooled transaction is READ ONLY. A preview deployment handed the
    production DATABASE_URL can read it and cannot change it."""
    api.get("/api/health")  # opens the pool against the test database
    with pytest.raises(main.DatabaseUnavailable) as raised:
        main.run_query("INSERT INTO public.pipeline_errors (error_message) "
                       "VALUES ('written by the API') RETURNING id")
    assert raised.value.reason == "query_failed"
    assert main.run_query("SELECT current_setting('transaction_read_only') AS ro")[0]["ro"] == "on"
    assert conn.execute(
        "SELECT count(*) FROM public.pipeline_errors WHERE error_message = 'written by the API'"
    ).fetchone()[0] == 0


def test_every_sql_constant_only_reads():
    for name in dir(main):
        if name.startswith("SQL_"):
            text = " ".join(getattr(main, name).split()).upper()
            assert text.startswith(("SELECT", "WITH", "(SELECT")), name
            for verb in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE", "DROP ", "ALTER ", "CREATE "):
                assert verb not in text, (name, verb)


def test_no_test_can_reach_a_real_database_by_accident():
    """conftest strips these before any test module is imported."""
    import os

    for name in ("NEON_DATABASE_URL", "MIGRATE_DATABASE_URL", "POSTGRES_HOST", "POSTGRES_PASSWORD"):
        assert name not in os.environ, name
    assert "port=1 " in os.environ["DATABASE_URL"]
