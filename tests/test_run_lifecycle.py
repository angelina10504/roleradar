"""How a run ends: completed by the workflow, or failed by the error workflow.

These tests call the two functions from migration 0009 through the SQL
statements written in docs/n8n-notes.md, with the parameters the canvas
expressions would produce. Rows are synthetic and written directly.

What this proves: the database rules. What it does not prove: that n8n calls
the Error Trigger workflow, or that the canvas expressions produce these
parameters. Every "error" below is a row this file wrote by calling the
function. None of them came from n8n.
"""

from __future__ import annotations

import json

import psycopg
import pytest
from psycopg import errors

from test_migrations import insert_dead, insert_job
from test_validator_records import execute_query_node

WF = "wf-ingest"


def start(conn, execution_id="100", workflow_id=WF):
    return execute_query_node(conn, "start-run", [execution_id, workflow_id])[0][0]


SAME = object()


def close(conn, run_id, fetched, accepted, rejected, submitted=SAME):
    """The Complete Run node. Returns the one row as a dict.

    submitted defaults to the number of verdicts, which is what a correctly
    wired canvas sends. Pass it to model a canvas that sends something else.
    """
    if submitted is SAME:
        submitted = len(accepted) + len(rejected)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM public.complete_pipeline_run("
            "%s::uuid, %s::integer, %s::integer, %s::jsonb, %s::jsonb)",
            (run_id, fetched, submitted, json.dumps(accepted), json.dumps(rejected)),
        )
        return cur.fetchone()


def record(conn, execution_id="100", workflow_id=WF, retry_of=None,
           node="Claude Extract", node_type="n8n-nodes-base.httpRequest",
           message="Request failed with status code 529", workflow_name="RoleRadar"):
    """The Record Error node, with the statement from the notes."""
    row = execute_query_node(
        conn, "record-error",
        [workflow_id, workflow_name, execution_id, retry_of, node, node_type, message],
    )[0]
    return dict(zip(("error_id", "run_id", "category", "run_marked_failed", "duplicate"), row))


def run_row(conn, run_id):
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM public.pipeline_runs WHERE run_id = %s", (run_id,))
        return cur.fetchone()


def error_count(conn):
    return conn.execute("SELECT count(*) FROM public.pipeline_errors").fetchone()[0]


def test_the_notes_statement_is_the_function_call(conn):
    """Complete Run in the notes and close() here are the same call."""
    run = start(conn)
    insert_job(conn, run, "a")
    returned = execute_query_node(conn, "complete-run", [str(run), 3, 1, '["a"]', "[]"])
    assert returned[0][0] == run and returned[0][1] == "completed"


# ---------------------------------------------------------------------------
# Completing
# ---------------------------------------------------------------------------


def test_a_fully_written_run_completes(conn):
    run = start(conn)
    for job in ("a", "b", "c"):
        insert_job(conn, run, job)
    insert_dead(conn, run, "d")

    closed = close(conn, run, 13, ["a", "b", "c"], ["d"])

    assert closed["status"] == "completed" and closed["already_closed"] is False
    assert (closed["accepted_count"], closed["rejected_count"]) == (3, 1)
    assert (closed["accepted_rows"], closed["rejected_rows"]) == (3, 1)
    stored = run_row(conn, run)
    assert (stored["fetched_count"], stored["processed_count"]) == (13, 4)
    assert stored["completed_at"] >= stored["started_at"]


def test_a_run_is_not_completed_before_its_accepted_rows_exist(conn):
    """Complete Run placed above the inserts on the canvas runs first."""
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="not fully written. 2 of 2 accepted"):
        close(conn, run, 13, ["a", "b"], [])
    assert run_row(conn, run)["status"] == "running"


def test_a_run_is_not_completed_when_one_insert_failed(conn):
    """Two of three accepted rows landed, then the insert node died."""
    run = start(conn)
    insert_job(conn, run, "a")
    insert_job(conn, run, "b")
    with pytest.raises(errors.RaiseException, match="1 of 3 accepted"):
        close(conn, run, 13, ["a", "b", "c"], [])
    stored = run_row(conn, run)
    assert stored["status"] == "running" and stored["accepted_count"] is None


def test_a_run_is_not_completed_before_its_rejected_rows_exist(conn):
    run = start(conn)
    insert_job(conn, run, "a")
    with pytest.raises(errors.RaiseException, match="1 of 1 rejected"):
        close(conn, run, 13, ["a"], ["d"])
    assert run_row(conn, run)["status"] == "running"


def test_rows_from_another_run_do_not_satisfy_the_check(conn):
    other = start(conn, "99")
    insert_job(conn, other, "a")
    run = start(conn, "100")
    with pytest.raises(errors.RaiseException, match="1 of 1 accepted"):
        close(conn, run, 13, ["a"], [])


def test_rejected_records_without_an_id_are_checked_by_number(conn):
    run = start(conn)
    insert_dead(conn, run, None)
    with pytest.raises(errors.RaiseException, match="1 of 2 rejected"):
        close(conn, run, 13, [], [None, None])
    insert_dead(conn, run, None)
    assert close(conn, run, 13, [], [None, None])["status"] == "completed"


def test_zero_accepted_records_completes(conn):
    run = start(conn)
    insert_dead(conn, run, "d")
    closed = close(conn, run, 13, [], ["d"])
    assert (closed["status"], closed["accepted_count"], closed["rejected_count"]) == ("completed", 0, 1)


def test_zero_rejected_records_completes(conn):
    run = start(conn)
    insert_job(conn, run, "a")
    closed = close(conn, run, 13, ["a"], [])
    assert (closed["status"], closed["accepted_count"], closed["rejected_count"]) == ("completed", 1, 0)


def test_an_empty_source_response_completes_with_zero_fetched(conn):
    run = start(conn)
    close(conn, run, 0, [], [])
    stored = run_row(conn, run)
    assert (stored["status"], stored["fetched_count"], stored["processed_count"]) == ("completed", 0, 0)


def test_replayed_inserts_do_not_inflate_anything(conn):
    run = start(conn)
    for _ in range(3):
        insert_job(conn, run, "a", "ON CONFLICT DO NOTHING")
        insert_dead(conn, run, "d", "ON CONFLICT DO NOTHING")
    closed = close(conn, run, 13, ["a"], ["d"])
    assert (closed["accepted_count"], closed["accepted_rows"]) == (1, 1)
    assert (closed["rejected_count"], closed["rejected_rows"]) == (1, 1)


def test_completing_twice_changes_nothing(conn):
    run = start(conn)
    insert_job(conn, run, "a")
    first = close(conn, run, 13, ["a"], [])
    second = close(conn, run, 99, ["a", "zzz"], ["nope"])
    assert second["already_closed"] is True
    assert second["completed_at"] == first["completed_at"]
    assert run_row(conn, run)["fetched_count"] == 13


def test_the_same_posting_twice_in_one_response_still_completes(conn):
    """Verdicts say two, the unique index keeps one row. Both are reported."""
    run = start(conn)
    insert_job(conn, run, "a", "ON CONFLICT DO NOTHING")
    insert_job(conn, run, "a", "ON CONFLICT DO NOTHING")
    closed = close(conn, run, 13, ["a", "a"], [])
    assert (closed["accepted_count"], closed["accepted_rows"]) == (2, 1)


def test_completing_one_run_leaves_others_alone(conn):
    mine, other = start(conn, "100"), start(conn, "101")
    close(conn, mine, 0, [], [])
    assert run_row(conn, other)["status"] == "running"


def test_bad_arguments_are_refused(conn):
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="no run"):
        close(conn, "00000000-0000-4000-8000-000000000000", 0, [], [])
    with pytest.raises(errors.RaiseException, match="must be JSON arrays"):
        close(conn, run, 0, {"not": "a list"}, [])
    assert run_row(conn, run)["status"] == "running"


# ---------------------------------------------------------------------------
# Failing
# ---------------------------------------------------------------------------


def test_an_error_marks_its_running_run_failed(conn):
    run = start(conn, "100")
    insert_job(conn, run, "a")

    result = record(conn, "100")

    assert result["run_id"] == run and result["run_marked_failed"] is True
    stored = run_row(conn, run)
    assert stored["status"] == "failed" and stored["completed_at"] is not None
    assert stored["error_category"] == "upstream_llm"
    assert "529" in stored["error_message"]
    # Counts stay unknown: the run never reported them.
    assert stored["processed_count"] is None
    linked = conn.execute(
        "SELECT run_id, workflow_id, workflow_name, execution_id, failed_node, error_category "
        "FROM public.pipeline_errors WHERE id = %s", (result["error_id"],)
    ).fetchone()
    assert linked == (run, WF, "RoleRadar", "100", "Claude Extract", "upstream_llm")


def test_a_failed_run_cannot_then_be_completed_without_its_rows(conn):
    run = start(conn, "100")
    record(conn, "100")
    with pytest.raises(errors.RaiseException, match="not fully written"):
        close(conn, run, 13, ["a"], [])
    assert run_row(conn, run)["status"] == "failed"


def test_a_failure_before_the_run_row_exists_is_recorded_without_a_run(conn):
    """Start Run itself failed: there is nothing to correlate with."""
    bystander = start(conn, "55")
    result = record(conn, "100", node="Start Run", node_type="n8n-nodes-base.postgres",
                    message="connection refused")
    assert result["run_id"] is None and result["run_marked_failed"] is False
    assert result["category"] == "database"
    assert error_count(conn) == 1
    assert run_row(conn, bystander)["status"] == "running"


def test_a_duplicate_notification_writes_one_row(conn):
    run = start(conn, "100")
    first = record(conn, "100")
    second = record(conn, "100")
    assert second["duplicate"] is True and second["error_id"] == first["error_id"]
    assert second["run_id"] == run
    assert error_count(conn) == 1


def test_a_different_error_on_the_same_execution_is_not_a_duplicate(conn):
    start(conn, "100")
    record(conn, "100", message="first")
    assert record(conn, "100", message="second")["duplicate"] is False
    assert error_count(conn) == 2


def test_a_missing_execution_id_is_recorded_and_touches_no_run(conn):
    run = start(conn, None)
    result = record(conn, None, node=None, node_type=None, message="trigger blew up")
    assert result["run_id"] is None and result["category"] == "trigger"
    assert run_row(conn, run)["status"] == "running"
    # Never deduplicated: two trigger failures are two failures.
    assert record(conn, None, node=None, node_type=None, message="trigger blew up")["duplicate"] is False
    assert error_count(conn) == 2


def test_an_empty_string_execution_id_is_treated_as_missing(conn):
    run = start(conn, "")
    assert record(conn, "  ")["run_id"] is None
    assert run_row(conn, run)["status"] == "running"


def test_two_workflows_with_the_same_execution_id_do_not_cross(conn):
    ingest = start(conn, "7", "wf-ingest")
    other = start(conn, "7", "wf-other")

    result = record(conn, "7", workflow_id="wf-other")

    assert result["run_id"] == other
    assert run_row(conn, other)["status"] == "failed"
    assert run_row(conn, ingest)["status"] == "running"


def test_a_recycled_execution_id_does_not_reach_an_old_run(conn):
    """n8n was rebuilt and is counting from 1 again."""
    old = start(conn, "7")
    conn.execute(
        "UPDATE public.pipeline_runs SET started_at = now() - interval '30 days' WHERE run_id = %s",
        (old,),
    )
    result = record(conn, "7", node="Start Run", node_type="n8n-nodes-base.postgres")
    assert result["run_id"] is None
    assert run_row(conn, old)["status"] == "running"


def test_of_two_runs_with_one_execution_id_the_recent_one_fails(conn):
    old = start(conn, "7")
    conn.execute(
        "UPDATE public.pipeline_runs SET started_at = now() - interval '2 hours' WHERE run_id = %s",
        (old,),
    )
    new = start(conn, "7")
    assert record(conn, "7")["run_id"] == new
    assert run_row(conn, old)["status"] == "running"


def test_an_error_never_reopens_or_rewrites_a_completed_run(conn):
    run = start(conn, "100")
    close(conn, run, 0, [], [])
    before = run_row(conn, run)

    result = record(conn, "100")

    assert result["run_id"] == run and result["run_marked_failed"] is False
    assert run_row(conn, run) == before


def test_a_run_without_a_workflow_id_still_matches_on_execution_id(conn):
    """Rows written by the Milestone 2 version of Start Run."""
    run = conn.execute(
        "INSERT INTO public.pipeline_runs (n8n_execution_id) VALUES ('100') RETURNING run_id"
    ).fetchone()[0]
    assert record(conn, "100")["run_id"] == run


def test_a_retry_that_fails_is_tied_to_the_original_run(conn):
    run = start(conn, "100")
    record(conn, "100", message="first attempt")
    again = record(conn, "101", retry_of="100", message="retry also failed")
    assert again["run_id"] == run and again["run_marked_failed"] is False
    assert error_count(conn) == 2


def test_a_retry_that_writes_everything_completes_the_failed_run(conn):
    run = start(conn, "100")
    record(conn, "100")
    insert_job(conn, run, "a")

    closed = close(conn, run, 13, ["a"], [])

    assert closed["status"] == "completed"
    stored = run_row(conn, run)
    # The earlier failure is still on record in both places.
    assert stored["error_category"] == "upstream_llm"
    assert conn.execute(
        "SELECT count(*) FROM public.pipeline_errors WHERE run_id = %s", (run,)
    ).fetchone()[0] == 1


@pytest.mark.parametrize(
    "node, node_type, expected",
    [
        ("Insert rows in a table", "n8n-nodes-base.postgres", "database"),
        ("Complete Run", "n8n-nodes-base.postgres", "database"),
        ("HTTP Request", "n8n-nodes-base.httpRequest", "upstream_source"),
        ("Claude Extract", "n8n-nodes-base.httpRequest", "upstream_llm"),
        ("Validator", "n8n-nodes-base.code", "pipeline_code"),
        ("Schedule Trigger", "n8n-nodes-base.scheduleTrigger", "trigger"),
        ("Stop and Error", "n8n-nodes-base.stopAndError", "unknown"),
        ("Claude Extract", None, "unknown"),
    ],
)
def test_category_comes_from_the_node_type(conn, node, node_type, expected):
    start(conn, "100")
    assert record(conn, "100", node=node, node_type=node_type)["category"] == expected


def test_a_long_message_is_cut_and_a_missing_one_is_named(conn):
    long = record(conn, "1", message="x" * 10000)
    none = record(conn, "2", message=None)
    stored = dict(conn.execute("SELECT id, error_message FROM public.pipeline_errors").fetchall())
    assert len(stored[long["error_id"]]) == 4000
    assert stored[none["error_id"]] == "(no message)"


def test_the_functions_are_removed_by_the_down_migration(migrated_db):
    import migrate

    migrate.down(migrated_db, to="0008", log=lambda _line: None)
    with psycopg.connect(migrated_db) as c:
        assert c.execute(
            "SELECT count(*) FROM pg_proc WHERE proname IN "
            "('complete_pipeline_run', 'record_pipeline_error')"
        ).fetchone()[0] == 0
        # 0008's columns are still there; only 0009 was reverted.
        assert c.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 'pipeline_runs' AND column_name = 'n8n_workflow_id'"
        ).fetchone()[0] == 1


# ---------------------------------------------------------------------------
# False completion: what the function is told against what is true
#
# The function only knows what the canvas sends. These model a canvas that
# sends the wrong thing, and require a refusal each time.
# ---------------------------------------------------------------------------


def test_closing_before_the_pipeline_ran_is_refused(conn):
    """The hole in the first design. Complete Run drawn above everything runs
    first; the canvas expressions then produce no fetched count and two empty
    lists. That used to close the run as complete with nothing written."""
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="closed too early"):
        close(conn, run, None, [], [], submitted=None)
    assert run_row(conn, run)["status"] == "running"
    # The rows then arrive, as they would. The run is still open to be closed properly.
    insert_job(conn, run, "a")
    assert close(conn, run, 13, ["a"], [])["status"] == "completed"


def test_closing_after_the_fetch_but_before_the_validator_is_refused(conn):
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="fetched 13 postings and submitted none"):
        close(conn, run, 13, [], [], submitted=0)
    with pytest.raises(errors.RaiseException, match="0 verdicts for 5 submitted"):
        close(conn, run, 13, [], [], submitted=5)
    assert run_row(conn, run)["status"] == "running"


def test_a_missing_submitted_count_is_refused(conn):
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="closed too early"):
        close(conn, run, 13, [], [], submitted=None)


def test_items_that_never_reached_the_validator_are_noticed(conn):
    """Five were sent to the model. Verdicts came back for three."""
    run = start(conn)
    for job in ("a", "b", "c"):
        insert_job(conn, run, job)
    with pytest.raises(errors.RaiseException, match="3 verdicts for 5 submitted"):
        close(conn, run, 13, ["a", "b", "c"], [], submitted=5)
    assert run_row(conn, run)["status"] == "running"


def test_empty_lists_sent_by_mistake_after_rows_were_written_are_refused(conn):
    """Every count says zero, and the tables say otherwise."""
    run = start(conn)
    insert_job(conn, run, "a")
    insert_dead(conn, run, "d")
    with pytest.raises(errors.RaiseException, match="2 rows the validator did not report"):
        close(conn, run, 0, [], [], submitted=0)
    assert run_row(conn, run)["status"] == "running"


def test_an_incomplete_id_list_is_refused_even_when_its_own_count_agrees(conn):
    """A failed insert hidden by a list that leaves the failed record out."""
    run = start(conn)
    for job in ("a", "b", "c"):
        insert_job(conn, run, job)
    with pytest.raises(errors.RaiseException, match="1 rows the validator did not report"):
        close(conn, run, 13, ["a", "b"], [], submitted=2)
    with pytest.raises(errors.RaiseException, match="2 verdicts for 3 submitted"):
        close(conn, run, 13, ["a", "b"], [], submitted=3)


def test_a_row_in_the_wrong_table_is_refused(conn):
    """Accepted by the validator, written to the dead-letter table."""
    run = start(conn)
    insert_dead(conn, run, "a")
    with pytest.raises(errors.RaiseException, match="accepted records have no row"):
        close(conn, run, 13, ["a"], [])


def test_impossible_counts_are_refused(conn):
    run = start(conn)
    with pytest.raises(errors.RaiseException, match="submitted 5 items but only 3 were fetched"):
        close(conn, run, 3, [], [], submitted=5)
    with pytest.raises(errors.RaiseException, match="cannot be negative"):
        close(conn, run, -1, [], [], submitted=0)


def test_several_rejected_records_without_ids_must_all_be_there(conn):
    run = start(conn)
    insert_dead(conn, run, "d")
    insert_dead(conn, run, None)
    insert_dead(conn, run, None)
    with pytest.raises(errors.RaiseException, match="1 of 4 rejected"):
        close(conn, run, 13, [], ["d", None, None, None])
    insert_dead(conn, run, None)
    closed = close(conn, run, 13, [], ["d", None, None, None])
    assert (closed["status"], closed["rejected_count"], closed["rejected_rows"]) == ("completed", 4, 4)


def test_a_replayed_row_without_an_id_cannot_stand_in_for_a_missing_one(conn):
    """The unique index cannot see a NULL id, so a replay writes it again.
    One record written twice must not pass for two records written once,
    and must not pass unnoticed either."""
    run = start(conn)
    insert_dead(conn, run, None, "ON CONFLICT DO NOTHING")
    insert_dead(conn, run, None, "ON CONFLICT DO NOTHING")
    with pytest.raises(errors.RaiseException, match="1 rows the validator did not report"):
        close(conn, run, 13, [], [None])
    assert run_row(conn, run)["status"] == "running"


def test_ids_sent_as_json_numbers_match_the_text_column(conn):
    run = start(conn)
    insert_job(conn, run, "2091098")
    insert_dead(conn, run, "2091105")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM public.complete_pipeline_run(%s::uuid, 13, 2, %s::jsonb, %s::jsonb)",
            (run, "[2091098]", "[2091105]"),
        )
        assert cur.fetchone()[0] == "completed"


def test_a_late_insert_does_not_reopen_or_recount_a_completed_run(conn):
    """Nothing forbids it. The run's stored verdicts stay as they were, which
    is what lets the API see the disagreement."""
    run = start(conn)
    insert_job(conn, run, "a")
    first = close(conn, run, 13, ["a"], [])
    insert_job(conn, run, "late")

    again = close(conn, run, 13, ["a"], [])

    assert again["already_closed"] is True and again["completed_at"] == first["completed_at"]
    assert (again["accepted_count"], again["accepted_rows"]) == (1, 2)


# ---------------------------------------------------------------------------
# Interruption and recovery
# ---------------------------------------------------------------------------


def test_a_crash_between_the_inserts_and_the_close_leaves_the_run_open(conn):
    """Rows are there. Nothing closes the run on its own, ever."""
    run = start(conn)
    insert_job(conn, run, "a")
    insert_dead(conn, run, "d")
    conn.execute("UPDATE public.pipeline_runs SET started_at = now() - interval '2 days' WHERE run_id = %s", (run,))
    stored = run_row(conn, run)
    assert stored["status"] == "running" and stored["completed_at"] is None
    assert stored["accepted_count"] is None


def test_an_interrupted_run_can_be_finished_by_a_retry(conn):
    """Two of three accepted rows landed, the insert node failed, the error
    workflow marked the run failed. A retry replays the insert node, which
    skips the two that exist and writes the third, then closes the run."""
    run = start(conn, "100")
    insert_job(conn, run, "a")
    insert_job(conn, run, "b")
    record(conn, "100", node="Insert rows in a table", node_type="n8n-nodes-base.postgres",
           message="connection reset")
    assert run_row(conn, run)["status"] == "failed"

    written = [insert_job(conn, run, job, "ON CONFLICT DO NOTHING") for job in ("a", "b", "c")]
    closed = close(conn, run, 13, ["a", "b", "c"], [])

    assert written == [0, 0, 1]
    assert (closed["status"], closed["accepted_count"], closed["accepted_rows"]) == ("completed", 3, 3)
    assert run_row(conn, run)["error_category"] == "database"


def test_a_retry_that_still_cannot_write_leaves_the_run_failed(conn):
    run = start(conn, "100")
    insert_job(conn, run, "a")
    record(conn, "100")
    with pytest.raises(errors.RaiseException, match="1 of 2 accepted"):
        close(conn, run, 13, ["a", "b"], [])
    assert run_row(conn, run)["status"] == "failed"


# ---------------------------------------------------------------------------
# Concurrency. Real connections, real locks.
# ---------------------------------------------------------------------------

import threading  # noqa: E402
import time  # noqa: E402

CLOSE_SQL = ("SELECT already_closed, completed_at FROM public.complete_pipeline_run("
             "%s::uuid, 13, 1, '[\"a\"]'::jsonb, '[]'::jsonb)")


def waiting_on_a_lock(conn, pid):
    return conn.execute(
        "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = %s", (pid,)
    ).fetchone()[0]


def wait_until_blocked(conn, pid, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if waiting_on_a_lock(conn, pid):
            return True
        time.sleep(0.02)
    return False


def test_two_completions_at_once_take_turns_and_one_wins(conn, migrated_db):
    run = start(conn)
    insert_job(conn, run, "a")
    result = {}

    with psycopg.connect(migrated_db) as first, psycopg.connect(migrated_db) as second:
        # First closes inside an open transaction and holds the row lock.
        one = first.execute(CLOSE_SQL, (run,)).fetchone()

        def second_closer():
            result["row"] = second.execute(CLOSE_SQL, (run,)).fetchone()
            second.commit()

        worker = threading.Thread(target=second_closer)
        worker.start()
        assert wait_until_blocked(conn, second.info.backend_pid), "the second closer did not wait"
        assert "row" not in result
        # Uncommitted, so nobody else sees a completed run yet.
        assert run_row(conn, run)["status"] == "running"

        first.commit()
        worker.join(timeout=10)

    assert not worker.is_alive()
    assert one[0] is False and result["row"][0] is True
    assert result["row"][1] == one[1], "the second closer changed completed_at"
    assert run_row(conn, run)["status"] == "completed"


def test_many_completions_at_once_close_the_run_exactly_once(conn, migrated_db):
    run = start(conn)
    insert_job(conn, run, "a")
    outcomes, failures = [], []
    gate = threading.Barrier(8)

    def closer():
        try:
            with psycopg.connect(migrated_db, autocommit=True) as c:
                gate.wait(timeout=10)
                outcomes.append(c.execute(CLOSE_SQL, (run,)).fetchone())
        except Exception as exc:  # noqa: BLE001
            failures.append(exc)

    workers = [threading.Thread(target=closer) for _ in range(8)]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=20)

    assert failures == [] and len(outcomes) == 8
    assert sorted(o[0] for o in outcomes) == [False] + [True] * 7
    assert len({o[1] for o in outcomes}) == 1


def test_a_first_closer_that_rolls_back_leaves_the_run_for_the_second(conn, migrated_db):
    run = start(conn)
    insert_job(conn, run, "a")
    result = {}

    with psycopg.connect(migrated_db) as first, psycopg.connect(migrated_db) as second:
        first.execute(CLOSE_SQL, (run,)).fetchone()

        def second_closer():
            result["row"] = second.execute(CLOSE_SQL, (run,)).fetchone()
            second.commit()

        worker = threading.Thread(target=second_closer)
        worker.start()
        assert wait_until_blocked(conn, second.info.backend_pid)
        first.rollback()
        worker.join(timeout=10)

    assert result["row"][0] is False
    assert run_row(conn, run)["status"] == "completed"


def test_an_error_arriving_during_a_completion_waits_and_then_changes_nothing(conn, migrated_db):
    """Completion holds the lock; the error report queues behind it and finds
    a completed run, which it must leave alone."""
    run = start(conn, "100")
    insert_job(conn, run, "a")
    result = {}
    error_sql = ("SELECT run_marked_failed, run_id FROM public.record_pipeline_error("
                 "'wf-ingest', 'RoleRadar', '100', NULL, 'Complete Run', "
                 "'n8n-nodes-base.postgres', 'timeout')")

    with psycopg.connect(migrated_db) as closer, psycopg.connect(migrated_db) as reporter:
        closer.execute(CLOSE_SQL, (run,)).fetchone()

        def report():
            result["row"] = reporter.execute(error_sql).fetchone()
            reporter.commit()

        worker = threading.Thread(target=report)
        worker.start()
        assert wait_until_blocked(conn, reporter.info.backend_pid)
        closer.commit()
        worker.join(timeout=10)

    assert result["row"] == (False, run)
    assert run_row(conn, run)["status"] == "completed"
    assert error_count(conn) == 1


def test_a_completion_arriving_during_an_error_report_sees_the_failure(conn, migrated_db):
    """The other order. The error wins the lock; the completion then finds a
    failed run with every row present and closes it, keeping the error."""
    run = start(conn, "100")
    insert_job(conn, run, "a")
    result = {}
    error_sql = ("SELECT run_marked_failed FROM public.record_pipeline_error("
                 "'wf-ingest', 'RoleRadar', '100', NULL, 'Edit Fields3', "
                 "'n8n-nodes-base.set', 'boom')")

    with psycopg.connect(migrated_db) as reporter, psycopg.connect(migrated_db) as closer:
        assert reporter.execute(error_sql).fetchone()[0] is True

        def complete():
            result["row"] = closer.execute(CLOSE_SQL, (run,)).fetchone()
            closer.commit()

        worker = threading.Thread(target=complete)
        worker.start()
        assert wait_until_blocked(conn, closer.info.backend_pid)
        reporter.commit()
        worker.join(timeout=10)

    stored = run_row(conn, run)
    assert result["row"][0] is False and stored["status"] == "completed"
    assert stored["error_message"] == "boom" and error_count(conn) == 1


def test_a_refused_completion_holds_no_lock_afterwards(conn, migrated_db):
    """A raise rolls the statement back. The next caller must not wait."""
    run = start(conn)
    with psycopg.connect(migrated_db, autocommit=True) as other:
        with pytest.raises(errors.RaiseException):
            other.execute(CLOSE_SQL, (run,))
    insert_job(conn, run, "a")
    conn.execute("SET lock_timeout = '2s'")
    assert conn.execute(CLOSE_SQL, (run,)).fetchone()[0] is False


# ---------------------------------------------------------------------------
# The trust boundary, stated as a test
# ---------------------------------------------------------------------------


def test_what_the_database_cannot_tell_every_count_from_a_node_that_has_not_run(conn):
    """If the canvas took the fetched count from a node that had not run yet
    and sent 0 instead of null, a completion before the pipeline looks exactly
    like an honest empty fetch: zero, zero, two empty lists, no rows. The
    function cannot tell them apart, and closes the run.

    This is why the documented expression sends null, not 0, when
    HTTP Request has not run. It is a property of the canvas expression, and
    no database check can replace it.

    What the database does give: the rows that arrive afterwards are visible
    as a disagreement between the stored verdicts and the tables."""
    run = start(conn)
    early = close(conn, run, 0, [], [])
    assert early["status"] == "completed"

    insert_job(conn, run, "a")
    insert_dead(conn, run, "d")
    after = close(conn, run, 0, [], [])

    assert after["already_closed"] is True
    assert (after["accepted_count"], after["accepted_rows"]) == (0, 1)
    assert (after["rejected_count"], after["rejected_rows"]) == (0, 1)
