"""What the migrations do to a database, and what they must not do.

Runs against a disposable Postgres 16. No test here touches Neon or the local
warehouse; see conftest.py for the guard.
"""

from __future__ import annotations

import psycopg
import pytest
from psycopg import errors

import migrate

QUIET = lambda _line: None  # noqa: E731

# The columns that existed before any migration. History is "preserved" when
# every value in these columns is byte-identical afterwards.
LEGACY_COLUMNS = {
    "jobs_extracted": (
        "id, source_job_id, source_url, raw_title, raw_company, ingested_at, "
        "model, extraction_version, normalized_title, company, seniority, "
        "tech_stack, remote_type, salary_min, salary_max, salary_period, "
        "mentions_n8n_model, mentions_n8n_actual, ai_ml_role"
    ),
    "dead_letter_log": (
        "id, source_job_id, source_url, raw_title, raw_company, description, "
        "ingested_at, model, extraction_version, error_codes, errors_json, "
        "raw_output"
    ),
    "pipeline_errors": (
        "id, workflow_name, execution_id, failed_node, error_message, occurred_at"
    ),
    "jobs_latest": (
        "source_job_id, source_url, raw_title, raw_company, ingested_at, model, "
        "extraction_version, normalized_title, company, seniority, tech_stack, "
        "remote_type, salary_min, salary_max, salary_period, mentions_n8n_model, "
        "mentions_n8n_actual, ai_ml_role"
    ),
}


def fingerprint(conninfo: str) -> dict[str, tuple[int, str]]:
    """(row count, md5 over every legacy column of every row) per relation.

    The same trick the README used to prove the Neon copy matched the local
    one. The timestamps are cast to text under a fixed zone so the hash does
    not depend on the session.
    """
    out = {}
    with psycopg.connect(conninfo, autocommit=True) as c:
        c.execute("SET TIME ZONE 'UTC'")
        for table, columns in LEGACY_COLUMNS.items():
            order = "id" if "id" in columns.split(", ") else "source_job_id"
            row = c.execute(
                f"SELECT count(*), "
                f"coalesce(md5(string_agg(md5(row({columns})::text), '' ORDER BY {order})), '') "
                f"FROM public.{table}"
            ).fetchone()
            out[table] = (row[0], row[1])
    return out


def columns_of(conninfo: str, table: str) -> dict[str, str]:
    with psycopg.connect(conninfo, autocommit=True) as c:
        rows = c.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        ).fetchall()
    return dict(rows)


def start_run(conn, execution_id="100", started_at=None):
    """What the workflow's first node does."""
    if started_at is None:
        return conn.execute(
            "INSERT INTO public.pipeline_runs (n8n_execution_id) "
            "VALUES (%s) RETURNING run_id",
            (execution_id,),
        ).fetchone()[0]
    return conn.execute(
        "INSERT INTO public.pipeline_runs (n8n_execution_id, started_at) "
        "VALUES (%s, %s) RETURNING run_id",
        (execution_id, started_at),
    ).fetchone()[0]


def insert_job(conn, run_id, source_job_id, on_conflict=""):
    return conn.execute(
        "INSERT INTO public.jobs_extracted "
        "(source_job_id, source_url, ingested_at, run_id) "
        f"VALUES (%s, 'https://example.test/x', now(), %s) {on_conflict}",
        (source_job_id, run_id),
    ).rowcount


def insert_dead(conn, run_id, source_job_id, on_conflict=""):
    return conn.execute(
        "INSERT INTO public.dead_letter_log "
        "(source_job_id, ingested_at, error_codes, errors_json, run_id) "
        f"VALUES (%s, now(), '[\"ENUM\"]', '[]', %s) {on_conflict}",
        (source_job_id, run_id),
    ).rowcount


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


def test_fresh_database_migrates_from_nothing(db):
    applied = migrate.up(db, log=QUIET)
    assert len(applied) == len(migrate.discover())
    assert all(state == "applied" for _label, state in migrate.status(db))

    assert "run_id" in columns_of(db, "jobs_extracted")
    assert "pipeline_runs" in {
        r[0]
        for r in psycopg.connect(db).execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
    }


def test_up_twice_is_a_no_op(db):
    migrate.up(db, log=QUIET)
    assert migrate.up(db, log=QUIET) == []


def test_fresh_and_legacy_databases_end_up_with_the_same_columns(db, server):
    """The baseline is a faithful copy of the live schema, not an approximation."""
    migrate.up(db, log=QUIET)

    def shape(conninfo):
        with psycopg.connect(conninfo) as c:
            return c.execute(
                "SELECT table_name, column_name, data_type, is_nullable, ordinal_position "
                "FROM information_schema.columns WHERE table_schema = 'public' "
                "ORDER BY table_name, ordinal_position"
            ).fetchall()

    fresh = shape(db)

    # Build the legacy one by hand here; a second `db` fixture is not available.
    from conftest import FIXTURES, run_file
    from psycopg.conninfo import make_conninfo

    with psycopg.connect(server, autocommit=True) as admin:
        admin.execute("CREATE DATABASE rr_shape_compare")
    try:
        legacy = make_conninfo(server, dbname="rr_shape_compare")
        run_file(legacy, FIXTURES / "legacy_schema.sql")
        migrate.up(legacy, log=QUIET)
        assert shape(legacy) == fresh
    finally:
        with psycopg.connect(server, autocommit=True) as admin:
            admin.execute("DROP DATABASE rr_shape_compare WITH (FORCE)")


# ---------------------------------------------------------------------------
# Historical-data preservation
# ---------------------------------------------------------------------------


def test_baseline_alone_changes_nothing_on_an_existing_database(legacy_db):
    before = fingerprint(legacy_db)
    columns_before = {t: columns_of(legacy_db, t) for t in LEGACY_COLUMNS}

    migrate.up(legacy_db, to="0001", log=QUIET)

    assert fingerprint(legacy_db) == before
    assert {t: columns_of(legacy_db, t) for t in LEGACY_COLUMNS} == columns_before


def test_every_historical_row_survives_every_migration_unchanged(legacy_db):
    before = fingerprint(legacy_db)
    assert before["jobs_extracted"][0] == 25
    assert before["dead_letter_log"][0] == 6
    assert before["jobs_latest"][0] == 12

    migrate.up(legacy_db, log=QUIET)

    assert fingerprint(legacy_db) == before


def test_historical_rows_are_not_backfilled(conn):
    """A NULL run_id is the honest value for a row written before runs existed."""
    for table, new_columns in {
        "jobs_extracted": ("run_id", "publication_date"),
        "dead_letter_log": ("run_id", "publication_date", "description_clean"),
    }.items():
        for column in new_columns:
            filled = conn.execute(
                f"SELECT count(*) FROM public.{table} WHERE {column} IS NOT NULL"
            ).fetchone()[0]
            assert filled == 0, f"{table}.{column} was backfilled"

    assert conn.execute("SELECT count(*) FROM public.pipeline_runs").fetchone()[0] == 0


def test_double_encoded_errors_json_is_left_as_it_was(conn):
    """Historical rows keep their encoding. The read path handles both."""
    kinds = conn.execute(
        "SELECT DISTINCT jsonb_typeof(errors_json) FROM public.dead_letter_log"
    ).fetchall()
    assert kinds == [("string",)]


def test_ingested_at_types_are_untouched(migrated_db):
    """The timestamp fix is deferred. These migrations must not sneak it in."""
    assert columns_of(migrated_db, "jobs_extracted")["ingested_at"] == (
        "timestamp without time zone"
    )
    assert columns_of(migrated_db, "dead_letter_log")["ingested_at"] == (
        "timestamp with time zone"
    )


def test_the_old_description_column_is_still_there(migrated_db):
    columns = columns_of(migrated_db, "dead_letter_log")
    assert "description" in columns
    assert "description_clean" in columns


# ---------------------------------------------------------------------------
# Append-only model and replay
# ---------------------------------------------------------------------------


def test_the_same_posting_on_two_runs_is_two_rows(conn):
    """The history decision (a) exists to keep. Uniqueness is per run only."""
    first = start_run(conn, "100")
    second = start_run(conn, "101")
    insert_job(conn, first, "555")
    insert_job(conn, second, "555")

    seen = conn.execute(
        "SELECT count(*) FROM public.jobs_extracted WHERE source_job_id = '555'"
    ).fetchone()[0]
    assert seen == 2


def test_a_replayed_insert_within_one_run_is_rejected(conn):
    run = start_run(conn)
    insert_job(conn, run, "555")
    with pytest.raises(errors.UniqueViolation):
        insert_job(conn, run, "555")


def test_a_replayed_insert_with_skip_on_conflict_writes_nothing(conn):
    """n8n's "Skip on Conflict" option emits ON CONFLICT DO NOTHING."""
    run = start_run(conn)
    for source_job_id in ("1", "2", "3"):
        insert_job(conn, run, source_job_id)

    replayed = sum(
        insert_job(conn, run, s, "ON CONFLICT DO NOTHING") for s in ("1", "2", "3")
    )

    assert replayed == 0
    rows = conn.execute(
        "SELECT count(*) FROM public.jobs_extracted WHERE run_id = %s", (run,)
    ).fetchone()[0]
    assert rows == 3


def test_a_replayed_dead_letter_within_one_run_writes_nothing(conn):
    run = start_run(conn)
    assert insert_dead(conn, run, "555", "ON CONFLICT DO NOTHING") == 1
    assert insert_dead(conn, run, "555", "ON CONFLICT DO NOTHING") == 0


def test_two_dead_letters_with_no_source_id_both_land(conn):
    """A missing source_job_id is itself a failure. Two of them are two events."""
    run = start_run(conn)
    insert_dead(conn, run, None)
    insert_dead(conn, run, None)
    rows = conn.execute(
        "SELECT count(*) FROM public.dead_letter_log WHERE run_id = %s", (run,)
    ).fetchone()[0]
    assert rows == 2


def test_a_posting_can_be_accepted_and_rejected_in_different_runs(conn):
    first = start_run(conn, "100")
    second = start_run(conn, "101")
    insert_job(conn, first, "555")
    insert_dead(conn, second, "555")


def test_legacy_duplicates_do_not_block_the_unique_index(conn):
    """The live table has one posting three times at one millisecond."""
    dupes = conn.execute(
        "SELECT count(*) FROM public.dead_letter_log "
        "WHERE source_job_id = '2091105' "
        "AND ingested_at = '2026-08-23 15:02:44.726+00'"
    ).fetchone()[0]
    assert dupes == 3


def test_rows_without_a_run_id_still_insert(conn):
    """The workflow as it is deployed today must keep working after migrating."""
    assert insert_job(conn, None, "2091098") == 1
    assert insert_job(conn, None, "2091098") == 1
    assert insert_dead(conn, None, "2091105") == 1


def test_a_row_cannot_point_at_a_run_that_does_not_exist(conn):
    with pytest.raises(errors.ForeignKeyViolation):
        insert_job(conn, "00000000-0000-0000-0000-000000000000", "555")


# ---------------------------------------------------------------------------
# pipeline_runs
# ---------------------------------------------------------------------------


def test_a_new_run_starts_running_with_a_generated_id(conn):
    run = start_run(conn, "100")
    row = conn.execute(
        "SELECT status, completed_at, n8n_execution_id, started_at IS NOT NULL "
        "FROM public.pipeline_runs WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert row == ("running", None, "100", True)


def test_a_successful_run_closes_with_its_counts(conn):
    run = start_run(conn)
    conn.execute(
        "UPDATE public.pipeline_runs SET status = 'completed', completed_at = now(), "
        "fetched_count = 13, processed_count = 5, accepted_count = 4, rejected_count = 1 "
        "WHERE run_id = %s",
        (run,),
    )
    row = conn.execute(
        "SELECT status, fetched_count, processed_count, accepted_count, rejected_count, "
        "completed_at >= started_at FROM public.pipeline_runs WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert row == ("completed", 13, 5, 4, 1, True)


def test_a_run_with_zero_accepted_records_is_a_valid_completed_run(conn):
    """Every item rejected is a data-quality result, not an infra failure."""
    run = start_run(conn)
    conn.execute(
        "UPDATE public.pipeline_runs SET status = 'completed', completed_at = now(), "
        "fetched_count = 13, processed_count = 5, accepted_count = 0, rejected_count = 5 "
        "WHERE run_id = %s",
        (run,),
    )


def test_a_run_that_fetched_nothing_is_a_valid_completed_run(conn):
    run = start_run(conn)
    conn.execute(
        "UPDATE public.pipeline_runs SET status = 'completed', completed_at = now(), "
        "fetched_count = 0, processed_count = 0, accepted_count = 0, rejected_count = 0 "
        "WHERE run_id = %s",
        (run,),
    )


@pytest.mark.parametrize(
    "assignment, constraint",
    [
        ("status = 'done', completed_at = now()", "pipeline_runs_status_check"),
        # closed without a completion time
        ("status = 'completed'", "pipeline_runs_open_iff_running_check"),
        # still running, yet stamped complete
        ("completed_at = now()", "pipeline_runs_open_iff_running_check"),
        (
            "status = 'completed', completed_at = started_at - interval '1 second'",
            "pipeline_runs_completed_after_started_check",
        ),
        ("accepted_count = -1", "pipeline_runs_counts_non_negative_check"),
        (
            "processed_count = 5, accepted_count = 4, rejected_count = 2",
            "pipeline_runs_counts_add_up_check",
        ),
    ],
)
def test_pipeline_runs_rejects_states_that_cannot_happen(conn, assignment, constraint):
    run = start_run(conn)
    with pytest.raises(errors.CheckViolation) as raised:
        conn.execute(
            f"UPDATE public.pipeline_runs SET {assignment} WHERE run_id = %s", (run,)
        )
    assert raised.value.diag.constraint_name == constraint


def test_runs_started_a_millisecond_apart_stay_separate(conn):
    """The case gap-grouping gets wrong: it would merge these into one run."""
    first = start_run(conn, "100", "2026-11-11 10:00:00.000+00")
    second = start_run(conn, "101", "2026-11-11 10:00:00.001+00")
    assert first != second

    for source_job_id in ("1", "2", "3"):
        insert_job(conn, first, source_job_id)
    for source_job_id in ("1", "2"):
        insert_job(conn, second, source_job_id)
    insert_dead(conn, second, "3")

    per_run = conn.execute(
        """
        SELECT r.n8n_execution_id,
               (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id),
               (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id)
        FROM public.pipeline_runs r
        ORDER BY r.started_at
        """
    ).fetchall()
    assert per_run == [("100", 3, 0), ("101", 2, 1)]


def test_a_reused_execution_id_does_not_block_a_new_run(conn):
    """n8n numbers executions from 1 again if its volume is recreated."""
    first = start_run(conn, "1")
    second = start_run(conn, "1")
    assert first != second


# ---------------------------------------------------------------------------
# Error-to-run correlation
# ---------------------------------------------------------------------------

# What the Error Trigger workflow does: record the error against the run that
# has this execution id, newest first in case the id was ever reused.
RECORD_ERROR = """
INSERT INTO public.pipeline_errors
    (workflow_name, execution_id, failed_node, error_message, error_category, run_id)
VALUES (
    %(workflow)s, %(execution_id)s, %(node)s, %(message)s, %(category)s,
    (SELECT run_id FROM public.pipeline_runs
     WHERE n8n_execution_id = %(execution_id)s
     ORDER BY started_at DESC LIMIT 1)
)
RETURNING run_id
"""

FAIL_RUN = """
UPDATE public.pipeline_runs
SET status = 'failed', completed_at = now(),
    error_category = %(category)s, error_message = %(message)s
WHERE run_id = %(run_id)s AND status = 'running'
"""


@pytest.mark.parametrize(
    "node, category, message",
    [
        ("HTTP Request", "source_api", "timeout of 30000ms exceeded"),
        ("Claude Extract", "llm_provider", "529 Overloaded"),
        ("Insert rows in a table", "warehouse", "connection terminated unexpectedly"),
    ],
)
def test_an_infra_error_is_tied_to_its_run_and_closes_it(conn, node, category, message):
    other = start_run(conn, "99")
    run = start_run(conn, "100")
    params = {
        "workflow": "RoleRadar",
        "execution_id": "100",
        "node": node,
        "message": message,
        "category": category,
    }

    found = conn.execute(RECORD_ERROR, params).fetchone()[0]
    assert found == run
    conn.execute(FAIL_RUN, {**params, "run_id": found})

    joined = conn.execute(
        """
        SELECT r.status, r.error_category, r.completed_at IS NOT NULL,
               e.failed_node, e.error_message
        FROM public.pipeline_errors e
        JOIN public.pipeline_runs r USING (run_id)
        WHERE e.execution_id = '100'
        """
    ).fetchone()
    assert joined == ("failed", category, True, node, message)

    untouched = conn.execute(
        "SELECT status FROM public.pipeline_runs WHERE run_id = %s", (other,)
    ).fetchone()[0]
    assert untouched == "running"


def test_an_error_with_no_matching_run_is_still_recorded(conn):
    """A failure in the trigger, or before the run row was inserted."""
    found = conn.execute(
        RECORD_ERROR,
        {
            "workflow": "RoleRadar",
            "execution_id": None,
            "node": "Schedule Trigger",
            "message": "trigger failed",
            "category": "trigger",
        },
    ).fetchone()[0]
    assert found is None
    assert conn.execute("SELECT count(*) FROM public.pipeline_errors").fetchone()[0] == 1


def test_a_failed_run_keeps_the_rows_it_wrote_before_failing(conn):
    run = start_run(conn, "100")
    insert_job(conn, run, "1")
    conn.execute(
        FAIL_RUN, {"category": "warehouse", "message": "boom", "run_id": run}
    )
    rows = conn.execute(
        "SELECT count(*) FROM public.jobs_extracted WHERE run_id = %s", (run,)
    ).fetchone()[0]
    assert rows == 1


def test_a_completed_run_is_not_reopened_by_a_late_error(conn):
    run = start_run(conn, "100")
    conn.execute(
        "UPDATE public.pipeline_runs SET status = 'completed', completed_at = now() "
        "WHERE run_id = %s",
        (run,),
    )
    conn.execute(FAIL_RUN, {"category": "unknown", "message": "late", "run_id": run})
    status = conn.execute(
        "SELECT status FROM public.pipeline_runs WHERE run_id = %s", (run,)
    ).fetchone()[0]
    assert status == "completed"


def test_an_unrecognised_error_category_is_stored_not_rejected(conn):
    """Losing the error to a typo in a canvas expression is the worse outcome."""
    conn.execute(
        "INSERT INTO public.pipeline_errors (error_message, error_category) "
        "VALUES ('x', 'sorce_api')"
    )


# ---------------------------------------------------------------------------
# jobs_latest
# ---------------------------------------------------------------------------


def test_jobs_latest_keeps_its_columns_in_place_and_gains_two(conn):
    names = [d.name for d in conn.execute("SELECT * FROM public.jobs_latest LIMIT 0").description]
    assert names[:18] == LEGACY_COLUMNS["jobs_latest"].split(", ")
    assert names[18:] == ["publication_date", "run_id"]


def test_jobs_latest_shows_the_newest_sighting_with_its_run(conn):
    run = start_run(conn)
    conn.execute(
        "INSERT INTO public.jobs_extracted "
        "(source_job_id, source_url, ingested_at, publication_date, run_id) "
        "VALUES ('2091098', 'https://example.test/x', '2026-11-11 10:00:00', "
        "'2026-08-21 05:54:39', %s)",
        (run,),
    )
    row = conn.execute(
        "SELECT ingested_at::text, publication_date::text, run_id "
        "FROM public.jobs_latest WHERE source_job_id = '2091098'"
    ).fetchone()
    assert row == ("2026-11-11 10:00:00", "2026-08-21 05:54:39", run)
    assert conn.execute("SELECT count(*) FROM public.jobs_latest").fetchone()[0] == 12


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def test_rolling_everything_back_restores_the_original_schema_and_rows(legacy_db):
    before_rows = fingerprint(legacy_db)
    before_columns = {t: columns_of(legacy_db, t) for t in LEGACY_COLUMNS}

    migrate.up(legacy_db, log=QUIET)
    reverted = migrate.down(legacy_db, to="0001", log=QUIET)

    assert len(reverted) == len(migrate.discover()) - 1
    assert fingerprint(legacy_db) == before_rows
    assert {t: columns_of(legacy_db, t) for t in LEGACY_COLUMNS} == before_columns
    assert columns_of(legacy_db, "pipeline_runs") == {}


def test_rollback_keeps_rows_written_after_the_migration(legacy_db):
    """Down drops the new columns. It does not delete observations."""
    migrate.up(legacy_db, log=QUIET)
    with psycopg.connect(legacy_db, autocommit=True) as c:
        run = start_run(c)
        insert_job(c, run, "777")

    migrate.down(legacy_db, to="0001", log=QUIET)

    with psycopg.connect(legacy_db) as c:
        kept = c.execute(
            "SELECT count(*) FROM public.jobs_extracted WHERE source_job_id = '777'"
        ).fetchone()[0]
        total = c.execute("SELECT count(*) FROM public.jobs_extracted").fetchone()[0]
    assert kept == 1
    assert total == 26


def test_down_then_up_again_works(legacy_db):
    migrate.up(legacy_db, log=QUIET)
    migrate.down(legacy_db, to="0001", log=QUIET)
    migrate.up(legacy_db, log=QUIET)
    assert all(state == "applied" for _label, state in migrate.status(legacy_db))


def test_each_migration_can_be_reverted_on_its_own(legacy_db):
    migrate.up(legacy_db, log=QUIET)
    versions = [m.version for m in migrate.discover()]
    for target in reversed(versions[:-1]):
        assert len(migrate.down(legacy_db, to=target, log=QUIET)) == 1
