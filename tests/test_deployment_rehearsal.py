"""A rehearsal of the release, on the disposable database only.

The release order in docs/release-runbook.md rests on a handful of claims.
Each one is a test here:

  - the migrations apply to an empty database and to one shaped like the
    live warehouse, and end in the same place
  - the API that is deployed today keeps working after the migrations
  - the new API does not work before them, and says so safely
  - old observations read the same before and after
  - the documented rollbacks do what the document says, and lose no rows
  - no migration changes or removes existing data

"The API that is deployed today" is tests/fixtures/api_main_at_ce0f286.py, a
frozen copy of api/main.py as last committed. Nothing here touches Neon,
Vercel or n8n, and passing here is not evidence about any of them.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import make_conninfo

from conftest import FIXTURES, REPO
from test_migrations import fingerprint, insert_dead, insert_job
from test_run_lifecycle import close, record, start

sys.path.insert(0, str(REPO / "api"))

import main as new_api  # noqa: E402
import migrate  # noqa: E402

QUIET = lambda _line: None  # noqa: E731
MIGRATIONS = REPO / "sql" / "migrations"
DASHBOARD = ["/api/health", "/api/runs", "/api/summary", "/api/error-classes", "/api/model-accuracy"]
MONITORING = ["/api/pipeline-runs", "/api/pipeline-errors", "/api/pipeline-status"]
VOLATILE = {"generated_at", "checked_at", "seconds_since_last_successful_run"}


def load_old_api():
    spec = importlib.util.spec_from_file_location(
        "api_main_at_ce0f286", FIXTURES / "api_main_at_ce0f286.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


old_api = load_old_api()


def stable(value):
    """A response with the fields that depend on the clock removed."""
    if isinstance(value, dict):
        return {k: stable(v) for k, v in value.items() if k not in VOLATILE}
    if isinstance(value, list):
        return [stable(v) for v in value]
    return value


def answers(module, conninfo, monkeypatch, paths=DASHBOARD):
    """Every path's (status code, stable body) from one API module."""
    monkeypatch.setenv("DATABASE_URL", conninfo)
    module._pool = None
    try:
        client = TestClient(module.app)
        out = {}
        for path in paths:
            response = client.get(path)
            out[path] = (response.status_code, stable(response.json()))
        return out
    finally:
        if module._pool is not None:
            module._pool.close()
            module._pool = None


def versions(conninfo):
    with psycopg.connect(conninfo) as c:
        return [r[0] for r in c.execute("SELECT version FROM public.schema_migrations ORDER BY version")]


def counts(conninfo):
    with psycopg.connect(conninfo) as c:
        return tuple(
            c.execute(f"SELECT count(*) FROM public.{t}").fetchone()[0]
            for t in ("jobs_extracted", "dead_letter_log")
        )


# ---------------------------------------------------------------------------
# The chain
# ---------------------------------------------------------------------------


def test_the_chain_is_nine_migrations_numbered_without_a_gap():
    found = [m.version for m in migrate.discover()]
    assert found == [f"{n:04d}" for n in range(1, 10)]
    # The deferred timestamp change is not part of this release.
    assert not list(MIGRATIONS.glob("0010*"))


def test_a_fresh_database_installs_to_0009(db):
    migrate.up(db, log=QUIET)
    assert versions(db) == [f"{n:04d}" for n in range(1, 10)]
    assert counts(db) == (0, 0)


def test_the_legacy_database_migrates_to_0009_with_every_row_intact(legacy_db):
    before = fingerprint(legacy_db)
    migrate.up(legacy_db, log=QUIET)
    assert versions(legacy_db) == [f"{n:04d}" for n in range(1, 10)]
    assert fingerprint(legacy_db) == before
    assert counts(legacy_db) == (25, 6)


def columns(conninfo):
    with psycopg.connect(conninfo) as c:
        return c.execute(
            "SELECT table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name <> 'schema_migrations' "
            "ORDER BY table_name, ordinal_position").fetchall()


@pytest.fixture
def second_legacy_db(server):
    """Another database shaped like the live one, beside the test's own."""
    name = f"rr_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(server, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    conninfo = make_conninfo(server, dbname=name)
    try:
        with psycopg.connect(conninfo, autocommit=True) as c:
            c.execute((FIXTURES / "legacy_schema.sql").read_text(encoding="utf-8"))
            c.execute((FIXTURES / "legacy_rows.sql").read_text(encoding="utf-8"))
        yield conninfo
    finally:
        with psycopg.connect(server, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


def test_migrating_one_step_at_a_time_ends_where_migrating_at_once_does(legacy_db, second_legacy_db):
    """The runbook applies them in reviewed steps."""
    for n in range(1, 10):
        migrate.up(second_legacy_db, to=f"{n:04d}", log=QUIET)
        assert versions(second_legacy_db)[-1] == f"{n:04d}"
    migrate.up(legacy_db, log=QUIET)

    assert columns(second_legacy_db) == columns(legacy_db)
    assert fingerprint(second_legacy_db) == fingerprint(legacy_db)


# ---------------------------------------------------------------------------
# Runbook step 7: the deployed API against the expanded schema
# ---------------------------------------------------------------------------


def test_the_deployed_api_answers_identically_before_and_after_migrating(legacy_db, monkeypatch):
    before = answers(old_api, legacy_db, monkeypatch)
    migrate.up(legacy_db, log=QUIET)
    after = answers(old_api, legacy_db, monkeypatch)

    assert [code for code, _ in before.values()] == [200] * 5
    assert after == before
    assert before["/api/runs"][1]["row_count"] > 1


@pytest.mark.parametrize("stop_at", ["0003", "0005", "0007", "0008"])
def test_the_deployed_api_survives_a_migration_that_stopped_partway(legacy_db, monkeypatch, stop_at):
    """A migration run interrupted between files leaves the live site working."""
    before = answers(old_api, legacy_db, monkeypatch)
    migrate.up(legacy_db, to=stop_at, log=QUIET)
    assert answers(old_api, legacy_db, monkeypatch) == before


def test_the_deployed_api_keeps_working_while_the_new_workflow_writes(legacy_db, monkeypatch):
    """Between migrating and deploying, tracked rows may already be arriving.
    The old API has never heard of run_id and must still count them."""
    migrate.up(legacy_db, log=QUIET)
    before = answers(old_api, legacy_db, monkeypatch)["/api/summary"][1]
    with psycopg.connect(legacy_db, autocommit=True) as c:
        run = start(c, "1")
        insert_job(c, run, "a")
        insert_dead(c, run, "d")
        close(c, run, 13, ["a"], ["d"])

    after = answers(old_api, legacy_db, monkeypatch)
    assert [code for code, _ in after.values()] == [200] * 5
    assert after["/api/summary"][1]["totals"]["ingested"] == before["totals"]["ingested"] + 2


# ---------------------------------------------------------------------------
# Runbook step 8: the new API
# ---------------------------------------------------------------------------


def test_the_hazard_the_new_api_before_the_migrations(legacy_db, monkeypatch):
    """Deploying first breaks the dashboard. This is why the order is fixed."""
    got = answers(new_api, legacy_db, monkeypatch, DASHBOARD + MONITORING)

    broken = ["/api/runs", "/api/summary", "/api/model-accuracy"] + MONITORING
    for path in broken:
        code, body = got[path]
        assert code == 503 and body["reason"] == "schema_outdated", path
    assert got["/api/error-classes"][0] == 200
    assert got["/api/health"] == (200, {"database": "up", "schema": {
        "version": None, "required": "0009", "compatible": False}})


def test_the_new_api_on_the_migrated_legacy_database(legacy_db, monkeypatch):
    old = answers(old_api, legacy_db, monkeypatch)
    migrate.up(legacy_db, log=QUIET)
    new = answers(new_api, legacy_db, monkeypatch, DASHBOARD + MONITORING)

    assert [code for code, _ in new.values()] == [200] * 8
    assert new["/api/health"][1]["schema"]["compatible"] is True
    # Old observations read the same through the new API as through the old.
    old_runs, new_runs = old["/api/runs"][1]["runs"], new["/api/runs"][1]["runs"]
    assert [{k: r[k] for k in old_runs[0]} for r in new_runs] == old_runs
    assert new["/api/summary"][1] == old["/api/summary"][1]
    assert new["/api/error-classes"][1] == old["/api/error-classes"][1]
    assert new["/api/model-accuracy"][1] == old["/api/model-accuracy"][1]
    # And nothing was invented for them.
    assert new["/api/pipeline-runs"][1]["total"] == 0
    assert new["/api/pipeline-status"][1]["reasons"] == ["legacy_data_only", "data_overdue"]


def test_the_new_api_on_a_fresh_install(db, monkeypatch):
    migrate.up(db, log=QUIET)
    new = answers(new_api, db, monkeypatch, DASHBOARD + MONITORING)
    assert [code for code, _ in new.values()] == [200] * 8
    assert new["/api/pipeline-status"][1]["pipeline_status"] == "no_data"
    assert new["/api/runs"][1]["row_count"] == 0
    assert answers(old_api, db, monkeypatch)["/api/runs"] == (200, new["/api/runs"][1] | {})


# ---------------------------------------------------------------------------
# Rollback, as documented
# ---------------------------------------------------------------------------


def with_a_tracked_run(conninfo):
    """One completed run and one failed one, on top of the legacy rows."""
    with psycopg.connect(conninfo, autocommit=True) as c:
        good = start(c, "1")
        insert_job(c, good, "new-a")
        insert_dead(c, good, "new-d")
        close(c, good, 13, ["new-a"], ["new-d"])
        start(c, "2")
        record(c, "2")


def test_emergency_rollback_is_the_old_api_on_the_new_schema(legacy_db, monkeypatch):
    """The recommended recovery: put the old code back, leave the schema.
    Nothing is reverted in the database and nothing is lost."""
    migrate.up(legacy_db, log=QUIET)
    with_a_tracked_run(legacy_db)
    rows = counts(legacy_db)

    got = answers(old_api, legacy_db, monkeypatch)

    assert [code for code, _ in got.values()] == [200] * 5
    assert counts(legacy_db) == rows == (26, 7)
    with psycopg.connect(legacy_db) as c:
        assert c.execute("SELECT count(*) FROM public.pipeline_runs").fetchone()[0] == 2


def test_down_to_0007_removes_the_functions_and_keeps_every_run(legacy_db, monkeypatch):
    migrate.up(legacy_db, log=QUIET)
    with_a_tracked_run(legacy_db)

    reverted = migrate.down(legacy_db, to="0007", log=QUIET)

    assert reverted == ["0009", "0008"] or len(reverted) == 2
    assert versions(legacy_db)[-1] == "0007"
    with psycopg.connect(legacy_db) as c:
        assert c.execute(
            "SELECT count(*) FROM pg_proc WHERE proname IN "
            "('complete_pipeline_run', 'record_pipeline_error')").fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM public.pipeline_runs").fetchone()[0] == 2
        assert c.execute(
            "SELECT count(*) FROM public.jobs_extracted WHERE run_id IS NOT NULL").fetchone()[0] == 1
    assert counts(legacy_db) == (26, 7)
    # Both APIs still read it: the new one needs the columns, not the functions.
    assert [c for c, _ in answers(old_api, legacy_db, monkeypatch).values()] == [200] * 5
    assert answers(new_api, legacy_db, monkeypatch, ["/api/runs"])["/api/runs"][0] == 200
    assert answers(new_api, legacy_db, monkeypatch, ["/api/health"])["/api/health"][1]["schema"]["compatible"] is False


def test_down_to_0001_keeps_every_observation_and_discards_which_run_it_was(legacy_db, monkeypatch):
    """The full schema rollback. It is documented as lossy, and this is the loss."""
    original = fingerprint(legacy_db)
    original_columns = columns(legacy_db)
    migrate.up(legacy_db, log=QUIET)
    with_a_tracked_run(legacy_db)

    migrate.down(legacy_db, to="0001", log=QUIET)

    assert versions(legacy_db) == ["0001"]
    # No observation deleted, including the two written after migrating.
    assert counts(legacy_db) == (26, 7)
    # Column for column, the schema is what it was before any migration.
    assert columns(legacy_db) == original_columns
    with psycopg.connect(legacy_db) as c:
        assert c.execute("SELECT to_regclass('public.pipeline_runs')").fetchone()[0] is None
        # The historical rows are byte for byte what they were.
        # The recorded error outlives the rollback too: pipeline_errors is a
        # baseline table. It loses its run_id and category, not the row.
        assert c.execute("SELECT count(*) FROM public.pipeline_errors").fetchone()[0] == 1
        # Set the three rows written after migrating aside, to compare the rest.
        c.execute("DELETE FROM public.jobs_extracted WHERE source_job_id = 'new-a'")
        c.execute("DELETE FROM public.dead_letter_log WHERE source_job_id = 'new-d'")
        c.execute("DELETE FROM public.pipeline_errors")
        c.commit()
    assert fingerprint(legacy_db) == original
    assert [c for c, _ in answers(old_api, legacy_db, monkeypatch).values()] == [200] * 5


def test_a_rolled_back_database_can_be_migrated_again(legacy_db):
    migrate.up(legacy_db, log=QUIET)
    migrate.down(legacy_db, to="0001", log=QUIET)
    migrate.up(legacy_db, log=QUIET)
    assert versions(legacy_db)[-1] == "0009"
    assert counts(legacy_db) == (25, 6)


# ---------------------------------------------------------------------------
# Nothing destructive
# ---------------------------------------------------------------------------


def strip_comments(sql: str) -> str:
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


@pytest.mark.parametrize("path", sorted(p for p in MIGRATIONS.glob("*.sql") if ".down." not in p.name),
                         ids=lambda p: p.name)
def test_no_up_migration_removes_or_rewrites_existing_data(path):
    sql = strip_comments(path.read_text(encoding="utf-8")).upper()
    for forbidden in (r"\bDROP\s+TABLE\b", r"\bDROP\s+COLUMN\b", r"\bTRUNCATE\b", r"\bDELETE\s+FROM\b",
                      r"\bALTER\s+COLUMN\b", r"\bRENAME\b", r"\bSET\s+NOT\s+NULL\b",
                      r"\bUPDATE\s+PUBLIC\.(JOBS_EXTRACTED|DEAD_LETTER_LOG)\b"):
        assert not re.search(forbidden, sql), f"{path.name} contains {forbidden}"


def test_no_down_migration_deletes_a_row():
    for path in MIGRATIONS.glob("*.down.sql"):
        sql = strip_comments(path.read_text(encoding="utf-8")).upper()
        assert not re.search(r"\bDELETE\s+FROM\b|\bTRUNCATE\b", sql), path.name
        assert not re.search(r"\bDROP\s+TABLE\s+(IF\s+EXISTS\s+)?PUBLIC\.(JOBS_EXTRACTED|DEAD_LETTER_LOG)\b", sql)


# ---------------------------------------------------------------------------
# Runbook step 4: the preflight
# ---------------------------------------------------------------------------

PREFLIGHT = (REPO / "sql" / "preflight.sql").read_text(encoding="utf-8")


def preflight(conninfo) -> dict:
    with psycopg.connect(conninfo, autocommit=True) as c:
        cur = c.execute(PREFLIGHT)
        rows = {}
        while True:
            if cur.description:
                rows.update(dict(cur.fetchall()))
            if not cur.nextset():
                break
        return rows


def test_preflight_describes_an_empty_database(db):
    got = preflight(db)
    assert got["ledger_present"] == "false" and got["rows.jobs_extracted"] == "(absent)"
    assert got["transaction_read_only"] == "on"


def test_preflight_describes_the_legacy_database_and_changes_nothing(legacy_db):
    before = fingerprint(legacy_db)
    got = preflight(legacy_db)

    assert (got["rows.jobs_extracted"], got["rows.dead_letter_log"]) == ("25", "6")
    assert got["rows.pipeline_runs"] == "(absent)" and got["ledger_latest_version"] == "(no ledger)"
    assert got["jobs_extracted_pk_is_id"] == "true"
    assert got["column.jobs_extracted.run_id"] == "absent"
    assert got["function.complete_pipeline_run"] == "absent"
    assert got["collision.pipeline_runs_without_ledger"] == "false"
    assert got["type.jobs_extracted.ingested_at"] == "timestamp without time zone"
    assert got["type.dead_letter_log.ingested_at"] == "timestamp with time zone"
    assert fingerprint(legacy_db) == before
    with psycopg.connect(legacy_db) as c:
        assert c.execute("SELECT to_regclass('public.schema_migrations')").fetchone()[0] is None


def test_preflight_describes_a_migrated_database(migrated_db):
    got = preflight(migrated_db)
    assert got["ledger_latest_version"] == "0009" and got["ledger_applied_count"] == "9"
    assert got["rows.pipeline_runs"] == "0"
    assert got["column.dead_letter_log.description_clean"] == "present"
    assert got["function.record_pipeline_error"] == "present"


def test_preflight_flags_a_table_someone_created_by_hand(legacy_db):
    with psycopg.connect(legacy_db, autocommit=True) as c:
        c.execute("CREATE TABLE public.pipeline_runs (run_id uuid)")
    assert preflight(legacy_db)["collision.pipeline_runs_without_ledger"] == "true"


def test_preflight_cannot_write(db):
    """The file is wrapped in a read-only transaction. A write inside it is
    refused by Postgres, not by convention."""
    tampered = PREFLIGHT.replace(
        "WITH facts(check_name, value) AS (",
        "CREATE TABLE public.oops (x int);\nWITH facts(check_name, value) AS (", 1)
    assert tampered != PREFLIGHT
    with psycopg.connect(db, autocommit=True) as c:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            c.execute(tampered)


# ---------------------------------------------------------------------------
# The backup check: sql/fingerprint.sql
# ---------------------------------------------------------------------------

FINGERPRINT = (REPO / "sql" / "fingerprint.sql").read_text(encoding="utf-8")


def fingerprint_lines(conninfo) -> list[str]:
    with psycopg.connect(conninfo, autocommit=True) as c:
        cur = c.execute(FINGERPRINT)
        lines = []
        while True:
            if cur.description:
                lines = [r[0] for r in cur.fetchall()]
            if not cur.nextset():
                break
        return lines


def test_the_backup_fingerprint_is_stable_and_changes_nothing(legacy_db):
    before = fingerprint(legacy_db)
    first, second = fingerprint_lines(legacy_db), fingerprint_lines(legacy_db)

    assert first == second and len(first) > 50
    assert fingerprint(legacy_db) == before
    assert "rows|jobs_extracted|25 " in "\n".join(first)
    assert any(line.startswith("constraint|jobs_extracted.jobs_extracted_pkey|") for line in first)
    # Same data, different session time zone: the same text.
    with psycopg.connect(legacy_db, autocommit=True) as c:
        c.execute("ALTER DATABASE " + c.info.dbname + " SET timezone = 'Asia/Kolkata'")
    assert fingerprint_lines(legacy_db) == first


def test_the_backup_fingerprint_notices_one_changed_value(legacy_db):
    """A restore that lost or altered a single value must not pass."""
    first = fingerprint_lines(legacy_db)
    with psycopg.connect(legacy_db, autocommit=True) as c:
        c.execute("UPDATE public.dead_letter_log SET model = 'altered' "
                  "WHERE id = (SELECT min(id) FROM public.dead_letter_log)")
    changed = [line for line in fingerprint_lines(legacy_db) if line not in first]
    assert changed and all(line.startswith("rows|dead_letter_log|6 ") for line in changed)
