"""Do the records validateBatch() produces fit the migrated schema.

The Node tests prove what the validator returns. The migration tests prove what
the schema accepts. Neither proves the two agree, and that is the join this
file checks: it runs the real validateBatch() under Node, then inserts the
records it returned the way n8n's Postgres node would.

"The way n8n would" is an emulation, read from the node's source in n8n 2.35.7:
every field must be a column, a NULL may not go to a NOT NULL column, values go
to Postgres as untyped literals, and "Skip on Conflict" appends
ON CONFLICT DO NOTHING. It is a faithful copy of those four rules. It is not
n8n, and passing here does not prove the canvas is wired correctly.

The SQL for the Start Run and Complete Run nodes is copied out of
docs/n8n-notes.md, so the statements in the instructions are the ones tested.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess

import psycopg
import pytest
from psycopg import errors

from conftest import REPO
from test_migrations import LEGACY_COLUMNS, fingerprint

INGESTED_AT = "2026-11-11T09:30:00.123Z"

NOTES = (REPO / "docs" / "n8n-notes.md").read_text(encoding="utf-8")

# Builds pairs from the captured fixtures and prints the batch as JSON.
# argv: runId, ingestedAt, JSON list of fixture indexes, JSON list of
# publication_date overrides (same length; the string "KEEP" leaves it alone).
NODE_SCRIPT = """
const { validateBatch } = require('./validate.js');
const sources = require('./fixtures/source-records.json');
const outputs = require('./fixtures/extraction-output.json');
const [runId, ingestedAt, indexesJson, datesJson] = process.argv.slice(1);
const indexes = JSON.parse(indexesJson);
const dates = JSON.parse(datesJson);
const pairs = indexes.map((i, n) => {
  const source = JSON.parse(JSON.stringify(sources[i]));
  if (dates[n] === 'DELETE') delete source.publication_date;
  else if (dates[n] !== 'KEEP') source.publication_date = dates[n];
  return { source, extracted: outputs[i] };
});
process.stdout.write(JSON.stringify(validateBatch(pairs, ingestedAt, runId)));
"""


def validate_batch(run_id, indexes=(0, 1, 2, 3, 4), dates=None, ingested_at=INGESTED_AT):
    """Run the real validateBatch() and return its result."""
    if shutil.which("node") is None:
        pytest.fail("node is not on PATH; these tests run the real validator")
    dates = list(dates) if dates is not None else ["KEEP"] * len(indexes)
    done = subprocess.run(
        ["node", "-e", NODE_SCRIPT, str(run_id), ingested_at,
         json.dumps(list(indexes)), json.dumps(dates)],
        cwd=REPO / "validator",
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def sql_from_notes(tag: str) -> str:
    """The fenced SQL block that follows <!-- sql:tag --> in docs/n8n-notes.md."""
    match = re.search(
        r"<!-- sql:" + re.escape(tag) + r" -->\s*```sql\n(.*?)```", NOTES, re.DOTALL
    )
    assert match, f"no sql block tagged {tag} in docs/n8n-notes.md"
    return match.group(1).strip()


def execute_query_node(conn, tag: str, parameters: list):
    """n8n's Execute Query: $1, $2 placeholders with a list of parameters."""
    with psycopg.RawCursor(conn) as cur:
        cur.execute(sql_from_notes(tag), parameters)
        return cur.fetchall()


def start_run(conn, execution_id="100", workflow_id="wf-ingest"):
    return execute_query_node(conn, "start-run", [execution_id, workflow_id])[0][0]


def complete_run(conn, run_id, fetched, batch):
    """The Complete Run node. Its parameters are the canvas expression's:
    the run, the fetched and submitted counts, and the two id lists as JSON
    strings. When the
    Validator node never ran, batch is None and both lists are empty."""
    accepted = [r["source_job_id"] for r in batch["accepted_records"]] if batch else []
    rejected = [r["source_job_id"] for r in batch["rejected_records"]] if batch else []
    submitted = batch["records_processed"] if batch else 0
    return execute_query_node(
        conn,
        "complete-run",
        [str(run_id), fetched, submitted, json.dumps(accepted), json.dumps(rejected)],
    )


# The two Edit Fields nodes, as docs/n8n-notes.md leaves them: one array each,
# stringified once. Nothing else is touched and nothing is excluded.
def edit_fields_accepted(record: dict) -> dict:
    return {**record, "tech_stack": json.dumps(record["tech_stack"])}


def edit_fields_rejected(record: dict) -> dict:
    return {**record, "error_codes": json.dumps(record["error_codes"])}


class NodeOperationError(Exception):
    """What n8n raises before it sends anything to Postgres."""


def insert_node(conn, table: str, items: list[dict], skip_on_conflict=True) -> int:
    """n8n's Postgres Insert with Map Automatically. Returns rows written."""
    schema = {
        name: nullable == "YES"
        for name, nullable in conn.execute(
            "SELECT column_name, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
    }
    written = 0
    for item in items:
        for key, value in item.items():
            if key not in schema:
                raise NodeOperationError(f"Column '{key}' does not exist in selected table")
            if value is None and not schema[key]:
                raise NodeOperationError(f"Column '{key}' is not nullable")
            if isinstance(value, (list, dict)):
                # pg-promise would send a Postgres array literal here, which a
                # jsonb column rejects. The Edit Fields node exists to stop it.
                raise NodeOperationError(f"'{key}' reached the insert as a raw array or object")
        columns = ", ".join(f'"{k}"' for k in item)
        placeholders = ", ".join(["%s"] * len(item))
        conflict = " ON CONFLICT DO NOTHING" if skip_on_conflict else ""
        # Python str is sent untyped, so Postgres parses it for the column,
        # the same as the quoted literals n8n sends.
        written += conn.execute(
            f"INSERT INTO public.{table} ({columns}) VALUES ({placeholders}){conflict}",
            list(item.values()),
        ).rowcount
    return written


def write_batch(conn, batch, skip_on_conflict=True) -> tuple[int, int]:
    accepted = insert_node(
        conn, "jobs_extracted",
        [edit_fields_accepted(r) for r in batch["accepted_records"]], skip_on_conflict,
    )
    rejected = insert_node(
        conn, "dead_letter_log",
        [edit_fields_rejected(r) for r in batch["rejected_records"]], skip_on_conflict,
    )
    return accepted, rejected


def rows(conn, table, run_id) -> int:
    return conn.execute(
        f"SELECT count(*) FROM public.{table} WHERE run_id = %s", (run_id,)
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# The statements in the instructions
# ---------------------------------------------------------------------------


def test_start_run_returns_a_run_id_validatebatch_accepts(conn):
    run = start_run(conn, "231")
    stored = conn.execute(
        "SELECT status, n8n_execution_id FROM public.pipeline_runs WHERE run_id = %s", (run,)
    ).fetchone()
    assert stored == ("running", "231")

    batch = validate_batch(run)
    assert batch["run_id"] == str(run)


def test_start_run_works_without_an_execution_id(conn):
    """A manual test execution may not have one."""
    assert start_run(conn, None) is not None


# ---------------------------------------------------------------------------
# Record shape against columns
# ---------------------------------------------------------------------------


def test_every_accepted_field_is_a_column(conn):
    batch = validate_batch(start_run(conn))
    columns = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'jobs_extracted'"
        )
    }
    for record in batch["accepted_records"]:
        assert set(record) <= columns
        # id is the only column the workflow leaves to the database.
        assert columns - set(record) == {"id"}


def test_every_rejected_field_is_a_column(conn):
    batch = validate_batch(start_run(conn))
    columns = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'dead_letter_log'"
        )
    }
    for record in batch["rejected_records"]:
        assert set(record) <= columns
        # id is generated. description is the deprecated column, left NULL.
        assert columns - set(record) == {"id", "description"}


def test_accepted_records_insert_with_their_run_id(conn):
    run = start_run(conn)
    batch = validate_batch(run)

    accepted, _ = write_batch(conn, batch)

    assert accepted == batch["records_accepted"] == 4
    stored = conn.execute(
        "SELECT source_job_id, run_id, jsonb_typeof(tech_stack), model, extraction_version, "
        "mentions_n8n_model = mentions_n8n_actual "
        "FROM public.jobs_extracted WHERE run_id = %s ORDER BY id",
        (run,),
    ).fetchall()
    assert [r[0] for r in stored] == [r["source_job_id"] for r in batch["accepted_records"]]
    for row in stored:
        assert row[1] == run
        assert row[2] == "array"
        assert row[3] == "claude-haiku-4-5-20251001"
        assert row[4] == batch["extraction_version"]
        assert row[5] is True


def test_rejected_records_insert_with_their_run_id(conn):
    run = start_run(conn)
    batch = validate_batch(run)

    _, rejected = write_batch(conn, batch)

    assert rejected == batch["records_rejected"] == 1
    stored = conn.execute(
        "SELECT source_job_id, run_id, description_clean, description, error_codes "
        "FROM public.dead_letter_log WHERE run_id = %s",
        (run,),
    ).fetchone()
    record = batch["rejected_records"][0]
    assert stored[0] == record["source_job_id"]
    assert stored[1] == run
    assert stored[2] == record["description_clean"]
    assert len(stored[2]) > 100, "the description must actually arrive"
    assert stored[3] is None
    assert stored[4] == ["SALARY_SPAN_IMPLAUSIBLE"]


def test_one_ingested_at_per_run_in_both_tables(conn):
    run = start_run(conn)
    write_batch(conn, validate_batch(run))

    conn.execute("SET TIME ZONE 'UTC'")
    stamps = conn.execute(
        "SELECT ingested_at::text FROM public.jobs_extracted WHERE run_id = %(r)s "
        "UNION "
        "SELECT (ingested_at AT TIME ZONE 'UTC')::text FROM public.dead_letter_log "
        "WHERE run_id = %(r)s",
        {"r": run},
    ).fetchall()
    # One instant, the same in the naive column and the zoned one.
    assert stamps == [("2026-11-11 09:30:00.123",)]


# ---------------------------------------------------------------------------
# errors_json
# ---------------------------------------------------------------------------


def test_errors_json_lands_as_an_array(conn):
    run = start_run(conn)
    batch = validate_batch(run)
    write_batch(conn, batch)

    stored = conn.execute(
        "SELECT jsonb_typeof(errors_json), jsonb_typeof(error_codes), "
        "jsonb_typeof(raw_output), errors_json "
        "FROM public.dead_letter_log WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert stored[:3] == ("array", "array", "object")
    assert stored[3] == json.loads(batch["rejected_records"][0]["errors_json"])
    assert stored[3][0]["code"] == "SALARY_SPAN_IMPLAUSIBLE"


def test_the_old_canvas_step_is_what_double_encoded_it(conn):
    """Stringifying errors_json a second time reproduces the live bug exactly."""
    run = start_run(conn)
    record = edit_fields_rejected(validate_batch(run)["rejected_records"][0])
    record["errors_json"] = json.dumps(record["errors_json"])

    insert_node(conn, "dead_letter_log", [record])

    kind = conn.execute(
        "SELECT jsonb_typeof(errors_json) FROM public.dead_letter_log WHERE run_id = %s",
        (run,),
    ).fetchone()[0]
    assert kind == "string"


def test_new_and_historical_errors_json_read_through_the_same_query(conn):
    """The dashboard's unwrap must give an array for both encodings."""
    run = start_run(conn)
    write_batch(conn, validate_batch(run))

    kinds = conn.execute(
        """
        SELECT run_id IS NULL AS historical,
               jsonb_typeof(errors_json) AS stored_as,
               jsonb_typeof(CASE WHEN jsonb_typeof(errors_json) = 'string'
                                 THEN (errors_json #>> '{}')::jsonb
                                 ELSE errors_json END) AS read_as
        FROM public.dead_letter_log
        GROUP BY 1, 2, 3 ORDER BY 1
        """
    ).fetchall()
    assert kinds == [(False, "array", "array"), (True, "string", "array")]


def test_an_unstringified_array_is_caught_before_it_reaches_postgres(conn):
    """Why the Edit Fields nodes stay."""
    run = start_run(conn)
    record = validate_batch(run)["accepted_records"][0]
    with pytest.raises(NodeOperationError, match="raw array"):
        insert_node(conn, "jobs_extracted", [record])


# ---------------------------------------------------------------------------
# publication_date
# ---------------------------------------------------------------------------


def test_publication_date_is_stored_as_remotive_sent_it(conn):
    run = start_run(conn)
    batch = validate_batch(run)
    write_batch(conn, batch)

    # Under a non-UTC session, to show the value is not shifted on the way in.
    conn.execute("SET TIME ZONE 'Asia/Kolkata'")
    stored = dict(
        conn.execute(
            "SELECT source_job_id, to_char(publication_date, 'YYYY-MM-DD\"T\"HH24:MI:SS') "
            "FROM public.jobs_extracted WHERE run_id = %(r)s "
            "UNION ALL "
            "SELECT source_job_id, to_char(publication_date, 'YYYY-MM-DD\"T\"HH24:MI:SS') "
            "FROM public.dead_letter_log WHERE run_id = %(r)s",
            {"r": run},
        ).fetchall()
    )
    sent = {
        r["source_job_id"]: r["publication_date"]
        for r in batch["accepted_records"] + batch["rejected_records"]
    }
    assert len(stored) == 5
    assert stored == sent


def test_a_missing_or_malformed_publication_date_is_stored_as_null(conn):
    run = start_run(conn)
    batch = validate_batch(
        run,
        indexes=(1, 2, 3, 4),
        dates=["DELETE", "yesterday", "2026-08-21T05:54:39Z", "KEEP"],
    )
    assert batch["records_accepted"] == 4
    assert [w["code"] for w in batch["warnings"]] == [
        "PUBLICATION_DATE_MISSING",
        "PUBLICATION_DATE_MALFORMED",
        "PUBLICATION_DATE_MALFORMED",
    ]

    write_batch(conn, batch)

    stored = conn.execute(
        "SELECT publication_date IS NULL FROM public.jobs_extracted "
        "WHERE run_id = %s ORDER BY id",
        (run,),
    ).fetchall()
    assert stored == [(True,), (True,), (True,), (False,)]


def test_jobs_latest_shows_the_new_columns_for_a_new_row(conn):
    run = start_run(conn)
    batch = validate_batch(run, indexes=(1,))
    write_batch(conn, batch)

    record = batch["accepted_records"][0]
    row = conn.execute(
        "SELECT run_id, publication_date IS NOT NULL FROM public.jobs_latest "
        "WHERE source_job_id = %s",
        (record["source_job_id"],),
    ).fetchone()
    assert row == (run, True)


# ---------------------------------------------------------------------------
# Replay, and what the counts do and do not mean
# ---------------------------------------------------------------------------


def test_replaying_a_whole_batch_writes_nothing_the_second_time(conn):
    run = start_run(conn)
    batch = validate_batch(run)

    first = write_batch(conn, batch)
    second = write_batch(conn, batch)

    assert first == (4, 1)
    assert second == (0, 0)
    assert rows(conn, "jobs_extracted", run) == 4
    assert rows(conn, "dead_letter_log", run) == 1


def test_without_skip_on_conflict_a_replay_fails_the_node(conn):
    run = start_run(conn)
    batch = validate_batch(run)
    write_batch(conn, batch, skip_on_conflict=False)

    with pytest.raises(errors.UniqueViolation):
        write_batch(conn, batch, skip_on_conflict=False)
    assert rows(conn, "jobs_extracted", run) == 4


def test_validator_counts_are_verdicts_not_rows_written(conn):
    """The same posting twice in one batch: accepted twice, written once."""
    run = start_run(conn)
    batch = validate_batch(run, indexes=(1, 1, 2))
    assert batch["records_accepted"] == 3

    accepted, _ = write_batch(conn, batch)
    complete_run(conn, run, 13, batch)

    stored_count, written = conn.execute(
        "SELECT r.accepted_count, "
        "(SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id) "
        "FROM public.pipeline_runs r WHERE r.run_id = %s",
        (run,),
    ).fetchone()
    assert accepted == written == 2
    assert stored_count == 3
    assert stored_count - written == 1, "one row skipped as already present"


def test_the_same_postings_on_a_second_run_are_new_rows(conn):
    """Append-only across runs: a second sighting is a second observation."""
    first = start_run(conn, "100")
    second = start_run(conn, "101")

    write_batch(conn, validate_batch(first))
    write_batch(conn, validate_batch(second, ingested_at="2026-11-12T09:30:00.000Z"))

    assert rows(conn, "jobs_extracted", first) == 4
    assert rows(conn, "jobs_extracted", second) == 4
    sightings = conn.execute(
        "SELECT count(*), count(DISTINCT run_id), count(DISTINCT ingested_at) "
        "FROM public.jobs_extracted WHERE source_job_id = %s AND run_id IS NOT NULL",
        (validate_batch(first)["accepted_records"][0]["source_job_id"],),
    ).fetchone()
    assert sightings == (2, 2, 2)


def test_historical_rows_are_untouched_by_new_writes(migrated_db):
    before = fingerprint(migrated_db)

    with psycopg.connect(migrated_db, autocommit=True) as c:
        run = start_run(c)
        batch = validate_batch(run)
        write_batch(c, batch)
        write_batch(c, batch)
        complete_run(c, run, 13, batch)
        untouched = c.execute(
            "SELECT (SELECT count(*) FROM public.jobs_extracted WHERE run_id IS NULL), "
            "(SELECT count(*) FROM public.dead_letter_log WHERE run_id IS NULL)"
        ).fetchone()

    assert untouched == (25, 6)
    # Hash only the rows that were there before, over their original columns.
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("SET TIME ZONE 'UTC'")
        for table in ("jobs_extracted", "dead_letter_log"):
            columns = LEGACY_COLUMNS[table]
            count, digest = c.execute(
                f"SELECT count(*), md5(string_agg(md5(row({columns})::text), '' ORDER BY id)) "
                f"FROM public.{table} WHERE run_id IS NULL"
            ).fetchone()
            assert (count, digest) == before[table]


def test_a_historical_posting_seen_again_gets_a_new_row_beside_the_old_ones(conn):
    """Fixture 0 is posting 2091105, already dead-lettered six times with no run."""
    run = start_run(conn)
    batch = validate_batch(run, indexes=(0,))
    assert batch["rejected_records"][0]["source_job_id"] == "2091105"

    write_batch(conn, batch)

    by_run = conn.execute(
        "SELECT run_id IS NULL, count(*) FROM public.dead_letter_log "
        "WHERE source_job_id = '2091105' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert by_run == [(False, 1), (True, 6)]


# ---------------------------------------------------------------------------
# Closing the run
# ---------------------------------------------------------------------------


def test_complete_run_stores_the_validator_counts(conn):
    run = start_run(conn)
    batch = validate_batch(run)
    write_batch(conn, batch)

    returned = complete_run(conn, run, 13, batch)

    assert returned[0][0] == run and returned[0][1] == "completed"
    stored = conn.execute(
        "SELECT status, fetched_count, processed_count, accepted_count, rejected_count, "
        "completed_at >= started_at FROM public.pipeline_runs WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert stored == ("completed", 13, 5, 4, 1, True)
    assert stored[3] == rows(conn, "jobs_extracted", run)
    assert stored[4] == rows(conn, "dead_letter_log", run)


def test_complete_run_when_the_validator_never_ran(conn):
    """Remotive returned nothing: no items, so the Validator node is skipped."""
    run = start_run(conn)
    complete_run(conn, run, 0, None)
    stored = conn.execute(
        "SELECT status, fetched_count, processed_count, accepted_count, rejected_count "
        "FROM public.pipeline_runs WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert stored == ("completed", 0, 0, 0, 0)


def test_complete_run_with_an_empty_batch(conn):
    """An empty batch is only believable when nothing was fetched."""
    run = start_run(conn)
    batch = validate_batch(run, indexes=())
    assert write_batch(conn, batch) == (0, 0)
    with pytest.raises(errors.RaiseException, match="fetched 13 postings and submitted none"):
        complete_run(conn, run, 13, batch)
    complete_run(conn, run, 0, batch)
    assert conn.execute(
        "SELECT status, processed_count FROM public.pipeline_runs WHERE run_id = %s", (run,)
    ).fetchone() == ("completed", 0)


def test_complete_run_twice_changes_nothing_the_second_time(conn):
    run = start_run(conn)
    batch = validate_batch(run)
    write_batch(conn, batch)
    first = complete_run(conn, run, 13, batch)
    completed_at = first[0][2]

    second = complete_run(conn, run, 99, batch)
    # Columns: run_id, status, completed_at, ..., already_closed last.
    assert first[0][-1] is False and second[0][-1] is True
    stored = conn.execute(
        "SELECT fetched_count, completed_at FROM public.pipeline_runs WHERE run_id = %s",
        (run,),
    ).fetchone()
    assert stored == (13, completed_at)


def test_complete_run_does_not_close_someone_elses_run(conn):
    mine = start_run(conn, "100")
    other = start_run(conn, "101")
    batch = validate_batch(mine)
    write_batch(conn, batch)
    complete_run(conn, mine, 13, batch)
    status = conn.execute(
        "SELECT status FROM public.pipeline_runs WHERE run_id = %s", (other,)
    ).fetchone()[0]
    assert status == "running"


# ---------------------------------------------------------------------------
# Deployment order
# ---------------------------------------------------------------------------


def test_new_records_are_refused_by_an_unmigrated_database(legacy_db):
    """Change the canvas before migrating and every insert fails."""
    batch = validate_batch("3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b", indexes=(1,))
    record = edit_fields_accepted(batch["accepted_records"][0])

    with psycopg.connect(legacy_db, autocommit=True) as c:
        with pytest.raises(NodeOperationError, match="does not exist in selected table"):
            insert_node(c, "jobs_extracted", [record])
        with pytest.raises(errors.UndefinedColumn):
            c.execute(
                "INSERT INTO public.jobs_extracted (source_job_id, source_url, run_id) "
                "VALUES ('1', 'x', NULL)"
            )


def test_the_workflow_as_deployed_today_still_inserts_after_migrating(conn):
    """Old-shaped rows: validate() records with no run_id and no publication_date."""
    done = subprocess.run(
        ["node", "-e",
         "const {validate}=require('./validate.js');"
         "const s=require('./fixtures/source-records.json');"
         "const o=require('./fixtures/extraction-output.json');"
         "process.stdout.write(JSON.stringify([0,1].map(i=>validate({source:s[i],extracted:o[i]},"
         "'2026-11-11T09:30:00.123Z'))))"],
        cwd=REPO / "validator", capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    rejected, accepted = json.loads(done.stdout)

    # Edit Fields3 as exported: stringify tech_stack.
    insert_node(conn, "jobs_extracted", [edit_fields_accepted(accepted["record"])], False)

    # Edit Fields1 as exported: stringify both, and drop description_clean.
    old = dict(rejected["record"])
    old.pop("description_clean")
    old["error_codes"] = json.dumps(old["error_codes"])
    old["errors_json"] = json.dumps(old["errors_json"])
    insert_node(conn, "dead_letter_log", [old], False)

    assert conn.execute(
        "SELECT count(*) FROM public.jobs_extracted WHERE run_id IS NULL"
    ).fetchone()[0] == 26
    assert conn.execute(
        "SELECT count(*) FROM public.dead_letter_log WHERE run_id IS NULL"
    ).fetchone()[0] == 7
