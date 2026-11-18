"""The runner's own behaviour: ordering, atomicity, and refusing the wrong target."""

from __future__ import annotations

import psycopg
import pytest

import migrate

QUIET = lambda _line: None  # noqa: E731

NEON_LIKE = (
    "postgresql://someone:pw@ep-example-123456-pooler.eu-central-1.aws.neon.tech"
    "/neondb?sslmode=require"
)


def write(directory, name, sql):
    (directory / name).write_text(sql, encoding="utf-8")


def tables(conninfo):
    with psycopg.connect(conninfo) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
        }


# ---------------------------------------------------------------------------
# Target safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "conninfo",
    [
        "postgresql://u:p@localhost:5433/db",
        "postgresql://u:p@127.0.0.1/db",
        "host=localhost dbname=db",
        "dbname=db",
        "host=/var/run/postgresql dbname=db",
    ],
)
def test_local_targets_are_allowed(conninfo):
    migrate.check_target(conninfo)


def test_a_remote_target_is_refused_by_default():
    with pytest.raises(migrate.MigrationError, match="refusing to touch remote host"):
        migrate.check_target(NEON_LIKE)


def test_allow_remote_alone_is_not_enough():
    with pytest.raises(migrate.MigrationError, match="--confirm-host"):
        migrate.check_target(NEON_LIKE, allow_remote=True)


def test_the_confirmed_host_has_to_match_exactly():
    with pytest.raises(migrate.MigrationError, match="--confirm-host"):
        migrate.check_target(NEON_LIKE, allow_remote=True, confirm_host="neon.tech")


def test_a_remote_target_named_twice_is_allowed():
    migrate.check_target(
        NEON_LIKE,
        allow_remote=True,
        confirm_host="ep-example-123456-pooler.eu-central-1.aws.neon.tech",
    )


def test_a_local_host_does_not_excuse_a_remote_one_beside_it():
    with pytest.raises(migrate.MigrationError):
        migrate.check_target("postgresql://u:p@localhost,db.example.com/db")


def test_the_target_description_never_contains_the_password():
    described = migrate.describe(NEON_LIKE)
    assert "pw" not in described.split("/")[0].split(":")
    assert "someone" not in described
    assert described.endswith("/neondb")


def test_the_cli_ignores_database_url(monkeypatch, capsys):
    """An exported DATABASE_URL must never become the migration target."""
    monkeypatch.setenv("DATABASE_URL", NEON_LIKE)
    monkeypatch.setenv("NEON_DATABASE_URL", NEON_LIKE)
    monkeypatch.delenv("MIGRATE_DATABASE_URL", raising=False)

    assert migrate.main(["status"]) == 1
    assert "no target" in capsys.readouterr().err


def test_the_cli_refuses_a_remote_url_without_connecting(capsys):
    assert migrate.main(["--database-url", NEON_LIKE, "up"]) == 1
    assert "refusing to touch remote host" in capsys.readouterr().err


def test_the_cli_applies_and_reports(db, capsys):
    assert migrate.main(["--database-url", db, "up"]) == 0
    out = capsys.readouterr().out
    assert "applied  0001_baseline" in out
    assert "roleradar_test" not in out.replace("target", "")  # no credentials echoed

    assert migrate.main(["--database-url", db, "up"]) == 0
    assert "nothing to apply" in capsys.readouterr().out

    assert migrate.main(["--database-url", db, "status"]) == 0
    assert "pending" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Ordering and history
# ---------------------------------------------------------------------------


def test_status_writes_nothing(db):
    migrate.status(db)
    assert "schema_migrations" not in tables(db)


def test_up_to_stops_at_the_named_version(db):
    migrate.up(db, to="0002", log=QUIET)
    states = dict(migrate.status(db))
    assert states["0002_pipeline_runs"] == "applied"
    assert states["0003_run_correlation"] == "pending"


def test_an_edited_applied_migration_is_refused(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")
    migrate.up(db, directory=tmp_path, log=QUIET)

    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id bigint);")

    with pytest.raises(migrate.MigrationError, match="has changed since it was applied"):
        migrate.up(db, directory=tmp_path, log=QUIET)


def test_a_migration_slipped_in_behind_an_applied_one_is_refused(db, tmp_path):
    write(tmp_path, "0002_two.sql", "CREATE TABLE public.two (id int);")
    migrate.up(db, directory=tmp_path, log=QUIET)

    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")

    with pytest.raises(migrate.MigrationError, match="must apply in order"):
        migrate.up(db, directory=tmp_path, log=QUIET)


def test_a_database_ahead_of_the_checkout_is_refused(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")
    migrate.up(db, directory=tmp_path, log=QUIET)
    (tmp_path / "0001_one.sql").unlink()
    write(tmp_path, "0002_two.sql", "CREATE TABLE public.two (id int);")

    with pytest.raises(migrate.MigrationError, match="no such file"):
        migrate.up(db, directory=tmp_path, log=QUIET)


@pytest.mark.parametrize("name", ["1_short.sql", "0001-dashes.sql", "0001_Caps.sql"])
def test_badly_named_files_are_refused(tmp_path, name):
    write(tmp_path, name, "SELECT 1;")
    with pytest.raises(migrate.MigrationError, match="is not named"):
        migrate.discover(tmp_path)


def test_two_files_with_one_version_are_refused(tmp_path):
    write(tmp_path, "0001_one.sql", "SELECT 1;")
    write(tmp_path, "0001_other.sql", "SELECT 1;")
    with pytest.raises(migrate.MigrationError, match="share version"):
        migrate.discover(tmp_path)


def test_an_orphan_down_file_is_refused(tmp_path):
    write(tmp_path, "0001_one.down.sql", "SELECT 1;")
    with pytest.raises(migrate.MigrationError, match="no matching up file"):
        migrate.discover(tmp_path)


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


def test_a_failing_migration_leaves_nothing_behind(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")
    write(
        tmp_path,
        "0002_broken.sql",
        "CREATE TABLE public.half_done (id int);\nSELECT 1 / 0;",
    )

    with pytest.raises(psycopg.Error):
        migrate.up(db, directory=tmp_path, log=QUIET)

    assert "one" in tables(db)
    assert "half_done" not in tables(db)
    assert dict(migrate.status(db, directory=tmp_path)) == {
        "0001_one": "applied",
        "0002_broken": "pending",
    }


def test_a_migration_may_not_commit_on_its_own(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);\nCOMMIT;")
    with pytest.raises(migrate.MigrationError, match="BEGIN, COMMIT or ROLLBACK"):
        migrate.up(db, directory=tmp_path, log=QUIET)
    assert "one" not in tables(db)


def test_plpgsql_begin_end_is_not_mistaken_for_transaction_control(db, tmp_path):
    write(
        tmp_path,
        "0001_one.sql",
        "DO $$\nBEGIN\n    CREATE TABLE public.one (id int);\nEND\n$$;",
    )
    migrate.up(db, directory=tmp_path, log=QUIET)
    assert "one" in tables(db)


# ---------------------------------------------------------------------------
# Down
# ---------------------------------------------------------------------------


def test_the_baseline_cannot_be_reverted(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")
    write(tmp_path, "0002_two.sql", "CREATE TABLE public.two (id int);")
    migrate.up(db, directory=tmp_path, log=QUIET)

    with pytest.raises(migrate.MigrationError, match="no migration with version 0000"):
        migrate.down(db, to="0000", directory=tmp_path, log=QUIET)


def test_a_missing_down_file_stops_the_revert_before_it_starts(db, tmp_path):
    write(tmp_path, "0001_one.sql", "CREATE TABLE public.one (id int);")
    write(tmp_path, "0002_two.sql", "CREATE TABLE public.two (id int);")
    write(tmp_path, "0003_three.sql", "CREATE TABLE public.three (id int);")
    write(tmp_path, "0003_three.down.sql", "DROP TABLE public.three;")
    migrate.up(db, directory=tmp_path, log=QUIET)

    with pytest.raises(migrate.MigrationError, match="cannot be reverted"):
        migrate.down(db, to="0001", directory=tmp_path, log=QUIET)

    # 0003 had a down file and would have gone first. It must still be there.
    assert "three" in tables(db)


# ---------------------------------------------------------------------------
# Baseline shape check
# ---------------------------------------------------------------------------


def test_the_baseline_refuses_the_old_upsert_shaped_table(db):
    """The schema before the primary-key fix: keyed on source_job_id, no id.

    This is the shape sql/_actual_schema.sql recorded, and a local snapshot
    could still have it.
    """
    from conftest import FIXTURES

    old_shape = (
        (FIXTURES / "legacy_schema.sql")
        .read_text(encoding="utf-8")
        .replace("source_job_id       text NOT NULL,", "source_job_id       text PRIMARY KEY,", 1)
        .replace(
            "    id                  bigserial,\n"
            "    CONSTRAINT jobs_extracted_pkey PRIMARY KEY (id)\n",
            "    placeholder_last    text\n",
            1,
        )
    )
    with psycopg.connect(db, autocommit=True) as c:
        c.execute(old_shape)
        assert "id" not in {
            r[0]
            for r in c.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'jobs_extracted'"
            )
        }

    with pytest.raises(psycopg.Error, match="baseline mismatch.*jobs_extracted.id"):
        migrate.up(db, log=QUIET)

    # Refused as a whole: nothing recorded, nothing half-applied.
    assert migrate.status(db)[0] == ("0001_baseline", "pending")
    assert "schema_migrations" not in tables(db)


def test_the_baseline_refuses_a_primary_key_on_the_wrong_column(db):
    from conftest import FIXTURES, run_file

    run_file(db, FIXTURES / "legacy_schema.sql")
    with psycopg.connect(db, autocommit=True) as c:
        c.execute("ALTER TABLE public.jobs_extracted DROP CONSTRAINT jobs_extracted_pkey")
        c.execute("ALTER TABLE public.jobs_extracted ADD PRIMARY KEY (source_job_id)")

    with pytest.raises(psycopg.Error, match="primary key is not"):
        migrate.up(db, log=QUIET)
