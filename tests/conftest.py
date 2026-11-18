"""Shared fixtures for the database tests.

Every test gets its own freshly created database on the disposable server from
docker-compose.test.yml, and that database is dropped afterwards. Nothing is
shared between tests and nothing outlives the run.

The server comes from TEST_DATABASE_URL, defaulting to the compose file's
127.0.0.1:5433. Anything that is not this machine is refused outright: these
tests create and drop databases.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

REPO = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# No test may reach a real database by accident. The API falls back to
# DATABASE_URL and then to localhost:5432, which is the real local warehouse;
# a shell with NEON_DATABASE_URL exported points at production. Strip every
# variable that names a real database before any test module is imported, and
# point DATABASE_URL at a port nothing listens on. A test that forgets to set
# its own connection then fails with "connection refused" instead of quietly
# reading, or writing, somewhere it should not.
UNREACHABLE = "host=127.0.0.1 port=1 dbname=no_database_configured user=nobody connect_timeout=1"
for _name in ("NEON_DATABASE_URL", "MIGRATE_DATABASE_URL", "OPS_TOKEN",
              "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
              "POSTGRES_USER", "POSTGRES_PASSWORD"):
    os.environ.pop(_name, None)
os.environ["DATABASE_URL"] = UNREACHABLE

# sql/ is not a package. Put it on the path so the runner imports as `migrate`.
sys.path.insert(0, str(REPO / "sql"))

import migrate  # noqa: E402

DEFAULT_SERVER = "postgresql://roleradar_test:roleradar_test@127.0.0.1:5433/roleradar_test"
SERVER = os.environ.get("TEST_DATABASE_URL", DEFAULT_SERVER)


def _refuse_unless_disposable() -> None:
    params = conninfo_to_dict(SERVER)
    if not migrate.is_local(SERVER):
        pytest.exit("TEST_DATABASE_URL is not a local host. Refusing to run.", 2)
    # 5432 is where the real local warehouse lives.
    if str(params.get("port", "5432")) == "5432":
        pytest.exit(
            "TEST_DATABASE_URL points at port 5432, the real local Postgres. "
            "Refusing to run. Use docker-compose.test.yml, which listens on 5433.",
            2,
        )


@pytest.fixture(scope="session")
def server() -> str:
    _refuse_unless_disposable()
    try:
        with psycopg.connect(SERVER, connect_timeout=5):
            pass
    except psycopg.OperationalError as exc:
        pytest.exit(
            "test database is not reachable. Start it with:\n"
            "  docker compose -f docker-compose.test.yml up -d --wait\n"
            f"({str(exc).strip()})",
            2,
        )
    return SERVER


@pytest.fixture
def db(server: str):
    """Connection string for an empty database that exists for one test."""
    name = f"rr_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(server, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        yield make_conninfo(server, dbname=name)
    finally:
        with psycopg.connect(server, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


def run_file(conninfo: str, path: Path) -> None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(path.read_text(encoding="utf-8"))


@pytest.fixture
def legacy_db(db: str) -> str:
    """A database shaped and filled like the live warehouse before migrations."""
    run_file(db, FIXTURES / "legacy_schema.sql")
    run_file(db, FIXTURES / "legacy_rows.sql")
    return db


@pytest.fixture
def migrated_db(legacy_db: str) -> str:
    """The legacy database with every migration applied."""
    migrate.up(legacy_db, log=lambda _line: None)
    return legacy_db


@pytest.fixture
def conn(migrated_db: str):
    """An autocommit connection to the migrated legacy database."""
    with psycopg.connect(migrated_db, autocommit=True) as connection:
        yield connection
