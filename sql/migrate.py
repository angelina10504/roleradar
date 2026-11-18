#!/usr/bin/env python3
"""RoleRadar migration runner.

Applies the numbered files in sql/migrations/ in order and records each one in
public.schema_migrations.

    python sql/migrate.py status
    python sql/migrate.py up
    python sql/migrate.py up --to 0004
    python sql/migrate.py down --to 0003

The target comes from --database-url or MIGRATE_DATABASE_URL. It deliberately
does not read DATABASE_URL or NEON_DATABASE_URL: those point at the hosted
warehouse, and a migration should never reach it because a variable happened
to be exported in the shell.

A host that is not this machine is refused unless both --allow-remote and
--confirm-host <hostname> are given, and the hostname has to match the one in
the connection string.

Each migration runs in one transaction together with its ledger row, so a file
either applies completely and is recorded, or leaves nothing behind.

The only dependency is psycopg, which the API already uses.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg
from psycopg.conninfo import conninfo_to_dict

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

# Arbitrary constant. Two runners against one database queue behind it.
LOCK_KEY = 726_001

UP_FILE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

# The runner owns the transaction. A file that commits on its own would leave
# the ledger out of step with the schema.
TRANSACTION_CONTROL = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\s*;",
    re.IGNORECASE | re.MULTILINE,
)

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     text PRIMARY KEY,
    name        text NOT NULL,
    checksum    text NOT NULL,
    applied_at  timestamp with time zone NOT NULL DEFAULT now()
)
"""


class MigrationError(Exception):
    """Anything that should stop the runner with a message, not a traceback."""


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    up_path: Path
    down_path: Optional[Path]

    @property
    def label(self) -> str:
        return f"{self.version}_{self.name}"

    def up_sql(self) -> str:
        return _read_sql(self.up_path)

    def down_sql(self) -> str:
        if self.down_path is None:
            raise MigrationError(
                f"{self.label} has no .down.sql file and cannot be reverted"
            )
        return _read_sql(self.down_path)

    def checksum(self) -> str:
        return hashlib.sha256(self.up_path.read_bytes()).hexdigest()


def _read_sql(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    if TRANSACTION_CONTROL.search(text):
        raise MigrationError(
            f"{path.name} contains BEGIN, COMMIT or ROLLBACK. The runner wraps "
            "each file in its own transaction; remove the statement."
        )
    return text


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Every migration in the directory, oldest first."""
    if not directory.is_dir():
        raise MigrationError(f"migrations directory not found: {directory}")

    found: dict[str, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        if path.name.endswith(".down.sql"):
            continue
        match = UP_FILE.match(path.name)
        if not match:
            raise MigrationError(
                f"{path.name} is not named NNNN_lower_snake_case.sql"
            )
        version, name = match.groups()
        if version in found:
            raise MigrationError(
                f"two migrations share version {version}: "
                f"{found[version].up_path.name} and {path.name}"
            )
        down = path.with_name(f"{version}_{name}.down.sql")
        found[version] = Migration(
            version, name, path, down if down.exists() else None
        )

    for path in directory.glob("*.down.sql"):
        if not path.with_name(path.name.replace(".down.sql", ".sql")).exists():
            raise MigrationError(f"{path.name} has no matching up file")

    return [found[v] for v in sorted(found)]


# --------------------------------------------------------------------------
# Target
# --------------------------------------------------------------------------


def hosts_of(conninfo: str) -> list[str]:
    """The hosts a connection string points at. Empty means a local socket."""
    params = conninfo_to_dict(conninfo)
    raw = params.get("host") or params.get("hostaddr") or ""
    return [h.strip() for h in str(raw).split(",") if h.strip()]


def is_local(conninfo: str) -> bool:
    return all(h in LOCAL_HOSTS or h.startswith("/") for h in hosts_of(conninfo))


def describe(conninfo: str) -> str:
    """Host, port and database, for messages. Never the password or the URL."""
    params = conninfo_to_dict(conninfo)
    host = ",".join(hosts_of(conninfo)) or "(local socket)"
    return f"{host}:{params.get('port', 5432)}/{params.get('dbname', '')}"


def check_target(
    conninfo: str, allow_remote: bool = False, confirm_host: Optional[str] = None
) -> None:
    """Refuse a remote database unless it was asked for twice, by name."""
    if is_local(conninfo):
        return
    hosts = hosts_of(conninfo)
    if not allow_remote:
        raise MigrationError(
            f"refusing to touch remote host {', '.join(hosts)}. This runner "
            "only migrates a local database by default. To migrate a remote "
            "one, pass --allow-remote and --confirm-host <hostname>."
        )
    if confirm_host is None or [confirm_host] != hosts:
        raise MigrationError(
            "--confirm-host does not match the host in the connection string. "
            f"Expected exactly: {', '.join(hosts)}"
        )


# --------------------------------------------------------------------------
# Ledger
# --------------------------------------------------------------------------


def applied(conn: psycopg.Connection) -> dict[str, str]:
    """version -> checksum for everything recorded. Reads only."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.schema_migrations')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute("SELECT version, checksum FROM public.schema_migrations")
        return dict(cur.fetchall())


def check_history(migrations: list[Migration], done: dict[str, str]) -> None:
    """The ledger and the files have to tell the same story before anything runs."""
    by_version = {m.version: m for m in migrations}

    for version, checksum in sorted(done.items()):
        migration = by_version.get(version)
        if migration is None:
            raise MigrationError(
                f"the database has migration {version} applied, but there is "
                "no such file. Is this the right checkout?"
            )
        if migration.checksum() != checksum:
            raise MigrationError(
                f"{migration.up_path.name} has changed since it was applied. "
                "Never edit an applied migration; write a new one."
            )

    pending = [m.version for m in migrations if m.version not in done]
    if pending and done and min(pending) < max(done):
        raise MigrationError(
            f"migration {min(pending)} is pending but {max(done)} is already "
            "applied. Migrations must apply in order."
        )


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------


def _connect(conninfo: str) -> psycopg.Connection:
    # autocommit, so the only transactions are the explicit ones below.
    return psycopg.connect(conninfo, autocommit=True, connect_timeout=15)


def status(conninfo: str, directory: Path = MIGRATIONS_DIR) -> list[tuple[str, str]]:
    """(label, 'applied' | 'pending') per migration. Writes nothing."""
    migrations = discover(directory)
    with _connect(conninfo) as conn:
        done = applied(conn)
    check_history(migrations, done)
    return [
        (m.label, "applied" if m.version in done else "pending")
        for m in migrations
    ]


def up(
    conninfo: str,
    directory: Path = MIGRATIONS_DIR,
    to: Optional[str] = None,
    log=print,
) -> list[str]:
    """Apply pending migrations, oldest first. Returns the labels applied."""
    migrations = discover(directory)
    if to is not None and to not in {m.version for m in migrations}:
        raise MigrationError(f"no migration with version {to}")

    ran: list[str] = []
    with _connect(conninfo) as conn:
        check_history(migrations, applied(conn))
        for migration in migrations:
            if to is not None and migration.version > to:
                break
            sql = migration.up_sql()
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                    cur.execute(LEDGER_DDL)
                    cur.execute(
                        "SELECT 1 FROM public.schema_migrations WHERE version = %s",
                        (migration.version,),
                    )
                    if cur.fetchone():
                        continue
                    # No parameters, so psycopg sends the file as one simple
                    # query and several statements are allowed.
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO public.schema_migrations "
                        "(version, name, checksum) VALUES (%s, %s, %s)",
                        (migration.version, migration.name, migration.checksum()),
                    )
            log(f"applied  {migration.label}")
            ran.append(migration.label)
    return ran


def down(
    conninfo: str,
    to: str,
    directory: Path = MIGRATIONS_DIR,
    log=print,
) -> list[str]:
    """Revert every applied migration newer than `to`, newest first."""
    migrations = discover(directory)
    if to not in {m.version for m in migrations}:
        raise MigrationError(f"no migration with version {to}")

    ran: list[str] = []
    with _connect(conninfo) as conn:
        done = applied(conn)
        check_history(migrations, done)
        targets = [
            m for m in reversed(migrations) if m.version in done and m.version > to
        ]
        # Read every down file first, so a missing one stops the run before
        # anything is reverted instead of halfway through.
        scripts = [(m, m.down_sql()) for m in targets]
        for migration, sql in scripts:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                    cur.execute(sql)
                    cur.execute(
                        "DELETE FROM public.schema_migrations WHERE version = %s",
                        (migration.version,),
                    )
            log(f"reverted {migration.label}")
            ran.append(migration.label)
    return ran


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply or revert RoleRadar schema migrations."
    )
    parser.add_argument(
        "--database-url",
        help="target database. Falls back to MIGRATE_DATABASE_URL. "
        "DATABASE_URL and NEON_DATABASE_URL are never read.",
    )
    parser.add_argument("--allow-remote", action="store_true")
    parser.add_argument("--confirm-host", metavar="HOSTNAME")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="list migrations and whether each is applied")
    up_parser = commands.add_parser("up", help="apply pending migrations")
    up_parser.add_argument("--to", metavar="NNNN", help="stop after this version")
    down_parser = commands.add_parser(
        "down", help="revert migrations newer than --to. Drops columns and their data."
    )
    down_parser.add_argument("--to", metavar="NNNN", required=True)
    args = parser.parse_args(argv)

    conninfo = args.database_url or os.environ.get("MIGRATE_DATABASE_URL")
    try:
        if not conninfo:
            raise MigrationError(
                "no target. Pass --database-url or set MIGRATE_DATABASE_URL."
            )
        check_target(conninfo, args.allow_remote, args.confirm_host)
        print(f"target   {describe(conninfo)}")

        if args.command == "status":
            for label, state in status(conninfo):
                print(f"{state:<8} {label}")
        elif args.command == "up":
            if not up(conninfo, to=args.to):
                print("nothing to apply")
        elif args.command == "down":
            if not down(conninfo, to=args.to):
                print("nothing to revert")
    except MigrationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except psycopg.Error as exc:
        # Whatever was in flight has been rolled back by the transaction block.
        print(f"error: database refused: {str(exc).strip()}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
