"""Run the backend suite against an isolated PostgreSQL test database.

The shared mode resets only the explicitly named diploma_test database and leaves
it clean at Alembic head. Temporary mode uses a unique database and removes it.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

BACKEND_DIR = Path(__file__).resolve().parents[1]
SHARED_NAME = "diploma_test"


def database_urls() -> tuple[str, str]:
    raw = os.environ.get("TEST_DATABASE_URL")
    if not raw:
        raise ValueError("Set TEST_DATABASE_URL to the dedicated diploma_test database")
    parsed = make_url(raw)
    if parsed.get_backend_name() != "postgresql" or parsed.database != SHARED_NAME:
        raise ValueError("TEST_DATABASE_URL must point exactly to PostgreSQL diploma_test")
    if parsed.query:
        raise ValueError("TEST_DATABASE_URL must not contain query parameters")
    admin_url = parsed.set(drivername="postgresql", database="postgres")
    return raw, admin_url.render_as_string(hide_password=False)


def database_exists(connection: psycopg.Connection, name: str) -> bool:
    return connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)", (name,)
    ).fetchone()[0]


def create_database(connection: psycopg.Connection, name: str) -> None:
    connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def drop_database(connection: psycopg.Connection, name: str) -> None:
    # Do not force-close another test run: concurrent use must fail visibly.
    connection.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def command(args: list[str], environment: dict[str, str]) -> int:
    return subprocess.run(
        [sys.executable, "-m", *args], cwd=BACKEND_DIR, env=environment, check=False
    ).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--shared", action="store_true", help="reset and reuse diploma_test")
    mode.add_argument("--temporary", action="store_true", help="create a unique disposable DB")
    args = parser.parse_args()

    base_url, admin_url = database_urls()
    name = SHARED_NAME if args.shared else f"diploma_test_review_{uuid4().hex[:12]}"
    test_url = make_url(base_url).set(database=name).render_as_string(hide_password=False)
    environment = {**os.environ, "TEST_DATABASE_URL": test_url, "DATABASE_URL": test_url}
    with psycopg.connect(admin_url, autocommit=True) as connection:
        if args.shared and database_exists(connection, name):
            drop_database(connection, name)
        create_database(connection, name)

    print(f"Running tests on {name}", flush=True)
    exit_code = 1
    restore_ok = True
    try:
        exit_code = command(["pytest", "-q"], environment)
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            if args.temporary:
                drop_database(connection, name)
            else:
                # Tests may intentionally downgrade migrations or leave fixture data.
                # Rebuild only the named test DB, then leave a clean, checked schema.
                drop_database(connection, name)
                create_database(connection, name)
        if args.shared:
            if command(["alembic", "upgrade", "head"], environment) != 0:
                print("Failed to restore diploma_test to Alembic head", file=sys.stderr)
                restore_ok = False
            elif command(["alembic", "check"], environment) != 0:
                print("diploma_test schema differs from models", file=sys.stderr)
                restore_ok = False
    return exit_code if restore_ok else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, psycopg.Error) as error:
        print(f"Test database setup failed: {error}", file=sys.stderr)
        sys.exit(1)
