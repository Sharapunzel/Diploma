import argparse
import getpass
import sys
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from .config import settings
from .core.security import hash_password
from .db import Database
from .repositories.sqlalchemy import SqlAlchemyRoleRepository, SqlAlchemyUserRepository

ADMIN_ROLE_ID = UUID("00000000-0000-4000-8000-000000000001")


def create_admin(
    username: str,
    display_name: str,
    database: Database | None = None,
) -> int:
    first = getpass.getpass("Password: ")
    second = getpass.getpass("Repeat password: ")
    if first != second:
        print("Passwords do not match", file=sys.stderr)
        return 2
    if not 12 <= len(first) <= 128:
        print("Password length must be between 12 and 128 characters", file=sys.stderr)
        return 2
    if not username.strip() or not display_name.strip():
        print("Username and display name must not be blank", file=sys.stderr)
        return 2

    owned_database = database is None
    configured_database = database or Database(settings.database_url)
    try:
        with configured_database.session_factory() as session:
            users = SqlAlchemyUserRepository(session)
            roles = SqlAlchemyRoleRepository(session)
            if users.find_local(username) is not None:
                print("Username already exists", file=sys.stderr)
                return 3
            role = roles.find_by_id(ADMIN_ROLE_ID)
            if role is None:
                print("Administrator role is not initialized", file=sys.stderr)
                return 4
            users.create_local(username, hash_password(first), display_name, role.id)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                print("Username already exists", file=sys.stderr)
                return 3
    finally:
        if owned_database:
            configured_database.dispose()
    print("Administrator created")
    return 0


def main():
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    admin = commands.add_parser("create-admin")
    admin.add_argument("--username", required=True)
    admin.add_argument("--display-name", required=True)
    args = parser.parse_args()
    if args.command == "create-admin":
        raise SystemExit(create_admin(args.username, args.display_name))


if __name__ == "__main__":
    main()
