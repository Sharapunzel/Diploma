"""Create local-only Kafka and WAL reader credentials for Docker development."""

import os
import secrets
import stat
import sys
from pathlib import Path


def prepare(root: Path) -> None:
    root.mkdir(exist_ok=True)
    names = ("kafka_admin_password", "kafka_reader_password", "wal_reader_password")
    # Validate everything before creating anything. Never repair unknown objects.
    for name in names:
        validate_existing(root / name)
    for name in names:
        path = root / name
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            validate_existing(path)
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            value = "diploma_wal_dev_password_2026" if name == "wal_reader_password" else secrets.token_urlsafe(48)
            output.write(value + "\n")


def validate_existing(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or not path.read_bytes().strip():
        raise ValueError(f"Invalid credential path: {path.name}; expected a nonempty regular file")


def main() -> None:
    try:
        prepare(Path(__file__).resolve().parents[1] / ".dev")
    except (OSError, ValueError) as error:
        print(f"Credential preparation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    print("Local Kafka and WAL reader credentials are ready in .dev")


if __name__ == "__main__":
    main()
