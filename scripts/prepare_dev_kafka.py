"""Create disposable, local-only Kafka credentials for Docker development."""

import os
import secrets
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1] / ".dev"
    root.mkdir(exist_ok=True)
    for name in ("kafka_admin_password", "kafka_reader_password"):
        path = root / name
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(secrets.token_urlsafe(48) + "\n")
    print("Local Kafka test credentials are ready in .dev")


if __name__ == "__main__":
    main()
