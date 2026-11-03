"""Entrypoint for logical publication, reader role and slot bootstrap."""

from .adapters.wal_bootstrap import bootstrap, read_setting


def main() -> None:
    bootstrap(read_setting("DATABASE_URL"), read_setting("WAL_READER_PASSWORD"))


if __name__ == "__main__":
    main()
