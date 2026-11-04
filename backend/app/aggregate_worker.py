"""Composition root for the permanent aggregate worker (no Kafka credentials)."""

import signal
from threading import Event

from .adapters.wal_bootstrap import read_setting
from .aggregate_composition import build_aggregate_service
from .db import Database
from .ecs import PackagedEcsCatalog
from .services.implementations.aggregate_worker import AggregateWorker


def main():
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    database = Database(read_setting("DATABASE_URL"))
    try:
        AggregateWorker(database.session_factory, build_aggregate_service, PackagedEcsCatalog.load(), stop).run()
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
