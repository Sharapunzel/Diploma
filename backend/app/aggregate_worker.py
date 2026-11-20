"""Composition root for the permanent aggregate worker (no Kafka credentials)."""

import json
import signal
from pathlib import Path
from threading import Event

from .adapters.wal_bootstrap import read_setting
from .aggregate_composition import aggregate_limits, build_aggregate_service
from .db import Database
from .ecs import PackagedEcsCatalog
from .services.implementations.aggregate_worker import AggregateWorker


def main():
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    database = Database(read_setting("DATABASE_URL"))
    try:
        status = Path("/tmp/aggregate-worker.json")
        def observe(payload):
            temporary = status.with_suffix(".next")
            temporary.write_text(json.dumps(payload), encoding="utf-8")
            temporary.replace(status)
        AggregateWorker(database.session_factory, build_aggregate_service, PackagedEcsCatalog.load(), stop,
            operation_memory_mb=aggregate_limits().total_memory_mb, observe=observe).run()
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
