"""Permanent detector-worker composition and process lifecycle."""
import signal
from threading import Event

from .adapters.detector_lock import DetectorSessionLock
from .adapters.wal_bootstrap import read_setting
from .db import Database
from .detector_composition import WorkerDetectorServices, bounded_database_url
from .ecs import PackagedEcsCatalog
from .services.implementations.detector_worker import DetectorWorker


def main():
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    url = read_setting("DATABASE_URL")
    database = Database(bounded_database_url(url))
    services = WorkerDetectorServices()
    try:
        DetectorWorker(database.session_factory, services, PackagedEcsCatalog.load(),
                       stop, DetectorSessionLock(url), release_runtime=services.close).run()
    finally:
        services.close()
        database.dispose()


if __name__ == "__main__":
    main()
