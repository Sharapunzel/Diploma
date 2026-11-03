"""Exit successfully only while capture is ready to accept new records."""

import argparse
import json
from dataclasses import asdict

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .adapters.replication_slot import PostgreSQLReplicationSlotAdapter
from .repositories.sqlalchemy.capture import SqlAlchemyCaptureStateRepository
from .services.implementations.wal_guard import CaptureGuard
from .wal_bootstrap import read_setting


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--details", action="store_true")
    args = parser.parse_args()
    engine = create_engine(read_setting("WAL_DATABASE_URL"), pool_pre_ping=True)
    try:
        with Session(engine) as session:
            status = CaptureGuard(SqlAlchemyCaptureStateRepository(session),
                                  PostgreSQLReplicationSlotAdapter(session)).status()
            if args.details:
                print(json.dumps(asdict(status), default=str))
            if not status.ready:
                raise SystemExit(status.reason)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
