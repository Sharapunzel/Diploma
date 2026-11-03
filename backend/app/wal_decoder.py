"""Decode the availability slot, committing materialization before feedback."""

import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from .adapters.pgoutput import PgOutputDecoder
from .adapters.pgoutput_stream import ReplicationStream
from .adapters.wal_bootstrap import PUBLICATION, ROLE, SLOT, read_setting
from .repositories.sqlalchemy import SqlAlchemyUnitOfWork
from .repositories.sqlalchemy.availability import SqlAlchemyAvailabilityProgressRepository
from .services.implementations.availability_materializer import AvailabilityMaterializer

LOGGER = logging.getLogger(__name__)


def main() -> None:
    reader_url = make_url(read_setting("WAL_DATABASE_URL"))
    if reader_url.username != ROLE or reader_url.drivername != "postgresql+psycopg":
        raise ValueError("WAL_DATABASE_URL must use the dedicated reader role and psycopg")
    engine = create_engine(reader_url, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    dsn = reader_url.set(drivername="postgresql").render_as_string(hide_password=False)
    decoder = PgOutputDecoder("logs", "availability_markers", ("id", "source_identity", "kind", "event_id"))
    try:
        with ReplicationStream(dsn, SLOT, PUBLICATION, decoder) as stream:
            next_heartbeat = 0.0
            while True:
                now = time.monotonic()
                if now >= next_heartbeat:
                    with sessions() as session:
                        AvailabilityMaterializer(
                            SqlAlchemyAvailabilityProgressRepository(session),
                            SqlAlchemyUnitOfWork(session), stream,
                        ).heartbeat()
                    next_heartbeat = now + 5
                commit = stream.read_commit(1)
                if commit is not None:
                    with sessions() as session:
                        AvailabilityMaterializer(
                            SqlAlchemyAvailabilityProgressRepository(session),
                            SqlAlchemyUnitOfWork(session), stream,
                        ).apply(commit)
    except Exception:
        LOGGER.exception("availability_decoder_stopped")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
