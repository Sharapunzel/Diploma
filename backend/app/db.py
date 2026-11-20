from collections.abc import Generator

from fastapi import Request
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .db_deadline import DeadlineQueuePool, install_deadline_hooks

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Database:
    def __init__(self, database_url: str):
        url = make_url(database_url)
        options = {"poolclass": DeadlineQueuePool} if url.get_backend_name() == "postgresql" and url.get_driver_name() == "psycopg" else {}
        self.engine: Engine = create_engine(database_url, pool_pre_ping=True, **options)
        install_deadline_hooks(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=Session,
            expire_on_commit=False,
        )

    def dispose(self) -> None:
        self.engine.dispose()


def get_session(request: Request) -> Generator[Session, None, None]:
    with request.app.state.database.session_factory() as session:
        yield session
