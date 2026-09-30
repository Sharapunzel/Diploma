from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from ...models import ExternalConnection, Source


class SqlAlchemyExternalRepository:
    def __init__(self, session: Session):
        self.session = session

    def connections(self, limit: int, offset: int):
        total = self.session.scalar(select(func.count()).select_from(ExternalConnection)) or 0
        items = self.session.scalars(select(ExternalConnection).order_by(
            ExternalConnection.name, ExternalConnection.id).limit(limit).offset(offset)).all()
        return list(items), total

    def connection(self, identity: UUID, lock: bool = False):
        if lock:
            return self.session.scalar(select(ExternalConnection).where(
                ExternalConnection.id == identity).with_for_update().execution_options(populate_existing=True))
        return self.session.get(ExternalConnection, identity)

    def sources(self, limit: int, offset: int):
        query = select(Source).where(Source.source_type == "external")
        total = self.session.scalar(select(func.count()).select_from(query.subquery())) or 0
        items = self.session.scalars(query.order_by(Source.name, Source.id).limit(limit).offset(offset)).all()
        return list(items), total

    def source(self, identity: UUID, lock: bool = False):
        query = select(Source).where(Source.id == identity, Source.source_type == "external")
        if lock:
            query = query.with_for_update().execution_options(populate_existing=True)
        return self.session.scalar(query)

    def add(self, item):
        self.session.add(item)
        self.session.flush()

    def delete(self, item):
        self.session.delete(item)

    def disable_sources(self, identity: UUID) -> None:
        self.session.execute(update(Source).where(
            Source.external_connection_id == identity, Source.source_type == "external"
        ).values(is_enabled=False))
