"""Application-owned position and heartbeat of the availability decoder."""

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...models import ReplicationProgress


class SqlAlchemyCaptureStateRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def read_durable_progress(self) -> tuple[int, datetime | None, datetime] | None:
        row = self.session.execute(select(
            ReplicationProgress.durable_lsn,
            ReplicationProgress.heartbeat_at,
            func.clock_timestamp(),
        ).where(ReplicationProgress.id == 1)).one_or_none()
        return (int(row[0]), row[1], row[2]) if row is not None else None
