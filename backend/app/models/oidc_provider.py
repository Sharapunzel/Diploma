from datetime import datetime
from uuid import UUID

from sqlalchemy import JSON, CheckConstraint, DateTime, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class OidcProvider(Base):
    __tablename__ = "oidc_provider"
    __table_args__ = (
        CheckConstraint("id = 1", name="single_provider"),
        CheckConstraint("mode IN ('env', 'managed', 'disabled')", name="mode_valid"),
        {"schema": "app"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    active_version: Mapped[int | None] = mapped_column(Integer)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    draft: Mapped[dict | None] = mapped_column(JSON)
    active: Mapped[dict | None] = mapped_column(JSON)
    checked_version: Mapped[int | None] = mapped_column(Integer)
    trial_version: Mapped[int | None] = mapped_column(Integer)
    trial_session_id: Mapped[UUID | None]
    trial_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
