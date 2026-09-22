from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, LargeBinary, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class AuthSession(Base):
    __tablename__ = "auth_sessions"
    __table_args__ = (
        CheckConstraint("octet_length(token_hash) = 32", name="token_hash_sha256"),
        CheckConstraint("octet_length(csrf_token_hash) = 32", name="csrf_token_hash_sha256"),
        CheckConstraint("last_seen_at >= created_at", name="last_seen_after_created"),
        CheckConstraint("last_seen_at <= expires_at", name="last_seen_before_expiry"),
        CheckConstraint("expires_at > created_at", name="expires_after_created"),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="revoked_after_created",
        ),
        CheckConstraint("authentication_method IN ('local', 'oidc')", name="auth_method_valid"),
        Index("ix_auth_sessions_user_id", "user_id"),
        Index("ix_auth_sessions_expiry", "expires_at"),
        {"schema": "app"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    user_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.users.id", ondelete="SET NULL")
    )
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, unique=True, nullable=False)
    csrf_token_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    authentication_method: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
