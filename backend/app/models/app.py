from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base
from .common import TimestampMixin


class Role(TimestampMixin, Base):
    __tablename__ = "roles"
    __table_args__ = (
        CheckConstraint(
            "jsonb_typeof(permissions) = 'array'", name="permissions_array"
        ),
        CheckConstraint("priority >= 0", name="priority_nonnegative"),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    permissions: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)


class User(TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "(username IS NULL) = (password_hash IS NULL)",
            name="local_credentials_pair",
        ),
        CheckConstraint(
            "(oidc_issuer IS NULL) = (oidc_subject IS NULL)",
            name="oidc_credentials_pair",
        ),
        CheckConstraint(
            "(username IS NOT NULL AND password_hash IS NOT NULL) OR "
            "(oidc_issuer IS NOT NULL AND oidc_subject IS NOT NULL)",
            name="one_login_method",
        ),
        CheckConstraint(
            "username IS NULL OR btrim(username) <> ''", name="username_not_blank"
        ),
        CheckConstraint(
            "password_hash IS NULL OR btrim(password_hash) <> ''",
            name="password_hash_not_blank",
        ),
        CheckConstraint(
            "oidc_issuer IS NULL OR btrim(oidc_issuer) <> ''",
            name="oidc_issuer_not_blank",
        ),
        CheckConstraint(
            "oidc_subject IS NULL OR btrim(oidc_subject) <> ''",
            name="oidc_subject_not_blank",
        ),
        CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        Index("ix_users_role_id", "role_id"),
        Index(
            "uq_users_username_lower",
            text("lower(username)"),
            unique=True,
            postgresql_where=text("username IS NOT NULL"),
        ),
        Index(
            "uq_users_oidc_identity",
            "oidc_issuer",
            "oidc_subject",
            unique=True,
            postgresql_where=text(
                "oidc_issuer IS NOT NULL AND oidc_subject IS NOT NULL"
            ),
        ),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    username: Mapped[str | None] = mapped_column(Text)
    password_hash: Mapped[str | None] = mapped_column(Text)
    oidc_issuer: Mapped[str | None] = mapped_column(Text)
    oidc_subject: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    role_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.roles.id", ondelete="SET NULL")
    )
    role_managed_by_oidc: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, server_default=text("true"), nullable=False
    )
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OidcRoleMapping(TimestampMixin, Base):
    __tablename__ = "oidc_role_mappings"
    __table_args__ = (
        UniqueConstraint("issuer", "claim_name", "claim_value"),
        Index("ix_oidc_role_mappings_role_id", "role_id"),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    issuer: Mapped[str] = mapped_column(Text, nullable=False)
    claim_name: Mapped[str] = mapped_column(Text, nullable=False)
    claim_value: Mapped[str] = mapped_column(Text, nullable=False)
    role_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.roles.id", ondelete="SET NULL")
    )


class AppSetting(TimestampMixin, Base):
    __tablename__ = "app_settings"
    __table_args__ = (
        CheckConstraint("btrim(key) <> ''", name="key_not_blank"),
        CheckConstraint("btrim(category) <> ''", name="category_not_blank"),
        CheckConstraint("version > 0", name="version_positive"),
        Index("ix_app_settings_updated_by_user_id", "updated_by_user_id"),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    key: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    value: Mapped[Any] = mapped_column(JSONB, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(
        Integer, server_default=text("1"), nullable=False
    )
    is_public: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )
    updated_by_user_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.users.id", ondelete="SET NULL")
    )


class Normalizer(TimestampMixin, Base):
    __tablename__ = "normalizers"
    __table_args__ = (
        CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        CheckConstraint("version > 0", name="version_positive"),
        Index("ix_normalizers_created_by_user_id", "created_by_user_id"),
        Index("ix_normalizers_updated_by_user_id", "updated_by_user_id"),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    rule: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(
        Integer, server_default=text("1"), nullable=False
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.users.id", ondelete="SET NULL")
    )
    updated_by_user_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.users.id", ondelete="SET NULL")
    )


class KafkaConnection(TimestampMixin, Base):
    __tablename__ = "kafka_connections"
    __table_args__ = (
        CheckConstraint(
            "cardinality(bootstrap_servers) >= 1 AND "
            "btrim(array_to_string(bootstrap_servers, '')) <> ''",
            name="bootstrap_servers_nonempty",
        ),
        CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        CheckConstraint(
            "btrim(security_protocol) <> ''", name="security_protocol_not_blank"
        ),
        CheckConstraint(
            "jsonb_typeof(extra_config) = 'object'", name="extra_config_object"
        ),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    bootstrap_servers: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    security_protocol: Mapped[str] = mapped_column(
        Text, server_default=text("'PLAINTEXT'"), nullable=False
    )
    extra_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, server_default=text("'{}'::jsonb"), nullable=False
    )


class Source(TimestampMixin, Base):
    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("connection_id", "topic_name"),
        CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        CheckConstraint("btrim(topic_name) <> ''", name="topic_name_not_blank"),
        Index("ix_sources_connection_id", "connection_id"),
        Index("ix_sources_normalizer_id", "normalizer_id"),
        {"schema": "app"},
    )
    id: Mapped[UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    connection_id: Mapped[UUID] = mapped_column(
        ForeignKey("app.kafka_connections.id", ondelete="CASCADE"), nullable=False
    )
    normalizer_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("app.normalizers.id", ondelete="SET NULL")
    )
    topic_name: Mapped[str] = mapped_column(Text, nullable=False)
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, server_default=text("false"), nullable=False
    )
