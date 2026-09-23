from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session

from ...models import AppSetting, Normalizer, Source, User


class SqlAlchemyAdministrationUserRepository:
    def __init__(self, session: Session):
        self.session = session

    def list(
        self,
        query: str | None,
        role_id: UUID | None,
        is_active: bool | None,
        authentication_method: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[User], int]:
        statement = select(User)
        conditions = []
        if query:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            pattern = f"%{escaped}%"
            conditions.append(or_(
                User.username.ilike(pattern, escape="\\"),
                User.display_name.ilike(pattern, escape="\\"),
                User.email.ilike(pattern, escape="\\"),
            ))
        if role_id is not None:
            conditions.append(User.role_id == role_id)
        if is_active is not None:
            conditions.append(User.is_active == is_active)
        if authentication_method == "local":
            conditions.append(User.username.is_not(None))
        elif authentication_method == "oidc":
            conditions.append(User.oidc_issuer.is_not(None))
        if conditions:
            statement = statement.where(*conditions)
        total = self.session.scalar(select(func.count()).select_from(statement.subquery())) or 0
        statement = statement.order_by(User.id).limit(limit).offset(offset)
        return list(self.session.scalars(statement)), total

    def find_by_id(self, user_id: UUID) -> User | None:
        return self.session.get(User, user_id)

    def find_local_by_username(self, username: str) -> User | None:
        return self.session.scalar(select(User).where(func.lower(User.username) == username.lower()))

    def add(self, user: User) -> User:
        self.session.add(user)
        self.session.flush()
        return user

    def update(self, user: User, values: dict[str, Any], at) -> None:
        for key, value in values.items():
            setattr(user, key, value)
        user.updated_at = at

    def delete(self, user: User) -> None:
        self.session.delete(user)


class SqlAlchemySettingRepository:
    def __init__(self, session: Session):
        self.session = session

    def list(self, category: str | None) -> list[AppSetting]:
        statement = select(AppSetting).order_by(AppSetting.category, AppSetting.key)
        if category is not None:
            statement = statement.where(AppSetting.category == category)
        return list(self.session.scalars(statement))

    def find_by_key(self, key: str) -> AppSetting | None:
        return self.session.scalar(select(AppSetting).where(AppSetting.key == key))

    def update_value(
        self, setting: AppSetting, value: Any, version: int, actor: UUID
    ) -> AppSetting | None:
        result = self.session.execute(
            update(AppSetting)
            .where(AppSetting.id == setting.id, AppSetting.version == version)
            .values(value=value, updated_by_user_id=actor)
            .returning(AppSetting)
        )
        updated = result.scalar_one_or_none()
        if updated is not None:
            self.session.refresh(setting)
        return updated


class SqlAlchemyNormalizerRepository:
    def __init__(self, session: Session):
        self.session = session

    def list(
        self, query: str | None, limit: int, offset: int
    ) -> tuple[list[Normalizer], int]:
        statement = select(Normalizer)
        if query:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            pattern = f"%{escaped}%"
            statement = statement.where(or_(
                Normalizer.name.ilike(pattern, escape="\\"),
                Normalizer.description.ilike(pattern, escape="\\"),
            ))
        total = self.session.scalar(select(func.count()).select_from(statement.subquery())) or 0
        statement = statement.order_by(Normalizer.id).limit(limit).offset(offset)
        return list(self.session.scalars(statement)), total

    def find_by_id(self, normalizer_id: UUID) -> Normalizer | None:
        return self.session.get(Normalizer, normalizer_id)

    def add(self, normalizer: Normalizer) -> Normalizer:
        self.session.add(normalizer)
        self.session.flush()
        return normalizer

    def update_versioned(
        self, normalizer: Normalizer, values: dict[str, Any], version: int, at
    ) -> Normalizer | None:
        result = self.session.execute(
            update(Normalizer)
            .where(Normalizer.id == normalizer.id, Normalizer.version == version)
            .values(**values, updated_at=at)
            .returning(Normalizer)
        )
        updated = result.scalar_one_or_none()
        if updated is not None:
            self.session.refresh(normalizer)
        return updated

    def delete_versioned(self, normalizer: Normalizer, version: int) -> bool:
        result = self.session.execute(
            delete(Normalizer)
            .where(Normalizer.id == normalizer.id, Normalizer.version == version)
            .returning(Normalizer.id)
        )
        return result.scalar_one_or_none() is not None

    def disable_sources(self, normalizer_id: UUID, at) -> None:
        self.session.execute(
            update(Source)
            .where(Source.normalizer_id == normalizer_id)
            .values(is_enabled=False, updated_at=at)
        )
