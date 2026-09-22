from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from ...models import AuthSession, OidcRoleMapping, Role, User


class SqlAlchemyUnitOfWork:
    def __init__(self, session: Session):
        self.session = session

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()


class SqlAlchemyUserRepository:
    def __init__(self, session: Session):
        self.session = session

    def find_local(self, username: str) -> User | None:
        return self.session.scalar(
            select(User).where(func.lower(User.username) == username.lower())
        )

    def find_by_id(self, user_id: UUID) -> User | None:
        return self.session.get(User, user_id)

    def find_oidc(self, issuer: str, subject: str) -> User | None:
        return self.session.scalar(
            select(User).where(
                User.oidc_issuer == issuer,
                User.oidc_subject == subject,
            )
        )

    def create_local(
        self, username: str, password_hash: str, display_name: str, role_id: UUID
    ) -> User:
        user = User(
            username=username,
            password_hash=password_hash,
            display_name=display_name,
            role_id=role_id,
        )
        self.session.add(user)
        self.session.flush()
        return user

    def create_oidc(
        self,
        issuer: str,
        subject: str,
        email: str | None,
        display_name: str,
        role_id: UUID,
    ) -> User:
        user = User(
            oidc_issuer=issuer,
            oidc_subject=subject,
            email=email,
            display_name=display_name,
            role_id=role_id,
            role_managed_by_oidc=True,
        )
        self.session.add(user)
        self.session.flush()
        return user

    def update_last_login(self, user: User, at: datetime) -> None:
        user.last_login_at = at

    def update_oidc_role(self, user: User, role_id: UUID) -> None:
        user.role_id = role_id


class SqlAlchemyRoleRepository:
    def __init__(self, session: Session):
        self.session = session

    def find_by_id(self, role_id: UUID) -> Role | None:
        return self.session.get(Role, role_id)


class SqlAlchemyOidcMappingRepository:
    def __init__(self, session: Session):
        self.session = session

    def find_for_issuer(self, issuer: str) -> list[OidcRoleMapping]:
        return list(
            self.session.scalars(
                select(OidcRoleMapping).where(OidcRoleMapping.issuer == issuer)
            )
        )


class SqlAlchemySessionRepository:
    def __init__(self, session: Session):
        self.session = session

    def find_by_token_hash(self, token_hash: bytes) -> AuthSession | None:
        return self.session.scalar(
            select(AuthSession).where(AuthSession.token_hash == token_hash)
        )

    def create(
        self,
        user_id: UUID,
        token_hash: bytes,
        csrf_token_hash: bytes,
        authentication_method: str,
        now: datetime,
        expires_at: datetime,
    ) -> AuthSession:
        auth_session = AuthSession(
            user_id=user_id,
            token_hash=token_hash,
            csrf_token_hash=csrf_token_hash,
            authentication_method=authentication_method,
            created_at=now,
            last_seen_at=now,
            expires_at=expires_at,
        )
        self.session.add(auth_session)
        self.session.flush()
        return auth_session

    def revoke(self, auth_session: AuthSession, at: datetime) -> None:
        auth_session.revoked_at = at

    def touch(self, auth_session: AuthSession, at: datetime) -> None:
        auth_session.last_seen_at = at


class SqlAlchemyReadinessRepository:
    def __init__(self, session: Session):
        self.session = session

    def check(self) -> None:
        self.session.execute(text("SELECT 1"))
