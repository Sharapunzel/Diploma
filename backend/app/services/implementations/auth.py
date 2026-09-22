from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from ...config import Settings
from ...core.errors import auth_method_disabled, invalid_credentials
from ...core.security import (
    dummy_verify,
    random_token,
    secure_digest_equal,
    token_digest,
    utcnow,
    verify_password,
)
from ...models import AuthSession, Role, User
from ...repositories.protocols import (
    OidcMappingRepository,
    RoleRepository,
    SessionRepository,
    UnitOfWork,
    UserRepository,
)

GUEST_ROLE_ID = UUID("00000000-0000-4000-8000-000000000002")


class AuthService:
    def __init__(
        self,
        users: UserRepository,
        roles: RoleRepository,
        mappings: OidcMappingRepository,
        sessions: SessionRepository,
        unit_of_work: UnitOfWork,
        settings: Settings,
        now_provider: Callable[[], datetime] = utcnow,
    ):
        self.users = users
        self.roles = roles
        self.mappings = mappings
        self.sessions = sessions
        self.unit_of_work = unit_of_work
        self.settings = settings
        self.now = now_provider

    def local_login(
        self, username: str, password: str, previous_token: str | None = None
    ) -> tuple[str, str, User, Role]:
        if not self.settings.local_auth_enabled:
            raise auth_method_disabled()
        user = self.users.find_local(username)
        if user is None:
            dummy_verify(password)
            raise invalid_credentials()
        valid = bool(user.password_hash) and verify_password(password, user.password_hash)
        if not valid or not user.is_active or user.role_id is None:
            raise invalid_credentials()
        role = self.roles.find_by_id(user.role_id)
        if role is None:
            raise invalid_credentials()
        return self._replace_session(user, role, "local", previous_token)

    def oidc_login(
        self, claims: dict[str, Any], previous_token: str | None = None
    ) -> tuple[str, str, User, Role]:
        if not self.settings.oidc_enabled:
            raise auth_method_disabled()
        issuer = claims.get("iss")
        subject = claims.get("sub")
        if not isinstance(issuer, str) or not issuer or not isinstance(subject, str) or not subject:
            raise invalid_credentials()
        user = self.users.find_oidc(issuer, subject)
        selected_role = self._mapped_role(issuer, claims)
        try:
            if user is None:
                role = selected_role or self.roles.find_by_id(GUEST_ROLE_ID)
                if role is None:
                    raise invalid_credentials()
                user = self.users.create_oidc(
                    issuer,
                    subject,
                    claims.get("email") if isinstance(claims.get("email"), str) else None,
                    self._display_name(claims, subject),
                    role.id,
                )
            elif not user.is_active:
                raise invalid_credentials()
            elif user.role_managed_by_oidc:
                role = selected_role or self.roles.find_by_id(GUEST_ROLE_ID)
                if role is None:
                    raise invalid_credentials()
                self.users.update_oidc_role(user, role.id)
            else:
                if user.role_id is None:
                    raise invalid_credentials()
                role = self.roles.find_by_id(user.role_id)
                if role is None:
                    raise invalid_credentials()
            return self._replace_session(user, role, "oidc", previous_token)
        except Exception:
            self.unit_of_work.rollback()
            raise

    def _mapped_role(self, issuer: str, claims: dict[str, Any]) -> Role | None:
        matches: list[Role] = []
        for mapping in self.mappings.find_for_issuer(issuer):
            if mapping.role_id is None:
                continue
            claim = claims.get(mapping.claim_name)
            matched = claim == mapping.claim_value or (
                isinstance(claim, list) and mapping.claim_value in claim
            )
            if matched:
                role = self.roles.find_by_id(mapping.role_id)
                if role is not None:
                    matches.append(role)
        return max(matches, key=lambda candidate: candidate.priority, default=None)

    @staticmethod
    def _display_name(claims: dict[str, Any], subject: str) -> str:
        for key in ("name", "preferred_username", "email"):
            value = claims.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return subject

    def _replace_session(
        self,
        user: User,
        role: Role,
        method: str,
        previous_token: str | None,
    ) -> tuple[str, str, User, Role]:
        raw_session = random_token()
        raw_csrf = random_token()
        now = self.now()
        try:
            previous = self._find_session(previous_token)
            if previous is not None and previous.revoked_at is None:
                self.sessions.revoke(previous, now)
            self.users.update_last_login(user, now)
            self.sessions.create(
                user.id,
                token_digest(raw_session),
                token_digest(raw_csrf),
                method,
                now,
                now + timedelta(seconds=self.settings.session_absolute_ttl_seconds),
            )
            self.unit_of_work.commit()
        except Exception:
            self.unit_of_work.rollback()
            raise
        return raw_session, raw_csrf, user, role

    def inspect_session(self, token: str | None) -> tuple[AuthSession, User, Role] | None:
        auth_session = self._find_session(token)
        if auth_session is None or auth_session.revoked_at is not None:
            return None
        now = self.now()
        idle = timedelta(seconds=self.settings.session_idle_ttl_seconds)
        if now >= auth_session.expires_at or now - auth_session.last_seen_at >= idle:
            return None
        if auth_session.user_id is None:
            return None
        user = self.users.find_by_id(auth_session.user_id)
        if user is None or not user.is_active or user.role_id is None:
            return None
        role = self.roles.find_by_id(user.role_id)
        if role is None:
            return None
        touch = timedelta(seconds=self.settings.session_touch_interval_seconds)
        if now - auth_session.last_seen_at >= touch:
            try:
                self.sessions.touch(auth_session, now)
                self.unit_of_work.commit()
            except Exception:
                self.unit_of_work.rollback()
                raise
        return auth_session, user, role

    def csrf_matches(
        self,
        token: str | None,
        csrf_header: str | None,
        csrf_cookie: str | None,
    ) -> bool:
        current = self.inspect_session(token)
        if current is None or not csrf_header or not csrf_cookie:
            return False
        expected = current[0].csrf_token_hash
        return (
            secure_digest_equal(token_digest(csrf_header), expected)
            and secure_digest_equal(token_digest(csrf_cookie), expected)
            and secure_digest_equal(token_digest(csrf_header), token_digest(csrf_cookie))
        )

    def revoke(self, token: str | None) -> None:
        auth_session = self._find_session(token)
        if auth_session is None or auth_session.revoked_at is not None:
            return
        try:
            self.sessions.revoke(auth_session, self.now())
            self.unit_of_work.commit()
        except Exception:
            self.unit_of_work.rollback()
            raise

    def _find_session(self, token: str | None) -> AuthSession | None:
        if not token:
            return None
        return self.sessions.find_by_token_hash(token_digest(token))
