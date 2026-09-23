from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from ...core.errors import DomainError
from ...core.security import hash_password
from ...models import Normalizer, OidcRoleMapping, User
from ...repositories.protocols import (
    OidcMappingRepository,
    RoleRepository,
    SessionRepository,
    UnitOfWork,
)
from ...repositories.protocols.administration import (
    AdministrationUserRepository,
    NormalizerRepository,
    SettingRepository,
)
from ...schemas.administration import (
    MappingCreate,
    MappingPatch,
    NormalizerCreate,
    NormalizerPatch,
    UserCreate,
    UserPatch,
)

ADMIN_ROLE_ID = UUID("00000000-0000-4000-8000-000000000001")
GUEST_ROLE_ID = UUID("00000000-0000-4000-8000-000000000002")
SYSTEM_ROLE_IDS = {ADMIN_ROLE_ID, GUEST_ROLE_ID}


def now() -> datetime:
    return datetime.now(UTC)


def fail(code: str, message: str, status: int = 404, details: dict | None = None) -> None:
    raise DomainError(code, message, status, details)


def integrity_conflict(
    error: IntegrityError,
    duplicate_code: str,
    duplicate_message: str,
    duplicate_constraints: set[str],
) -> None:
    constraint = getattr(getattr(error.orig, "diag", None), "constraint_name", "")
    if constraint in duplicate_constraints:
        fail(duplicate_code, duplicate_message, 409)
    fail("integrity_conflict", "A database integrity constraint was violated", 409)


class UserAdministration:
    def __init__(
        self,
        users: AdministrationUserRepository,
        roles: RoleRepository,
        sessions: SessionRepository,
        uow: UnitOfWork,
    ) -> None:
        self.users = users
        self.roles = roles
        self.sessions = sessions
        self.uow = uow

    def list(
        self,
        query: str | None,
        role_id: UUID | None,
        is_active: bool | None,
        authentication_method: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[User], int]:
        return self.users.list(
            query, role_id, is_active, authentication_method, limit, offset
        )

    def get(self, user_id: UUID) -> User:
        user = self.users.find_by_id(user_id)
        if user is None:
            fail("user_not_found", "User not found")
        return user

    def _self_check(self, user_id: UUID, actor_id: UUID) -> None:
        if user_id == actor_id:
            fail("self_administration_forbidden", "Self-administration is forbidden", 409)

    def create(self, data: UserCreate, actor_id: UUID) -> User:
        role = self.roles.find_by_id(data.role_id)
        if role is None:
            fail("role_not_found", "Role not found")
        if role.id not in SYSTEM_ROLE_IDS:
            fail("role_not_allowed", "Only system roles may be assigned", 409)
        user = User(
            id=data.id,
            username=data.username,
            password_hash=hash_password(data.password),
            display_name=data.display_name,
            email=data.email,
            role_id=role.id,
            is_active=data.is_active,
            role_managed_by_oidc=False,
        )
        try:
            result = self.users.add(user)
            self.uow.commit()
            return result
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "username_conflict",
                "Username already exists",
                {"uq_users_username_lower"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def update(self, user_id: UUID, data: UserPatch, actor_id: UUID) -> User:
        user = self.get(user_id)
        values = data.model_dump(exclude_unset=True)
        if values.get("username") is None and "username" in values:
            fail("invalid_username", "Local users require a username", 422)
        if "username" in values and user.username is None:
            fail("user_type_immutable", "OIDC identity cannot become local", 409)
        try:
            self.users.update(user, values, now())
            self.uow.commit()
            return user
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "username_conflict",
                "Username already exists",
                {"uq_users_username_lower"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def delete(self, user_id: UUID, actor_id: UUID) -> None:
        self._self_check(user_id, actor_id)
        user = self.get(user_id)
        try:
            self.sessions.revoke_for_user(user.id, now())
            self.users.delete(user)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise

    def set_active(self, user_id: UUID, active: bool, actor_id: UUID) -> User:
        self._self_check(user_id, actor_id) if not active else None
        user = self.get(user_id)
        try:
            user.is_active = active
            user.updated_at = now()
            if not active:
                self.sessions.revoke_for_user(user.id, now())
            self.uow.commit()
            return user
        except Exception:
            self.uow.rollback()
            raise

    def set_role(
        self, user_id: UUID, role_id: UUID, managed: bool, actor_id: UUID
    ) -> User:
        self._self_check(user_id, actor_id)
        user = self.get(user_id)
        role = self.roles.find_by_id(role_id)
        if role is None:
            fail("role_not_found", "Role not found")
        if role.id not in SYSTEM_ROLE_IDS:
            fail("role_not_allowed", "Only system roles may be assigned", 409)
        if user.username is not None and managed:
            fail("role_management_invalid", "Local user cannot be OIDC-managed", 409)
        user.role_id = role.id
        user.role_managed_by_oidc = managed
        user.updated_at = now()
        try:
            self.uow.commit()
            return user
        except Exception:
            self.uow.rollback()
            raise

    def set_password(self, user_id: UUID, password: str, actor_id: UUID) -> User:
        user = self.get(user_id)
        if user.username is None:
            fail("user_type_immutable", "Password is available only for local users", 409)
        try:
            user.password_hash = hash_password(password)
            user.updated_at = now()
            self.sessions.revoke_for_user(user.id, now())
            self.uow.commit()
            return user
        except Exception:
            self.uow.rollback()
            raise


class RoleAdministration:
    def __init__(self, roles: RoleRepository, uow: UnitOfWork) -> None:
        self.roles, self.uow = roles, uow

    def list(self):
        return self.roles.list_all()

    def get(self, role_id: UUID):
        role = self.roles.find_by_id(role_id)
        if role is None:
            fail("role_not_found", "Role not found")
        return role

    def rename(self, role_id: UUID, name: str):
        role = self.get(role_id)
        try:
            self.roles.rename(role, name, now())
            self.uow.commit()
            return role
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "role_name_conflict",
                "Role name already exists",
                {"uq_roles_name"},
            )
        except Exception:
            self.uow.rollback()
            raise


class MappingAdministration:
    def __init__(
        self, mappings: OidcMappingRepository, roles: RoleRepository, uow: UnitOfWork
    ) -> None:
        self.mappings, self.roles, self.uow = mappings, roles, uow

    def list(
        self, issuer: str | None, role_id: UUID | None, limit: int, offset: int
    ):
        return self.mappings.list(issuer, role_id, limit, offset)

    def get(self, mapping_id: UUID):
        mapping = self.mappings.find_by_id(mapping_id)
        if mapping is None:
            fail("oidc_mapping_not_found", "OIDC mapping not found")
        return mapping

    def create(self, data: MappingCreate):
        if self.roles.find_by_id(data.role_id) is None:
            fail("role_not_found", "Role not found")
        if data.role_id not in SYSTEM_ROLE_IDS:
            fail("role_not_allowed", "Only system roles may be assigned", 409)
        mapping = OidcRoleMapping(
            id=data.id,
            issuer=data.issuer,
            claim_name=data.claim_name,
            claim_value=data.claim_value,
            role_id=data.role_id,
        )
        try:
            self.mappings.add(mapping)
            self.uow.commit()
            return mapping
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "oidc_mapping_conflict",
                "OIDC mapping already exists",
                {"uq_oidc_role_mappings_issuer"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def update(self, mapping_id: UUID, data: MappingPatch):
        mapping = self.get(mapping_id)
        values = data.model_dump(exclude_unset=True)
        if values.get("role_id") is None and "role_id" in values:
            fail("invalid_role", "OIDC mapping requires a role", 422)
        if "role_id" in values and self.roles.find_by_id(data.role_id) is None:
            fail("role_not_found", "Role not found")
        if "role_id" in values and data.role_id not in SYSTEM_ROLE_IDS:
            fail("role_not_allowed", "Only system roles may be assigned", 409)
        try:
            self.mappings.update(mapping, values, now())
            self.uow.commit()
            return mapping
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "oidc_mapping_conflict",
                "OIDC mapping already exists",
                {"uq_oidc_role_mappings_issuer"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def delete(self, mapping_id: UUID) -> None:
        mapping = self.get(mapping_id)
        try:
            self.mappings.delete(mapping)
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            raise


class SettingAdministration:
    def __init__(self, settings: SettingRepository, uow: UnitOfWork) -> None:
        self.settings, self.uow = settings, uow

    def list(self, category: str | None):
        return self.settings.list(category)

    def get(self, key: str):
        setting = self.settings.find_by_key(key)
        if setting is None:
            fail("setting_not_found", "Setting not found")
        return setting

    def update(self, key: str, value, version: int, actor_id: UUID):
        setting = self.get(key)
        try:
            updated = self.settings.update_value(setting, value, version, actor_id)
            if updated is None:
                self.uow.rollback()
                current = self.settings.find_by_key(key)
                fail(
                    "version_conflict",
                    "Setting version is stale",
                    409,
                    {"current_version": current.version if current else None},
                )
            self.uow.commit()
            return updated
        except DomainError:
            raise
        except Exception:
            self.uow.rollback()
            raise


class NormalizerAdministration:
    def __init__(self, normalizers: NormalizerRepository, uow: UnitOfWork) -> None:
        self.normalizers, self.uow = normalizers, uow

    def list(self, query: str | None, limit: int, offset: int):
        return self.normalizers.list(query, limit, offset)

    def get(self, normalizer_id: UUID):
        normalizer = self.normalizers.find_by_id(normalizer_id)
        if normalizer is None:
            fail("normalizer_not_found", "Normalizer not found")
        return normalizer

    def create(self, data: NormalizerCreate, actor_id: UUID):
        normalizer = Normalizer(
            id=data.id,
            name=data.name,
            description=data.description,
            rule=data.rule,
            created_by_user_id=actor_id,
            updated_by_user_id=actor_id,
        )
        try:
            self.normalizers.add(normalizer)
            self.uow.commit()
            return normalizer
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "normalizer_name_conflict",
                "Normalizer name already exists",
                {"uq_normalizers_name"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def update(self, normalizer_id: UUID, data: NormalizerPatch, actor_id: UUID):
        normalizer = self.get(normalizer_id)
        values = data.model_dump(exclude_unset=True, exclude={"version"})
        values["updated_by_user_id"] = actor_id
        try:
            updated = self.normalizers.update_versioned(normalizer, values, data.version, now())
            if updated is None:
                self.uow.rollback()
                current = self.normalizers.find_by_id(normalizer_id)
                fail(
                    "version_conflict",
                    "Normalizer version is stale",
                    409,
                    {"current_version": current.version if current else None},
                )
            self.uow.commit()
            return updated
        except IntegrityError as error:
            self.uow.rollback()
            integrity_conflict(
                error,
                "normalizer_name_conflict",
                "Normalizer name already exists",
                {"uq_normalizers_name"},
            )
        except Exception:
            self.uow.rollback()
            raise

    def delete(self, normalizer_id: UUID, version: int) -> None:
        normalizer = self.get(normalizer_id)
        try:
            self.normalizers.disable_sources(normalizer.id, now())
            if not self.normalizers.delete_versioned(normalizer, version):
                self.uow.rollback()
                current = self.normalizers.find_by_id(normalizer_id)
                fail(
                    "version_conflict",
                    "Normalizer version is stale",
                    409,
                    {"current_version": current.version if current else None},
                )
            self.uow.commit()
        except DomainError:
            raise
        except Exception:
            self.uow.rollback()
            raise
