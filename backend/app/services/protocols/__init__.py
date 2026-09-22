from typing import Any, Protocol

from ...models import AuthSession, Role, User

AuthResult = tuple[str, str, User, Role]
Principal = tuple[AuthSession, User, Role]


class AuthenticationService(Protocol):
    def local_login(
        self, username: str, password: str, previous_token: str | None = None
    ) -> AuthResult: ...
    def oidc_login(
        self, claims: dict[str, Any], previous_token: str | None = None
    ) -> AuthResult: ...
    def inspect_session(self, token: str | None) -> Principal | None: ...
    def csrf_matches(
        self, token: str | None, csrf_header: str | None, csrf_cookie: str | None
    ) -> bool: ...
    def revoke(self, token: str | None) -> None: ...


class HealthService(Protocol):
    def ready(self) -> None: ...
