from typing import Any

from authlib.integrations.starlette_client import OAuth

from .config import Settings


class AuthlibOidcClient:
    def __init__(self, settings: Settings, transport=None, metadata=None, verify=None):
        self.redirect_uri = settings.oidc_redirect_uri
        self.oauth = OAuth()
        client_kwargs = {"scope": " ".join(settings.oidc_scopes)}
        if transport is not None:
            client_kwargs["transport"] = transport
        if verify is not None:
            client_kwargs["verify"] = verify
            client_kwargs["trust_env"] = False
        self.oauth.register(
            name="oidc",
            server_metadata_url=(
                settings.oidc_issuer_url.rstrip("/") + "/.well-known/openid-configuration"
                if settings.oidc_issuer_url and metadata is None
                else None
            ),
            client_id=settings.oidc_client_id,
            client_secret=(
                settings.oidc_client_secret.get_secret_value()
                if settings.oidc_client_secret
                else None
            ),
            client_kwargs=client_kwargs,
        )
        if metadata is not None:
            self.oauth.oidc.server_metadata.update(metadata)

    async def authorization_redirect(self, request: Any, nonce: str):
        return await self.oauth.oidc.authorize_redirect(
            request,
            self.redirect_uri,
            nonce=nonce,
        )

    async def verified_claims(self, request: Any, nonce: str) -> dict[str, Any]:
        token = await self.oauth.oidc.authorize_access_token(request)
        if not token.get("id_token"):
            raise ValueError("OIDC response has no ID token")
        claims = await self.oauth.oidc.parse_id_token(token, nonce=nonce)
        result = dict(claims)
        if not result.get("iss") or not result.get("sub"):
            raise ValueError("OIDC ID token has no issuer or subject")
        return result

    def clear_handshake(self, request: Any) -> None:
        request.session.clear()
