"""Administrator controls for the single OIDC provider."""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ....config import Settings
from ....core.errors import DomainError
from ....db import get_session
from ....dependencies import get_settings, require_permission
from ....oidc_provider import (
    ProviderInput,
    ProviderStatus,
    activate,
    check,
    disable,
    remove_secret,
    save,
    set_ca,
    status,
)

router = APIRouter(prefix="/oidc-provider", tags=["administration"])


class VersionInput(BaseModel):
    version: int = Field(ge=0)


class SaveInput(ProviderInput):
    version: int = Field(ge=0)


@router.get("", response_model=ProviderStatus)
def get_provider(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    principal=Depends(require_permission("settings.read")),
):
    return status(session, settings, principal[0].id)


@router.put("/draft", response_model=ProviderStatus)
def save_draft(
    body: SaveInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    return save(session, settings, body, body.version)


@router.delete("/draft/secret", response_model=ProviderStatus)
def delete_draft_secret(
    version: int,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    return remove_secret(session, settings, version)


@router.put("/draft/ca", response_model=ProviderStatus)
async def upload_ca(
    version: int,
    request: Request,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > 65536:
            raise DomainError("oidc_ca_invalid", "Invalid CA PEM", 422)
    try:
        return set_ca(session, settings, version, bytes(content))
    except ValueError:
        raise DomainError("oidc_ca_invalid", "Invalid CA PEM", 422) from None


@router.delete("/draft/ca", response_model=ProviderStatus)
def remove_ca(
    version: int,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    return set_ca(session, settings, version, None)


@router.post("/draft/check", response_model=ProviderStatus)
async def check_draft(
    body: VersionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    return await check(session, settings, body.version)


@router.post("/activate", response_model=ProviderStatus)
def activate_draft(
    body: VersionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    principal=Depends(require_permission("settings.write")),
):
    return activate(session, settings, body.version, principal[0].id)


@router.post("/disable", response_model=ProviderStatus)
def disable_provider(
    body: VersionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    _=Depends(require_permission("settings.write")),
):
    return disable(session, settings, body.version)
