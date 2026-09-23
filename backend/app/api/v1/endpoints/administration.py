from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response

from ....dependencies import (
    get_mapping_admin,
    get_normalizer_admin,
    get_role_admin,
    get_setting_admin,
    get_user_admin,
    require_permission,
)
from ....schemas.administration import (
    MappingCreate,
    MappingDTO,
    MappingPage,
    MappingPatch,
    NormalizerCreate,
    NormalizerDTO,
    NormalizerPage,
    NormalizerPatch,
    PasswordUpdate,
    RoleDTO,
    RolePatch,
    SettingDTO,
    SettingUpdate,
    UserCreate,
    UserDTO,
    UserPage,
    UserPatch,
    UserRoleUpdate,
)
from ....schemas.auth import ErrorResponse
from ....services.protocols.administration import (
    MappingAdministrationService,
    NormalizerAdministrationService,
    RoleAdministrationService,
    SettingAdministrationService,
    UserAdministrationService,
)

router = APIRouter(tags=["administration"])
ERRORS = {"401": {"model": ErrorResponse}, "403": {"model": ErrorResponse},
          "404": {"model": ErrorResponse}, "409": {"model": ErrorResponse},
          "422": {"model": ErrorResponse}, "500": {"model": ErrorResponse}}


def user_dto(user):
    dto = UserDTO.model_validate(user)
    dto.authentication_method = "local" if user.username is not None else "oidc"
    return dto


@router.get("/users", response_model=UserPage, responses=ERRORS)
def list_users(
    limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
    q: str | None = None, role_id: UUID | None = None, is_active: bool | None = None,
    authentication_method: Literal["local", "oidc"] | None = None,
    service: UserAdministrationService = Depends(get_user_admin),
    _: object = Depends(require_permission("users.read")),
):
    items, total = service.list(
        q, role_id, is_active, authentication_method, limit, offset
    )
    return UserPage(items=[user_dto(item) for item in items], total=total, limit=limit, offset=offset)


@router.get("/users/{user_id}", response_model=UserDTO, responses=ERRORS)
def get_user(user_id: UUID, service: UserAdministrationService = Depends(get_user_admin),
             _: object = Depends(require_permission("users.read"))):
    return user_dto(service.get(user_id))


@router.post("/users", response_model=UserDTO, status_code=201, responses=ERRORS)
def create_user(data: UserCreate, principal=Depends(require_permission("users.write")),
                service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.create(data, principal[1].id))


@router.patch("/users/{user_id}", response_model=UserDTO, responses=ERRORS)
def update_user(user_id: UUID, data: UserPatch, principal=Depends(require_permission("users.write")),
                service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.update(user_id, data, principal[1].id))


@router.delete("/users/{user_id}", status_code=204, responses=ERRORS)
def delete_user(user_id: UUID, principal=Depends(require_permission("users.write")),
                service: UserAdministrationService = Depends(get_user_admin)):
    service.delete(user_id, principal[1].id)
    return Response(status_code=204)


@router.post("/users/{user_id}/activate", response_model=UserDTO, responses=ERRORS)
def activate_user(user_id: UUID, principal=Depends(require_permission("users.write")),
                  service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.set_active(user_id, True, principal[1].id))


@router.post("/users/{user_id}/deactivate", response_model=UserDTO, responses=ERRORS)
def deactivate_user(user_id: UUID, principal=Depends(require_permission("users.write")),
                    service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.set_active(user_id, False, principal[1].id))


@router.put("/users/{user_id}/role", response_model=UserDTO, responses=ERRORS)
def set_user_role(user_id: UUID, data: UserRoleUpdate,
                  principal=Depends(require_permission("users.write")),
                  service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.set_role(user_id, data.role_id, data.role_managed_by_oidc, principal[1].id))


@router.put("/users/{user_id}/password", response_model=UserDTO, responses=ERRORS)
def set_user_password(user_id: UUID, data: PasswordUpdate,
                      principal=Depends(require_permission("users.write")),
                      service: UserAdministrationService = Depends(get_user_admin)):
    return user_dto(service.set_password(user_id, data.password, principal[1].id))


@router.get("/roles", response_model=list[RoleDTO], responses=ERRORS)
def list_roles(service: RoleAdministrationService = Depends(get_role_admin),
               _: object = Depends(require_permission("users.read"))):
    return service.list()


@router.get("/roles/{role_id}", response_model=RoleDTO, responses=ERRORS)
def get_role(role_id: UUID, service: RoleAdministrationService = Depends(get_role_admin),
             _: object = Depends(require_permission("users.read"))):
    return service.get(role_id)


@router.patch("/roles/{role_id}", response_model=RoleDTO, responses=ERRORS)
def rename_role(role_id: UUID, data: RolePatch,
                _: object = Depends(require_permission("users.write")),
                service: RoleAdministrationService = Depends(get_role_admin)):
    return service.rename(role_id, data.name)


@router.get("/oidc-role-mappings", response_model=MappingPage, responses=ERRORS)
def list_mappings(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
                  issuer: str | None = None, role_id: UUID | None = None,
                  service: MappingAdministrationService = Depends(get_mapping_admin),
                  _: object = Depends(require_permission("users.read"))):
    items, total = service.list(issuer, role_id, limit, offset)
    return MappingPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/oidc-role-mappings/{mapping_id}", response_model=MappingDTO, responses=ERRORS)
def get_mapping(mapping_id: UUID, service: MappingAdministrationService = Depends(get_mapping_admin),
                _: object = Depends(require_permission("users.read"))):
    return service.get(mapping_id)


@router.post("/oidc-role-mappings", response_model=MappingDTO, status_code=201, responses=ERRORS)
def create_mapping(data: MappingCreate, _: object = Depends(require_permission("users.write")),
                   service: MappingAdministrationService = Depends(get_mapping_admin)):
    return service.create(data)


@router.patch("/oidc-role-mappings/{mapping_id}", response_model=MappingDTO, responses=ERRORS)
def update_mapping(mapping_id: UUID, data: MappingPatch,
                   _: object = Depends(require_permission("users.write")),
                   service: MappingAdministrationService = Depends(get_mapping_admin)):
    return service.update(mapping_id, data)


@router.delete("/oidc-role-mappings/{mapping_id}", status_code=204, responses=ERRORS)
def delete_mapping(mapping_id: UUID, _: object = Depends(require_permission("users.write")),
                   service: MappingAdministrationService = Depends(get_mapping_admin)):
    service.delete(mapping_id)
    return Response(status_code=204)


@router.get("/app-settings", response_model=list[SettingDTO], responses=ERRORS)
def list_settings(category: str | None = None,
                  service: SettingAdministrationService = Depends(get_setting_admin),
                  _: object = Depends(require_permission("settings.read"))):
    return service.list(category)


@router.get("/app-settings/{key}", response_model=SettingDTO, responses=ERRORS)
def get_setting(key: str, service: SettingAdministrationService = Depends(get_setting_admin),
                _: object = Depends(require_permission("settings.read"))):
    return service.get(key)


@router.put("/app-settings/{key}", response_model=SettingDTO, responses=ERRORS)
def update_setting(key: str, data: SettingUpdate,
                   principal=Depends(require_permission("settings.write")),
                   service: SettingAdministrationService = Depends(get_setting_admin)):
    return service.update(key, data.value, data.version, principal[1].id)


@router.get("/normalizers", response_model=NormalizerPage, responses=ERRORS)
def list_normalizers(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
                     q: str | None = None,
                     service: NormalizerAdministrationService = Depends(get_normalizer_admin),
                     _: object = Depends(require_permission("normalizers.read"))):
    items, total = service.list(q, limit, offset)
    return NormalizerPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/normalizers/{normalizer_id}", response_model=NormalizerDTO, responses=ERRORS)
def get_normalizer(normalizer_id: UUID, service: NormalizerAdministrationService = Depends(get_normalizer_admin),
                   _: object = Depends(require_permission("normalizers.read"))):
    return service.get(normalizer_id)


@router.post("/normalizers", response_model=NormalizerDTO, status_code=201, responses=ERRORS)
def create_normalizer(data: NormalizerCreate,
                      principal=Depends(require_permission("normalizers.write")),
                      service: NormalizerAdministrationService = Depends(get_normalizer_admin)):
    return service.create(data, principal[1].id)


@router.patch("/normalizers/{normalizer_id}", response_model=NormalizerDTO, responses=ERRORS)
def update_normalizer(normalizer_id: UUID, data: NormalizerPatch,
                      principal=Depends(require_permission("normalizers.write")),
                      service: NormalizerAdministrationService = Depends(get_normalizer_admin)):
    return service.update(normalizer_id, data, principal[1].id)


@router.delete("/normalizers/{normalizer_id}", status_code=204, responses=ERRORS)
def delete_normalizer(normalizer_id: UUID, version: int = Query(..., ge=1),
                      _: object = Depends(require_permission("normalizers.write")),
                      service: NormalizerAdministrationService = Depends(get_normalizer_admin)):
    service.delete(normalizer_id, version)
    return Response(status_code=204)
