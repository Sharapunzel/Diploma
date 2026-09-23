from fastapi import APIRouter

from .endpoints.administration import router as administration_router
from .endpoints.auth import router as auth_router
from .endpoints.connections import router as connections_router
from .endpoints.ecs import router as ecs_router
from .endpoints.health import router as health_router
from .endpoints.parsed_logs import router as parsed_logs_router

router = APIRouter(prefix="/api/v1")
router.include_router(health_router)
router.include_router(auth_router)
router.include_router(administration_router)
router.include_router(connections_router)
router.include_router(ecs_router)
router.include_router(parsed_logs_router)
