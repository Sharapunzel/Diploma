from fastapi import APIRouter

from .endpoints.auth import router as auth_router
from .endpoints.health import router as health_router

router = APIRouter(prefix="/api/v1")
router.include_router(health_router)
router.include_router(auth_router)
