from fastapi import APIRouter

from app.api.routes import auth, health, jobs, report

router = APIRouter()
router.include_router(auth.router)
router.include_router(health.router)
router.include_router(jobs.router)
router.include_router(report.router)

api_router = router
