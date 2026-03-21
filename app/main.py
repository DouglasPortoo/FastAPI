from contextlib import asynccontextmanager
import logging
from time import perf_counter

from fastapi import FastAPI
from fastapi import Request

from app.api.router import router as api_router
from app.core.config import get_settings
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.db.base import Base
from app.db.session import engine
from app.services.report_scheduler_service import ReportSchedulerService

settings = get_settings()
scheduler = ReportSchedulerService()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging(settings.log_level)
    Base.metadata.create_all(bind=engine)
    scheduler.start()
    yield
    scheduler.stop()


def create_app() -> FastAPI:
    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )

    @application.middleware("http")
    async def request_audit_middleware(request: Request, call_next):
        started = perf_counter()
        response = await call_next(request)
        elapsed_ms = (perf_counter() - started) * 1000
        client_ip = request.client.host if request.client else "unknown"
        logger.info(
            "request method=%s path=%s status=%s ip=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            response.status_code,
            client_ip,
            elapsed_ms,
        )
        return response

    register_exception_handlers(application)
    application.include_router(api_router, prefix=settings.api_prefix)
    return application


app = create_app()

#.\venv\Scripts\pip install -r requirements.txt
#.\venv\Scripts\python.exe

#uvicorn main:app --reload
