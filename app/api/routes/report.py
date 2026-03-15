from fastapi import APIRouter

from app.schemas.report import ReportBootstrapResponse
from app.services.report_service import ReportService

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/bootstrap", response_model=ReportBootstrapResponse)
async def bootstrap_report_module() -> ReportBootstrapResponse:
    return ReportService().bootstrap()
