from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from dependencies import verify_token
from models import User
from schemas import ReportGenerateRequestSchema, ReportGenerateResponseSchema
from services.report_service import ReportServiceError, generate_report_payload, get_report_file_path

report_router = APIRouter(prefix="/reports", tags=["reports"])


@report_router.post("/generate", response_model=ReportGenerateResponseSchema, status_code=201)
async def generate_report_endpoint(
    request_data: ReportGenerateRequestSchema,
    _: User = Depends(verify_token),
):
    try:
        return generate_report_payload(request_data)
    except ReportServiceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@report_router.get("/download/{report_id}")
async def download_report_endpoint(report_id: str, _: User = Depends(verify_token)):
    try:
        report_path = get_report_file_path(report_id)
    except ReportServiceError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return FileResponse(path=report_path, media_type="application/pdf", filename=report_path.name)