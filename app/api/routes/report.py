from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from fastapi.responses import FileResponse

from app.schemas.report import (
    GenerateReportRequest,
    ReportAsyncAcceptedResponse,
    ReportBootstrapResponse,
    ReportEmailResponse,
    ReportGenerationResponse,
    ReportMetadataResponse,
)
from app.services.report_job_service import ReportJobService
from app.services.report_service import ReportService

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/bootstrap", response_model=ReportBootstrapResponse)
async def bootstrap_report_module() -> ReportBootstrapResponse:
    return ReportService().bootstrap()


@router.post("/daily", response_model=ReportGenerationResponse)
async def generate_daily_report(payload: GenerateReportRequest) -> ReportGenerationResponse:
    report = ReportService().generate_daily_report(run_email=payload.run_email)
    return ReportGenerationResponse(report=report)


@router.post(
    "/daily/async",
    response_model=ReportAsyncAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate_daily_report_async(
    payload: GenerateReportRequest,
    background_tasks: BackgroundTasks,
) -> ReportAsyncAcceptedResponse:
    job_service = ReportJobService()
    job = job_service.create_job(run_email=payload.run_email)
    background_tasks.add_task(job_service.run_job, job.job_id)
    return ReportAsyncAcceptedResponse(
        message="Report job accepted",
        job=job,
    )


@router.get("/{report_id}", response_model=ReportMetadataResponse)
async def get_report_metadata(report_id: str) -> ReportMetadataResponse:
    metadata = ReportService().get_report_metadata(report_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Relatório não encontrado")
    return metadata


@router.get("/{report_id}/download")
async def download_report(report_id: str) -> FileResponse:
    metadata = ReportService().get_report_metadata(report_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Relatório não encontrado")

    report_path = Path(metadata.report_path)
    if not report_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo do relatório não encontrado")

    return FileResponse(path=report_path, media_type="application/pdf", filename=report_path.name)


@router.post("/{report_id}/send-email", response_model=ReportEmailResponse)
async def send_report_email(report_id: str) -> ReportEmailResponse:
    metadata = ReportService().get_report_metadata(report_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Relatório não encontrado")
    if not metadata.report_exists:
        raise HTTPException(status_code=404, detail="Arquivo do relatório não encontrado")

    sent = ReportService().send_report_email(report_id)
    if not sent:
        raise HTTPException(status_code=400, detail="Não foi possível enviar e-mail do relatório")

    return ReportEmailResponse(
        report_id=report_id,
        email_sent=True,
        message="E-mail enviado com sucesso",
    )
