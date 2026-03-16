from fastapi import APIRouter, Depends, HTTPException

from app.core.report_security import require_report_security
from app.schemas.report import ReportJobStatusResponse
from app.services.report_job_service import ReportJobService

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    dependencies=[Depends(require_report_security)],
)


@router.get(
    "/{job_id}",
    response_model=ReportJobStatusResponse,
    summary="Consultar status de um job de relatorio",
    description=(
        "Autenticacao via OAuth2 Password Bearer. "
        "Use o access_token JWT gerado no login e envie no botao Authorize do OpenAPI ou no header Authorization: Bearer <token>."
    ),
)
async def get_job_status(job_id: str) -> ReportJobStatusResponse:
    job = ReportJobService().get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
