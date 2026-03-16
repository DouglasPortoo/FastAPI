from fastapi import APIRouter, HTTPException

from app.schemas.report import ReportJobStatusResponse
from app.services.report_job_service import ReportJobService

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=ReportJobStatusResponse)
async def get_job_status(job_id: str) -> ReportJobStatusResponse:
    job = ReportJobService().get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
