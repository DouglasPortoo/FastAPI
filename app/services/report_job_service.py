import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from app.core.config import get_settings
from app.schemas.report import ReportJobStatusResponse
from app.services.report_service import ReportService

logger = logging.getLogger(__name__)


class ReportJobService:
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.settings = get_settings()
        self._job_index_path = self.settings.get_report_output_dir_path() / "report_jobs.json"

    def _load_jobs(self) -> dict[str, dict]:
        if not self._job_index_path.exists():
            return {}
        try:
            return json.loads(self._job_index_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not read jobs index; recreating.")
            return {}

    def _save_jobs(self, jobs_data: dict[str, dict]) -> None:
        self._job_index_path.parent.mkdir(parents=True, exist_ok=True)
        self._job_index_path.write_text(
            json.dumps(jobs_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def create_job(self, run_email: bool) -> ReportJobStatusResponse:
        with self._lock:
            job_id = uuid4().hex
            job = ReportJobStatusResponse(
                job_id=job_id,
                status="queued",
                created_at=datetime.utcnow(),
                run_email=run_email,
            )
            jobs_data = self._load_jobs()
            jobs_data[job_id] = job.model_dump(mode="json")
            self._save_jobs(jobs_data)
            return job

    def get_job(self, job_id: str) -> ReportJobStatusResponse | None:
        with self._lock:
            jobs_data = self._load_jobs()
            payload = jobs_data.get(job_id)
            if not payload:
                return None
            return ReportJobStatusResponse.model_validate(payload)

    def _update_job(self, job_id: str, **kwargs: object) -> None:
        with self._lock:
            jobs_data = self._load_jobs()
            payload = jobs_data.get(job_id)
            if not payload:
                return
            payload.update(kwargs)
            jobs_data[job_id] = payload
            self._save_jobs(jobs_data)

    def run_job(self, job_id: str) -> None:
        current_job = self.get_job(job_id)
        if current_job is None:
            return

        self._update_job(
            job_id,
            status="running",
            started_at=datetime.utcnow().isoformat(),
            error=None,
        )

        try:
            report = ReportService().generate_daily_report(run_email=current_job.run_email)
            final_status = "completed" if report.status != "failed" else "failed"
            self._update_job(
                job_id,
                status=final_status,
                finished_at=datetime.utcnow().isoformat(),
                report_id=report.report_id,
                error="; ".join(report.problems) if report.status == "failed" else None,
            )
        except Exception as exc:  # pragma: no cover
            logger.exception("Async report job failed")
            self._update_job(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                error=str(exc),
            )
