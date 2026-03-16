import logging
import threading
from datetime import datetime

from app.core.config import get_settings
from app.services.report_job_service import ReportJobService

logger = logging.getLogger(__name__)


class ReportSchedulerService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.job_service = ReportJobService()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._last_run_date: str | None = None

    def start(self) -> None:
        if not self.settings.report_schedule_enabled:
            logger.info("Report scheduler disabled by configuration.")
            return

        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Report scheduler started with cron time %s", self.settings.report_schedule_time)

    def stop(self) -> None:
        if not self._thread:
            return

        self._stop_event.set()
        self._thread.join(timeout=5)
        logger.info("Report scheduler stopped.")

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                now = datetime.now()
                schedule_time = self.settings.report_schedule_time
                current_time = now.strftime("%H:%M")
                current_date = now.strftime("%Y-%m-%d")

                if current_time == schedule_time and self._last_run_date != current_date:
                    self._last_run_date = current_date
                    job = self.job_service.create_job(run_email=self.settings.report_schedule_run_email)
                    worker = threading.Thread(target=self.job_service.run_job, args=(job.job_id,), daemon=True)
                    worker.start()
                    logger.info("Scheduled report job started: %s", job.job_id)
            except Exception:  # pragma: no cover
                logger.exception("Unexpected scheduler loop error")

            self._stop_event.wait(timeout=30)
