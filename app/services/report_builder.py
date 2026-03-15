from datetime import datetime
from pathlib import Path

from app.core.config import get_settings


class ReportBuilder:
    def __init__(self) -> None:
        self.settings = get_settings()

    def get_output_path(self) -> str:
        filename = f"report_diario_{datetime.now().strftime('%Y%m%d')}.pdf"
        return str(Path(self.settings.report_output_dir) / filename)
