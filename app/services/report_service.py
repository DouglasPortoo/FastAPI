from app.collectors.mssql import MssqlCollector
from app.collectors.mysql import MysqlCollector
from app.collectors.zabbix import ZabbixCollector
from app.core.config import get_settings
from app.schemas.report import ReportBootstrapResponse, ReportSourceSummary
from app.services.email_service import EmailService


class ReportService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.collectors = {
            "mssql": MssqlCollector(),
            "mysql_aux": MysqlCollector(),
            "zabbix": ZabbixCollector(),
        }
        self.email_service = EmailService()

    def bootstrap(self) -> ReportBootstrapResponse:
        collector_summaries = [
            ReportSourceSummary(
                source=name,
                configured=bool(summary.get("configured")),
                details=summary,
            )
            for name, summary in ((key, collector.describe()) for key, collector in self.collectors.items())
        ]
        return ReportBootstrapResponse(
            status="phase_1_ready",
            output_dir=self.settings.report_output_dir,
            collectors=collector_summaries,
            email_enabled=self.email_service.is_configured(),
        )
