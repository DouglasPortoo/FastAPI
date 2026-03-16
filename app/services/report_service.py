import logging
from dataclasses import dataclass, field
from datetime import datetime

from app.collectors.mssql import MssqlCollector
from app.collectors.mysql import MysqlCollector
from app.collectors.zabbix import ZabbixCollector
from app.core.config import get_settings
from app.schemas.report import (
    ReportBootstrapResponse,
    ReportDatabaseSnapshot,
    ReportResult,
    ReportSourceSummary,
)
from app.services.report_builder import ReportBuilder
from app.services.email_service import EmailService

logger = logging.getLogger(__name__)


@dataclass
class ReportRunContext:
    problems: list[str] = field(default_factory=list)

    def add_problem(self, message: str) -> None:
        if message and message not in self.problems:
            self.problems.append(message)


class ReportService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.collectors = {
            "mssql": MssqlCollector(),
            "mysql_aux": MysqlCollector(),
            "zabbix": ZabbixCollector(),
        }
        self.report_builder = ReportBuilder()
        self.email_service = EmailService()

    def _collector_summaries(self) -> list[ReportSourceSummary]:
        return [
            ReportSourceSummary(
                source=name,
                configured=bool(summary.get("configured")),
                details=summary,
            )
            for name, summary in ((key, collector.describe()) for key, collector in self.collectors.items())
        ]

    def _collect_database_snapshots(self, context: ReportRunContext) -> list[ReportDatabaseSnapshot]:
        snapshots: list[ReportDatabaseSnapshot] = []
        if not self.settings.report_db_list:
            context.add_problem("Nenhum banco configurado em REPORT_DB_LIST.")
            return snapshots

        mssql_collector = self.collectors["mssql"]
        for db in self.settings.report_db_list:
            try:
                snapshot_data = mssql_collector.collect_database_snapshot(db)
                snapshot = ReportDatabaseSnapshot(
                    database=snapshot_data["database"],
                    port=str(snapshot_data["port"]),
                    collector_status=str(snapshot_data["collector_status"]),
                    details=snapshot_data,
                )
                snapshots.append(snapshot)
                if not snapshot_data.get("configured"):
                    context.add_problem(
                        f"Configuração incompleta para o banco {db.mysql_banco} (porta {db.port})."
                    )
            except Exception as exc:  # pragma: no cover
                logger.exception("Falha ao preparar snapshot do banco %s", db.mysql_banco)
                context.add_problem(f"Falha ao preparar dados do banco {db.mysql_banco}: {exc}")
        return snapshots

    def bootstrap(self) -> ReportBootstrapResponse:
        collector_summaries = self._collector_summaries()
        return ReportBootstrapResponse(
            status="phase_2_ready",
            output_dir=self.settings.report_output_dir,
            collectors=collector_summaries,
            email_enabled=self.email_service.is_configured(),
        )

    def generate_daily_report(self, run_email: bool = True) -> ReportResult:
        context = ReportRunContext()
        collector_summaries = self._collector_summaries()
        database_snapshots = self._collect_database_snapshots(context)

        report_path: str | None = None
        try:
            report_path = self.report_builder.build_daily_report(
                sources=collector_summaries,
                databases=database_snapshots,
                problems=context.problems,
            )
        except Exception as exc:  # pragma: no cover
            logger.exception("Falha ao gerar relatório diário")
            context.add_problem(f"Falha na geração do PDF: {exc}")

        email_attempted = bool(run_email and self.email_service.is_configured() and report_path)
        email_sent = False
        if run_email and not self.email_service.is_configured():
            context.add_problem("Envio de e-mail solicitado, mas SMTP não está completamente configurado.")

        if email_attempted and report_path:
            try:
                email_sent = self.email_service.send_report(report_path)
                if not email_sent:
                    context.add_problem("Falha ao enviar e-mail do relatório.")
            except Exception as exc:  # pragma: no cover
                logger.exception("Falha ao enviar e-mail do relatório")
                context.add_problem(f"Falha ao enviar e-mail: {exc}")

        status = "completed" if report_path and not context.problems else "completed_with_warnings"
        if not report_path:
            status = "failed"

        return ReportResult(
            status=status,
            generated_at=datetime.utcnow(),
            report_path=report_path,
            run_email=run_email,
            email_attempted=email_attempted,
            email_sent=email_sent,
            sources=collector_summaries,
            databases=database_snapshots,
            problems=context.problems,
        )
