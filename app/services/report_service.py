import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import json

from app.collectors.mssql import MssqlCollector
from app.collectors.mysql import MysqlCollector
from app.collectors.zabbix import ZabbixCollector
from app.core.config import get_settings
from app.schemas.report import (
    ReportBootstrapResponse,
    ReportDatabaseSnapshot,
    ReportMetadataResponse,
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
        self._report_index_path = Path(self.settings.report_output_dir) / "report_index.json"

    def _load_report_index(self) -> dict[str, dict]:
        if not self._report_index_path.exists():
            return {}
        try:
            return json.loads(self._report_index_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Não foi possível ler índice de relatórios, recriando arquivo.")
            return {}

    def _save_report_index(self, index_data: dict[str, dict]) -> None:
        self._report_index_path.parent.mkdir(parents=True, exist_ok=True)
        self._report_index_path.write_text(
            json.dumps(index_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _persist_report(self, report: ReportResult) -> str:
        report_id = report.report_id or uuid4().hex
        payload = report.model_dump(mode="json")
        payload["report_id"] = report_id

        index_data = self._load_report_index()
        index_data[report_id] = payload
        self._save_report_index(index_data)
        return report_id

    def get_report_metadata(self, report_id: str) -> ReportMetadataResponse | None:
        index_data = self._load_report_index()
        payload = index_data.get(report_id)
        if not payload:
            return None

        report_result = ReportResult.model_validate(payload)
        report_path = report_result.report_path or ""
        report_exists = bool(report_path and Path(report_path).exists())

        return ReportMetadataResponse(
            report_id=report_id,
            status=report_result.status,
            generated_at=report_result.generated_at,
            report_path=report_path,
            report_exists=report_exists,
            run_email=report_result.run_email,
            email_attempted=report_result.email_attempted,
            email_sent=report_result.email_sent,
            sources=report_result.sources,
            databases=report_result.databases,
            problems=report_result.problems,
        )

    def send_report_email(self, report_id: str) -> bool:
        metadata = self.get_report_metadata(report_id)
        if not metadata or not metadata.report_exists:
            return False

        if not self.email_service.is_configured():
            return False

        email_sent = self.email_service.send_report(metadata.report_path)

        index_data = self._load_report_index()
        payload = index_data.get(report_id)
        if payload:
            payload["email_attempted"] = True
            payload["email_sent"] = bool(email_sent)
            index_data[report_id] = payload
            self._save_report_index(index_data)

        return email_sent

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
        report_id = uuid4().hex

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

        report_result = ReportResult(
            report_id=report_id,
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
        self._persist_report(report_result)
        return report_result
