import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import json
from typing import Any

import mysql.connector
import pyodbc

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
        self._report_index_path = self.settings.get_report_output_dir_path() / "report_index.json"

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

    def _friendly_error_message(self, source: str, exc: Exception) -> str:
        if isinstance(exc, mysql.connector.Error):
            if getattr(exc, "errno", None) == 2003:
                if source == "zabbix_host":
                    return (
                        "Nao foi possivel conectar ao MySQL do Zabbix. "
                        f"Verifique REPORT_ZABBIX_HOST={self.settings.get_effective_zabbix_host()} "
                        f"e REPORT_ZABBIX_PORT={self.settings.report_zabbix_port}."
                    )
                return (
                    "Nao foi possivel conectar ao MySQL auxiliar. "
                    f"Verifique REPORT_AUX_HOST={self.settings.get_effective_aux_host()} "
                    f"e REPORT_AUX_PORT={self.settings.report_aux_port}."
                )
            return f"Falha de acesso ao MySQL em {source}."

        if isinstance(exc, pyodbc.Error) or isinstance(exc, RuntimeError):
            message = str(exc).lower()
            if "nenhum driver odbc" in message or "im002" in message:
                return (
                    "Nao foi possivel acessar o SQL Server via ODBC. "
                    "No Windows local, confirme se existe um ODBC Driver 17 ou 18 para SQL Server instalado."
                )
            if "timed out" in message or "sql server inexistente" in message or "08001" in message:
                ports = ", ".join(sorted({str(db.port) for db in self.settings.report_db_list}))
                return (
                    "Nao foi possivel conectar ao SQL Server remoto. "
                    f"Verifique acesso de rede/firewall para {self.settings.report_mssql_host} nas portas {ports}."
                )
            return (
                "Falha na conexao MSSQL via ODBC. "
                "Confirme driver ODBC, host e portas configuradas."
            )

        return f"Falha em {source}: {exc}"

    def _collect_host_runtime_data(self, context: ReportRunContext) -> dict[str, Any]:
        empty_data: dict[str, Any] = {
            "host_status": [],
            "host_metrics": [],
            "host_alarms": [],
            "docker_status": [],
            "docker_directories": [],
        }

        try:
            return self.collectors["zabbix"].collect_host_data()
        except Exception as exc:  # pragma: no cover
            logger.exception("Falha ao coletar dados do host via Zabbix")
            context.add_problem(self._friendly_error_message("zabbix_host", exc))
            return empty_data

    def _collect_database_runtime_data(
        self,
        context: ReportRunContext,
    ) -> tuple[list[ReportDatabaseSnapshot], list[dict[str, Any]]]:
        snapshots: list[ReportDatabaseSnapshot] = []
        runtime_sections: list[dict[str, Any]] = []
        if not self.settings.report_db_list:
            context.add_problem("Nenhum banco configurado em REPORT_DB_LIST.")
            return snapshots, runtime_sections

        mssql_collector = self.collectors["mssql"]
        mysql_collector = self.collectors["mysql_aux"]
        zabbix_collector = self.collectors["zabbix"]

        for db in self.settings.report_db_list:
            section_problems: list[str] = []
            section_data: dict[str, Any] = {
                "database": db.mysql_banco,
                "port": db.port,
                "database_growth": [],
                "largest_tables": [],
                "jobs": [],
                "open_connections": [],
                "cpu_queries": [],
                "table_growth": [],
                "problems": section_problems,
            }

            try:
                snapshot_data = mssql_collector.collect_database_snapshot(db)
                if not snapshot_data.get("configured"):
                    section_problems.append(
                        f"Configuração incompleta para o banco {db.mysql_banco} (porta {db.port})."
                    )
                else:
                    try:
                        zabbix_data = zabbix_collector.collect_database_data(db)
                        section_data["database_growth"] = zabbix_data.get("database_growth", [])
                    except Exception as exc:  # pragma: no cover
                        logger.exception("Falha ao coletar crescimento via Zabbix para %s", db.mysql_banco)
                        section_problems.append(self._friendly_error_message("zabbix_growth", exc))

                    try:
                        mysql_data = mysql_collector.collect_database_data(db)
                        section_data["open_connections"] = mysql_data.get("open_connections", [])
                        section_data["cpu_queries"] = mysql_data.get("cpu_queries", [])
                        section_data["table_growth"] = mysql_data.get("table_growth", [])
                    except Exception as exc:  # pragma: no cover
                        logger.exception("Falha ao coletar dados auxiliares MySQL para %s", db.mysql_banco)
                        section_problems.append(self._friendly_error_message("mysql_aux", exc))

                    try:
                        mssql_data = mssql_collector.collect_database_data(db)
                        section_data["largest_tables"] = mssql_data.get("largest_tables", [])
                        section_data["jobs"] = mssql_data.get("jobs", [])
                    except Exception as exc:  # pragma: no cover
                        logger.exception("Falha ao coletar dados MSSQL para %s", db.mysql_banco)
                        section_problems.append(self._friendly_error_message("mssql", exc))

                dataset_count = sum(
                    len(section_data[key])
                    for key in (
                        "database_growth",
                        "largest_tables",
                        "jobs",
                        "open_connections",
                        "cpu_queries",
                        "table_growth",
                    )
                )
                collector_status = str(snapshot_data["collector_status"])
                if collector_status == "ready" and section_problems:
                    collector_status = "partial" if dataset_count > 0 else "error"

                details = {
                    **snapshot_data,
                    "database_growth_count": len(section_data["database_growth"]),
                    "largest_tables_count": len(section_data["largest_tables"]),
                    "jobs_count": len(section_data["jobs"]),
                    "open_connections_count": len(section_data["open_connections"]),
                    "cpu_queries_count": len(section_data["cpu_queries"]),
                    "table_growth_count": len(section_data["table_growth"]),
                }
                snapshot = ReportDatabaseSnapshot(
                    database=snapshot_data["database"],
                    port=str(snapshot_data["port"]),
                    collector_status=collector_status,
                    details=details,
                )
                snapshots.append(snapshot)
                runtime_sections.append({**section_data, "collector_status": collector_status})
            except Exception as exc:  # pragma: no cover
                logger.exception("Falha ao preparar snapshot do banco %s", db.mysql_banco)
                section_problems.append(f"Falha ao preparar dados do banco {db.mysql_banco}: {exc}")
                snapshots.append(
                    ReportDatabaseSnapshot(
                        database=db.mysql_banco,
                        port=str(db.port),
                        collector_status="error",
                        details={"host": self.settings.report_mssql_host or "not-configured", "hostid": db.hostid},
                    )
                )
                runtime_sections.append({**section_data, "collector_status": "error"})

            for problem in section_problems:
                context.add_problem(problem)

        return snapshots, runtime_sections

    def bootstrap(self) -> ReportBootstrapResponse:
        collector_summaries = self._collector_summaries()
        return ReportBootstrapResponse(
            status="phase_5_ready",
            output_dir=self.settings.get_report_output_dir(),
            collectors=collector_summaries,
            email_enabled=self.email_service.is_configured(),
        )

    def generate_daily_report(self, run_email: bool = True) -> ReportResult:
        context = ReportRunContext()
        collector_summaries = self._collector_summaries()
        host_runtime_data = self._collect_host_runtime_data(context)
        database_snapshots, database_runtime_sections = self._collect_database_runtime_data(context)
        report_id = uuid4().hex

        report_path: str | None = None
        try:
            report_path = self.report_builder.build_daily_report(
                sources=collector_summaries,
                databases=database_snapshots,
                problems=context.problems,
                host_data=host_runtime_data,
                database_sections=database_runtime_sections,
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
