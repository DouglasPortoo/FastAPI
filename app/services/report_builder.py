from datetime import datetime
from pathlib import Path
from textwrap import wrap
from typing import Any, Iterable

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.core.config import get_settings
from app.schemas.report import ReportDatabaseSnapshot, ReportSourceSummary


class ReportBuilder:
    def __init__(self) -> None:
        self.settings = get_settings()

    def get_output_path(self) -> str:
        filename = f"report_diario_{datetime.now().strftime('%Y%m%d')}.pdf"
        output_dir = self.settings.get_report_output_dir_path()
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / filename)

    def build_daily_report(
        self,
        sources: Iterable[ReportSourceSummary],
        databases: Iterable[ReportDatabaseSnapshot],
        problems: Iterable[str],
        host_data: dict[str, Any] | None = None,
        database_sections: list[dict[str, Any]] | None = None,
    ) -> str:
        output_path = self.get_output_path()
        report = canvas.Canvas(output_path, pagesize=A4)

        lines: list[str] = []
        lines.append("Relatório Diário de Banco")
        lines.append(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        lines.append("")
        lines.append("Fontes de dados")
        for source in sources:
            lines.append(f"- {source.source}: {'OK' if source.configured else 'NÃO CONFIGURADO'}")

        lines.append("")
        lines.append("Bancos monitorados")
        for db in databases:
            lines.append(f"- {db.database} (porta {db.port}): {db.collector_status}")

        if host_data:
            lines.extend(self._format_section("Status do host", host_data.get("host_status", []), ["item_name", "final_value"]))
            lines.extend(self._format_section("Métricas 24h", host_data.get("host_metrics", []), ["item", "max_value_1d"]))
            lines.extend(self._format_section("Alarmes 24h", host_data.get("host_alarms", []), ["criticidade", "event_name", "duracao"], max_rows=10))
            lines.extend(self._format_section("Containers Docker", host_data.get("docker_status", []), ["container", "cpu_percent", "memory_gib", "running"], max_rows=10))
            lines.extend(self._format_section("Diretórios Docker", host_data.get("docker_directories", []), ["name", "max_hoje_gb", "max_30_dias_gb"], max_rows=10))

        for section in database_sections or []:
            lines.append("")
            lines.append(f"Banco {section['database']}:{section['port']} [{section['collector_status']}]")
            lines.extend(self._format_section("Crescimento do banco", section.get("database_growth", []), ["name", "max_hoje_mb", "max_30_dias_mb"]))
            lines.extend(self._format_section("Maiores tabelas", section.get("largest_tables", []), ["DatabaseName", "TableName", "TotalSpaceKB"], max_rows=10))
            lines.extend(self._format_section("Jobs", section.get("jobs", []), ["JobName", "RunDateTime", "JobStatus"], max_rows=10))
            lines.extend(self._format_section("Conexões abertas", section.get("open_connections", []), ["banco", "login_name", "media_conexoes", "conexoes_no_pico"], max_rows=10))
            lines.extend(self._format_section("Queries CPU", section.get("cpu_queries", []), ["banco", "query_id", "total_cpu_hhmmss", "execution_count"], max_rows=10))
            lines.extend(self._format_section("Crescimento de tabelas", section.get("table_growth", []), ["banco", "tabela", "hoje_mb"], max_rows=10))

        lines.append("")
        lines.append("Observações")
        problem_list = list(problems)
        if problem_list:
            lines.extend(f"- {problem}" for problem in problem_list)
        else:
            lines.append("- Nenhuma inconsistência detectada na preparação do relatório.")

        self._render_lines(report, lines)

        report.save()
        return output_path

    def _format_section(
        self,
        title: str,
        rows: list[dict[str, Any]],
        keys: list[str],
        max_rows: int = 8,
    ) -> list[str]:
        lines = ["", title]
        if not rows:
            lines.append("- Sem dados para esta seção.")
            return lines

        for row in rows[:max_rows]:
            parts = []
            for key in keys:
                if key in row and row[key] not in (None, ""):
                    parts.append(f"{key}={row[key]}")
            lines.append("- " + " | ".join(parts) if parts else "- registro vazio")

        remaining = len(rows) - max_rows
        if remaining > 0:
            lines.append(f"- ... {remaining} registro(s) adicionais omitidos")
        return lines

    def _render_lines(self, report: canvas.Canvas, lines: list[str]) -> None:
        y = 800
        report.setTitle("Relatório Diário de Banco")
        report.setFont("Helvetica", 10)

        for line in lines:
            wrapped_lines = wrap(line, width=110) or [""]
            for wrapped_line in wrapped_lines:
                if y < 60:
                    report.showPage()
                    report.setFont("Helvetica", 10)
                    y = 800
                if wrapped_line:
                    report.drawString(40, y, wrapped_line)
                y -= 14
