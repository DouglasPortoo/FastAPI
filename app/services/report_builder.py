from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

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
        doc = SimpleDocTemplate(
            output_path,
            pagesize=landscape(A4),
            leftMargin=18 * mm,
            rightMargin=18 * mm,
            topMargin=18 * mm,
            bottomMargin=18 * mm,
        )

        styles = getSampleStyleSheet()
        style_title = ParagraphStyle("Title", parent=styles["Heading1"], fontSize=20, alignment=TA_CENTER)
        style_h2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=12, spaceAfter=4)
        style_small = ParagraphStyle("Small", parent=styles["Normal"], fontSize=8, leading=10)

        flow: list[Any] = []
        self._build_cover(flow, style_title, style_h2, style_small, sources, databases, host_data or {})
        for section in database_sections or []:
            self._build_database_section(flow, style_h2, style_small, section)
        self._build_summary(flow, style_h2, style_small, list(problems))

        doc.build(flow)
        return output_path

    def _build_cover(
        self,
        flow: list[Any],
        style_title: ParagraphStyle,
        style_h2: ParagraphStyle,
        style_small: ParagraphStyle,
        sources: Iterable[ReportSourceSummary],
        databases: Iterable[ReportDatabaseSnapshot],
        host_data: dict[str, Any],
    ) -> None:
        logo_path = self.settings.get_report_logo_path()
        if logo_path:
            logo = Image(logo_path)
            logo.drawHeight = 20 * mm
            logo.drawWidth = 55 * mm
            flow.append(logo)
            flow.append(Spacer(1, 6))

        flow.append(Paragraph("Relatório diário Banco de Dados", style_title))
        flow.append(Paragraph(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", style_small))
        flow.append(Spacer(1, 10))

        flow.append(Paragraph("Fontes de dados", style_h2))
        source_rows = [["Fonte", "Host", "Status"]]
        for source in sources:
            source_rows.append([
                source.source,
                str(source.details.get("host", "-")),
                "OK" if source.configured else "NÃO CONFIGURADO",
            ])
        flow.append(self._build_table(source_rows, [55 * mm, 85 * mm, 35 * mm]))
        flow.append(Spacer(1, 10))

        flow.append(Paragraph("Status do host", style_h2))
        flow.append(self._dict_table(host_data.get("host_status", []), ["item_name", "final_value"], [90 * mm, 120 * mm], ["Informação", "Valor"], style_small))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Métricas 24h", style_h2))
        flow.append(self._dict_table(host_data.get("host_metrics", []), ["item", "max_value_1d"], [120 * mm, 40 * mm], ["Item", "Máximo"], style_small))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Status Docker", style_h2))
        flow.append(self._dict_table(host_data.get("docker_status", []), ["container", "cpu_percent", "memory_gib", "running"], [60 * mm, 30 * mm, 35 * mm, 25 * mm], ["Container", "CPU", "Memória", "Running"], style_small, 12))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 50 Alarmes - Últimas 24 horas", style_h2))
        flow.append(self._dict_table(host_data.get("host_alarms", []), ["inicio_problema", "fim_problema", "duracao", "criticidade", "event_name"], [35 * mm, 35 * mm, 25 * mm, 25 * mm, 110 * mm], ["Início", "Fim", "Duração", "Criticidade", "Descrição"], style_small, 10))
        flow.append(PageBreak())

        flow.append(Paragraph("Bancos monitorados", style_h2))
        db_rows = [["Banco", "Porta", "Status", "Growth", "Tabelas", "Jobs", "Conexões"]]
        for db in databases:
            db_rows.append([
                db.database,
                db.port,
                db.collector_status,
                str(db.details.get("database_growth_count", 0)),
                str(db.details.get("largest_tables_count", 0)),
                str(db.details.get("jobs_count", 0)),
                str(db.details.get("open_connections_count", 0)),
            ])
        flow.append(self._build_table(db_rows, [55 * mm, 20 * mm, 25 * mm, 20 * mm, 20 * mm, 20 * mm, 22 * mm]))
        flow.append(PageBreak())

    def _build_database_section(
        self,
        flow: list[Any],
        style_h2: ParagraphStyle,
        style_small: ParagraphStyle,
        section: dict[str, Any],
    ) -> None:
        flow.append(Paragraph(f"MSSQL - Banco {section['port']} ({section['database']})", style_h2))
        flow.append(self._dict_table(section.get("database_growth", []), ["name", "max_hoje_mb", "max_15_dias_mb", "max_30_dias_mb", "max_60_dias_mb"], [65 * mm, 28 * mm, 28 * mm, 28 * mm, 28 * mm], ["Banco", "Hoje MB", "15 dias", "30 dias", "60 dias"], style_small, 10))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 10 - Jobs Última execução", style_h2))
        flow.append(self._dict_table(section.get("jobs", []), ["JobName", "RunDateTime", "DurationHHMMSS", "JobStatus"], [80 * mm, 50 * mm, 35 * mm, 35 * mm], ["Job", "Últ. Execução", "Duração", "Status"], style_small, 10))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 10 - Conexões abertas", style_h2))
        flow.append(self._dict_table(section.get("open_connections", []), ["banco", "login_name", "media_conexoes", "conexoes_no_pico"], [55 * mm, 60 * mm, 35 * mm, 35 * mm], ["Banco", "Login", "Média", "Pico"], style_small, 10))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 10 - Queries com maior consumo de CPU", style_h2))
        flow.append(self._dict_table(section.get("cpu_queries", []), ["banco", "query_id", "execution_count", "total_cpu_hhmmss", "avg_cpu_hhmmss"], [45 * mm, 20 * mm, 20 * mm, 30 * mm, 30 * mm], ["Banco", "Query", "Exec", "CPU Total", "CPU Média"], style_small, 10))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 10 - Histórico maiores tabelas", style_h2))
        flow.append(self._dict_table(section.get("table_growth", []), ["banco", "tabela", "hoje_mb", "dias_15_mb", "dias_30_mb", "dias_60_mb"], [40 * mm, 60 * mm, 22 * mm, 22 * mm, 22 * mm, 22 * mm], ["Banco", "Tabela", "Hoje", "15 dias", "30 dias", "60 dias"], style_small, 10))
        flow.append(Spacer(1, 8))
        flow.append(Paragraph("Top 10 - Maiores tabelas", style_h2))
        flow.append(self._dict_table(section.get("largest_tables", []), ["DatabaseName", "TableName", "RowCounts", "TotalSpaceKB", "UsedSpaceKB", "UnusedSpaceKB"], [35 * mm, 65 * mm, 25 * mm, 28 * mm, 28 * mm, 28 * mm], ["Banco", "Tabela", "Linhas", "Total KB", "Uso KB", "Livre KB"], style_small, 10))
        if section.get("problems"):
            flow.append(Spacer(1, 8))
            flow.append(Paragraph("Problemas desta seção", style_h2))
            problem_rows = [["Mensagem"]] + [[item] for item in section["problems"]]
            flow.append(self._build_table(problem_rows, [220 * mm]))
        flow.append(PageBreak())

    def _build_summary(self, flow: list[Any], style_h2: ParagraphStyle, style_small: ParagraphStyle, problems: list[str]) -> None:
        flow.append(Paragraph("Observações finais", style_h2))
        rows = [["Mensagem"]]
        if problems:
            rows.extend([[problem] for problem in problems])
        else:
            rows.append(["Nenhuma inconsistência foi identificada durante a execução da coleta."])
        flow.append(self._build_table(rows, [220 * mm]))

    def _dict_table(
        self,
        rows: list[dict[str, Any]],
        keys: list[str],
        widths: list[float],
        headers: list[str],
        style_small: ParagraphStyle,
        limit: int = 8,
    ) -> Table:
        table_rows: list[list[Any]] = [headers]
        for row in rows[:limit]:
            table_rows.append([
                Paragraph(str(row.get(key, "-")), style_small) if isinstance(row.get(key), str) else str(row.get(key, "-"))
                for key in keys
            ])
        if len(rows) > limit:
            table_rows.append([f"... {len(rows) - limit} registro(s) adicionais omitidos"] + [""] * (len(headers) - 1))
        if len(table_rows) == 1:
            table_rows.append(["Sem dados disponíveis"] + [""] * (len(headers) - 1))
        return self._build_table(table_rows, widths)

    def _build_table(self, data: list[list[Any]], widths: list[float]) -> Table:
        table = Table(data, colWidths=widths, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f2f2")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d9d9d9")),
                    ("BOX", (0, 0), (-1, -1), 0.75, colors.HexColor("#bfbfbf")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ]
            )
        )
        return table
