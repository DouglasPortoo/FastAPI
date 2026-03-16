from datetime import datetime
from pathlib import Path
from typing import Iterable

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
    ) -> str:
        output_path = self.get_output_path()
        report = canvas.Canvas(output_path, pagesize=A4)

        y = 800
        report.setTitle("Relatório Diário de Banco")
        report.setFont("Helvetica-Bold", 14)
        report.drawString(50, y, "Relatório Diário - Fase 2 (Domínio)")

        y -= 25
        report.setFont("Helvetica", 10)
        report.drawString(50, y, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

        y -= 30
        report.setFont("Helvetica-Bold", 11)
        report.drawString(50, y, "Fontes de dados")
        y -= 18
        report.setFont("Helvetica", 10)
        for source in sources:
            report.drawString(60, y, f"- {source.source}: {'OK' if source.configured else 'NÃO CONFIGURADO'}")
            y -= 15

        y -= 10
        report.setFont("Helvetica-Bold", 11)
        report.drawString(50, y, "Bancos no escopo")
        y -= 18
        report.setFont("Helvetica", 10)
        for db in databases:
            report.drawString(60, y, f"- {db.database} (porta {db.port}): {db.collector_status}")
            y -= 15
            if y < 80:
                report.showPage()
                y = 800
                report.setFont("Helvetica", 10)

        problem_list = list(problems)
        y -= 10
        report.setFont("Helvetica-Bold", 11)
        report.drawString(50, y, "Observações")
        y -= 18
        report.setFont("Helvetica", 10)
        if problem_list:
            for problem in problem_list:
                report.drawString(60, y, f"- {problem}")
                y -= 15
                if y < 80:
                    report.showPage()
                    y = 800
                    report.setFont("Helvetica", 10)
        else:
            report.drawString(60, y, "- Nenhuma inconsistência detectada na preparação do relatório.")

        report.save()
        return output_path
