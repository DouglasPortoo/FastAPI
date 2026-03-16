import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path

from app.core.config import get_settings


class EmailService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def is_configured(self) -> bool:
        return all(
            [
                self.settings.report_smtp_server,
                self.settings.report_smtp_user,
                self.settings.report_smtp_pass,
                self.settings.report_from_email,
                self.settings.report_email_recipients,
            ]
        )

    def send_report(self, pdf_path: str) -> bool:
        if not self.is_configured():
            return False

        message = EmailMessage()
        message["Subject"] = "POWER CHECKLIST - Monitoramento Diário Banco de Dados"
        message["From"] = self.settings.report_from_email
        message["To"] = ", ".join(self.settings.report_email_recipients)
        message.set_content("Seu cliente de e-mail não suporta HTML.")
        message.add_alternative(
            "<p>Relatório diário de banco gerado com sucesso.</p>",
            subtype="html",
        )

        attachment_path = Path(pdf_path)
        with attachment_path.open("rb") as pdf_file:
            message.add_attachment(
                pdf_file.read(),
                maintype="application",
                subtype="pdf",
                filename=attachment_path.name,
            )

        context = ssl.create_default_context()
        with smtplib.SMTP(
            self.settings.report_smtp_server,
            self.settings.report_smtp_port,
            timeout=self.settings.report_smtp_timeout_seconds,
        ) as server:
            server.starttls(context=context)
            server.login(self.settings.report_smtp_user, self.settings.report_smtp_pass)
            server.send_message(message)

        return True
