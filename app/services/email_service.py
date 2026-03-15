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
