from app.core.config import get_settings


class MysqlCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.report_zabbix_host or "not-configured",
            "database": self.settings.report_aux_db,
            "configured": bool(self.settings.report_zabbix_host and self.settings.report_aux_db),
        }
