from app.core.config import get_settings


class ZabbixCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.report_zabbix_host or "not-configured",
            "database": self.settings.report_zabbix_db,
            "server_hostid": self.settings.report_server_hostid,
            "configured": bool(self.settings.report_zabbix_host and self.settings.report_zabbix_db),
        }
