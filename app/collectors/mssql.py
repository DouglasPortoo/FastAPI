from app.core.config import get_settings


class MssqlCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.report_mssql_host or "not-configured",
            "databases": len(self.settings.report_db_list),
            "configured": bool(self.settings.report_mssql_host and self.settings.report_db_list),
        }
