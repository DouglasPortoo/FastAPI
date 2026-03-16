from app.core.config import get_settings
from app.core.config import ReportDatabaseConfig


class MssqlCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.report_mssql_host or "not-configured",
            "databases": len(self.settings.report_db_list),
            "configured": bool(self.settings.report_mssql_host and self.settings.report_db_list),
        }

    def collect_database_snapshot(self, db: ReportDatabaseConfig) -> dict[str, str | int | bool]:
        has_credentials = bool(db.user and db.password and db.port)
        has_mapping = bool(db.hostid and db.mysql_banco)
        configured = bool(self.settings.report_mssql_host and has_credentials and has_mapping)

        return {
            "database": db.mysql_banco,
            "port": db.port,
            "collector_status": "ready" if configured else "invalid_config",
            "host": self.settings.report_mssql_host or "not-configured",
            "hostid": db.hostid,
            "configured": configured,
        }
