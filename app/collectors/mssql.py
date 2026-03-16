from typing import Any
from urllib.parse import quote_plus

import pyodbc
from sqlalchemy import create_engine, text

from app.core.config import ReportDatabaseConfig, get_settings

QUERY_LARGEST_TABLES = """
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF OBJECT_ID('tempdb..#AllTableSizes') IS NOT NULL
    DROP TABLE #AllTableSizes;
CREATE TABLE #AllTableSizes (
    DatabaseName SYSNAME,
    TableName SYSNAME,
    RowCounts BIGINT,
    TotalSpaceKB BIGINT,
    UsedSpaceKB BIGINT,
    UnusedSpaceKB BIGINT
);
DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql = @sql + '
USE [' + name + '];
INSERT INTO #AllTableSizes
SELECT
    ''' + name + ''' AS DatabaseName,
    t.name AS TableName,
    SUM(p.rows) AS RowCounts,
    SUM(a.total_pages) * 8 AS TotalSpaceKB,
    SUM(a.used_pages) * 8 AS UsedSpaceKB,
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name;
'
FROM sys.databases
WHERE database_id > 4
AND state_desc = 'ONLINE';
EXEC sp_executesql @sql;
SELECT TOP 10
    DatabaseName,
    TableName,
    RowCounts,
    TotalSpaceKB,
    UsedSpaceKB,
    UnusedSpaceKB
FROM #AllTableSizes
ORDER BY TotalSpaceKB DESC;
"""

QUERY_JOBS = """
SELECT TOP (10)
    j.name AS JobName,
    msdb.dbo.agent_datetime(h.run_date, h.run_time) AS RunDateTime,
    STUFF(STUFF(RIGHT('000000' + CAST(h.run_duration AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':') AS DurationHHMMSS,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'Running'
    END AS JobStatus
FROM msdb.dbo.sysjobs AS j
INNER JOIN (
    SELECT job_id, MAX(instance_id) AS max_instance_id
    FROM msdb.dbo.sysjobhistory
    GROUP BY job_id
) AS l ON j.job_id = l.job_id
INNER JOIN msdb.dbo.sysjobhistory AS h ON h.job_id = l.job_id AND h.instance_id = l.max_instance_id
WHERE j.enabled = 1 AND h.step_id = 0
ORDER BY JobName, RunDateTime DESC;
"""


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

    def _connection_string(self, db: ReportDatabaseConfig) -> str:
        driver = self._resolve_odbc_driver()
        return "".join(
            [
                f"DRIVER={{{driver}}};",
                f"SERVER={self.settings.report_mssql_host},{db.port};",
                "DATABASE=master;",
                f"UID={db.user};",
                f"PWD={db.password};",
                "Encrypt=yes;",
                "TrustServerCertificate=yes;",
                "Connection Timeout=5;",
            ]
        )

    @staticmethod
    def _resolve_odbc_driver() -> str:
        available_drivers = list(pyodbc.drivers())
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
        ]

        for driver in preferred_drivers:
            if driver in available_drivers:
                return driver

        for driver in reversed(available_drivers):
            if "SQL Server" in driver:
                return driver

        raise RuntimeError("Nenhum driver ODBC para SQL Server foi encontrado no ambiente.")

    def _run_sqlalchemy_query(self, db: ReportDatabaseConfig, query: str) -> list[dict[str, Any]]:
        connection_string = self._connection_string(db)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=" + quote_plus(connection_string))
        with engine.connect() as connection:
            result = connection.execute(text(query))
            return [dict(row) for row in result.mappings()]

    @staticmethod
    def _run_pyodbc_query(connection_string: str, query: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        connection = pyodbc.connect(connection_string, autocommit=True)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            while True:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows.extend(dict(zip(columns, row)) for row in cursor.fetchall())
                if not cursor.nextset():
                    break
        finally:
            cursor.close()
            connection.close()

        return rows

    def collect_database_data(self, db: ReportDatabaseConfig) -> dict[str, Any]:
        connection_string = self._connection_string(db)
        return {
            "largest_tables": self._run_pyodbc_query(connection_string, QUERY_LARGEST_TABLES),
            "jobs": self._run_sqlalchemy_query(db, QUERY_JOBS),
            "problems": [],
        }
