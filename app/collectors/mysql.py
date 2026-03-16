from typing import Any

import mysql.connector

from app.core.config import ReportDatabaseConfig, get_settings

QUERY_OPEN_CONNECTIONS = """
WITH base AS (
    SELECT 
        nome_odbc,
        banco,
        login_name,
        data_coleta,
        SUM(open_connections) AS total_conexoes
    FROM conexoes_abertas_sqlserver
    WHERE nome_odbc = '{mysql_banco}'
    AND DATE(data_coleta) = CURDATE()
    AND TIME(data_coleta) BETWEEN '07:00:00' AND '19:00:00'
    GROUP BY nome_odbc, banco, login_name, data_coleta
),
media_login AS (
    SELECT nome_odbc, banco, login_name, AVG(total_conexoes) AS media_conexoes
    FROM base
    GROUP BY nome_odbc, banco, login_name
),
pico_login AS (
    SELECT
        nome_odbc,
        banco,
        login_name,
        data_coleta AS horario_pico,
        total_conexoes AS conexoes_no_pico,
        ROW_NUMBER() OVER (
            PARTITION BY nome_odbc, banco, login_name
            ORDER BY total_conexoes DESC
        ) AS rn
    FROM base
)
SELECT
    m.nome_odbc,
    m.banco,
    m.login_name,
    m.media_conexoes,
    p.horario_pico,
    p.conexoes_no_pico
FROM media_login m
LEFT JOIN pico_login p 
    ON m.nome_odbc = p.nome_odbc
    AND m.banco = p.banco
    AND m.login_name = p.login_name
    AND p.rn = 1
ORDER BY m.media_conexoes DESC
LIMIT 10;
"""

QUERY_CPU_QUERIES = """
SELECT 
    banco,
    query_id,
    MAX(execution_count) AS execution_count,
    MAX(total_cpu_hhmmss) AS total_cpu_hhmmss,
    MAX(avg_cpu_hhmmss) AS avg_cpu_hhmmss,
    MAX(data_coleta) AS ultima_coleta,
    query_text
FROM top10_querystore_sqlserver 
WHERE nome_odbc='{mysql_banco}' and DATE(data_coleta) = CURDATE() 
GROUP BY nome_odbc, banco, query_id, query_text
ORDER BY total_cpu_hhmmss DESC
LIMIT 10;
"""

QUERY_TABLE_GROWTH = """
SELECT
    nome_odbc,
    banco,
    tabela,
    ROUND(MAX(CASE WHEN data_coleta <= NOW() THEN tamanho_mb END), 2) AS hoje_mb,
    ROUND(MAX(CASE WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN tamanho_mb END), 2) AS dias_15_mb,
    ROUND(MAX(CASE WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN tamanho_mb END), 2) AS dias_30_mb,
    ROUND(MAX(CASE WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 60 DAY) THEN tamanho_mb END), 2) AS dias_60_mb
FROM tamanho_tabelas_sqlserver
WHERE nome_odbc = '{mysql_banco}'
GROUP BY nome_odbc, banco, tabela
ORDER BY hoje_mb DESC
LIMIT 10;
"""


class MysqlCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.get_effective_aux_host() or "not-configured",
            "port": self.settings.report_aux_port,
            "database": self.settings.report_aux_db,
            "configured": bool(
                self.settings.get_effective_aux_host()
                and self.settings.get_effective_aux_user()
                and self.settings.get_effective_aux_pass()
                and self.settings.report_aux_db
            ),
        }

    def _connect(self):
        return mysql.connector.connect(
            host=self.settings.get_effective_aux_host(),
            port=self.settings.report_aux_port,
            user=self.settings.get_effective_aux_user(),
            password=self.settings.get_effective_aux_pass(),
            database=self.settings.report_aux_db,
            connection_timeout=5,
        )

    @staticmethod
    def _run_query(cursor, query: str) -> list[dict[str, Any]]:
        cursor.execute(query)
        return cursor.fetchall()

    def collect_database_data(self, db: ReportDatabaseConfig) -> dict[str, Any]:
        connection = self._connect()
        cursor = connection.cursor(dictionary=True)
        try:
            open_connections = self._run_query(
                cursor,
                QUERY_OPEN_CONNECTIONS.format(mysql_banco=db.mysql_banco),
            )
            cpu_queries = self._run_query(
                cursor,
                QUERY_CPU_QUERIES.format(mysql_banco=db.mysql_banco),
            )
            table_growth = self._run_query(
                cursor,
                QUERY_TABLE_GROWTH.format(mysql_banco=db.mysql_banco),
            )
        finally:
            cursor.close()
            connection.close()

        return {
            "open_connections": open_connections,
            "cpu_queries": cpu_queries,
            "table_growth": table_growth,
            "problems": [],
        }
