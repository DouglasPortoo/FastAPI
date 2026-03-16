from typing import Any

import mysql.connector

from app.core.config import ReportDatabaseConfig, get_settings

QUERY_HOST_STATUS = """
SELECT 
    h.host AS host,
    REPLACE(REPLACE(i.name, 'Linux: ', ''), 'Docker: ', '') AS item_name,
    hs.value AS final_value,
    FROM_UNIXTIME(hs.clock) AS last_update
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_str GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_str hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.value_type = 1
AND (
    i.name LIKE '%Host name of Zabbix agent running%' OR
    i.name LIKE '%System name%'
)

UNION ALL

SELECT 
    h.host,
    'System uptime' AS item_name,
    CONCAT(
        FLOOR(hs.value / 86400), 'd ',
        FLOOR(MOD(hs.value, 86400) / 3600), 'h ',
        FLOOR(MOD(hs.value, 3600) / 60), 'm'
    ) AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_uint GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name LIKE '%System uptime%'

UNION ALL

SELECT 
    h.host,
    'System boot time' AS item_name,
    FROM_UNIXTIME(hs.value) AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_uint GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name LIKE '%System boot time%'

UNION ALL

SELECT 
    h.host,
    'Number of cores' AS item_name,
    CONCAT(hs.value, ' cores') AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_uint GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name LIKE '%Number of cores%'

UNION ALL

SELECT 
    h.host,
    'Total memory' AS item_name,
    CONCAT(ROUND(hs.value / 1073741824, 2), ' GB') AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_uint GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name LIKE '%Linux: Total memory%'

UNION ALL

SELECT 
    h.host,
    i.name AS item_name,
    CONCAT(ROUND(hs.value / 1073741824, 2), ' GB') AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock FROM history_uint GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name = 'Docker: Memory total'

UNION ALL

SELECT 
    h.host,
    'Disk total (/)' AS item_name,
    CONCAT(ROUND(hs.value / 1073741824, 2), ' GB') AS final_value,
    FROM_UNIXTIME(hs.clock)
FROM items i
JOIN hosts h ON i.hostid = h.hostid
JOIN (
    SELECT itemid, MAX(clock) AS max_clock 
    FROM history_uint 
    GROUP BY itemid
) last ON i.itemid = last.itemid
JOIN history_uint hs ON hs.itemid = last.itemid AND hs.clock = last.max_clock
WHERE h.hostid = {hostid}
AND i.name = '/: Total space';
"""

QUERY_HOST_METRICS_1D = """
SELECT
  i.name AS item,
  CAST(
    ROUND(
      CASE
        WHEN i.name LIKE '%CPU percent usage%'
        THEN MAX(hs.value) / 8
        ELSE MAX(hs.value)
      END
    , 2)
  AS DECIMAL(10,2)) AS max_value_1d
FROM items i
JOIN history hs ON hs.itemid = i.itemid
WHERE i.hostid = {hostid}
  AND hs.clock >= UNIX_TIMESTAMP(NOW() - INTERVAL 1 DAY)
  AND (
    i.name IN (
      'Container /sql_5020: CPU percent usage',
      'Container /sql_5020: Memory percent usage'
    )
    OR i.name IN (
      'Linux: CPU utilization',
      'Linux: Load average (5m avg)',
      'Linux: Memory utilization'
    )
  )
GROUP BY i.name
ORDER BY
  CASE
    WHEN i.name LIKE 'Linux:%' THEN 0
    ELSE 1
  END,
  i.name;
"""

QUERY_HOST_ALARMS_24H = """
SELECT
    h.name AS host,
    e.name AS event_name,
    CASE t.priority
        WHEN 0 THEN 'Not classified'
        WHEN 1 THEN 'Information'
        WHEN 2 THEN 'Warning'
        WHEN 3 THEN 'Average'
        WHEN 4 THEN 'High'
        WHEN 5 THEN 'Disaster'
    END AS criticidade,
    FROM_UNIXTIME(e.clock) AS inicio_problema,
    FROM_UNIXTIME(e_ok.clock) AS fim_problema,
    SEC_TO_TIME(
        CASE
            WHEN e_ok.clock IS NULL THEN UNIX_TIMESTAMP(NOW()) - e.clock
            ELSE e_ok.clock - e.clock
        END
    ) AS duracao,
    'PROBLEM' AS status_evento
FROM events e
LEFT JOIN event_recovery er ON er.eventid = e.eventid
LEFT JOIN events e_ok ON e_ok.eventid = er.r_eventid
INNER JOIN triggers t ON t.triggerid = e.objectid
INNER JOIN functions f ON f.triggerid = t.triggerid
INNER JOIN items i ON i.itemid = f.itemid
INNER JOIN hosts h ON h.hostid = i.hostid
WHERE e.source = 0
  AND e.object = 0
  AND e.value = 1
  AND h.hostid = {hostid}
  AND e.clock >= UNIX_TIMESTAMP(NOW() - INTERVAL 1 DAY)
GROUP BY e.eventid
ORDER BY e.clock ASC
LIMIT 50;
"""

QUERY_DOCKER_STATUS = """
SELECT
    SUBSTRING_INDEX(SUBSTRING_INDEX(i.name, ':', 1), '/', -1) AS container,
    MAX(CASE WHEN i.name LIKE '%Online CPUs%' THEN hu.value END) AS cpus,
    MAX(CASE WHEN i.name LIKE '%CPU percent usage%' THEN CONCAT(FLOOR(h.value),' %') END) AS cpu_percent,
    MAX(CASE WHEN i.name LIKE '%Memory usage%' THEN CONCAT(ROUND(hu.value / 1073741824, 2),' GB') END) AS memory_gib,
    MAX(CASE WHEN i.name LIKE '%Memory percent usage%' THEN CONCAT(FLOOR(h.value),' %') END) AS memory_percent,
    MAX(CASE WHEN i.name LIKE '%Running%' THEN IF(hu.value=1,'True','False') END) AS running,
    MAX(CASE WHEN i.name LIKE '%Restarting%' THEN IF(hu.value=1,'True','False') END) AS restarting,
    MAX(CASE WHEN i.name LIKE '%Paused%' THEN IF(hu.value=1,'True','False') END) AS paused,
    MAX(CASE WHEN i.name LIKE '%Dead%' THEN IF(hu.value=1,'True','False') END) AS dead,
    MAX(CASE WHEN i.name LIKE '%OOMKilled%' THEN IF(hu.value=1,'True','False') END) AS oomkilled,
    MAX(CASE WHEN i.name LIKE '%MSSQL Version%' THEN hs.value END) AS mssql_version
FROM items i
LEFT JOIN history h
    ON h.itemid = i.itemid
    AND h.clock = (
        SELECT MAX(h2.clock)
        FROM history h2
        WHERE h2.itemid = i.itemid
    )
LEFT JOIN history_uint hu
    ON hu.itemid = i.itemid
    AND hu.clock = (
        SELECT MAX(h3.clock)
        FROM history_uint h3
        WHERE h3.itemid = i.itemid
    )
LEFT JOIN history_text hs
    ON hs.itemid = i.itemid
    AND hs.clock = (
        SELECT MAX(h4.clock)
        FROM history_text h4
        WHERE h4.itemid = i.itemid
    )
WHERE i.hostid = {hostid}
    AND i.name LIKE 'Container /%'
    AND i.name NOT LIKE '%portainer%'
GROUP BY container
ORDER BY container;
"""

QUERY_DOCKER_DIRECTORIES = """
SELECT
    h.host,
    i.name,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(CURDATE())
            AND UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))
        THEN t.value_max / 1073741824
    END) AS max_hoje_gb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 30 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 29 DAY))
        THEN t.value_max / 1073741824
    END) AS max_30_dias_gb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 60 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 59 DAY))
        THEN t.value_max / 1073741824
    END) AS max_60_dias_gb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 90 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 89 DAY))
        THEN t.value_max / 1073741824
    END) AS max_90_dias_gb
FROM trends_uint t
JOIN items i ON t.itemid = i.itemid
JOIN hosts h ON i.hostid = h.hostid
JOIN hosts_groups hg ON h.hostid = hg.hostid
WHERE h.hostid = {hostid}
    AND i.name LIKE '%Directory size%'
GROUP BY h.host, i.itemid, i.name
ORDER BY max_hoje_gb DESC;
"""

QUERY_DATABASE_GROWTH = """
SELECT
    h.host,
    i.itemid,
    REGEXP_REPLACE(i.name, '^SQL Server: Database Size\\s*-?', '') AS name,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(CURDATE())
            AND UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))
        THEN t.value_max / 1024
    END) AS max_hoje_mb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 15 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 14 DAY))
        THEN t.value_max / 1024
    END) AS max_15_dias_mb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 30 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 29 DAY))
        THEN t.value_max / 1024
    END) AS max_30_dias_mb,
    MAX(CASE
        WHEN t.clock BETWEEN UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 60 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 59 DAY))
        THEN t.value_max / 1024
    END) AS max_60_dias_mb
FROM trends_uint t
JOIN items i ON t.itemid = i.itemid
JOIN hosts h ON i.hostid = h.hostid
WHERE i.hostid = {hostid}
AND i.name LIKE 'SQL Server: Database Size%'
GROUP BY h.host, i.itemid, i.name
ORDER BY max_hoje_mb DESC
LIMIT 10;
"""


class ZabbixCollector:
    def __init__(self) -> None:
        self.settings = get_settings()

    def describe(self) -> dict[str, str | int | bool]:
        return {
            "host": self.settings.report_zabbix_host or "not-configured",
            "database": self.settings.report_zabbix_db,
            "server_hostid": self.settings.report_server_hostid,
            "configured": bool(
                self.settings.report_zabbix_host
                and self.settings.report_zabbix_user
                and self.settings.report_zabbix_pass
                and self.settings.report_zabbix_db
            ),
        }

    def _connect(self):
        return mysql.connector.connect(
            host=self.settings.report_zabbix_host,
            user=self.settings.report_zabbix_user,
            password=self.settings.report_zabbix_pass,
            database=self.settings.report_zabbix_db,
            connection_timeout=5,
        )

    @staticmethod
    def _run_query(cursor, query: str) -> list[dict[str, Any]]:
        cursor.execute(query)
        return cursor.fetchall()

    def collect_host_data(self) -> dict[str, Any]:
        hostid = self.settings.report_server_hostid
        data: dict[str, Any] = {
            "host_status": [],
            "host_metrics": [],
            "host_alarms": [],
            "docker_status": [],
            "docker_directories": [],
            "problems": [],
        }

        connection = self._connect()
        cursor = connection.cursor(dictionary=True)
        try:
            data["host_status"] = self._run_query(cursor, QUERY_HOST_STATUS.format(hostid=hostid))
            data["host_metrics"] = self._run_query(cursor, QUERY_HOST_METRICS_1D.format(hostid=hostid))
            data["host_alarms"] = self._run_query(cursor, QUERY_HOST_ALARMS_24H.format(hostid=hostid))
            data["docker_status"] = self._run_query(cursor, QUERY_DOCKER_STATUS.format(hostid=hostid))
            data["docker_directories"] = self._run_query(
                cursor,
                QUERY_DOCKER_DIRECTORIES.format(hostid=hostid),
            )
        finally:
            cursor.close()
            connection.close()

        return data

    def collect_database_data(self, db: ReportDatabaseConfig) -> dict[str, Any]:
        connection = self._connect()
        cursor = connection.cursor(dictionary=True)
        try:
            db_growth = self._run_query(cursor, QUERY_DATABASE_GROWTH.format(hostid=db.hostid))
        finally:
            cursor.close()
            connection.close()

        return {"database_growth": db_growth, "problems": []}
