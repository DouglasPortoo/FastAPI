#!/usr/bin/env python3

import math
import json
import os
import pyodbc
import smtplib
import ssl
import mysql.connector
from reportlab.platypus import Image
from datetime import datetime
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.units import mm, cm
from email.message import EmailMessage
from reportlab.platypus import TableStyle

DB_PROBLEMS = []


def _get_env(name, default=None):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _get_int_env(name, default):
    value = _get_env(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_json_env(name, default):
    value = _get_env(name)
    if value is None:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _get_csv_env(name, default):
    value = _get_env(name)
    if value is None:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or default

#----- Configuráveis -----#
DEFAULT_OUTPUT_DIR = os.path.join(os.getcwd(), "generated_reports")
OUTPUT_DIR = _get_env("REPORT_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)
PAGE_SIZE = landscape(A4)
LEFT_MARGIN = RIGHT_MARGIN = 18 * mm
TOP_MARGIN = BOTTOM_MARGIN = 18 * mm
LOGO_PATH = _get_env("REPORT_LOGO_PATH", "")
SERVER_HOSTID = _get_int_env("REPORT_SERVER_HOSTID", 10636)

# Mês em Português (para nome do arquivo)
MONTHS_PT = [
    "janeiro","fevereiro","março","abril","maio","junho",
    "julho","agosto","setembro","outubro","novembro","dezembro"
]
#----- Credenciais dos Banco MSSQL -----#
DEFAULT_DB_LIST = [
    {'user': 'infomix', 'pass': 'infomix@9080', 'port': '5020', 'hostid': 10640, 'mysql_banco':'folhagoogle_5020'},
    {'user': 'grafana', 'pass': 'freire1234', 'port': '5001', 'hostid': 10643, 'mysql_banco':'folhagoogle'},
]
DB_LIST = _get_json_env("REPORT_DB_LIST", DEFAULT_DB_LIST)
#----- Credenciais do Banco do Zabbix -----#
ZABBIX_HOST = _get_env("REPORT_ZABBIX_HOST", "")
ZABBIX_USER = _get_env("REPORT_ZABBIX_USER", "")
ZABBIX_PASS = _get_env("REPORT_ZABBIX_PASS", "")
ZABBIX_DB = _get_env("REPORT_ZABBIX_DB", "zabbix_db")
ZABBIX_DB2 = _get_env("REPORT_AUX_DB", "coleta_bancos")
MSSQL_HOST = _get_env("REPORT_MSSQL_HOST", "")
SMTP_SERVER = _get_env("REPORT_SMTP_SERVER", "")
SMTP_PORT = _get_int_env("REPORT_SMTP_PORT", 587)
SMTP_USER = _get_env("REPORT_SMTP_USER", "")
SMTP_PASS = _get_env("REPORT_SMTP_PASS", "")
FROM_EMAIL = _get_env("REPORT_FROM_EMAIL", SMTP_USER)
DEFAULT_EMAIL_RECIPIENTS = _get_csv_env("REPORT_EMAIL_RECIPIENTS", [])


def _replace_host_id(query, hostid):
    return query.replace("10636", str(hostid))


def _ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def _build_mssql_connection_string(db):
    return "".join([
        "DRIVER={ODBC Driver 17 for SQL Server};",
        f"SERVER={MSSQL_HOST},{db['port']};",
        "DATABASE=master;",
        f"UID={db['user']};",
        f"PWD={db['pass']};",
        "Encrypt=yes;",
        "TrustServerCertificate=yes;",
    ])


def _validate_runtime_configuration(require_email=False):
    missing = []

    if not DB_LIST:
        missing.append("REPORT_DB_LIST")

    for setting_name, setting_value in [
        ("REPORT_MSSQL_HOST", MSSQL_HOST),
        ("REPORT_ZABBIX_HOST", ZABBIX_HOST),
        ("REPORT_ZABBIX_USER", ZABBIX_USER),
        ("REPORT_ZABBIX_PASS", ZABBIX_PASS),
    ]:
        if not setting_value:
            missing.append(setting_name)

    if require_email:
        for setting_name, setting_value in [
            ("REPORT_SMTP_SERVER", SMTP_SERVER),
            ("REPORT_SMTP_USER", SMTP_USER),
            ("REPORT_SMTP_PASS", SMTP_PASS),
            ("REPORT_FROM_EMAIL", FROM_EMAIL),
        ]:
            if not setting_value:
                missing.append(setting_name)

    if missing:
        raise RuntimeError(
            "Configuração incompleta para o relatório. Defina: " + ", ".join(sorted(set(missing)))
        )

#----- Query com o Status do Servidor -----#
QUERY_HOST_STATUS = '''
    -- Hostname e System name
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
    WHERE h.hostid = 10636 
    AND i.value_type = 1
    AND (
        i.name LIKE '%Host name of Zabbix agent running%' OR
        i.name LIKE '%System name%'
    )

    UNION ALL

    -- System uptime (segundos para dias, horas, minutos)
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
    WHERE h.hostid = 10636 
    AND i.name LIKE '%System uptime%'

    UNION ALL

    -- System boot time (timestamp → data legível)
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
    WHERE h.hostid = 10636 
    AND i.name LIKE '%System boot time%'

    UNION ALL

    -- Number of cores
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
    WHERE h.hostid = 10636 
    AND i.name LIKE '%Number of cores%'

    UNION ALL

    -- Linux: Total memory (para GB)
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
    WHERE h.hostid = 10636 
    AND i.name LIKE '%Linux: Total memory%'

    UNION ALL

    -- Docker: Memory total (mantém prefixo Docker: , em GB)
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
    WHERE h.hostid = 10636 
    AND i.name = 'Docker: Memory total'
    
    UNION ALL

    -- Disco total do host (partição /)
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
    WHERE h.hostid = 10636
    AND i.name = '/: Total space';
'''

#----- Query com os picos de metricas do servidor -----#
QUERY_HOST_METRICS_1D = '''
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
WHERE i.hostid = 10636
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
'''

#----- Query com alarmes das ultimas 24h -----#
QUERY_HOST_ALARMS_24H = '''
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
  AND h.hostid = 10636
  AND e.clock >= UNIX_TIMESTAMP(NOW() - INTERVAL 1 DAY)
GROUP BY e.eventid
ORDER BY e.clock ASC
LIMIT 50;
'''

#----- Query com o Status dos Containers -----#
QUERY_DOCKER_STATUS = '''
SELECT
    SUBSTRING_INDEX(SUBSTRING_INDEX(i.name, ':', 1), '/', -1) AS container,

    MAX(CASE 
        WHEN i.name LIKE '%Online CPUs%' 
        THEN hu.value 
    END) AS cpus,

    MAX(CASE 
        WHEN i.name LIKE '%CPU percent usage%' 
        THEN CONCAT(FLOOR(h.value),' %')
    END) AS cpu_percent,

    MAX(CASE 
        WHEN i.name LIKE '%Memory usage%' 
        THEN CONCAT(ROUND(hu.value / 1073741824, 2),' GB')
    END) AS memory_gib,

    MAX(CASE 
        WHEN i.name LIKE '%Memory percent usage%' 
        THEN CONCAT(FLOOR(h.value),' %')
    END) AS memory_percent,

    MAX(CASE 
        WHEN i.name LIKE '%Running%' 
        THEN IF(hu.value=1,'True','False') 
    END) AS running,

    MAX(CASE 
        WHEN i.name LIKE '%Restarting%' 
        THEN IF(hu.value=1,'True','False') 
    END) AS restarting,

    MAX(CASE 
        WHEN i.name LIKE '%Paused%' 
        THEN IF(hu.value=1,'True','False') 
    END) AS paused,

    MAX(CASE 
        WHEN i.name LIKE '%Dead%' 
        THEN IF(hu.value=1,'True','False') 
    END) AS dead,

    MAX(CASE 
        WHEN i.name LIKE '%OOMKilled%' 
        THEN IF(hu.value=1,'True','False') 
    END) AS oomkilled,

    MAX(CASE 
        WHEN i.name LIKE '%MSSQL Version%' 
        THEN hs.value 
    END) AS mssql_version

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

WHERE
    i.hostid = 10636
    AND i.name LIKE 'Container /%'
    AND i.name NOT LIKE '%portainer%'

GROUP BY container
ORDER BY container;
'''

#----- Função para formatar valores MB -----#
def format_mb(value):
    if value is None:
        return "-"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "-"
    if value < 1024:
        return f"{value:.2f} MB"
    elif value < 1024 * 1024:
        return f"{value / 1024:.2f} GB"
    else:
        return f"{value / (1024 * 1024):.2f} TB"

#----- Configuração de todas as tabelas -----#
def apply_table_style(tbl):
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f2f2f2")),  # header cinza
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),              # header em negrito
        ("ALIGN", (0,0), (-1,0), "CENTER"),                         # header centralizado
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#d9d9d9")),
        ("BOX", (0,0), (-1,-1), 0.75, colors.HexColor("#bfbfbf")),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))

#----- Convert query information para json -----#
def get_host_status(cursor_zabbix, hostid: int):
    cursor_zabbix.execute(QUERY_HOST_STATUS.replace("10636", str(hostid)))
    return cursor_zabbix.fetchall()

#----- Funções utilitárias -----#
def humanbytes(B):
    """
    Convert bytes to a human-friendly KB, MB, GB, or TB string.
    """
    if B == 0:
        return "0 Bytes"
    if B < 0:
        sign = '-'
        B = B * -1
    else:
        sign = '' 
    base = 1024
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    i = int(math.floor(math.log(B, base)))
    i = min(i, len(units) - 1)
    
    converted_size = B / (1 << (i * 10))

    if converted_size.is_integer():
        return f"{sign}{int(converted_size)} {units[i]}"
    else:
        return f"{sign}{converted_size:.2f} {units[i]}"

#----- Funções para correção de tabelas -----#
def normalize_rows(rows, columns=None):
    """
    Garante que todas as linhas venham como dict.
    Se vier tuple, converte usando os nomes das colunas.
    """
    if not rows:
        return rows

    if isinstance(rows[0], dict):
        return rows

    # se veio tuple
    if columns is None:
        # fallback genérico
        return [dict(enumerate(r)) for r in rows]

    return [dict(zip(columns, r)) for r in rows]

#----- Configuração Capa do PDF -----#
def build_capa_zabbix(
    flow,
    style_title,
    style_h2,
    style_h2_center,
    style_small,
    host_status_data,
    docker_status_data,
    host_metrics_1d,
    host_alarms_24h,
    docker_dirs_data
):
    title = Paragraph("Relatório diário Banco de Dados", style_title)
    data_execucao = datetime.now().strftime("%d/%m/%Y às %H:%M")
    data_paragraph = Paragraph(f"Gerado em: {data_execucao}", style_small)

    if LOGO_PATH and os.path.exists(LOGO_PATH):
        logo = Image(LOGO_PATH)
        logo.drawHeight = 25 * mm
        logo.drawWidth = 70 * mm
    else:
        logo = Paragraph("Relatório diário Banco de Dados", style_h2)

    header_table = Table([[logo, title]], colWidths=[80*mm, None])
    header_table.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 12),
    ]))

    flow.append(header_table)
    flow.append(Spacer(1, 4))
    flow.append(data_paragraph)
    flow.append(Spacer(1, 12))

    # ==================================================
    # STATUS SERVER
    # ==================================================
    flow.append(Paragraph("Status Server", style_h2_center))
    flow.append(Spacer(1, 6))

    status_table_data = [["Information", "Value"]]
    for row in host_status_data:
        status_table_data.append([
            Paragraph(str(row.get("item_name", "")), style_small),
            Paragraph(str(row.get("final_value", "")), style_small),
        ])

    status_tbl = Table(status_table_data, colWidths=[80*mm, None], repeatRows=1)
    apply_table_style(status_tbl)

    # ==================================================
    # MÉTRICAS 24H
    # ==================================================
    metrics_table_data = [["Item", "Max 1d"]]
    for row in host_metrics_1d:
        metrics_table_data.append([
            Paragraph(str(row.get("item", "")), style_small),
            Paragraph(str(row.get("max_value_1d", "")), style_small),
        ])

    metrics_tbl = Table(metrics_table_data, colWidths=[95*mm, 25*mm], repeatRows=1)
    apply_table_style(metrics_tbl)

    # Tabelas lado a lado
    side_by_side = Table([[status_tbl, metrics_tbl]], colWidths=[120*mm, 120*mm])
    side_by_side.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
    ]))

    flow.append(side_by_side)
    flow.append(Spacer(1, 14))
    flow.append(PageBreak())
        
    # ==================================================
    # STATUS DOCKER
    # ==================================================
    flow.append(Paragraph("Status Docker", style_h2_center))
    summary_table_data = [["Container", "MsSQL Version", "CPU", "CPU Utilization", "Mmoery ", "Mmoery Utilization", "Running", "Paused"]]
    
    for row in docker_status_data:
        summary_table_data.append([
            Paragraph(str(row.get("container", "")), style_small),
            Paragraph(str(row.get("mssql_version", "")), style_small),
            Paragraph(str(row.get("cpus", "")), style_small),
            Paragraph(str(row.get("cpu_percent", "")), style_small),
            Paragraph(str(row.get("memory_gib", "")), style_small),
            Paragraph(str(row.get("memory_percent", "")), style_small),
            Paragraph(str(row.get("running", "")), style_small),
            Paragraph(str(row.get("paused", "")), style_small),
        ])

    tbl = Table(
        summary_table_data,
        colWidths=[40*mm, 40*mm, 15*mm, 30*mm, 30*mm, 35*mm, 20*mm, 20*mm],
        repeatRows=1
    )
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))

    # ==================================================
    # DIRETÓRIOS DOCKER
    # ==================================================
    #flow.append(Paragraph("Diretórios Docker (GB)", style_h2))
    summary_table_data = [["Diretório", "Atual", "30 Dias", "60 Dias", "90 Dias"]]
    
    for entry in docker_dirs_data:
        summary_table_data.append([
            #Paragraph(entry.get('host', ''), style_small),
            Paragraph(entry.get('name', ''), style_small),
            f"{entry.get('max_hoje_gb', 0):.2f}" if entry.get('max_hoje_gb') else "0.00",
            f"{entry.get('max_30_dias_gb', 0):.2f}" if entry.get('max_30_dias_gb') else "-",
            f"{entry.get('max_60_dias_gb', 0):.2f}" if entry.get('max_60_dias_gb') else "-",
            f"{entry.get('max_90_dias_gb', 0):.2f}" if entry.get('max_90_dias_gb') else "-",
        ])
    docker_tbl = Table(
        summary_table_data,
        colWidths=[80*mm, 25*mm, 25*mm, 25*mm, 25*mm],
        repeatRows=1
    )
    apply_table_style(docker_tbl)

    flow.append(docker_tbl)
    flow.append(Spacer(1, 12))
    flow.append(PageBreak())

    # ==================================================
    # ALARMES - ÚLTIMAS 24H
    # ==================================================
    flow.append(Paragraph("Top 50 Alarmes - Últimas 24 horas", style_h2))
    flow.append(Spacer(1, 6))
    alarms_table_data = [[
        "Início",
        "Fim",
        "Duração",
        "Criticidade",
        "Descrição"
    ]]
    for row in host_alarms_24h:
        alarms_table_data.append([
            row.get("inicio_problema", ""),
            row.get("fim_problema", "") if row.get("fim_problema") else "Em aberto",
            row.get("duracao", ""),
            row.get("criticidade", ""),
            Paragraph(row.get("event_name", ""), style_small),
        ])
    alarms_tbl = Table(
        alarms_table_data,
        colWidths=[35*mm, 35*mm, 30*mm, 30*mm, None],
        repeatRows=1
    )
    apply_table_style(alarms_tbl)
    flow.append(alarms_tbl)

    # ==================================================
    # FIM DA CAPA
    # ==================================================
    flow.append(PageBreak())

#----- Configuração Tabelas de MSSQL do PDF -----#
def build_mssql_section(flow, style_h2, style_small, style_normal, db):
    port, queries_data, _, _, _, _, = db

    queries_data = [normalize_rows(q) for q in queries_data]

    # =========================================================
    # TÍTULO DA SEÇÃO
    # =========================================================
    flow.append(Paragraph(f"MSSQL - Banco {port}", style_h2))
    flow.append(Spacer(1, 12))

    # =========================================================
    # Top 10 - Crescimento de Bancos
    # =========================================================
    flow.append(Paragraph("Top 10 - Crescimento de Bancos", style_h2))
    summary_table_data = [["Banco", "Hoje (MB)", "15 Dias (MB)", "30 Dias (MB)", "60 Dias (MB)"]]

    for entry in queries_data[0]:
        summary_table_data.append([
            Paragraph(entry["name"], style_small),
            f"{entry['max_hoje_mb']:.2f} MB" if entry["max_hoje_mb"] is not None else "-",
            f"{entry['max_15_dias_mb']:.2f} MB" if entry["max_15_dias_mb"] is not None else "-",
            f"{entry['max_30_dias_mb']:.2f} MB" if entry["max_30_dias_mb"] is not None else "-",
            f"{entry['max_60_dias_mb']:.2f} MB" if entry["max_60_dias_mb"] is not None else "-",
        ])

    tbl = Table(summary_table_data, repeatRows=1)
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))
    flow.append(PageBreak())

    # =========================================================
    # Top 10 - Jobs Ultima execução e Tempo
    # =========================================================
    flow.append(Paragraph("Top 10 - Jobs Ultima execução e Tempo", style_h2))
    summary_table_data = [["Ordem", "Job Name", "Ult.Exec", "Total Runtime", "Job Status"]]

    for i, entry in enumerate(queries_data[2], start=1):
        summary_table_data.append([
            i,
            Paragraph(entry['JobName'], style_small),
            entry['RunDateTime'],
            entry['DurationHHMMSS'],
            entry['JobStatus']
        ])

    tbl = Table(summary_table_data, repeatRows=1)
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))
    flow.append(PageBreak())

    # =========================================================
    # Top 10 - Conexões Abertas por Usuários
    # =========================================================
    flow.append(Paragraph("Top 10 - Conexões Abertas por Usuários", style_h2))
    summary_table_data = [["Ordem", "Database", "Login", "Media de Conexões", "Horario de Pico", "Pico de Conexões"]]

    for i, entry in enumerate(queries_data[4], start=1):
        summary_table_data.append([
            i,
            Paragraph(entry['banco'], style_small),
            Paragraph(entry['login_name'], style_small),
            f"{float(entry['media_conexoes']):.0f}" if str(entry.get('media_conexoes','')).replace('.','',1).isdigit() else "-",
            Paragraph(entry['horario_pico'].strftime("%d/%m/%Y %H:%M:%S") if entry.get('horario_pico') else "-", style_small),
            f"{float(entry['conexoes_no_pico']):.0f}" if str(entry.get('conexoes_no_pico','')).replace('.','',1).isdigit() else "-"
        ])

    tbl = Table(summary_table_data, repeatRows=1)
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))
    flow.append(PageBreak())

    # # =========================================================
    # # Top 10 - Queries Demoradas (Dia Anterior)
    # # =========================================================
    # flow.append(Paragraph("Top 10 - Queries Demoradas - Dia Anterior", style_h2))
    # summary_table_data = [["Ordem", "Total CPU", "Execuções", "Média CPU", "Statement"]]

    # for i, entry in enumerate(queries_data[3], start=1):
    #     summary_table_data.append([
    #         i,
    #         entry['total_cpu_time_hhmmss'],
    #         entry['execution_count'],
    #         entry['avg_cpu_time_hhmmss'],
    #         Paragraph(entry['statement_text'][:150] + "...", style_normal)
    #     ])

    # tbl = Table(summary_table_data, repeatRows=1, colWidths=[20*mm, 30*mm, 30*mm, 30*mm, None])
    # apply_table_style(tbl)
    # flow.append(tbl)
    # flow.append(Spacer(1, 14))
    # flow.append(PageBreak())

    # # =========================================================
    # # Top 10 - Queries Lentas por Databases (Dia Anterior)
    # # =========================================================
    # flow.append(Paragraph("Top 10 - Queries com maior consumo de CPU", style_h2))
    # summary_table_data = [["Database", "Query ID", "Num.Exc", "Total CPU", "Media CPU", "Ult.Coleta", "Query"]]

    # for i, entry in enumerate(queries_data[5], start=1):
    #     summary_table_data.append([
    #         #i,
    #         Paragraph(entry['banco'], style_small),
    #         entry['query_id'],
    #         entry['execution_count'],
    #         entry['total_cpu_hhmmss'],
    #         entry['avg_cpu_hhmmss'],
    #         entry['ultima_coleta'],
    #         Paragraph(entry['query_text'][:120] + "...", style_normal)
    #     ])

    # tbl = Table(summary_table_data, repeatRows=1, colWidths=[35*mm, 18*mm, 18*mm, 25*mm, 23*mm, 32*mm, 80*mm, None])
    # apply_table_style(tbl)
    # flow.append(tbl)
    # flow.append(Spacer(1, 14))
    # flow.append(PageBreak())

    # =========================================================
    # Top 10 - Queries com maior consumo de CPU
    # =========================================================
    flow.append(Paragraph("Top 10 - Queries com maior consumo de CPU", style_h2))
    summary_table_data = [["Ordem", "Database", "Query ID", "Num.Exc", "Total CPU", "Media CPU", "Ult.Coleta"]]
    queries_text = []

    for i, entry in enumerate(queries_data[5], start=1):
        # linha da tabela
        summary_table_data.append([
            i,
            Paragraph(entry['banco'], style_small),
            entry['query_id'],
            entry['execution_count'],
            entry['total_cpu_hhmmss'],
            entry['avg_cpu_hhmmss'],
            entry['ultima_coleta'],
        ])
        # guarda query para imprimir depois
        queries_text.append(entry['query_text'])
    # cria tabela compacta
    tbl = Table(summary_table_data, repeatRows=1, colWidths=[18*mm, 50*mm, 18*mm, 18*mm, 25*mm, 23*mm, 40*mm])
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 12))
    flow.append(PageBreak())

    # =========================================================
    # Queries abaixo da tabela (fora dela!)
    # =========================================================
    flow.append(Paragraph("Detalhamento das Queries", style_h2))
    for i, q in enumerate(queries_text, start=1):
        flow.append(Paragraph(
            f"<b>Query {i}:</b>",
            style_small
        ))
        flow.append(Paragraph(
            f"<font color='#555555'>{q}</font>",
            style_normal
        ))
        flow.append(Spacer(1, 10))
        flow.append(PageBreak())

    # =========================================================
    # Top 10 - Histórico Maiores Tabelas (MB)
    # =========================================================
    flow.append(Paragraph("Top 10 - Histórico Maiores Tabelas (MB)", style_h2))
    flow.append(Spacer(1, 6))

    summary_table_data = [["Banco", "Tabela", "Hoje (MB)", "15 Dias (MB)", "30 Dias (MB)", "60 Dias (MB)"]]

    for entry in queries_data[7]:
        summary_table_data.append([
            Paragraph(entry["banco"], style_small),
            Paragraph(entry["tabela"], style_small),
            format_mb(entry["hoje_mb"]),
            format_mb(entry["dias_15_mb"]),
            format_mb(entry["dias_30_mb"]),
            format_mb(entry["dias_60_mb"]),
        ])

    tbl = Table(summary_table_data, repeatRows=1)
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))
    flow.append(PageBreak())

    # =========================================================
    # Top 10 - Maiores Tabelas
    # =========================================================
    flow.append(Paragraph("Top 10 - Maiores Tabelas", style_h2))
    summary_table_data = [["Ordem", "Database", "Tabela", "Linhas", "Total", "Em Uso", "Livre"]]

    for i, entry in enumerate(queries_data[1], start=1):
        summary_table_data.append([
            i,
            Paragraph(entry['DatabaseName'], style_small),
            Paragraph(entry['TableName'], style_small),
            entry['RowCounts'],
            humanbytes(entry['TotalSpaceKB']),
            humanbytes(entry['UsedSpaceKB']),
            humanbytes(entry['UnusedSpaceKB'])
        ])

    tbl = Table(summary_table_data, repeatRows=1, colWidths=[20*mm, 50*mm, 60*mm, 25*mm, 30*mm, 30*mm, 30*mm])
    apply_table_style(tbl)
    flow.append(tbl)
    flow.append(Spacer(1, 14))

#----- Geração do PDF Completo -----#
def generate_pdf_unificado(output_dir, db_results, output_filename=None):
    now = datetime.now()
    _ensure_output_dir(output_dir)
    if output_filename is None:
        data_str = now.strftime("%d-%m-%Y")
        output_filename = f"Report_diario_Banco-Folha_{data_str}.pdf"
    output_path = os.path.join(output_dir, output_filename)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=PAGE_SIZE,
        leftMargin=LEFT_MARGIN,
        rightMargin=RIGHT_MARGIN,
        topMargin=TOP_MARGIN,
        bottomMargin=BOTTOM_MARGIN
    )

    styles = getSampleStyleSheet()

    style_title = ParagraphStyle(
        "Title",
        parent=styles["Heading1"],
        fontSize=20,
        alignment=TA_CENTER,
        spaceAfter=12
    )
    style_h2 = ParagraphStyle(
        "H2",
        parent=styles["Heading2"],
        fontSize=14,
        spaceAfter=6
    )
    style_h2_center = ParagraphStyle(
        "H2Center",
        parent=style_h2,
        alignment=TA_CENTER
    )
    style_normal = ParagraphStyle(
        "Normal",
        parent=styles["Normal"],
        fontSize=10,
        leading=13
    )
    style_small = ParagraphStyle(
        "Small",
        parent=styles["Normal"],
        fontSize=8,
        leading=10
    )

    flow = []

    # =========================================================
    # CAPA ÚNICA (Logo + Zabbix)
    # =========================================================
    first_db = db_results[0]
    _, queries_data, host_status_data, docker_status_data, host_metrics_1d, host_alarms_24h, = first_db

    build_capa_zabbix(
        flow,
        style_title,
        style_h2,
        style_h2_center,
        style_small,
        host_status_data,
        docker_status_data,
        host_metrics_1d,
        host_alarms_24h,
        queries_data[6]
    )

    # =========================================================
    # SEÇÕES MSSQL (uma por banco)
    # =========================================================
    for idx, db in enumerate(db_results):
        if idx > 0:
            flow.append(PageBreak())  # quebra ANTES de cada novo banco
        build_mssql_section(flow, style_h2, style_small, style_normal, db)

    # =========================================================
    # ÚLTIMA PÁGINA – OBSERVAÇÕES FINAIS
    # =========================================================
    flow.append(PageBreak())
    flow.append(Paragraph("Observações finais", style_h2))
    flow.append(Spacer(1, 8))

    if DB_PROBLEMS:
        text = "Os seguintes bancos apresentaram problemas durante a execução do relatório:<br/><br/>"
        for problem in sorted(set(DB_PROBLEMS)):
            text += f"- {problem}<br/>"

        flow.append(Paragraph(text, style_small))
        flow.append(Spacer(1, 8))

        flow.append(Paragraph(
            "Este relatório é gerado automaticamente a partir dos dados coletados no ambiente. "
            "Em caso de inconsistências, falhas de coleta ou necessidade de ajustes, entre em contato pelo e-mail "
            "backup@infomixtecnologia.com.br.<br/><br/>"
            "Segue o link do Dashboard para acompanhamento do Status do Servidor de Banco "
            "<a href='http://44.216.162.106:3000/d/f229429f-8c70-4b0f-8ee6-69b2eff6d132/mssql-folha-google?orgId=1&refresh=1m'>Dashbord MSSQL Folha - Google</a>.",
            style_small
        ))

    else:
        flow.append(Paragraph(
            "Nenhuma inconsistência foi identificada durante a execução da coleta. "
            "Todos os bancos de dados monitorados responderam conforme o esperado no período analisado.<br/><br/>"
            "Segue o link do Dashboard para acompanhamento do Status do Servidor de Banco "
            "<a href='http://44.216.162.106:3000/d/f229429f-8c70-4b0f-8ee6-69b2eff6d132/mssql-folha-google?orgId=1&refresh=1m'>Dashbord MSSQL Folha - Google</a>.",
            style_small
        ))

        flow.append(Paragraph(
            "Este relatório é gerado automaticamente a partir dos dados coletados no ambiente. "
            "Em caso de dúvidas, solicitações de ajuste ou necessidade de suporte, entre em contato pelo e-mail "
            "backup@infomixtecnologia.com.br.<br/><br/>"
            "Segue o link do Dashboard para acompanhamento do Status do Servidor de Banco "
            "<a href='http://44.216.162.106:3000/d/f229429f-8c70-4b0f-8ee6-69b2eff6d132/mssql-folha-google?orgId=1&refresh=1m'>Dashbord MSSQL Folha - Google</a>.",
            style_small
        ))


    # =========================================================
    # GERAÇÃO DO PDF
    # =========================================================
    doc.build(flow)
    return output_path

#----- Discovery dos Bancos do MSSQL -----#
def get_db_query1_data():
    from sqlalchemy import create_engine, text
    from urllib.parse import quote_plus

    QUERY = '''
    SELECT
        DB_NAME(database_id) AS DatabaseName,
        SUM(size) AS SizeMB
    FROM sys.master_files
    GROUP BY database_id
    ORDER BY SizeMB DESC;

    '''
    if not DB_LIST:
        raise RuntimeError("REPORT_DB_LIST precisa estar configurado para executar a descoberta de bancos.")

    conn_str = _build_mssql_connection_string(DB_LIST[0])

    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)
    )

    data = []
    with engine.connect() as conn:
        #result = conn.execute(text("SELECT TOP 5 * FROM MinhaTabela"))
        result = conn.execute(text(QUERY))
        for row in result:
            data.append({"database_name": row[0], "size": float(row[1])})
    return data

#----- Queries utilizadas no codigo -----# 
def get_db_tops(db : dict):
    from sqlalchemy import create_engine, text
    from urllib.parse import quote_plus
    # Crescimento de tamanho dos bancos
    QUERY1 = f'''
    SELECT
        h.host,
        i.itemid,
        REGEXP_REPLACE(i.name, '^SQL Server: Database Size\\s*-?', '') AS name,
        MAX(CASE
            WHEN t.clock BETWEEN
                UNIX_TIMESTAMP(CURDATE())
                AND UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))
            THEN t.value_max / 1024
        END) AS max_hoje_mb,
        MAX(CASE
            WHEN t.clock BETWEEN
                UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 15 DAY))
                AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 14 DAY))
            THEN t.value_max / 1024
        END) AS max_15_dias_mb,
        MAX(CASE
            WHEN t.clock BETWEEN
                UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 30 DAY))
                AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 29 DAY))
            THEN t.value_max / 1024
        END) AS max_30_dias_mb,
        MAX(CASE
            WHEN t.clock BETWEEN
                UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 60 DAY))
                AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 59 DAY))
            THEN t.value_max / 1024
        END) AS max_60_dias_mb
    FROM trends_uint t
    JOIN items i ON t.itemid = i.itemid
    JOIN hosts h ON i.hostid = h.hostid
    WHERE i.hostid = {db["hostid"]}
    AND i.name LIKE 'SQL Server: Database Size%'
    GROUP BY
            h.host,
            i.itemid,
            i.name
    ORDER BY
            max_hoje_mb DESC
    LIMIT 10;
    '''
    # Tabelas que mais ocupam espaço
    QUERY2 = """
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
        AND state_desc = 'ONLINE';   -- <<< AQUI ESTÁ A CORREÇÃO CRÍTICA
        EXEC sp_executesql @sql;
        SELECT TOP 10
            DatabaseName,
            TableName,
            RowCounts,
            TotalSpaceKB,
            UsedSpaceKB,
            UnusedSpaceKB
        FROM #AllTableSizes
        ORDER BY TotalSpaceKB 
        DESC;
    """
    # Top 10 - Jobs Ultima execução e Tempo
    QUERY3 = '''
        SELECT TOP (10)
            j.name AS JobName,
            msdb.dbo.agent_datetime(h.run_date, h.run_time) AS RunDateTime,
            STUFF(STUFF(RIGHT('000000' + CAST(h.run_duration AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
                AS DurationHHMMSS,
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
        ) AS l
            ON j.job_id = l.job_id
        INNER JOIN msdb.dbo.sysjobhistory AS h ON h.job_id = l.job_id AND h.instance_id = l.max_instance_id
        WHERE
            j.enabled = 1 AND h.step_id = 0
        ORDER BY
            JobName,
            RunDateTime DESC;
    '''
    # 
    QUERY4 = '''
    SELECT TOP 10
        CONVERT(varchar(8),
            DATEADD(SECOND, qs.total_worker_time / 1000000, 0),
            108
        ) AS total_cpu_time_hhmmss,

        qs.execution_count,

        CONVERT(varchar(8),
            DATEADD(SECOND,
                (qs.total_worker_time / NULLIF(qs.execution_count, 0)) / 1000000,
                0
            ),
            108
        ) AS avg_cpu_time_hhmmss,

        SUBSTRING(qt.text,
            (qs.statement_start_offset / 2) + 1,
            ((CASE qs.statement_end_offset
                WHEN -1 THEN DATALENGTH(qt.text)
                ELSE qs.statement_end_offset
            END - qs.statement_start_offset) / 2) + 1
        ) AS statement_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    ORDER BY qs.total_worker_time DESC;
    '''
    # Top 10 Conexões 
    QUERY5 = f'''
        WITH base AS (
            SELECT 
                nome_odbc,
                banco,
                login_name,
                data_coleta,
                SUM(open_connections) AS total_conexoes
            FROM conexoes_abertas_sqlserver
            WHERE nome_odbc = '{db["mysql_banco"]}'
            AND DATE(data_coleta) = CURDATE()
            AND TIME(data_coleta) BETWEEN '07:00:00' AND '19:00:00'
            GROUP BY nome_odbc, banco, login_name, data_coleta
        ),
        media_login AS (
            SELECT     
                nome_odbc,     
                banco,     
                login_name,     
                AVG(total_conexoes) AS media_conexoes
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
    '''
    # Top tempo médio por query
    QUERY6 = f'''
        SELECT 
            banco,     
            query_id,     
            MAX(execution_count)    AS execution_count,     
            MAX(total_cpu_hhmmss)  AS total_cpu_hhmmss,     
            MAX(avg_cpu_hhmmss)    AS avg_cpu_hhmmss,     
            MAX(data_coleta)       AS ultima_coleta,
            query_text
        FROM top10_querystore_sqlserver 
        WHERE nome_odbc='{db["mysql_banco"]}' and DATE(data_coleta) = CURDATE() 
        GROUP BY nome_odbc, banco, query_id 
        ORDER BY total_cpu_hhmmss 
        DESC LIMIT 10;
    '''
    # DIRETORIOS SIZE DOCKER
    QUERY7 = '''
        SELECT
            h.host,
            i.name,
            MAX(CASE
            WHEN t.clock BETWEEN
            UNIX_TIMESTAMP(CURDATE())
            AND UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))
            THEN t.value_max / 1073741824
            END) AS max_hoje_gb,

            MAX(CASE
            WHEN t.clock BETWEEN
            UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 30 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 29 DAY))
            THEN t.value_max / 1073741824
            END) AS max_30_dias_gb,

            MAX(CASE
            WHEN t.clock BETWEEN
            UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 60 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 59 DAY))
            THEN t.value_max / 1073741824
            END) AS max_60_dias_gb,

            MAX(CASE
            WHEN t.clock BETWEEN
            UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 90 DAY))
            AND UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 89 DAY))
            THEN t.value_max / 1073741824
            END) AS max_90_dias_gb

        FROM trends_uint t
        JOIN items i ON t.itemid = i.itemid
        JOIN hosts h ON i.hostid = h.hostid
        JOIN hosts_groups hg ON h.hostid = hg.hostid
        WHERE h.hostid = 10636
            AND i.name LIKE '%Directory size%'
        GROUP BY
            h.host,
            i.itemid,
            i.name
        ORDER BY
            max_hoje_gb DESC;
    '''
    #----- Query para crescimento das tabelas -----#
    QUERY_TOP_TABELAS_MB = f"""
    SELECT
        nome_odbc,
        banco,
        tabela,

        ROUND(MAX(CASE
            WHEN data_coleta <= NOW()
            THEN tamanho_mb
        END), 2) AS hoje_mb,

        ROUND(MAX(CASE
            WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 15 DAY)
            THEN tamanho_mb
        END), 2) AS dias_15_mb,

        ROUND(MAX(CASE
            WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 30 DAY)
            THEN tamanho_mb
        END), 2) AS dias_30_mb,

        ROUND(MAX(CASE
            WHEN data_coleta <= DATE_SUB(NOW(), INTERVAL 60 DAY)
            THEN tamanho_mb
        END), 2) AS dias_60_mb

    FROM tamanho_tabelas_sqlserver
    WHERE nome_odbc = '{db["mysql_banco"]}'
    GROUP BY nome_odbc, banco, tabela
    ORDER BY hoje_mb DESC
    LIMIT 10;
    """

#----- Drive utilizado para conectar nos MSSQL -----#    
    conn_str = _build_mssql_connection_string(db)

    return run_queries(
    db,
    [
        QUERY1,
        QUERY2,
        QUERY3,
        QUERY4,
        QUERY5,
        QUERY6,
        QUERY7,
        QUERY_TOP_TABELAS_MB,
    ],
    conn_str
)

#----- Função criada devido ao tempo de Query executada -----#
def run_query2_pyodbc(conn_str, query):
    rows = []
    try:
        cn = pyodbc.connect(conn_str, autocommit=True)
        cur = cn.cursor()
        try:
            cur.execute(query)
            while True:
                try:
                    if cur.description:
                        columns = [col[0] for col in cur.description]
                        for r in cur.fetchall():
                            rows.append(dict(zip(columns, r)))
                except Exception:
                    pass
                if not cur.nextset():
                    break
        except pyodbc.ProgrammingError as e:
            error_msg = str(e).lower()

            # ===== TRATAMENTO DE BANCO OFFLINE / ERRO =====
            if "offline" in error_msg:
                import re
                match = re.search(r"database '([^']+)'", str(e), re.IGNORECASE)
                if match:
                    dbname = match.group(1)
                else:
                    dbname = "desconhecido"
                DB_PROBLEMS.append(f"{dbname} (OFFLINE)")
                print(f"[AVISO] Banco OFFLINE ignorado: {dbname}")
                return []
            else:
                DB_PROBLEMS.append(f"Erro ao executar query: {str(e)}")
                print(f"[ERRO] Erro ao executar query: {e}")
                return []
        finally:
            cur.close()
            cn.close()
    except Exception as conn_err:
        DB_PROBLEMS.append(f"Falha de conexão: {str(conn_err)}")
        print(f"[ERRO] Falha ao conectar: {conn_err}")
        return []
    return rows

#----- Definição de execução das queries -----#
def run_queries(db, queries, conn_str):
    from sqlalchemy import create_engine, text
    from urllib.parse import quote_plus

    data = []

    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)
    )

    # conexão MySQL (Zabbix)
    conn_zabbix = mysql.connector.connect(
        host=ZABBIX_HOST,
        user=ZABBIX_USER,
        password=ZABBIX_PASS,
        database=ZABBIX_DB
    )
    cursor_zabbix = conn_zabbix.cursor(dictionary=True)

    # conexão MySQL
    conn_mysql = mysql.connector.connect(
        host=ZABBIX_HOST,
        user=ZABBIX_USER,
        password=ZABBIX_PASS,
        database=ZABBIX_DB2
    )
    cursor_mysql = conn_mysql.cursor(dictionary=True)

    # --- Status Server ---
    cursor_zabbix.execute(_replace_host_id(QUERY_HOST_STATUS, SERVER_HOSTID))
    host_status_data = cursor_zabbix.fetchall()
    
    # --- Métricas 24h (Zabbix) ---
    cursor_zabbix.execute(_replace_host_id(QUERY_HOST_METRICS_1D, SERVER_HOSTID))
    host_metrics_1d = cursor_zabbix.fetchall()

    # --- Alarmes 24h (Zabbix) ---
    cursor_zabbix.execute(_replace_host_id(QUERY_HOST_ALARMS_24H, SERVER_HOSTID))
    host_alarms_24h = cursor_zabbix.fetchall()

    # # --- Crescimento das tabelas ---
    # cursor_mysql.execute(QUERY_TOP_TABELAS_MB)
    # top_tabelas_mb = cursor_mysql.fetchall()

    # --- Crescimento das tabelas ---
    cursor_zabbix.execute(_replace_host_id(QUERY_DOCKER_STATUS, SERVER_HOSTID))
    docker_status_data = cursor_zabbix.fetchall()

    # ---------- Queries MSSQL / Zabbix ----------
    for idx, query in enumerate(queries, start=1):

        # QUERY1 e QUERY7 → Zabbix (MySQL)
        if idx in (1, 7):
            cursor_zabbix.execute(query)
            data.append(cursor_zabbix.fetchall())

        # QUERY2 → pyodbc (SQL dinâmico pesado)
        elif idx == 2:
            data.append(run_query2_pyodbc(conn_str, query))
        
        # QUERIES 5, 6 e 7 → Executada no banco banco MySQL
        elif idx in (5, 6, 8):
            cursor_mysql.execute(query)
            data.append(cursor_mysql.fetchall())
            # cursor_mysql.execute(query)
            # rows = cursor_mysql.fetchall()
            # columns = [col[0] for col in cursor_mysql.description]
            # data.append([dict(zip(columns, row)) for row in rows])

        # Demais → SQL Server via SQLAlchemy
        else:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                data.append([dict(row) for row in result.mappings()])

    cursor_zabbix.close()
    conn_zabbix.close()
    conn_mysql.close()

    return db['port'], data, host_status_data, docker_status_data, host_metrics_1d, host_alarms_24h

#----- Criação de HTML para envio do email -----#
def build_power_checklist_html():
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    html = f"""
    <div style="width:100%; display:flex; justify-content:center;">
      <table style="border-collapse:collapse; width:620px; font-family:Arial, sans-serif; background-color:#f9f9f9; border:1px solid #d9d9d9;">
        <tr>
          <td style="background-color:#2f2f2f; color:#ffffff; padding:12px; text-align:center; font-size:18px; font-weight:bold;">
            POWER CHECKLIST – Monitoramento Diário de Banco de Dados
          </td>
        </tr>

        <tr>
          <td style="padding:15px; color:#333333; font-size:13px; line-height:1.6;">
            Este relatório apresenta uma visão consolidada do ambiente de banco de dados, permitindo o acompanhamento diário
            sem a necessidade de acesso direto ao SQL Server. Seu objetivo é facilitar a identificação de anomalias,
            falhas de serviço ou indisponibilidades, contribuindo para a estabilidade e confiabilidade do ambiente.
            <br/><br/>
            O recebimento regular deste relatório indica que os processos de coleta, validação e envio estão operando corretamente.
            Caso o relatório não seja recebido, recomenda-se a verificação imediata dos serviços de monitoramento e do fluxo de envio de e-mails.
            <br/><br/>
            Além do acompanhamento diário, este checklist também pode ser utilizado para análises pontuais e validações rápidas
            do estado geral do ambiente, auxiliando no diagnóstico preventivo e na tomada de decisão, sem a necessidade de acesso direto aos servidores.
          </td>
        </tr>

        <tr>
          <td style="padding:12px; background-color:#ffffff; font-size:13px;">
            <b>Informações do Relatório</b><br/>
            Data e Hora do Envio: {now_str}<br/>
            Ambiente Monitorado: Folha de Pagamento<br/>
            Empresa: Infomix Tecnologia
          </td>
        </tr>

        <tr>
          <td style="padding:12px; background-color:#ffffff; font-size:13px;">
            <b>Contato de Emergência</b><br/>
            André Andrade<br/>
            andrade@infomixtecnologia.com.br | Tel.: (71) 98600-4797<br/><br/>
            Marcelo Brito<br/>
            marcelo@infomixtecnologia.com.br | Tel.: (71) 98876-3757
          </td>
        </tr>
      </table>
    </div>
    """
    return html

#----- Configuração de SMTP -----#
def send_email_with_pdf(pdf_path, recipients=None):
    _validate_runtime_configuration(require_email=True)
    to_emails = recipients or DEFAULT_EMAIL_RECIPIENTS
    if not to_emails:
        raise RuntimeError("Nenhum destinatário de email foi configurado para o relatório.")

    subject = "POWER CHECKLIST - Monitoramento Diário Banco de Dados"

    html_body = build_power_checklist_html()

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(to_emails)

    msg.set_content("Seu cliente de e-mail não suporta HTML.")
    msg.add_alternative(html_body, subtype="html")

    # Anexar PDF
    with open(pdf_path, "rb") as f:
        pdf_data = f.read()
        msg.add_attachment(
            pdf_data,
            maintype="application",
            subtype="pdf",
            filename=os.path.basename(pdf_path)
        )

    context = ssl.create_default_context()

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

    print("[OK] E-mail enviado com sucesso via Zoho (587 + STARTTLS)!")


def generate_report(output_dir=None, db_list=None, output_filename=None):
    _validate_runtime_configuration(require_email=False)
    DB_PROBLEMS.clear()

    selected_dbs = db_list or DB_LIST
    dbs_tops = []

    for db in selected_dbs:
        result = get_db_tops(db)
        if result:
            dbs_tops.append(result)

    if not dbs_tops:
        raise RuntimeError("Nenhum banco válido retornou dados. PDF não será gerado.")

    report_output_dir = output_dir or OUTPUT_DIR
    return generate_pdf_unificado(report_output_dir, dbs_tops, output_filename=output_filename)


def execute_report(output_dir=None, db_list=None, output_filename=None, send_email=False, recipients=None):
    report_path = generate_report(
        output_dir=output_dir,
        db_list=db_list,
        output_filename=output_filename,
    )

    emailed = False
    if send_email:
        send_email_with_pdf(report_path, recipients=recipients)
        emailed = True

    return {
        "output_path": report_path,
        "emailed": emailed,
        "warnings": sorted(set(DB_PROBLEMS)),
    }

if __name__ == "__main__":
    result = execute_report(send_email=True)
    print(f"[OK] PDF unificado gerado: {result['output_path']}")
