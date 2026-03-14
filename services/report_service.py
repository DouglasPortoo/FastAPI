from datetime import datetime, timezone
from email.utils import parseaddr
from pathlib import Path
from uuid import uuid4

from relatorio_folha_diario_new import DB_LIST, DEFAULT_EMAIL_RECIPIENTS, OUTPUT_DIR, execute_report


class ReportServiceError(Exception):
    pass


def _validate_recipients(recipients):
    invalid_recipients = []
    for recipient in recipients:
        parsed = parseaddr(recipient)[1]
        if not parsed or "@" not in parsed:
            invalid_recipients.append(recipient)

    if invalid_recipients:
        raise ReportServiceError(
            "Destinatários de email inválidos: " + ", ".join(sorted(invalid_recipients))
        )


def _resolve_db_list(ports):
    if not ports:
        return DB_LIST

    available_dbs = {str(db["port"]): db for db in DB_LIST}
    unknown_ports = sorted({str(port) for port in ports if str(port) not in available_dbs})
    if unknown_ports:
        raise ReportServiceError(
            "Portas de banco não configuradas para o relatório: " + ", ".join(unknown_ports)
        )

    return [available_dbs[str(port)] for port in ports]


def generate_report_payload(request_data):
    selected_dbs = _resolve_db_list(request_data.ports)
    recipients = request_data.recipients or DEFAULT_EMAIL_RECIPIENTS

    if request_data.send_email:
        _validate_recipients(recipients)

    report_id = f"report_{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{uuid4().hex[:8]}"
    output_filename = f"{report_id}.pdf"

    try:
        result = execute_report(
            output_dir=OUTPUT_DIR,
            db_list=selected_dbs,
            output_filename=output_filename,
            send_email=request_data.send_email,
            recipients=recipients,
        )
    except RuntimeError as exc:
        raise ReportServiceError(str(exc)) from exc

    output_path = Path(result["output_path"])
    return {
        "report_id": report_id,
        "file_name": output_path.name,
        "download_url": f"/reports/download/{report_id}",
        "generated_at": datetime.now(timezone.utc),
        "emailed": result["emailed"],
        "warnings": result["warnings"],
        "selected_ports": [str(db["port"]) for db in selected_dbs],
    }


def get_report_file_path(report_id):
    safe_report_id = Path(report_id).stem
    if safe_report_id != report_id:
        raise ReportServiceError("Identificador de relatório inválido.")

    report_path = Path(OUTPUT_DIR) / f"{safe_report_id}.pdf"
    if not report_path.exists():
        raise ReportServiceError("Relatório não encontrado para download.")

    return report_path