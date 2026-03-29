#!/bin/sh
set -e

echo "Aplicando migrations..."
alembic upgrade head

if [ "${AUTO_CREATE_ADMIN:-false}" = "true" ]; then
	ADMIN_USERNAME_VALUE="${ADMIN_USERNAME:-Administrador}"
	ADMIN_EMAIL_VALUE="${ADMIN_EMAIL:-admin@local}"

	if [ -z "${ADMIN_PASSWORD:-}" ]; then
		echo "AUTO_CREATE_ADMIN=true, mas ADMIN_PASSWORD nao foi definido. Pulando criacao automatica do admin."
	else
		echo "Tentando criar admin inicial (se nao existir)..."
		python -m app.scripts.create_admin \
			--username "${ADMIN_USERNAME_VALUE}" \
			--email "${ADMIN_EMAIL_VALUE}" \
			--password "${ADMIN_PASSWORD}" \
			|| echo "Admin inicial ja existe (ou houve aviso na criacao). Seguindo inicializacao."
	fi
fi

echo "Iniciando API..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
