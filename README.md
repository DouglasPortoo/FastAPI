# Relatorio Folha API

Guia rapido para rodar o projeto do zero, com Docker (recomendado) ou localmente no Windows.

## Rodando com Docker (recomendado)

### 1. Clonar e entrar na pasta

  git clone <URL_DO_REPOSITORIO>
  cd python

### 2. Configurar variaveis no .env (opcional, recomendado)

Use o .env para definir credenciais do primeiro admin criado automaticamente no startup:

  ADMIN_USERNAME=<admin_username>
  ADMIN_EMAIL=<admin_email@example.com>
  ADMIN_PASSWORD=<admin_password>
  AUTO_CREATE_ADMIN=<true_or_false>

Observacao: o servico `api-relatorio` no Docker Compose carrega as variaveis do arquivo `.env`.

### 3. Subir a stack

  docker compose up --build -d

### 4. Acessar a API

  http://127.0.0.1:8000/docs

### 5. Ver logs

  docker compose logs -f api-relatorio

### 6. Parar a stack

  docker compose down

## Atualizar containers apos alteracoes no codigo

### Rebuild padrao

  docker compose up --build -d

### Rebuild somente da API

  docker compose up --build -d api-relatorio

### Rebuild sem cache

  docker compose build --no-cache api-relatorio
  docker compose up -d api-relatorio

### Validar se subiu corretamente

  docker compose logs -f api-relatorio
  curl.exe -I http://127.0.0.1:8000/docs

## Rodando local (sem Docker)

### 1. Pre-requisitos

- Python 3.11+
- PowerShell

### 2. Criar e ativar ambiente virtual

  python -m venv venv
  .\venv\Scripts\Activate.ps1

Se der erro de politica de execucao:

  Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

### 3. Instalar dependencias

  pip install -r requirements.txt

### 4. Configurar .env

Crie um arquivo .env na raiz com, no minimo:

  DATABASE_URL=postgresql+psycopg://<db_user>:<db_password>@localhost:5432/<db_name>

### 5. Aplicar migrations

  alembic upgrade head

### 6. Subir a API

  uvicorn app.main:app --reload

Acesse:

  http://127.0.0.1:8000/docs

## Criar usuario admin manualmente (modo local)

Necessario apenas na primeira execucao, quando o banco estiver vazio.

Com prompt de senha:

  python -m app.scripts.create_admin

Com parametros:

  python -m app.scripts.create_admin --username "<admin_username>" --email "<admin_email@example.com>" --password "<admin_password>"

Login:

  POST /api/auth/login

Body de exemplo:

  {
    "email": "<admin_email@example.com>",
    "password": "<admin_password>"
  }

Importante: altere a senha inicial apos o primeiro acesso.

## Observacoes

- Nome do projeto no Docker Compose: relatorio-folha-api
- Servico do banco no Docker: auth-server-db
- Volume persistente do Postgres: postgres_data
- A API tenta criar admin automaticamente no Docker quando AUTO_CREATE_ADMIN=true e ADMIN_PASSWORD estiver definido
- Diretorio de relatorios no host: generated_reports/
- Diretorio de relatorios no container: /app/generated_reports
- Para coletores externos (Zabbix/MySQL auxiliar), nao use `localhost` no `.env` quando rodar em Docker; use IP/DNS acessivel pelo container.

## Comandos rapidos (local)

  python -m venv venv
  .\venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  alembic upgrade head
  uvicorn app.main:app --reload