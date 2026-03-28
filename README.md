# Relatorio Folha API

Guia rapido para rodar este projeto em outro computador Windows.

## 1) Pre-requisitos

- Python 3.11+ instalado
- PowerShell
- Git (opcional, para clonar)

## 2) Clonar e entrar na pasta

Se ainda nao clonou:

    git clone <URL_DO_REPOSITORIO>
    cd python

Se ja baixou o zip, apenas entre na pasta do projeto.

## 3) Criar e ativar ambiente virtual

    python -m venv venv
    .\venv\Scripts\Activate.ps1

Se der erro de politica de execucao no PowerShell, rode uma vez:

    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

## 4) Instalar dependencias

    pip install -r requirements.txt

## 5) Configurar variaveis de ambiente (.env)

Este projeto usa o arquivo .env na raiz.

- Se ja existir .env, mantenha.
- Se nao existir, crie copiando de um .env valido do projeto.

Variavel principal de banco local:

    DATABASE_URL=sqlite:///banco.db

## 6) Criar banco e tabelas

Opcao recomendada (migrations Alembic):

    alembic upgrade head

Alternativa: iniciar a API tambem cria tabelas (via SQLAlchemy create_all no startup).

## 7) Subir a API

    uvicorn app.main:app --reload

API sobe em:

    http://127.0.0.1:8000

## 8) Teste rapido

Abra no navegador:

    http://127.0.0.1:8000/docs

## 9) Projeto novo: criar primeiro usuario admin

Como a rota de cadastro exige um usuario autenticado e admin, em banco vazio voce precisa criar o primeiro admin manualmente uma unica vez.

Com o venv ativado, rode:

    python -m app.scripts.create_admin

Padrao do script:

- username: Administrador
- email: admin@local
- senha: solicitada no terminal

Exemplo com parametros:

    python -m app.scripts.create_admin --username "Administrador" --email admin@local --password "Admin@123"

Depois faca login normalmente:

    POST /api/auth/login

Body exemplo:

    {
      "email": "admin@local",
      "password": "Admin@123"
    }

Importante: altere a senha inicial logo apos o primeiro acesso.

## Observacoes

- Em outro PC, o arquivo banco.db normalmente nao existe no inicio. Ele e criado apos migration/startup.
- Se voce quiser manter os dados antigos, copie o arquivo banco.db do computador anterior para a raiz do projeto.
- Em Windows local, caminhos Linux em REPORT_OUTPUT_DIR e REPORT_LOGO_PATH sao tratados pelo codigo para fallback local quando necessario.

## Comandos resumidos (colar e executar)

    python -m venv venv
    .\venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    alembic upgrade head
    uvicorn app.main:app --reload
