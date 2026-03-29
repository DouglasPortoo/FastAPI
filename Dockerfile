FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias de sistema para compilar/extensoes Python e suporte do pyodbc.
RUN apt-get update && apt-get install -y --no-install-recommends \
  gcc \
  g++ \
  unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt

# requirements.txt do projeto esta em UTF-16; converte para UTF-8 para pip.
RUN python -c "from pathlib import Path; p = Path('requirements.txt'); p.write_text(p.read_text(encoding='utf-16'), encoding='utf-8')"

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x ./entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
