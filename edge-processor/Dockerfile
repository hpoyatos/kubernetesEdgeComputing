FROM python:3.11-slim

# Instala o utilitário de linha de comando sqlite3 para permitir inspeção direta no Pod
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

# Cria o diretório de dados para o SQLite
RUN mkdir -p /data

CMD ["python", "processor.py"]
