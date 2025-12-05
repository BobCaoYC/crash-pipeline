# Small, production-friendly image
FROM python:3.11-slim

# DuckDB uses mmap; add minimal system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt prometheus-client

COPY cleaner.py cleaning_rules.py duckdb_writer.py minio_io.py ./

# Create data dir for the DB (mount this as a volume in compose)
VOLUME ["/data"]

ENV PYTHONUNBUFFERED=1

# Typical envs (override in Compose)
ENV GOLD_DB_PATH=/data/gold.duckdb \
    CLEAN_QUEUE=clean

CMD ["python", "cleaner.py"]
