import json
import logging
import os
import sys
import traceback
from typing import Optional
from time import perf_counter

import pandas as pd
import pika
from dotenv import load_dotenv

from cleaning_rules import apply_cleaning
from duckdb_writer import DuckDBWriter
from minio_io import MinioIO, MinioConfig

# --- Prometheus instrumentation ---
from prometheus_client import Counter, Histogram, Gauge, start_http_server

load_dotenv(override=False)

def getenv_any(keys, *, required=True, default=None):
    """Return the first non-empty env var among `keys`."""
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
    if required and default is None:
        raise RuntimeError(f"Missing required environment variable: one of {', '.join(keys)}")
    return default

def as_bool(s: Optional[str], default=False) -> bool:
    if s is None:
        return default
    return str(s).strip().lower() in {"1", "true", "t", "yes", "y", "on"}

# ---------- required (with aliases) ----------
RABBITMQ_URL  = getenv_any(["RABBITMQ_URL"])
MINIO_ENDPOINT= getenv_any(["MINIO_ENDPOINT", "S3_ENDPOINT", "MINIO_URL"])
MINIO_ACCESS  = getenv_any(["MINIO_ACCESS_KEY", "MINIO_USER", "AWS_ACCESS_KEY_ID"])
MINIO_SECRET  = getenv_any(["MINIO_SECRET_KEY", "MINIO_PASS", "AWS_SECRET_ACCESS_KEY"])

# Prefer an explicit Silver/Transform bucket; fall back to generic names
MINIO_BUCKET  = getenv_any(["MINIO_BUCKET", "XFORM_BUCKET", "CLEAN_BUCKET"])

# ---------- optional ----------
MINIO_SECURE  = as_bool(getenv_any(["MINIO_SECURE", "MINIO_SSL"], required=False, default="false"))
CLEAN_QUEUE   = os.getenv("CLEAN_QUEUE", "clean")
GOLD_DB_PATH  = os.getenv("GOLD_DB_PATH", "/data/gold.duckdb")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "")
SILVER_NAME   = os.getenv("SILVER_NAME", "merged.csv")
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO")

# Metrics HTTP server port
METRICS_PORT  = int(os.getenv("METRICS_PORT", "8000"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cleaner")

# ----------------- Prometheus metrics -----------------
# 1. Service uptime (record start time)
SERVICE_START_TIME = Gauge(
    "cleaner_start_time_seconds",
    "Unix timestamp when the cleaner service started"
)

# 2. Run / request count (+ 7. success vs failure)
CLEANER_RUNS_TOTAL = Counter(
    "cleaner_runs_total",
    "Total number of cleaner runs by status",
    ["status"]  # status = started | success | failure
)

# 3. Error count
CLEANER_ERRORS_TOTAL = Counter(
    "cleaner_errors_total",
    "Total number of errors in cleaner runs by exception type",
    ["type"]
)

# 4. Latency of each run
CLEANER_RUN_DURATION_SECONDS = Histogram(
    "cleaner_run_duration_seconds",
    "Duration of each cleaner run in seconds",
)

# 5. Rows processed
CLEANER_ROWS_PROCESSED_TOTAL = Counter(
    "cleaner_rows_processed_total",
    "Total number of rows processed by the cleaner"
)

# 6. Duration of each major function
CLEANER_STAGE_DURATION_SECONDS = Histogram(
    "cleaner_stage_duration_seconds",
    "Duration of each major stage in the cleaner (download / clean / write)",
    ["stage"]  # stage = download_csv | apply_cleaning | upsert_duckdb
)

def get_corr_id_from_message(body: bytes, props: pika.BasicProperties) -> Optional[str]:
    if props and props.headers and "corr_id" in props.headers:
        return str(props.headers["corr_id"])
    try:
        payload = json.loads(body.decode("utf-8"))
        return str(payload.get("corr_id"))
    except Exception:
        return None

def main():
    # Start Prometheus metrics HTTP server (non-blocking thread)
    start_http_server(METRICS_PORT)
    SERVICE_START_TIME.set_to_current_time()
    log.info("Prometheus metrics server started on port %d", METRICS_PORT)

    mio = MinioIO(MinioConfig(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
        bucket=MINIO_BUCKET,
        silver_prefix=SILVER_PREFIX,
        silver_filename=SILVER_NAME,
    ))
    writer = DuckDBWriter(GOLD_DB_PATH)

    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=CLEAN_QUEUE, durable=True)
    log.info(
        "Cleaner listening on '%s' (Gold DB: %s, Silver name: %s)",
        CLEAN_QUEUE, GOLD_DB_PATH, SILVER_NAME
    )

    def handle(ch, method, properties, body):
        corr_id = get_corr_id_from_message(body, properties)
        if not corr_id:
            log.error("Rejecting message with no corr_id; body=%s", body[:200])
            CLEANER_ERRORS_TOTAL.labels(type="missing_corr_id").inc()
            CLEANER_RUNS_TOTAL.labels(status="failure").inc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        log.info("Start clean corr_id=%s", corr_id)
        CLEANER_RUNS_TOTAL.labels(status="started").inc()
        run_start = perf_counter()

        try:
            # --- Stage: download CSV from MinIO ---
            stage_t0 = perf_counter()
            csv_path = mio.download_csv_for_corr(corr_id)
            CLEANER_STAGE_DURATION_SECONDS.labels(stage="download_csv").observe(
                perf_counter() - stage_t0
            )

            if not csv_path:
                raise FileNotFoundError(f"No '{SILVER_NAME}' in Silver for corr_id={corr_id}")

            # --- Read & clean data ---
            stage_t0 = perf_counter()
            df = pd.read_csv(csv_path)
            clean_df = apply_cleaning(df)
            CLEANER_STAGE_DURATION_SECONDS.labels(stage="apply_cleaning").observe(
                perf_counter() - stage_t0
            )

            # --- Stage: upsert into DuckDB ---
            stage_t0 = perf_counter()
            affected = writer.upsert("crashes", clean_df, pk="crash_record_id")
            writer.record_run(corr_id=corr_id, table="crashes", rows=len(clean_df))
            CLEANER_STAGE_DURATION_SECONDS.labels(stage="upsert_duckdb").observe(
                perf_counter() - stage_t0
            )

            # Rows processed
            CLEANER_ROWS_PROCESSED_TOTAL.inc(len(clean_df))

            # Success metrics
            CLEANER_RUNS_TOTAL.labels(status="success").inc()
            CLEANER_RUN_DURATION_SECONDS.observe(perf_counter() - run_start)

            log.info("Done corr_id=%s -> affected rows=%s", corr_id, affected)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            CLEANER_ERRORS_TOTAL.labels(type=e.__class__.__name__).inc()
            CLEANER_RUNS_TOTAL.labels(status="failure").inc()
            CLEANER_RUN_DURATION_SECONDS.observe(perf_counter() - run_start)

            log.error("Error corr_id=%s: %s", corr_id, e)
            log.debug(traceback.format_exc())
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=CLEAN_QUEUE, on_message_callback=handle)
    channel.start_consuming()

if __name__ == "__main__":
    main()
