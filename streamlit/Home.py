# app.py
import os
import socket
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import streamlit as st
import requests
from textwrap import dedent

CARD_CSS = """
<style>
.card { border-radius:16px; padding:16px; box-shadow:0 4px 18px rgba(0,0,0,.08);
        border:1px solid rgba(0,0,0,.06); height:120px; }
.card h3 { margin:0 0 6px 0; font-size:1.05rem; }
.pill { display:inline-block; padding:4px 10px; border-radius:999px; font-weight:600; font-size:.85rem; }
.ok { background:#e6ffed; color:#087f5b; border:1px solid #c3f3d6; }
.down { background:#ffe3e3; color:#c92a2a; border:1px solid #ffb3b3; }
.small { color:#5f6c72; font-size:.85rem; }
</style>
"""

# --- Optional deps for deeper checks (handled if installed) ---
try:
    import boto3  # for MinIO S3 check
    from botocore.client import Config
except Exception:
    boto3 = None
try:
    import pika  # for RabbitMQ AMQP check
except Exception:
    pika = None

# at top (near other imports)
try:
    from streamlit_autorefresh import st_autorefresh  # tiny helper pkg
except Exception:
    st_autorefresh = None

# ----------------------------- Utilities -----------------------------
@dataclass
class Status:
    name: str
    ok: bool
    detail: str
    latency_ms: Optional[int] = None

def _tcp_ping(host: str, port: int, timeout: float = 1.5) -> Tuple[bool, int, str]:
    t0 = time.perf_counter()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, int(port)))
            return True, int((time.perf_counter() - t0) * 1000), "TCP connected"
        except Exception as e:
            return False, int((time.perf_counter() - t0) * 1000), f"TCP failed: {e}"

def _http_health(url: str, timeout: float = 1.5) -> Tuple[bool, int, str]:
    t0 = time.perf_counter()
    try:
        r = requests.get(url, timeout=timeout)
        ok = 200 <= r.status_code < 300
        return ok, int((time.perf_counter() - t0) * 1000), f"HTTP {r.status_code}"
    except Exception as e:
        return False, int((time.perf_counter() - t0) * 1000), f"HTTP failed: {e}"

import base64

def _rabbit_mgmt_get(path: str, timeout: float = 1.5):
    host = os.getenv("RABBIT_MGMT_HOST", "rabbitmq")
    port = os.getenv("RABBIT_MGMT_PORT", "15672")
    user = os.getenv("RABBIT_USER", os.getenv("RABBITMQ_USER", "guest"))
    pw   = os.getenv("RABBIT_PASS", os.getenv("RABBITMQ_PASS", "guest"))
    url = f"http://{host}:{port}{path}"
    try:
        r = requests.get(url, timeout=timeout, auth=(user, pw))
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}

# ----------------------------- Checks -----------------------------
def check_minio() -> Status:
    """
    Prefers a real S3 call if boto3 is available & creds are set,
    otherwise falls back to TCP.
    """
    name = "MinIO"
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    host, port = (endpoint.split(":") + ["9000"])[:2]

    # Try a real S3 list-buckets call if possible
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "0") not in ("0", "false", "False", "FALSE")

    if boto3 and access_key and secret_key:
        scheme = "https" if secure else "http"
        s3 = boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            verify=False if not secure else True,
        )
        t0 = time.perf_counter()
        try:
            s3.list_buckets()
            latency = int((time.perf_counter() - t0) * 1000)
            return Status(name, True, "S3 list_buckets OK", latency)
        except Exception as e:
            # fall back to TCP ping if S3 call fails
            ok, ms, detail = _tcp_ping(host, port)
            return Status(name, ok, f"S3 failed: {e} | {detail}", ms)
    else:
        ok, ms, detail = _tcp_ping(host, port)
        return Status(name, ok, detail, ms)

def check_rabbitmq() -> Status:
    name = "RabbitMQ"
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))

    # If pika is installed, try a real AMQP open/close
    if pika:
        creds_user = os.getenv("RABBITMQ_USER", "guest")
        creds_pass = os.getenv("RABBITMQ_PASS", "guest")
        params = pika.ConnectionParameters(
            host=host, port=port, credentials=pika.PlainCredentials(creds_user, creds_pass), blocked_connection_timeout=1.5
        )
        t0 = time.perf_counter()
        try:
            conn = pika.BlockingConnection(params)
            conn.close()
            latency = int((time.perf_counter() - t0) * 1000)
            return Status(name, True, "AMQP handshake OK", latency)
        except Exception as e:
            # fall back to TCP ping
            ok, ms, detail = _tcp_ping(host, port)
            return Status(name, ok, f"AMQP failed: {e} | {detail}", ms)
    else:
        ok, ms, detail = _tcp_ping(host, port)
        return Status(name, ok, detail, ms)
    
def check_worker_by_queue(queue_env: str, default_queue: str, friendly: str) -> Status:
    """
    Check a worker by looking at a RabbitMQ queue's consumers.
    - If {queue_env} is set, use that as the queue name.
    - Otherwise, fall back to default_queue (e.g. "extract").
    """
    vhost = os.getenv("RABBIT_VHOST", "/")
    queue = os.getenv(queue_env, default_queue)

    data = _rabbit_mgmt_get(
        f"/api/queues/"
        f"{requests.utils.quote(vhost, safe='')}/"
        f"{requests.utils.quote(queue, safe='')}"
    )

    if "_error" in data:
        return Status(friendly, False, f"Mgmt API error for '{queue}': {data['_error']}")

    consumers = data.get("consumers", 0)
    msgs_ready = data.get("messages_ready", 0)
    ok = consumers > 0              # at least one consumer attached
    detail = f"queue={queue}, consumers={consumers}, ready={msgs_ready}"
    return Status(friendly, ok, detail)


def check_extractor() -> Status:
    # prefer a health URL if you ever expose one
    if os.getenv("EXTRACTOR_HEALTH_URL"):
        ok, ms, detail = _http_health(os.getenv("EXTRACTOR_HEALTH_URL"))
        return Status("Extractor", ok, detail, ms)
    # otherwise infer from RabbitMQ queue
    return check_worker_by_queue("EXTRACTOR_QUEUE", "extract", "Extractor")


def check_transformer() -> Status:
    if os.getenv("TRANSFORMER_HEALTH_URL"):
        ok, ms, detail = _http_health(os.getenv("TRANSFORMER_HEALTH_URL"))
        return Status("Transformer", ok, detail, ms)
    return check_worker_by_queue("TRANSFORMER_QUEUE", "transform", "Transformer")


def check_cleaner() -> Status:
    if os.getenv("CLEANER_HEALTH_URL"):
        ok, ms, detail = _http_health(os.getenv("CLEANER_HEALTH_URL"))
        return Status("Cleaner", ok, detail, ms)
    return check_worker_by_queue("CLEANER_QUEUE", "clean", "Cleaner")

def _service_check_generic(service_env_prefix: str, friendly_name: str) -> Status:
    """
    If {PREFIX}_HEALTH_URL is set, call it (expects 2xx).
    Else, try TCP with {PREFIX}_HOST and {PREFIX}_PORT.
    """
    url = os.getenv(f"{service_env_prefix}_HEALTH_URL")
    if url:
        ok, ms, detail = _http_health(url)
        return Status(friendly_name, ok, detail, ms)

    host = os.getenv(f"{service_env_prefix}_HOST", "localhost")
    port = int(os.getenv(f"{service_env_prefix}_PORT", "8000"))
    ok, ms, detail = _tcp_ping(host, port)
    return Status(friendly_name, ok, detail, ms)




# ----------------------------- UI -----------------------------
CARD_CSS = """
<style>
.card-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 18px;
  margin-top: 12px;
}

.card {
  border-radius: 18px;
  padding: 16px 18px;
  background: #ffffff;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.10);
  border: 1px solid rgba(148, 163, 184, 0.25);
  display: flex;
  flex-direction: column;
  gap: 8px;
  transition: transform 120ms ease-out, box-shadow 120ms ease-out,
              border-color 120ms ease-out, background 120ms ease-out;
}

.card:hover {
  transform: translateY(-2px);
  box-shadow: 0 16px 40px rgba(15, 23, 42, 0.16);
  border-color: rgba(94, 234, 212, 0.7);
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.card h3 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}

.pill {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 4px 10px;
  border-radius: 999px;
  font-weight: 600;
  font-size: 0.80rem;
  letter-spacing: .02em;
}

.ok {
  background: #e6ffed;
  color: #087f5b;
  border: 1px solid #c3f3d6;
}

.down {
  background: #ffe3e3;
  color: #c92a2a;
  border: 1px solid #ffb3b3;
}

.small {
  color: #6b7280;
  font-size: 0.86rem;
  line-height: 1.3;
}
</style>
"""


def render_card(status: Status):
    state = "ok" if status.ok else "down"
    emoji = "✅" if status.ok else "❌"
    pill_text = "HEALTHY" if status.ok else "DOWN"

    html = dedent(f"""
    <div class="card">
      <div class="card-header">
        <h3>{emoji} {status.name}</h3>
        <span class="pill {state}">{pill_text}</span>
      </div>
      <div class="small">
        {status.detail}
      </div>
    </div>
    """)
    st.markdown(html, unsafe_allow_html=True)



def main():
    st.markdown(CARD_CSS, unsafe_allow_html=True)
    st.set_page_config(page_title="Pipeline Health", page_icon="🩺", layout="wide")
    st.title("🩺 Pipeline Health Dashboard")

    st.info(
    """
### Pipeline Overview

**Label predicted:** `crash_type` • **Type:** `Categorical` •  

**Pipeline:**  
We built a model to predict `crash_type` using context like `Road Conditions, Lighting Conditions, and Speed Limit`.

**Key features:**
- `roadway_surface_cond` — Surface conditions with high correlation with crash type might require an infrastructure improvement
- `weather_conditions` — Weather conditions might work in combination with other features to help predict crash type
- `posted_speed_limit` — Speed Limit is usually a relevent metric for crash information
- `lighting_condition` — Low lighting conditions with high crash frequency may be an indicator for infrastructure improvements

**Source columns:**
- **crashes:** `crash_type, crash_record_id, posted_speed_limit, traffic_control_device, device_condition, weather_condition, lighting_condition, trafficway_type, lane_cnt, alignment, roadway_surface_cond, road_defect, street_no, street_name, street_direction`  

**Class imbalance:**
- Ratio: 34%  

**Data grain & filters:**
- One row = `crash`  
- Filters: `drop null`

**Gold table:** `gold.duckdb`
""",
    icon="ℹ️",
)


    st.caption("MinIO • RabbitMQ • Extractor • Transformer • Cleaner")

    # Sidebar controls
    col1, col2 = st.sidebar.columns([2, 1])
    with col1:
        refresh_sec = st.number_input("Auto-refresh (seconds)", min_value=0, max_value=120, value=10, step=1)
    with col2:
        do_refresh = st.checkbox("Enable", value=True)

# --- Auto-refresh (uses package if available; else provide a manual refresh) ---
    if do_refresh and refresh_sec > 0 and st_autorefresh:
    # real timer-based refresh
        st_autorefresh(interval=int(refresh_sec * 1000), key="health_autorefresh")
    elif st.button("Refresh now"):
    # manual fallback
        st.query_params.update({"_": str(int(time.time()))})
        st.rerun()


    # Run checks
    checks = [
        check_minio(),
        check_rabbitmq(),
        check_extractor(),
        check_transformer(),
        check_cleaner(),
    ]

    # Layout: 5 cards in a responsive grid
    st.markdown('<div class="card-grid">', unsafe_allow_html=True)
    for s in checks:
        render_card(s)
    st.markdown('</div>', unsafe_allow_html=True)

    # Raw details (useful for debugging)
    with st.expander("Raw status JSON"):
        st.json([s.__dict__ for s in checks])

if __name__ == "__main__":
    main()
