# pages/3_Data_Fetching.py
import os
import json
from datetime import datetime, timedelta, time as dtime, timezone
from typing import List
import base64
from urllib.parse import quote

import streamlit as st
import requests

st.set_page_config(page_title="Data Fetching", page_icon="📥", layout="wide")
st.title("📥 Data Fetching")

# --------------------------- Config ---------------------------
STREAMING_ENDPOINT = os.getenv("STREAMING_ENDPOINT", "http://extractor:8000/stream")
BACKFILL_ENDPOINT  = os.getenv("BACKFILL_ENDPOINT",  "http://extractor:8000/backfill")

STREAMING_TEMPLATE_PATH = os.getenv("STREAMING_TEMPLATE_PATH", "/app/streaming.json")
BACKFILL_TEMPLATE_PATH  = os.getenv("BACKFILL_TEMPLATE_PATH",  "/app/backfill.json")

AVAILABLE_COLUMNS = [c.strip() for c in os.getenv("AVAILABLE_COLUMNS", "").split(",") if c.strip()]

# RabbitMQ HTTP API defaults (inside docker network)
RABBIT_API_BASE = os.getenv("RABBIT_API_BASE", "http://rabbitmq:15672")
RABBIT_VHOST    = os.getenv("RABBIT_VHOST", "/")
RABBIT_USER     = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS     = os.getenv("RABBIT_PASS", "guest")
RABBIT_RK       = os.getenv("RABBIT_ROUTING_KEY", "extract")  # queue name or binding key

# --------------------------- Helpers ---------------------------
def load_template(path: str, default_obj: dict) -> dict:
    try:
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default_obj

def to_iso8601(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def curl_cmd(url: str, payload: dict) -> str:
    data = json.dumps(payload, separators=(",", ":"))
    return f"curl -s -X POST '{url}' -H 'Content-Type: application/json' -d '{data}'"

def post_json(url: str, payload: dict):
    try:
        r = requests.post(url, json=payload, timeout=30)
        return r.status_code, (r.text[:4000] if r.text else "")
    except Exception as e:
        return None, f"Request error: {e}"

def parse_free_text_columns(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]

def rabbit_publish(payload: dict, routing_key: str = RABBIT_RK):
    """Publish via RabbitMQ Management HTTP API (amq.default). Returns (status_code, json or text)."""
    url = f"{RABBIT_API_BASE}/api/exchanges/{quote(RABBIT_VHOST, safe='')}/amq.default/publish"
    b64 = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    body = {
        "properties": {"content_type": "application/json", "delivery_mode": 2},
        "routing_key": routing_key,
        "payload": b64,
        "payload_encoding": "base64",
    }
    try:
        r = requests.post(url, json=body, auth=(RABBIT_USER, RABBIT_PASS), timeout=20)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text
    except Exception as e:
        return None, f"Request error: {e}"

def render_response(status, body):
    import json as _json
    if status is None:
        st.error(body)
        return
    if 200 <= status < 300:
        st.success(f"OK • HTTP {status}")
    else:
        st.error(f"HTTP {status}")
    if isinstance(body, (dict, list)):
        st.code(_json.dumps(body, indent=2), language="json")
    else:
        st.code(body or "(no response body)")
    if isinstance(body, dict) and "routed" in body:
        st.write(f"**routed:** `{body['routed']}`")

# --------------------------- Mode Selector ---------------------------
delivery_mode = st.radio(
    "Delivery via",
    ["RabbitMQ API", "Extractor HTTP"],
    index=0,
    help="Send the job directly to RabbitMQ (Management API) or call the extractor HTTP shim.",
    key="delivery_mode",
)

# --------------------------- UI: Tabs ---------------------------
tab_stream, tab_backfill = st.tabs(["Streaming (last N days)", "Backfill (start/end)"])

# ======================= STREAMING =======================
with tab_stream:
    st.subheader("Streaming — fetch recent data window")

    streaming_template = load_template(
        STREAMING_TEMPLATE_PATH,
        {
            "mode": "streaming",
            "last_n_days": 1,
            "columns": [],
        },
    )

    col_left, col_right = st.columns([2, 3])

    with col_left:
        n_days = st.number_input(
            "Last N days",
            min_value=1,
            max_value=90,
            value=int(streaming_template.get("last_n_days", 1)),
            step=1,
            key="stream_n_days",
        )

        if AVAILABLE_COLUMNS:
            cols = st.multiselect(
                "Columns to fetch",
                options=AVAILABLE_COLUMNS,
                default=[c for c in streaming_template.get("columns", []) if c in AVAILABLE_COLUMNS],
                help="Hold Ctrl/Cmd to select multiple",
                key="stream_cols_pick",
            )
            free_cols = st.text_input("Additional columns (comma-separated)", value="", key="stream_cols_free")
            columns = cols + parse_free_text_columns(free_cols)
        else:
            columns = parse_free_text_columns(
                st.text_input(
                    "Columns (comma-separated)",
                    value=",".join(streaming_template.get("columns", [])),
                    placeholder="e.g., crash_date, posted_speed_limit, weather_condition",
                    key="stream_cols_text",
                )
            )

        stream_payload = dict(streaming_template)  # shallow copy
        stream_payload["last_n_days"] = int(n_days)
        stream_payload["columns"] = columns

    with col_right:
        st.caption("Generated JSON payload")
        st.code(json.dumps(stream_payload, indent=2), language="json")

        st.caption("Equivalent cURL")
        st.code(curl_cmd(STREAMING_ENDPOINT, stream_payload), language="bash")

        c1, c2 = st.columns([1, 3])
        with c1:
            run_stream = st.button("Run streaming", key="btn_run_streaming")
        with c2:
            st.write(f"Endpoint: `{STREAMING_ENDPOINT}`")

        if run_stream:
            if delivery_mode == "RabbitMQ API":
                status, body = rabbit_publish(stream_payload, routing_key=RABBIT_RK)
            else:
                status, body = post_json(STREAMING_ENDPOINT, stream_payload)
            render_response(status, body)

# ======================= BACKFILL =======================
with tab_backfill:
    st.subheader("Backfill — fetch specific time range")

    backfill_template = load_template(
        BACKFILL_TEMPLATE_PATH,
        {
            "mode": "backfill",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-02T00:00:00Z",
            "columns": [],
        },
    )

    bL, bR = st.columns([2, 3])
    with bL:
        today = datetime.utcnow().date()
        start_date = st.date_input("Start date", value=today - timedelta(days=1), key="bf_start_date")
        start_time = st.time_input("Start time (UTC)", value=dtime(0, 0, 0), key="bf_start_time")
        end_date   = st.date_input("End date", value=today, key="bf_end_date")
        end_time   = st.time_input("End time (UTC)", value=dtime(0, 0, 0), key="bf_end_time")

        start_dt = datetime.combine(start_date, start_time, tzinfo=timezone.utc)
        end_dt   = datetime.combine(end_date, end_time, tzinfo=timezone.utc)

        if AVAILABLE_COLUMNS:
            bcols = st.multiselect(
                "Columns to fetch",
                options=AVAILABLE_COLUMNS,
                default=[c for c in backfill_template.get("columns", []) if c in AVAILABLE_COLUMNS],
                key="bf_cols_pick",
            )
            bfree = st.text_input("Additional columns (comma-separated)", value="", key="bf_cols_free")
            backfill_columns = bcols + parse_free_text_columns(bfree)
        else:
            backfill_columns = parse_free_text_columns(
                st.text_input(
                    "Columns (comma-separated)",
                    value=",".join(backfill_template.get("columns", [])),
                    key="bf_cols_text",
                )
            )

        backfill_payload = dict(backfill_template)
        backfill_payload["start"]   = to_iso8601(start_dt)
        backfill_payload["end"]     = to_iso8601(end_dt)
        backfill_payload["columns"] = backfill_columns

    with bR:
        if end_dt <= start_dt:
            st.error("End must be after Start.")
        st.caption("Generated JSON payload")
        st.code(json.dumps(backfill_payload, indent=2), language="json")

        st.caption("Equivalent cURL")
        st.code(curl_cmd(BACKFILL_ENDPOINT, backfill_payload), language="bash")

        c1, c2 = st.columns([1, 3])
        with c1:
            run_backfill = st.button("Run backfill", key="btn_run_backfill")
        with c2:
            st.write(f"Endpoint: `{BACKFILL_ENDPOINT}`")

        if run_backfill:
            if end_dt <= start_dt:
                st.error("Refusing to run: End must be after Start.")
            else:
                if delivery_mode == "RabbitMQ API":
                    status, body = rabbit_publish(backfill_payload, routing_key=RABBIT_RK)
                else:
                    status, body = post_json(BACKFILL_ENDPOINT, backfill_payload)
                render_response(status, body)
