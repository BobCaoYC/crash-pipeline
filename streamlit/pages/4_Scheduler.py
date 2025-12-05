# pages/4_Scheduler.py
import os, json
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Optional, Dict, Any, List

import streamlit as st
import requests
from croniter import croniter

st.set_page_config(page_title="Scheduler", page_icon="⏰", layout="wide")
st.title("⏰ Scheduler — Weekly / Cron")

# ---------- Config ----------
STREAMING_ENDPOINT = os.getenv("STREAMING_ENDPOINT", "http://extractor:8000/stream")
SCHED_STATE_PATH = os.getenv("SCHEDULER_STATE_PATH", "/app/data/scheduler_state.json")
DEFAULT_LAST_N_DAYS = int(os.getenv("SCHEDULER_LAST_N_DAYS", "7"))
DEFAULT_CRON = os.getenv("SCHEDULER_DEFAULT_CRON", "0 0 * * 1")  # Mondays 00:00 UTC
AVAILABLE_COLUMNS = [c.strip() for c in os.getenv("AVAILABLE_COLUMNS", "").split(",") if c.strip()]

# ---------- Helpers ----------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def load_state() -> Dict[str, Any]:
    try:
        with open(SCHED_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "enabled": False,
            "mode": "weekly",           # "weekly" | "cron"
            "cron": DEFAULT_CRON,       # valid if mode=="cron"
            "last_run": None,
            "next_run": None,
            "cadence_days": 7,          # weekly-only
            "last_status": None,
            "last_error": None,
            "last_columns": [],
            "last_payload": None,
            "history": [],
            "last_n_days": DEFAULT_LAST_N_DAYS,
            "dow": 0,                   # weekly-only (Mon=0)
            "run_time_utc": "00:00",    # weekly-only
        }

def save_state(s: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SCHED_STATE_PATH), exist_ok=True)
    tmp = SCHED_STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)
    os.replace(tmp, SCHED_STATE_PATH)

def next_weekly_run(dow: int, run_time_utc: str) -> datetime:
    hh, mm = [int(x) for x in run_time_utc.split(":")]
    now = utcnow()
    target = datetime.combine(now.date(), dtime(hh, mm), tzinfo=timezone.utc)
    days_ahead = (dow - target.weekday()) % 7
    candidate = target + timedelta(days=days_ahead)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate

def next_cron_run(cron: str) -> datetime:
    base = utcnow()
    try:
        itr = croniter(cron, base)
        return datetime.fromtimestamp(itr.get_next(float), tz=timezone.utc)
    except Exception as e:
        raise ValueError(f"Invalid cron: {e}")

def post_streaming(payload: dict):
    try:
        r = requests.post(STREAMING_ENDPOINT, json=payload, timeout=30)
        return r.status_code, (r.text[:4000] if r.text else "")
    except Exception as e:
        return None, f"{e}"

def curl_cmd(url: str, payload: dict) -> str:
    import json as _json
    data = _json.dumps(payload, separators=(",", ":"))
    return f"curl -s -X POST '{url}' -H 'Content-Type: application/json' -d '{data}'"

# ---------- UI: Configure ----------
state = load_state()
st.caption(f"Endpoint: `{STREAMING_ENDPOINT}`")

# mode toggle
mode = st.radio("Schedule mode", ["weekly", "cron"], index=0 if state.get("mode","weekly")=="weekly" else 1,
                format_func=lambda m: "Weekly" if m=="weekly" else "Custom cron")

cA, cB, cC = st.columns([1.2, 1, 1])
with cA:
    enabled = st.toggle("Enable schedule", value=state.get("enabled", False))
with cB:
    last_n_days = st.number_input("Streaming window (last N days)", 1, 90, int(state.get("last_n_days", DEFAULT_LAST_N_DAYS)))
with cC:
    cadence_days = st.number_input("Cadence (days)", 7, 14, int(state.get("cadence_days", 7)),
                                   help="Used only in weekly mode; weekly = 7", disabled=(mode=="cron"))

w1, w2 = st.columns([1.5, 1.5])
if mode == "weekly":
    with w1:
        dow = st.selectbox("Day of week", options=list(enumerate(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"])),
                           index=int(state.get("dow", 0)), format_func=lambda x: x[1])[0]
    with w2:
        run_time = st.time_input("Run time (UTC)", value=datetime.strptime(state.get("run_time_utc","00:00"), "%H:%M").time())
else:
    with w1:
        cron_str = st.text_input("Cron (UTC)", value=state.get("cron", DEFAULT_CRON),
                                 placeholder="e.g., 0 3 * * MON  or  */15 6-18 * * *")
    with w2:
        # show next fire time preview
        try:
            nxt = next_cron_run(cron_str)
            st.info(f"Next fire (UTC): {to_iso(nxt)}")
        except Exception as e:
            st.error(str(e))

# columns selection
if AVAILABLE_COLUMNS:
    pick = st.multiselect("Columns", options=AVAILABLE_COLUMNS, default=state.get("last_columns", []))
else:
    pick = st.text_input("Columns (comma-separated)", value=",".join(state.get("last_columns", [])))
    pick = [c.strip() for c in pick.split(",") if c.strip()]

payload = {"mode": "streaming", "last_n_days": int(last_n_days), "columns": pick}

st.caption("Generated JSON")
st.code(json.dumps(payload, indent=2), language="json")
st.caption("Equivalent cURL")
st.code(curl_cmd(STREAMING_ENDPOINT, payload), language="bash")

# Save
if st.button("Save schedule"):
    state["enabled"] = enabled
    state["last_n_days"] = int(last_n_days)
    state["last_columns"] = pick
    state["mode"] = mode
    if mode == "weekly":
        state["cadence_days"] = int(cadence_days)
        state["dow"] = int(dow)
        state["run_time_utc"] = f"{run_time.hour:02d}:{run_time.minute:02d}"
        state["next_run"] = to_iso(next_weekly_run(state["dow"], state["run_time_utc"]))
    else:
        state["cron"] = cron_str
        try:
            state["next_run"] = to_iso(next_cron_run(cron_str))
        except Exception as e:
            state["next_run"] = None
            st.error(f"Invalid cron: {e}")
    save_state(state)
    if state.get("next_run"):
        st.success(f"Saved. Next run: {state['next_run']} (UTC)")
    st.rerun()

st.divider()

# ---------- Status + Manual Run ----------
left, right = st.columns([1.3, 1])
with left:
    st.subheader("Status")
    st.write(f"**Enabled:** {state.get('enabled', False)}")
    st.write(f"**Mode:** {state.get('mode','weekly')}")
    st.write(f"**Last run:** {state.get('last_run') or '—'}")
    st.write(f"**Next run:** {state.get('next_run') or '—'}")
    if state.get("last_status") is not None:
        st.write(f"**Last result:** HTTP {state['last_status']}")
    if state.get("last_error"):
        st.error(state["last_error"])
with right:
    if st.button("Run now"):
        code, body = post_streaming(payload)
        state["last_run"] = to_iso(utcnow())
        state["last_payload"] = payload
        if code is None:
            state["last_status"] = None
            state["last_error"] = body
            outcome = {"ts": state["last_run"], "status": None, "error": body}
            st.error(f"Run failed: {body}")
        elif 200 <= code < 300:
            state["last_status"] = int(code)
            state["last_error"] = None
            outcome = {"ts": state["last_run"], "status": int(code), "error": None}
            st.success(f"OK • HTTP {code}")
            st.code(body or "(no response body)")
        else:
            state["last_status"] = int(code)
            state["last_error"] = body or ""
            outcome = {"ts": state["last_run"], "status": int(code), "error": body or ""}
            st.error(f"HTTP {code}")
            st.code(body or "(no response body)")
        state.setdefault("history", []).insert(0, outcome)
        # advance next_run
        try:
            if state.get("mode") == "cron":
                state["next_run"] = to_iso(next_cron_run(state.get("cron", DEFAULT_CRON)))
            else:
                state["next_run"] = to_iso(from_iso(state["last_run"]) + timedelta(days=state.get("cadence_days", 7)))
        except Exception:
            state["next_run"] = None
        save_state(state)
        st.rerun()

# ---------- Auto-trigger when due ----------
try:
    if state.get("enabled") and state.get("next_run"):
        due_dt = from_iso(state["next_run"])
        if utcnow() >= due_dt:
            code, body = post_streaming(payload)
            state["last_run"] = to_iso(utcnow())
            state["last_payload"] = payload
            if code is None:
                state["last_status"] = None
                state["last_error"] = body
                outcome = {"ts": state["last_run"], "status": None, "error": body}
            elif 200 <= code < 300:
                state["last_status"] = int(code)
                state["last_error"] = None
                outcome = {"ts": state["last_run"], "status": int(code), "error": None}
            else:
                state["last_status"] = int(code)
                state["last_error"] = body or ""
                outcome = {"ts": state["last_run"], "status": int(code), "error": body or ""}
            state.setdefault("history", []).insert(0, outcome)
            # advance per mode
            try:
                if state.get("mode") == "cron":
                    state["next_run"] = to_iso(next_cron_run(state.get("cron", DEFAULT_CRON)))
                else:
                    state["next_run"] = to_iso(due_dt + timedelta(days=state.get("cadence_days", 7)))
            except Exception:
                state["next_run"] = None
            save_state(state)
            st.info("Auto-run executed because it was due. (This page acts as the scheduler.)")
except Exception as e:
    st.warning(f"Auto-trigger skipped: {e}")

st.divider()
st.subheader("History")
hist = state.get("history", [])[:50]
if hist:
    import pandas as pd
    st.dataframe(pd.DataFrame(hist), use_container_width=True)
else:
    st.write("No runs yet.")

st.caption("Note: This page mimics a scheduler. It triggers when you open/refresh if a run is due. For true background scheduling, wire cron/K8s/Airflow later.")
