# pages/6_Reports.py

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import streamlit as st
from minio import Minio


from reportlab.lib.pagesizes import LETTER
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Table,
    TableStyle,
    SimpleDocTemplate,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import getSampleStyleSheet

st.set_page_config(page_title="Reports", page_icon="📄", layout="wide")
st.title("📄 Reports")

GOLD_DB_PATH = os.getenv("GOLD_DB_PATH", "/data/gold.duckdb")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "0")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-data")  # where extractor writes _runs/manifest.json

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def to_bool(x: str) -> bool:
    return str(x).lower() not in ("0", "false", "no", "")


def duck_con(ro: bool = True):
    return duckdb.connect(GOLD_DB_PATH, read_only=ro)


def ident(qualified: str) -> str:
    parts = [p for p in qualified.split(".") if p]
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def gold_row_count() -> int:
    try:
        with duck_con() as con:
            df = con.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                  AND table_schema NOT IN ('information_schema','pg_catalog')
                """
            ).df()
            total = 0
            for _, row in df.iterrows():
                schema, name = row["table_schema"], row["table_name"]
                tbl = name if schema == "main" else f"{schema}.{name}"
                try:
                    total += con.execute(
                        f"SELECT COUNT(*) FROM {ident(tbl)}"
                    ).fetchone()[0]
                except Exception:
                    pass
            return int(total)
    except Exception:
        return 0


def latest_crash_date() -> Optional[str]:
    """Look for a max crash_date or (crash_year,month,day) across all tables."""
    try:
        with duck_con() as con:
            df = con.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema','pg_catalog')
                """
            ).df()
            best: Optional[str] = None
            for _, row in df.iterrows():
                schema, name = row["table_schema"], row["table_name"]
                tbl = name if schema == "main" else f"{schema}.{name}"
                # does it have crash_date?
                cols = con.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = ? AND table_name = ?
                    """,
                    [schema, name],
                ).df()["column_name"].str.lower().tolist()

                if "crash_date" in cols:
                    d = con.execute(
                        f"SELECT MAX(crash_date) AS d FROM {ident(tbl)}"
                    ).fetchone()[0]
                    if d is not None:
                        s = str(d)
                        if best is None or s > best:
                            best = s
                elif all(x in cols for x in ["crash_year", "crash_month", "crash_day"]):
                    d = con.execute(
                        f"""
                        WITH d AS (
                          SELECT strptime(
                              printf('%04d-%02d-%02d',
                                     CAST(crash_year AS INTEGER),
                                     CAST(crash_month AS INTEGER),
                                     CAST(crash_day AS INTEGER)),
                              '%Y-%m-%d') AS d
                          FROM {ident(tbl)}
                          WHERE crash_year IS NOT NULL
                            AND crash_month IS NOT NULL
                            AND crash_day IS NOT NULL
                        )
                        SELECT MAX(d) FROM d
                        """
                    ).fetchone()[0]
                    if d is not None:
                        s = str(d)
                        if best is None or s > best:
                            best = s
            return best
    except Exception:
        return None


def minio_client() -> Minio:
    """
    MinIO client matching the rest of the app.
    MINIO_ENDPOINT should be like 'minio:9000'.
    """
    secure = to_bool(MINIO_SECURE)
    endpoint = MINIO_ENDPOINT
    # If someone ever passes just 'minio', add default port
    if ":" not in endpoint:
        endpoint = endpoint + ":9000"

    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )



def list_manifests() -> List[Dict[str, Any]]:
    """
    Scan RAW_BUCKET for _runs/corr=.../manifest.json and parse them.
    Manifest JSON shape (from Go):
      { "corr": "...", "mode": "...", "where": "...",
        "started_at": "...", "finished_at": "..." }
    """
    cli = minio_client()
    manifests: List[Dict[str, Any]] = []

    # IMPORTANT: prefix exactly "_runs/" (no leading slash, no bucket name)
    for obj in cli.list_objects(RAW_BUCKET, prefix="_runs/", recursive=True):
        key = obj.object_name
        if not key.endswith("manifest.json"):
            continue

        try:
            resp = cli.get_object(RAW_BUCKET, key)
            try:
                b = resp.read()
            finally:
                resp.close()
                resp.release_conn()

            data = json.loads(b.decode("utf-8"))
            data["_s3_key"] = key

            # try to parse timestamps
            for f in ("started_at", "finished_at"):
                v = data.get(f)
                if isinstance(v, str):
                    try:
                        data[f] = datetime.fromisoformat(
                            v.replace("Z", "+00:00")
                        )
                    except Exception:
                        pass

            manifests.append(data)

        except Exception as e:
            # best-effort; just skip bad manifests
            print("manifest read error", key, e)

    # sort by finished_at (or started_at) if available
    def sort_key(m: Dict[str, Any]):
        v = m.get("finished_at") or m.get("started_at") or datetime.min.replace(
            tzinfo=timezone.utc
        )
        return v

    manifests.sort(key=sort_key)
    return manifests


    # sort by finished_at (or started_at) if available
    def sort_key(m: Dict[str, Any]):
        v = m.get("finished_at") or m.get("started_at") or datetime.min.replace(
            tzinfo=timezone.utc
        )
        return v

    manifests.sort(key=sort_key)
    return manifests


def to_local_str(x: Any) -> str:
    if x is None:
        return "—"
    if isinstance(x, str):
        # already a string (maybe ISO)
        try:
            dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
            x = dt
        except Exception:
            return x
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        return x.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    return str(x)


# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------

manifests = list_manifests()
total_runs = len(manifests)
latest: Optional[Dict[str, Any]] = manifests[-1] if manifests else None

gold_rows = gold_row_count()
max_crash_date = latest_crash_date()

corrid = latest.get("corr") if latest else "—"
mode = latest.get("mode") if latest else "—"
where_clause = latest.get("where") if latest else "—"
started_at = latest.get("started_at") if latest else None
finished_at = latest.get("finished_at") if latest else None

started_local = to_local_str(started_at)
finished_local = to_local_str(finished_at)

# ---------------------------------------------------------------------
# Summary cards
# ---------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Total runs completed", f"{total_runs:,}")
with c2:
    st.caption("Latest corrid (click-to-copy)")
    st.code(str(corrid))
with c3:
    st.metric("Gold row count (current)", f"{gold_rows:,}")
with c4:
    st.metric("Latest data date fetched", max_crash_date or "—")
with c5:
    st.metric("Last run timestamp", finished_local)

st.divider()
st.subheader("Latest Run Summary")

if not latest:
    st.info(
        f"No manifests found in bucket `{RAW_BUCKET}` under prefix `_runs/`."
    )
    st.stop()

# Config / window
st.markdown(
    f"""
- **Mode:** `{mode}`
- **WHERE:** `{where_clause}`
- **Started:** {started_local}
- **Finished:** {finished_local}
"""
)

# Rows processed: not tracked yet, show N/A but leave a placeholder
st.write("**Rows processed (per table)**: not tracked in manifests yet.")

# Errors / warnings / artifacts: not available with current manifests
with st.expander("Errors (0)", expanded=False):
    st.write("No error information is currently tracked in the manifest.")
with st.expander("Warnings (0)", expanded=False):
    st.write("No warning information is currently tracked in the manifest.")

st.write("**Artifacts**")
st.write(
    "Current manifests only store basic fields (`corr`, `mode`, `where`, "
    "`started_at`, `finished_at`). Paths to logs / requests / outputs are "
    "not stored yet."
)

st.divider()

# ---------------------------------------------------------------------
# PDF export
# ---------------------------------------------------------------------


def build_pdf(buffer: io.BytesIO):
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=LETTER,
        leftMargin=40,
        rightMargin=40,
        topMargin=36,
        bottomMargin=36,
    )
    story = []

    story.append(Paragraph("Pipeline Run Report", styles["Title"]))
    story.append(Spacer(1, 8))

    # Summary
    summary_data = [
        ["Total runs", f"{total_runs:,}"],
        ["Latest corrid", str(corrid)],
        ["Gold rows", f"{gold_rows:,}"],
        ["Latest crash date", max_crash_date or "—"],
        ["Last run (local)", finished_local],
    ]
    tbl = Table(summary_data, hAlign="LEFT", colWidths=[160, 360])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.grey),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 10))

    # Latest run details
    story.append(Paragraph("Latest Run Summary", styles["Heading2"]))
    bullets = [
        f"Mode: {mode}",
        f"WHERE: {where_clause}",
        f"Started: {started_local}",
        f"Finished: {finished_local}",
    ]
    for b in bullets:
        story.append(Paragraph(f"• {b}", styles["Normal"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Rows processed", styles["Heading3"]))
    story.append(
        Paragraph(
            "Not currently tracked in manifests. "
            "Consider adding row counts per table in future runs.",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 6))

    story.append(Paragraph("Errors / Warnings", styles["Heading3"]))
    story.append(
        Paragraph(
            "No error or warning metadata is stored in the manifest yet.",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 6))

    story.append(Paragraph("Artifacts", styles["Heading3"]))
    story.append(
        Paragraph(
            "Paths to logs, request JSON, or sample outputs are not yet "
            "stored in the manifest.",
            styles["Normal"],
        )
    )

    doc.build(story)


buf = io.BytesIO()
build_pdf(buf)
buf.seek(0)
st.download_button(
    "⬇️ Download PDF report",
    data=buf.read(),
    file_name=f"pipeline-report-{datetime.now().strftime('%Y%m%d-%H%M%S')}.pdf",
    mime="application/pdf",
)
