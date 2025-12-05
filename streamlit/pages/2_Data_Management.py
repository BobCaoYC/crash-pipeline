# pages/2_Data_Management.py
import os
import io
import time
import math
import pathlib
from typing import Optional, Tuple

import streamlit as st

# ---- deps: boto3, duckdb, pandas must be available in the image ----
import pandas as pd
import duckdb
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

st.set_page_config(page_title="Data Management", page_icon="🗂️", layout="wide")
st.title("🗂️ Data Management")

# ---------------- MinIO client helpers ----------------
def make_s3():
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_USER", "minioadmin")
    secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_PASS", "minioadmin")
    secure = os.getenv("MINIO_SECURE", "0") not in ("0", "false", "False", "FALSE")
    return boto3.client(
        "s3",
        endpoint_url=("https://" if secure else "http://") + endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        verify=False if not secure else True,
    )

def list_buckets(s3):
    resp = s3.list_buckets()
    buckets = [b["Name"] for b in resp.get("Buckets", [])]
    return sorted(buckets)

def list_prefix(s3, bucket: str, prefix: str, max_keys: int = 1000):
    # Return (folders, objects) under a bucket/prefix
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/",
        MaxKeys=max_keys,
    )
    folders = [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])]
    objects = resp.get("Contents", [])
    return folders, objects

def sizeof_fmt(num, suffix="B"):
    if num is None:
        return "—"
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"

# --- replace the old helpers with these version-agnostic ones ---
GOLD_DB_PATH = os.getenv("GOLD_DB_PATH", "/data/gold.duckdb")

def gold_exists() -> bool:
    return pathlib.Path(GOLD_DB_PATH).exists()

def open_con(read_only: bool = True):
    return duckdb.connect(GOLD_DB_PATH, read_only=read_only)

def _quote_ident(s: str) -> str:
    # quote with double-quotes and escape any embedded quotes
    return '"' + s.replace('"', '""') + '"'

def _escape_qualified(qualified: str) -> str:
    # supports schema.table or just table
    parts = [p for p in qualified.split(".") if p]
    return ".".join(_quote_ident(p) for p in parts)

def list_tables() -> list[str]:
    # works on older/newer DuckDB; filters out system schemas
    q = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type IN ('BASE TABLE','VIEW')
      AND table_schema NOT IN ('information_schema','pg_catalog')
    ORDER BY table_schema, table_name
    """
    with open_con() as con:
        df = con.sql(q).df()
    # Show schema-qualified names unless it's the default 'main'
    names = []
    for _, r in df.iterrows():
        schema = str(r["table_schema"])
        tname  = str(r["table_name"])
        names.append(tname if schema in ("main",) else f"{schema}.{tname}")
    return names

def table_head(table: str, n: int = 25) -> pd.DataFrame:
    ident = _escape_qualified(table)
    with open_con() as con:
        return con.sql(f"SELECT * FROM {ident} LIMIT {int(n)}").df()

def table_count(table: str) -> int:
    ident = _escape_qualified(table)
    with open_con() as con:
        return con.sql(f"SELECT COUNT(*) AS c FROM {ident}").fetchone()[0]


# ---------------- UI: Tabs ----------------
tab_minio, tab_gold = st.tabs(["MinIO Buckets & Objects", "Gold Database"])

# ============ TAB 1: MINIO ============
with tab_minio:
    s3 = make_s3()
    st.subheader("Buckets")

    colL, colR = st.columns([3, 2])
    with colL:
        buckets = list_buckets(s3)
        bucket = st.selectbox("Bucket", options=buckets, index=0 if buckets else None, key="bucket_select")
    with colR:
        new_bucket = st.text_input("Create bucket (name)", placeholder="e.g., raw")
        if st.button("Create bucket"):
            if not new_bucket:
                st.error("Bucket name required.")
            else:
                try:
                    s3.create_bucket(Bucket=new_bucket)
                    st.success(f"Created bucket `{new_bucket}`")
                    time.sleep(0.5)
                    st.rerun()
                except ClientError as e:
                    st.error(f"Create failed: {e}")

    st.divider()
    st.subheader("Browse")

    if not bucket:
        st.info("No buckets found.")
    else:
        # Manage prefix "navigation"
        prefix = st.text_input("Prefix (folder path)", value=st.session_state.get("prefix", ""), placeholder="folder1/folder2/")
        st.session_state["prefix"] = prefix

        folders, objects = list_prefix(s3, bucket, prefix)

        c1, c2 = st.columns(2)
        with c1:
            st.caption("Folders")
            if not folders:
                st.write("—")
            else:
                for f in folders:
                    short = f[len(prefix):] if f.startswith(prefix) else f
                    cols = st.columns([6, 1, 1])
                    with cols[0]:
                        if st.button(f"📁 {short}", key=f"nav_{f}"):
                            st.session_state["prefix"] = f
                            st.rerun()
                    with cols[1]:
                        # delete folder => delete all keys with that prefix
                        if st.button("🗑️", key=f"del_prefix_{f}", help="Delete this folder (all objects under it)"):
                            st.session_state[f"confirm_prefix_{f}"] = True
                    with cols[2]:
                        pass
                    if st.session_state.get(f"confirm_prefix_{f}"):
                        with st.expander(f"Confirm delete folder `{f}`", expanded=True):
                            st.warning("This will delete ALL objects under this prefix.")
                            typed = st.text_input("Type the folder (prefix) to confirm", key=f"type_{f}")
                            if st.button("Delete folder objects", key=f"go_{f}"):
                                if typed == f:
                                    # delete all with prefix
                                    to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=f)
                                    keys = [{"Key": obj["Key"]} for obj in to_delete.get("Contents", [])]
                                    if keys:
                                        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                                    st.success("Deleted.")
                                    st.session_state.pop(f"confirm_prefix_{f}", None)
                                    st.rerun()
                                else:
                                    st.error("Typed value doesn't match.")

        with c2:
            st.caption("Objects")
            if not objects:
                st.write("—")
            else:
                for obj in objects:
                    key = obj["Key"]
                    if key.endswith("/") and key == prefix:
                        continue  # skip the current folder placeholder
                    size = sizeof_fmt(obj.get("Size"))
                    cols = st.columns([6, 2, 1])
                    with cols[0]:
                        st.write(f"🗎 {key}")
                        st.caption(f"{size} • LastMod: {obj.get('LastModified')}")
                    with cols[1]:
                        if st.button("Preview", key=f"prev_{key}"):
                            try:
                                # stream a small preview
                                data = s3.get_object(Bucket=bucket, Key=key)["Body"].read(256 * 1024)
                                st.code(data.decode(errors="replace")[:5000])
                            except Exception as e:
                                st.error(f"Preview failed: {e}")
                    with cols[2]:
                        if st.button("🗑️", key=f"del_{key}", help="Delete object"):
                            st.session_state[f"confirm_{key}"] = True
                    if st.session_state.get(f"confirm_{key}"):
                        with st.expander(f"Confirm delete `{key}`", expanded=True):
                            typed = st.text_input("Type the full key to confirm", key=f"type_{key}")
                            if st.button("Delete object", key=f"go_{key}"):
                                if typed == key:
                                    s3.delete_object(Bucket=bucket, Key=key)
                                    st.success("Deleted.")
                                    st.session_state.pop(f"confirm_{key}", None)
                                    st.rerun()
                                else:
                                    st.error("Typed value doesn't match.")

    st.divider()
    st.subheader("Delete Bucket")
    if bucket:
        colA, colB = st.columns([3, 1])
        with colA:
            typed_b = st.text_input("Type bucket name to confirm", key="type_bucket")
            force = st.checkbox("Force (delete all objects in bucket)", help="If checked, empties bucket first.")
        with colB:
            if st.button("Delete bucket"):
                if typed_b != bucket:
                    st.error("Bucket name mismatch.")
                else:
                    try:
                        if force:
                            # empty bucket
                            pager = s3.list_objects_v2(Bucket=bucket)
                            while True:
                                keys = [{"Key": o["Key"]} for o in pager.get("Contents", [])]
                                if keys:
                                    s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                                if pager.get("IsTruncated"):
                                    pager = s3.list_objects_v2(Bucket=bucket, ContinuationToken=pager["NextContinuationToken"])
                                else:
                                    break
                        s3.delete_bucket(Bucket=bucket)
                        st.success(f"Deleted bucket `{bucket}`")
                        time.sleep(0.6)
                        st.rerun()
                    except ClientError as e:
                        st.error(f"Delete failed: {e}")

# ============ TAB 2: GOLD DB ============
with tab_gold:
    st.subheader("Gold DuckDB")
    path = GOLD_DB_PATH
    exists = gold_exists()

    cols = st.columns([3, 2, 1])
    with cols[0]:
        st.write(f"**Path:** `{path}`")
        if exists:
            size = pathlib.Path(path).stat().st_size
            st.write(f"**Size:** {sizeof_fmt(size)}")
        else:
            st.info("Gold DB not found.")

    if exists:
        tables = list_tables()
        sel = st.selectbox("Table", options=tables, index=0 if tables else None)

        colh1, colh2 = st.columns([1, 3])
        with colh1:
            nrows = st.number_input("Preview rows", min_value=5, max_value=2000, value=25, step=5)
        with colh2:
            if sel:
                try:
                    cnt = table_count(sel)
                    st.caption(f"Rows: {cnt:,}")
                except Exception as e:
                    st.error(f"Count failed: {e}")

        if sel:
            try:
                df = table_head(sel, int(nrows))
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Preview failed: {e}")

        st.divider()
        st.warning("Danger zone", icon="⚠️")
        c1, c2 = st.columns([3, 1])
        with c1:
            confirm_text = st.text_input("Type DELETE to remove the entire Gold DB file")
        with c2:
            if st.button("Delete Gold DB file"):
                if confirm_text == "DELETE":
                    try:
                        duckdb.connect(GOLD_DB_PATH).close()  # close if any handle
                    except Exception:
                        pass
                    try:
                        pathlib.Path(GOLD_DB_PATH).unlink(missing_ok=True)
                        st.success("Gold DB deleted.")
                        time.sleep(0.6)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")
                else:
                    st.error("Confirmation text mismatch (type exactly: DELETE)")
