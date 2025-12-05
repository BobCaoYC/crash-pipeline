# pages/7_Model.py

import os
from datetime import date
from typing import Optional, Tuple, List

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

import joblib  # <-- use joblib for sklearn artifacts

# Try to import sklearn metrics (needed for live metrics)
try:
    from sklearn.metrics import (
        roc_auc_score,
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
    )
    SKLEARN_METRICS_AVAILABLE = True
except Exception:
    SKLEARN_METRICS_AVAILABLE = False

# ------------------------- Config / constants -------------------------

st.set_page_config(page_title="Model", page_icon="🧠", layout="wide")
st.title("🧠 Model")

GOLD_DB_PATH = os.getenv("GOLD_DB_PATH", "/data/gold.duckdb")
GOLD_TABLE_NAME = os.getenv("GOLD_TABLE_NAME", "crashes")

# --- artifacts directory inside the container ---
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "/app/artifacts")

MODEL_ARTIFACT_PATH = os.getenv(
    "MODEL_ARTIFACT_PATH",
    os.path.join(ARTIFACT_DIR, "model.pkl"),
)

THRESHOLD_PATH = os.getenv(
    "THRESHOLD_PATH",
    os.path.join(ARTIFACT_DIR, "threshold.txt"),
)

LABELS_JSON_PATH = os.getenv(
    "LABELS_JSON_PATH",
    os.path.join(ARTIFACT_DIR, "labels.json"),
)

# Label column & positive class (only used if you have ground truth in data)
LABEL_COLUMN = os.getenv("LABEL_COLUMN", "label")  # update to real label col
POSITIVE_CLASS = 1  # or whatever you used

DEFAULT_THRESHOLD = 0.50  # fallback if threshold.txt not readable

# Feature columns expected by the pipeline
# ⚠️ These MUST match what you used in training.

# Numeric features
# Feature columns expected by the pipeline
# ⚠️ These MUST match what you actually used in training
#    and what exists in the Gold `crashes` table.

NUMERIC_FEATURES: List[str] = [
    "posted_speed_limit",
    "street_no",
    "crash_year",
    "crash_month",
    "crash_day",
    "crash_hour",
]

CATEGORICAL_FEATURES: List[str] = [
    "traffic_control_device",
    "device_condition",
    "weather_condition",
    "lighting_condition",
    "trafficway_type",
    "alignment",
    "roadway_surface_cond",
    "road_defect",
    "street_name",
    "street_direction",
    "crash_type",
]

FEATURE_COLUMNS: List[str] = NUMERIC_FEATURES + CATEGORICAL_FEATURES




# Static “official” metrics from training notebook (fill in your own values)
# None => shown as "N/A"
STATIC_METRICS = {
    "ROC AUC (test)": None,
    "Accuracy (test)": None,
    "F1 (pos, test)": None,
}

# Max rows the user can request from Gold
MAX_GOLD_ROWS_LIMIT = 20000

def format_metric_value(v):
    """Safely format floats for display in the metrics panel."""
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.3f}"
    except Exception:
        return str(v)


# ------------------------- Helpers -------------------------

@st.cache_data(show_spinner=False)
def load_default_threshold_from_file() -> float:
    try:
        with open(THRESHOLD_PATH, "r") as f:
            val = f.read().strip()
        th = float(val)
        if 0.0 <= th <= 1.0:
            return th
    except Exception:
        pass
    return DEFAULT_THRESHOLD


@st.cache_resource(show_spinner="Loading model artifact…")
def load_model(path: str = MODEL_ARTIFACT_PATH) -> Tuple[Optional[object], Optional[Exception]]:
    """
    Cached model-loading helper.

    Returns
    -------
    model : object or None
    error : Exception or None
    """
    if not os.path.exists(path):
        return None, FileNotFoundError(f"Model file not found at {path}")

    try:
        model = joblib.load(path)
        return model, None
    except Exception as e:
        return None, e


@st.cache_resource(show_spinner="Connecting to Gold DuckDB…")
def get_duck_con():
    """
    Cached DuckDB connection helper.
    Reuses the same connection for all queries.
    """
    return duckdb.connect(GOLD_DB_PATH, read_only=True)

# ... keep your load_gold_data, feature_preparation, format_metric_value
# functions exactly as you already have them ...


# ------------------------- Section 1 – Model Summary -------------------------

model, load_err = load_model()

if load_err is not None or model is None:
    st.error(
        f"❌ Failed to load model artifact from {MODEL_ARTIFACT_PATH}.\n\n"
        f"Error: {load_err}\n\n"
        "Make sure the path is correct and that the artifact was saved with "
        "joblib using a compatible Python + scikit-learn version."
    )
    st.stop()

with st.container():
    st.subheader("Model Summary")

    # Decision threshold control (stored in session_state)
    if "decision_threshold" not in st.session_state:
        st.session_state["decision_threshold"] = load_default_threshold_from_file()

    threshold = st.slider(
        "Decision threshold (probability → class label)",
        min_value=0.0,
        max_value=1.0,
        value=float(st.session_state["decision_threshold"]),
        step=0.01,
    )
    st.session_state["decision_threshold"] = threshold

    outer_name = type(model).__name__
    inner = getattr(model, "base_estimator", None) or getattr(model, "estimator", None)
    inner_name = type(inner).__name__ if inner is not None else "Unknown"

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Outer model class", outer_name)
    with c2:
        st.metric("Underlying estimator", inner_name)
    with c3:
        st.metric("Decision threshold", f"{threshold:.2f}")

    st.markdown(
        """
**Expected input**

- The model expects the following **feature columns** (raw, not one-hot encoded):

    - **Numeric**: `""" + "`, `".join(NUMERIC_FEATURES) + """`
    - **Categorical**: `""" + "`, `".join(CATEGORICAL_FEATURES) + """`

- All **one-hot encoding and numerical preprocessing are handled *inside* the sklearn pipeline**.
- This Streamlit app must pass a DataFrame with **raw columns and correct column names**.
- Do **not** pass manually encoded matrices (no pre-one-hot, no pre-scaled arrays).
- If you change the feature set in your training notebook, **update the lists above**
  so this description stays accurate.
"""
    )

st.divider()
# ------------------------- Gold Table Loader -------------------------

@st.cache_data(show_spinner="Loading Gold data…")
def load_gold_data(
    start: Optional[date],
    end: Optional[date],
    limit: int,
    sample: bool,
) -> pd.DataFrame:
    """
    Load data from the Gold table with an optional date window and row limit.

    Assumes crash_year / crash_month / crash_day are present; if not, falls
    back to no date filter.
    """
    con = get_duck_con()
    tbl = GOLD_TABLE_NAME

    params = []
    where_parts = []

    # Build date expression (if exists)
    date_expr = "make_date(crash_year::INTEGER, crash_month::INTEGER, crash_day::INTEGER)"

    if start:
        where_parts.append(f"{date_expr} >= ?")
        params.append(start.isoformat())

    if end:
        where_parts.append(f"{date_expr} <= ?")
        params.append(end.isoformat())

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    order_clause = "ORDER BY random()" if sample else ""

    sql = f"""
        SELECT *
        FROM {tbl}
        {where_clause}
        {order_clause}
        LIMIT {int(limit)}
    """

    df = con.execute(sql, params).df()
    return df

# ------------------------- Section 2 – Data Selection -------------------------

st.subheader("Data Selection")

data_source = st.radio(
    "Choose data source",
    ["Gold table (full / sample)", "Upload test CSV"],
    help="Use Gold data from DuckDB or upload your own held-out test CSV.",
)

# Keep the last loaded dataset across reruns
loaded_df: Optional[pd.DataFrame] = st.session_state.get("model_loaded_df")
data_warning = None


if data_source == "Gold table (full / sample)":
    # Controls for Gold data
    c1, c2, c3 = st.columns(3)
    with c1:
        max_rows = st.number_input(
            "Max rows to score",
            min_value=100,
            max_value=MAX_GOLD_ROWS_LIMIT,
            value=5000,
            step=100,
        )
    with c2:
        start_date = st.date_input("Start date (optional)", value=None)
    with c3:
        end_date = st.date_input("End date (optional)", value=None)

    sample_mode = st.radio(
        "Row selection mode",
        ["First N rows", "Random sample of N rows"],
        horizontal=True,
    )
    sample_flag = sample_mode == "Random sample of N rows"

    if st.button("Load data from Gold"):
        if start_date and end_date and end_date < start_date:
            st.error("End date must be on or after start date.")
        else:
            loaded_df = load_gold_data(start_date, end_date, int(max_rows), sample_flag)
            if loaded_df is not None:
                st.session_state["model_loaded_df"] = loaded_df      # 👈 persist
                st.success(f"Loaded {len(loaded_df):,} rows from Gold table `{GOLD_TABLE_NAME}`.")
                st.dataframe(loaded_df.head(), use_container_width=True)
            else:
                st.error("Failed to load data from Gold.")


elif data_source == "Upload test CSV":
    st.markdown(
        """
**Test data upload**

- Allowed file type: **CSV (.csv)** only.
- The uploaded file **must** contain the same feature columns the model expects.
"""
    )
    uploaded = st.file_uploader("Upload test CSV", type=["csv"])

    if uploaded is not None:
        if not uploaded.name.lower().endswith(".csv"):
            st.error("❌ Unsupported file type. Only `.csv` files are allowed for test data.")
        else:
            try:
                loaded_df = pd.read_csv(uploaded)
                st.session_state["model_loaded_df"] = loaded_df      # 👈 persist
                st.success(
                    f"Loaded test data from `{uploaded.name}` with {len(loaded_df):,} rows "
                    f"and {loaded_df.shape[1]} columns."
                )
                st.dataframe(loaded_df.head(), use_container_width=True)

            except Exception as e:
                st.error(f"❌ Failed to read CSV: {e}")

if loaded_df is not None:
    st.markdown("**Current data preview (used for predictions):**")
    st.dataframe(loaded_df.head(), use_container_width=True)

st.divider()
from typing import Tuple  # make sure this import is at the top

def feature_preparation(df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series]]:
    """
    Prepare features for the model:

    - Verifies that all expected feature columns exist.
    - Returns X (features) and y (if LABEL_COLUMN present, else None).
    """
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        st.error(
            "❌ The input data is missing required feature columns:\n\n"
            + ", ".join(missing)
        )
        return None, None

    # Optional: basic type coercion for numeric columns
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    X = df[FEATURE_COLUMNS].copy()

    y = None
    if LABEL_COLUMN in df.columns:
        y = df[LABEL_COLUMN].copy()

    return X, y

# ------------------------- Section 3 – Prediction & Metrics -------------------------

st.subheader("Prediction & Metrics")

if model is None:
    st.info("Model artifact could not be loaded, so predictions are unavailable.")
    st.stop()

if loaded_df is None:
    st.info("Load data above (from Gold or an uploaded CSV) to enable predictions.")
    st.stop()

# Prepare features
X, y_true = feature_preparation(loaded_df)

if X is None:
    st.stop()

if st.button("Run predictions"):
    try:
        # Probabilities: assume binary and take positive class column
        probs = model.predict_proba(X)[:, 1]
    except Exception as e:
        st.error(f"❌ Failed to run model.predict_proba: {e}")
        st.stop()

    thr = float(st.session_state.get("decision_threshold", DEFAULT_THRESHOLD))
    preds = (probs >= thr).astype(int)

    results = loaded_df.copy()
    results["prob_positive"] = probs
    results["pred_label"] = preds

    st.success("Predictions computed.")
    st.dataframe(results.head(), use_container_width=True)

    # --- Static vs live metrics ---

    c_static, c_live = st.columns(2)

    with c_static:
        st.markdown("### Static metrics (from training notebook)")
        for name, val in STATIC_METRICS.items():
            st.write(f"- **{name}**: {format_metric_value(val)}")

    with c_live:
        st.markdown("### Live metrics (computed on this data)")
        if y_true is None:
            st.info(
                f"No ground-truth column `{LABEL_COLUMN}` found in the loaded data, "
                "so live metrics cannot be computed. Predictions only."
            )
        elif not SKLEARN_METRICS_AVAILABLE:
            st.warning(
                "scikit-learn metrics are not available in this environment, "
                "so live metrics cannot be computed."
            )
        else:
            try:
                # We assume binary label with POSITIVE_CLASS
                y_bin = (y_true == POSITIVE_CLASS).astype(int)

                live_metrics = {
                    "ROC AUC": roc_auc_score(y_bin, probs),
                    "Accuracy": accuracy_score(y_bin, preds),
                    "F1 (pos)": f1_score(y_bin, preds, zero_division=0),
                    "Precision (pos)": precision_score(y_bin, preds, zero_division=0),
                    "Recall (pos)": recall_score(y_bin, preds, zero_division=0),
                }

                for name, val in live_metrics.items():
                    st.write(f"- **{name}**: {format_metric_value(val)}")

                st.caption(
                    f"Threshold used for live metrics: **{thr:.2f}** (positive class = `{POSITIVE_CLASS}`)"
                )
            except Exception as e:
                st.error(f"❌ Failed to compute live metrics: {e}")