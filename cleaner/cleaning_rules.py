# cleaning_rules.py
from __future__ import annotations
from typing import List, Dict
import pandas as pd
import numpy as np



# ---- schema hints pulled from your column lists ----
ID_COLS = ["crash_record_id", "vehicle_id", "person_id"]

NUMERIC_COLS = [
    "posted_speed_limit", "lane_cnt", "street_no"
]

CATEGORICAL_COLS = [
    "traffic_control_device", "device_condition", "weather_condition",
    "lighting_condition", "trafficway_type", "alignment",
    "roadway_surface_cond", "road_defect", "street_name",
    "street_direction", "crash_type", "make", "model",
    "travel_direction", "person_type"
]

DATE_COL = "crash_date"   # will produce crash_year/month/day/hour

# cleaning_rules.py (add near the top)
DERIVED_DATE_COLS = ["crash_year", "crash_month", "crash_day", "crash_hour"]
KEEP_COLS = ID_COLS + NUMERIC_COLS + CATEGORICAL_COLS + DERIVED_DATE_COLS


YES_NO_TRUE_FALSE = {
    "y": 1, "yes": 1, "true": 1, "t": 1, "1": 1,
    "n": 0, "no": 0,  "false": 0, "f": 0, "0": 0,
}


def _to_lower_stripped(series: pd.Series) -> pd.Series:
    """lowercase + strip for string-like columns, leaving NaN intact."""
    return (
        series
        .astype("string")  # pandas NA-aware strings
        .str.strip()
        .str.lower()
    )


def _map_yes_no_to_int(series: pd.Series) -> pd.Series:
    """
    Try to convert y/yes/true and n/no/false to nullable Int8 (1/0).
    Only finalize as Int8 if every non-null value maps to 0/1; otherwise return original.
    """
    s = series.copy()

    # Work on string view for mapping
    s_str = s.astype("string").str.strip().str.lower()
    mapped = s_str.map(YES_NO_TRUE_FALSE).astype("Int8")  # nullable Int8
    # Decide if this column is truly boolean-like
    nonnull = mapped.dropna()
    if len(nonnull) > 0 and set(nonnull.unique()) <= {0, 1}:
        return mapped  # keep as boolean-like (nullable Int8)
    return series  # not truly boolean-like


def _split_crash_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if DATE_COL in df.columns:
        dt = pd.to_datetime(df[DATE_COL], errors="coerce", utc=True)
        df["crash_year"]  = dt.dt.year.astype("Int16")
        df["crash_month"] = dt.dt.month.astype("Int8")
        df["crash_day"]   = dt.dt.day.astype("Int8")
        df["crash_hour"]  = dt.dt.hour.astype("Int8")
    return df


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase/strip all textual columns EXCEPT ID columns (IDs stay as-is).
    """
    for c in df.columns:
        if c in ID_COLS:
            # keep IDs stable (trim whitespace)
            df[c] = df[c].astype("string").str.strip()
            continue
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
            df[c] = _to_lower_stripped(df[c])
    return df


def _convert_yes_no(df: pd.DataFrame) -> (pd.DataFrame, List[str]):
    """
    Attempt yes/no→1/0 conversion on all string-like columns.
    Return the list of columns that ended up boolean-like (nullable Int8).
    """
    bool_cols: List[str] = []
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
            new_s = _map_yes_no_to_int(df[c])
            if new_s is not df[c] and str(new_s.dtype) == "Int8":
                df[c] = new_s
                bool_cols.append(c)
    return df, bool_cols


def _impute_missing(df: pd.DataFrame, bool_cols: List[str]) -> pd.DataFrame:
    """
    - numeric (excluding bool_cols) → fill with column median
    - categorical/text → fill with "missing"
    - boolean-like (nullable Int8) → leave NaN (as requested)
    """
    # numeric (but not our boolean Int8s)
    for c in df.columns:
        if c in bool_cols:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            med = df[c].median(skipna=True)
            if pd.isna(med):
                # if the entire column is NaN, use 0 as a neutral median
                med = 0
            df[c] = df[c].fillna(med)

    # categorical / string
    for c in df.columns:
        if (pd.api.types.is_object_dtype(df[c]) or
            pd.api.types.is_string_dtype(df[c])) and c not in ID_COLS:
            df[c] = df[c].fillna("missing")

    # boolean-like Int8: do nothing (keep NaN)
    return df


def apply_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure PK exists
    if "crash_record_id" not in df.columns:
        for alt in ["record_id", "id", "crash_id"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "crash_record_id"})
                break
    if "crash_record_id" not in df.columns:
        raise ValueError("Required primary key 'crash_record_id' is missing from Silver CSV.")

    # Deduplicate on PK
    df = df.drop_duplicates(subset=["crash_record_id"], keep="last")

    # Coerce known numeric columns first
    df = _coerce_numeric(df)
    df = _normalize_text(df)
    df, bool_cols = _convert_yes_no(df)

    # Split crash_date to separate columns
    df = _split_crash_datetime(df)
    df = _impute_missing(df, bool_cols)

    # Final PK hygiene
    df["crash_record_id"] = df["crash_record_id"].astype("string").str.strip()
    df = df[df["crash_record_id"].notna() & (df["crash_record_id"] != "")]

    # <<< NEW: whitelist projection >>> 
    keep = [c for c in KEEP_COLS if c in df.columns]
    df = df[keep]

    return df

