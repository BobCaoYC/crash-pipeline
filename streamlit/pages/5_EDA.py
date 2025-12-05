# pages/5_EDA.py
import os
import pathlib
import pandas as pd
import duckdb
import streamlit as st
import altair as alt

st.set_page_config(page_title="EDA", page_icon="📊", layout="wide")
st.title("📊 Exploratory Data Analysis (Gold)")

GOLD_DB_PATH = os.getenv("GOLD_DB_PATH", "/data/gold.duckdb")

# ----------------------------- helpers -----------------------------
def gold_exists() -> bool:
    return pathlib.Path(GOLD_DB_PATH).exists()

def open_con(ro=True):
    return duckdb.connect(GOLD_DB_PATH, read_only=ro)

@st.cache_data(show_spinner=False)
def list_tables() -> list[str]:
    q = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type IN ('BASE TABLE','VIEW')
      AND table_schema NOT IN ('information_schema','pg_catalog')
    ORDER BY table_schema, table_name
    """
    with open_con() as con:
        df = con.sql(q).df()
    out = []
    for _, r in df.iterrows():
        schema, name = str(r["table_schema"]), str(r["table_name"])
        out.append(name if schema == "main" else f"{schema}.{name}")
    return out

def q(sql: str) -> pd.DataFrame:
    with open_con() as con:
        return con.sql(sql).df()

def table_has_col(table: str, col: str) -> bool:
    schema, _, name = table.rpartition(".")
    schema = schema or "main"
    sql = f"""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='{schema}'
          AND table_name='{name}'
          AND column_name='{col}'
        LIMIT 1
    """
    return not q(sql).empty

def ident(qualified: str) -> str:
    parts = [p for p in qualified.split(".") if p]
    return ".".join('"' + p.replace('"','""') + '"' for p in parts)

def has_cols(tbl: str, cols: list[str]) -> bool:
    return all(table_has_col(tbl, c) for c in cols)

# ----------------------------- pick table -----------------------------
if not gold_exists():
    st.error(f"Gold DB not found at `{GOLD_DB_PATH}`.")
    st.stop()

tables = list_tables()
if not tables:
    st.warning("No tables found in Gold DB.")
    st.stop()

default_idx = 0
if "crashes" in tables:
    default_idx = tables.index("crashes")
table = st.selectbox("Table", tables, index=default_idx)

t = ident(table)

# ----------------------------- summary -----------------------------
left, right = st.columns([2, 3])
with left:
    row_cnt = q(f"SELECT COUNT(*) AS c FROM {t}")["c"][0]
    st.metric("Rows", f"{row_cnt:,}")

# --- Date range: support either crash_date OR (year,month,day) triple ---
if table_has_col(table, "crash_date"):
    dr = q(f"""
        SELECT MIN(crash_date) AS min_d, MAX(crash_date) AS max_d
        FROM {t}
        WHERE crash_date IS NOT NULL
    """)
    min_d, max_d = dr["min_d"][0], dr["max_d"][0]
    with right:
        st.write("**Date range:**")
        st.write(f"{min_d} → {max_d}")

elif has_cols(table, ["crash_year", "crash_month", "crash_day"]):
    dr = q(f"""
        WITH d AS (
          SELECT
            crash_year,
            crash_month,
            crash_day,
            strptime(
              printf('%04d-%02d-%02d', CAST(crash_year AS INTEGER),
                                      CAST(crash_month AS INTEGER),
                                      CAST(crash_day AS INTEGER)),
              '%Y-%m-%d'
            ) AS dte
          FROM {t}
          WHERE crash_year IS NOT NULL
            AND crash_month IS NOT NULL
            AND crash_day IS NOT NULL
        )
        SELECT MIN(dte) AS min_d, MAX(dte) AS max_d FROM d
    """)
    min_d, max_d = dr["min_d"][0], dr["max_d"][0]
    with right:
        st.write("**Date range (from year/month/day):**")
        st.write(f"{min_d} → {max_d}")
else:
    with right:
        st.info("No `crash_date` or (`crash_year`,`crash_month`,`crash_day`) found—skipping date range.")

# =====================================================================
# Original 4 plots
# =====================================================================

# 1) Histogram: posted_speed_limit
st.subheader("1️⃣ Histogram — posted_speed_limit")
if table_has_col(table, "posted_speed_limit"):
    df_speed = q(f"""
        SELECT posted_speed_limit
        FROM {t}
        WHERE posted_speed_limit IS NOT NULL
    """)
    chart = (
        alt.Chart(df_speed)
        .mark_bar()
        .encode(
            x=alt.X("posted_speed_limit:Q", bin=alt.Bin(maxbins=30), title="posted_speed_limit"),
            y=alt.Y("count()", title="count"),
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`posted_speed_limit` not found.")

st.info("The most populated posted speed limit is 30, possibly because the area contain the most roads with 30 speed limit.")
st.divider()

# 2) Bar chart: weather_condition
st.subheader("2️⃣ Bar chart — weather_condition")
if table_has_col(table, "weather_condition"):
    df_weather = q(f"""
        SELECT weather_condition, COUNT(*) AS n
        FROM {t}
        GROUP BY weather_condition
        ORDER BY n DESC
        LIMIT 30
    """)
    chart = (
        alt.Chart(df_weather)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("weather_condition:N", sort="-x", title="weather_condition"),
            tooltip=["weather_condition", "n"],
        )
        .properties(height=450)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`weather_condition` not found.")

st.info("The weather is mostly clear, with very little rain, likely because this time frame was dry.")
st.divider()

# 3) Line chart: crash_hour (0–23)
st.subheader("3️⃣ Line chart — crash_hour")
if table_has_col(table, "crash_hour"):
    df_hour = q(f"""
        SELECT crash_hour::INTEGER AS hour, COUNT(*) AS n
        FROM {t}
        WHERE crash_hour IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """)
    chart = (
        alt.Chart(df_hour)
        .mark_line(point=True)
        .encode(
            x=alt.X("hour:Q", title="hour of day (0–23)"),
            y=alt.Y("n:Q", title="count"),
            tooltip=["hour", "n"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`crash_hour` not found.")
st.info("The most common crash times were around the middle of the day or rush hour, since the most traffic is around those times.")
st.divider()

# 4) Pie chart: roadway_surface_cond
st.subheader("4️⃣ Pie chart — roadway_surface_cond")
if table_has_col(table, "roadway_surface_cond"):
    df_surface = q(f"""
        SELECT roadway_surface_cond AS label, COUNT(*) AS n
        FROM {t}
        GROUP BY label
        ORDER BY n DESC
        LIMIT 12
    """)
    pie = (
        alt.Chart(df_surface)
        .mark_arc(innerRadius=60)
        .encode(
            theta="n:Q",
            color=alt.Color("label:N", legend=alt.Legend(title="roadway_surface_cond")),
            tooltip=["label", "n"],
        )
        .properties(height=340)
    )
    st.altair_chart(pie, use_container_width=True)
else:
    st.warning("`roadway_surface_cond` not found.")
st.info("The road conditions are mostly dry, likely related to the recent weather also being dry.")
st.divider()

# =====================================================================
# 10 additional plots
# =====================================================================

# 5) Bar chart — crash_type
st.subheader("5️⃣ Bar chart — crash_type")
if table_has_col(table, "crash_type"):
    df_type = q(f"""
        SELECT crash_type, COUNT(*) AS n
        FROM {t}
        GROUP BY crash_type
        ORDER BY n DESC
        LIMIT 20
    """)
    chart = (
        alt.Chart(df_type)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("crash_type:N", sort="-x", title="crash_type"),
            tooltip=["crash_type", "n"],
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`crash_type` not found.")
st.info("Around 2/3 of the crashes are minor and can be driven away from, and 1/3 are serious or require towing.")
st.divider()

# 6) Stacked bar — crash_type by weather_condition (top 5 weather)
st.subheader("6️⃣ Stacked bar — crash_type by weather_condition")
if table_has_col(table, "crash_type") and table_has_col(table, "weather_condition"):
    df_ct_w = q(f"""
        WITH weather_top AS (
          SELECT weather_condition
          FROM {t}
          GROUP BY weather_condition
          ORDER BY COUNT(*) DESC
          LIMIT 5
        )
        SELECT
          crash_type,
          weather_condition,
          COUNT(*) AS n
        FROM {t}
        WHERE weather_condition IN (SELECT weather_condition FROM weather_top)
        GROUP BY crash_type, weather_condition
    """)
    chart = (
        alt.Chart(df_ct_w)
        .mark_bar()
        .encode(
            x=alt.X("sum(n):Q", title="count"),
            y=alt.Y("crash_type:N", sort="-x", title="crash_type"),
            color=alt.Color("weather_condition:N", legend=alt.Legend(title="weather")),
            tooltip=["crash_type", "weather_condition", "n"],
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Need both `crash_type` and `weather_condition` for this chart.")
st.info("Both crash types occur mostly in clear weather as expected, but minor crashes interestingly have more unknown weather.")
st.divider()

# 7) Bar chart — lighting_condition
st.subheader("7️⃣ Bar chart — lighting_condition")
if table_has_col(table, "lighting_condition"):
    df_light = q(f"""
        SELECT lighting_condition, COUNT(*) AS n
        FROM {t}
        GROUP BY lighting_condition
        ORDER BY n DESC
        LIMIT 20
    """)
    chart = (
        alt.Chart(df_light)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("lighting_condition:N", sort="-x", title="lighting_condition"),
            tooltip=["lighting_condition", "n"],
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`lighting_condition` not found.")
st.info("Around 2/3 of crashes happen during daylight and 1/3 during night, but usually there are many more people driving during the day.")
st.divider()

# 8) Bar chart — trafficway_type
st.subheader("8️⃣ Bar chart — trafficway_type")
if table_has_col(table, "trafficway_type"):
    df_traf = q(f"""
        SELECT trafficway_type, COUNT(*) AS n
        FROM {t}
        GROUP BY trafficway_type
        ORDER BY n DESC
        LIMIT 20
    """)
    chart = (
        alt.Chart(df_traf)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("trafficway_type:N", sort="-x", title="trafficway_type"),
            tooltip=["trafficway_type", "n"],
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`trafficway_type` not found.")
st.info("The majority of crashes are in a traffic way that isn't divided, but there are many more cars driving on undivided lanes than intersections.")
st.divider()

# 9) Boxplot — posted_speed_limit by crash_type (top 4 crash types)
st.subheader("9️⃣ Boxplot — posted_speed_limit by crash_type")
if table_has_col(table, "posted_speed_limit") and table_has_col(table, "crash_type"):
    df_box = q(f"""
        WITH top_types AS (
          SELECT crash_type
          FROM {t}
          GROUP BY crash_type
          ORDER BY COUNT(*) DESC
          LIMIT 4
        )
        SELECT posted_speed_limit::INTEGER AS posted_speed_limit,
               crash_type
        FROM {t}
        WHERE posted_speed_limit IS NOT NULL
          AND crash_type IN (SELECT crash_type FROM top_types)
    """)
    chart = (
        alt.Chart(df_box)
        .mark_boxplot()
        .encode(
            x=alt.X("crash_type:N", title="crash_type"),
            y=alt.Y("posted_speed_limit:Q", title="posted_speed_limit"),
            color="crash_type:N",
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Need both `posted_speed_limit` and `crash_type` for this chart.")
st.info("Because of the skewed speed limit values, the vast majorityof crashes are on the 30 speed mark.")
st.divider()

# 10) Heatmap — crash_hour vs weather_condition (top 5 weather)
st.subheader("🔟 Heatmap — crash_hour vs weather_condition")
if table_has_col(table, "crash_hour") and table_has_col(table, "weather_condition"):
    df_hm = q(f"""
        WITH weather_top AS (
          SELECT weather_condition
          FROM {t}
          GROUP BY weather_condition
          ORDER BY COUNT(*) DESC
          LIMIT 5
        )
        SELECT
          crash_hour::INTEGER AS crash_hour,
          weather_condition,
          COUNT(*) AS n
        FROM {t}
        WHERE crash_hour IS NOT NULL
          AND weather_condition IN (SELECT weather_condition FROM weather_top)
        GROUP BY crash_hour, weather_condition
    """)
    chart = (
        alt.Chart(df_hm)
        .mark_rect()
        .encode(
            x=alt.X("crash_hour:O", title="hour of day"),
            y=alt.Y("weather_condition:N", title="weather_condition"),
            color=alt.Color("n:Q", title="count"),
            tooltip=["crash_hour", "weather_condition", "n"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Need both `crash_hour` and `weather_condition` for this chart.")
st.info("The clear day data dominates this heatmap because of the quantity, but the general time trends can also be seen here.")
st.divider()

# 11) Histogram — street_no
st.subheader("1️⃣1️⃣ Histogram — street_no")
if table_has_col(table, "street_no"):
    df_street_no = q(f"""
        SELECT street_no
        FROM {t}
        WHERE street_no IS NOT NULL
    """)
    chart = (
        alt.Chart(df_street_no)
        .mark_bar()
        .encode(
            x=alt.X("street_no:Q", bin=alt.Bin(maxbins=40), title="street_no"),
            y=alt.Y("count()", title="count"),
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`street_no` not found.")
st.info("More crashes occur in lower street numerations likely because of there existing more lower street numbers.")
st.divider()

# 12) Bar chart — alignment
st.subheader("1️⃣2️⃣ Bar chart — alignment")
if table_has_col(table, "alignment"):
    df_align = q(f"""
        SELECT alignment, COUNT(*) AS n
        FROM {t}
        GROUP BY alignment
        ORDER BY n DESC
        LIMIT 20
    """)
    chart = (
        alt.Chart(df_align)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("alignment:N", sort="-x", title="alignment"),
            tooltip=["alignment", "n"],
        )
        .properties(height=380)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`alignment` not found.")
st.info("The data indicates that the street alignment is almost always straight and level.")
st.divider()

# 13) Bar chart — road_defect
st.subheader("1️⃣3️⃣ Bar chart — road_defect")
if table_has_col(table, "road_defect"):
    df_def = q(f"""
        SELECT road_defect, COUNT(*) AS n
        FROM {t}
        GROUP BY road_defect
        ORDER BY n DESC
        LIMIT 15
    """)
    chart = (
        alt.Chart(df_def)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("road_defect:N", sort="-x", title="road_defect"),
            tooltip=["road_defect", "n"],
        )
        .properties(height=380)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`road_defect` not found.")
st.info("the road conditions are generally good, but there is also a large amount of unknown quantities.")
st.divider()

# 14) Top streets — bar chart of street_name
st.subheader("1️⃣4️⃣ Top streets — street_name")
if table_has_col(table, "street_name"):
    df_street = q(f"""
        SELECT street_name, COUNT(*) AS n
        FROM {t}
        GROUP BY street_name
        ORDER BY n DESC
        LIMIT 20
    """)
    chart = (
        alt.Chart(df_street)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="count"),
            y=alt.Y("street_name:N", sort="-x", title="street_name"),
            tooltip=["street_name", "n"],
        )
        .properties(height=500)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("`street_name` not found.")
st.info("Western Avenue seems to have the most crashes, likely due to its high frequency of traffic.")