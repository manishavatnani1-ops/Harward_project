"""
Harvard Art Museums — ETL, SQL & Analytics Explorer
====================================================
A Streamlit front-end for the same pipeline built in
harvard_artifacts_project_mysql_fixed.ipynb:

    Extract  -> paginate the Harvard Art Museums /object API
    Transform -> normalize raw JSON into 3 tables (metadata / media / colors)
    Load      -> create MySQL tables and insert the data (via pymysql)
    Query     -> run 26 pre-written analysis queries with optional charts

Run with:
    streamlit run app.py
"""

import time

import numpy as np
import pandas as pd
import pymysql
import requests
import streamlit as st
from sqlalchemy import create_engine

# ============================================================================
# Page config
# ============================================================================
st.set_page_config(
    page_title="Harvard Art Museums — ETL & SQL Explorer",
    page_icon="🏛️",
    layout="wide",
)

# ============================================================================
# Constants
# ============================================================================
CLASSIFICATION_URL = "https://api.harvardartmuseums.org/classification"
OBJECT_URL = "https://api.harvardartmuseums.org/object"

OBJECT_FIELDS = [
    "id", "title", "culture", "period", "century", "medium", "dimensions",
    "description", "department", "classification", "accessionyear",
    "accessionmethod", "images", "colors", "rank", "objectnumber",
    "datebegin", "dateend", "mediacount", "imagecount", "colorcount", "dated",
]

# Fallback list shown in the dropdown if the live classification lookup fails
# (e.g. no API key entered yet, or a network hiccup).
FALLBACK_CLASSIFICATIONS = [
    "Coins", "Paintings", "Sculpture", "Jewelry", "Drawings",
    "Photographs", "Prints", "Vessels", "Textiles", "Manuscripts",
]

PAGE_SIZE = 100
MIN_TARGET_COUNT = 2500  # project spec: minimum 2500 records per classification

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS artifact_metadata (
        id              INT PRIMARY KEY,
        title           TEXT,
        culture         VARCHAR(255),
        period          VARCHAR(255),
        century         VARCHAR(255),
        medium          TEXT,
        dimensions      VARCHAR(255),
        description     TEXT,
        department      VARCHAR(255),
        classification  VARCHAR(255),
        accessionyear   INT,
        accessionmethod VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_media (
        objectid   INT PRIMARY KEY,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        `rank`     INT,
        datebegin  INT,
        dateend    INT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS artifact_colors (
        color_id  INT AUTO_INCREMENT PRIMARY KEY,
        objectid  INT,
        color     VARCHAR(50),
        spectrum  VARCHAR(50),
        hue       VARCHAR(50),
        percent   FLOAT,
        css3      VARCHAR(50),
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    );
    """,
]

# 20 required + 6 bonus queries, identical to the notebook / schema.sql
QUERIES = {
    "Q1. Artifacts from the 11th century, Byzantine culture": """
        SELECT * FROM artifact_metadata
        WHERE century LIKE '%11th%' AND culture = 'Byzantine';
    """,
    "Q2. Unique cultures represented": """
        SELECT DISTINCT culture FROM artifact_metadata
        WHERE culture IS NOT NULL ORDER BY culture;
    """,
    "Q3. Artifacts from the Archaic Period": """
        SELECT * FROM artifact_metadata
        WHERE period LIKE '%Archaic%';
    """,
    "Q4. Artifact titles ordered by accession year (desc)": """
        SELECT title, accessionyear FROM artifact_metadata
        WHERE accessionyear IS NOT NULL
        ORDER BY accessionyear DESC;
    """,
    "Q5. Artifact count per department": """
        SELECT department, COUNT(*) AS artifact_count
        FROM artifact_metadata
        GROUP BY department
        ORDER BY artifact_count DESC;
    """,
    "Q6. Artifacts with more than 1 image": """
        SELECT md.id, md.title, m.imagecount
        FROM artifact_media m
        JOIN artifact_metadata md ON md.id = m.objectid
        WHERE m.imagecount > 1;
    """,
    "Q7. Average rank of all artifacts": """
        SELECT AVG(`rank`) AS average_rank FROM artifact_media;
    """,
    "Q8. Artifacts with colorcount > mediacount": """
        SELECT md.id, md.title, m.colorcount, m.mediacount
        FROM artifact_media m
        JOIN artifact_metadata md ON md.id = m.objectid
        WHERE m.colorcount > m.mediacount;
    """,
    "Q9. Artifacts created between 1500 and 1600": """
        SELECT md.id, md.title, m.datebegin, m.dateend
        FROM artifact_media m
        JOIN artifact_metadata md ON md.id = m.objectid
        WHERE m.datebegin >= 1500 AND m.dateend <= 1600;
    """,
    "Q10. Artifacts with no media files": """
        SELECT COUNT(*) AS no_media_count
        FROM artifact_media
        WHERE mediacount IS NULL OR mediacount = 0;
    """,
    "Q11. Distinct hues in the dataset": """
        SELECT DISTINCT hue FROM artifact_colors
        WHERE hue IS NOT NULL ORDER BY hue;
    """,
    "Q12. Top 5 most used colors by frequency": """
        SELECT color, COUNT(*) AS frequency
        FROM artifact_colors
        WHERE color IS NOT NULL
        GROUP BY color
        ORDER BY frequency DESC
        LIMIT 5;
    """,
    "Q13. Average coverage percentage per hue": """
        SELECT hue, AVG(percent) AS avg_percent
        FROM artifact_colors
        WHERE hue IS NOT NULL
        GROUP BY hue
        ORDER BY avg_percent DESC;
    """,
    "Q14. Colors used for the first collected artifact": """
        SELECT * FROM artifact_colors
        WHERE objectid = (SELECT MIN(id) FROM artifact_metadata);
    """,
    "Q15. Total number of color entries": """
        SELECT COUNT(*) AS total_color_entries FROM artifact_colors;
    """,
    "Q16. Titles and hues for Byzantine culture artifacts": """
        SELECT md.title, c.hue
        FROM artifact_metadata md
        JOIN artifact_colors c ON c.objectid = md.id
        WHERE md.culture = 'Byzantine';
    """,
    "Q17. Each artifact title with its associated hues": """
        SELECT md.title, GROUP_CONCAT(DISTINCT c.hue) AS hues
        FROM artifact_metadata md
        JOIN artifact_colors c ON c.objectid = md.id
        GROUP BY md.title;
    """,
    "Q18. Titles, cultures, media ranks where period is not null": """
        SELECT md.title, md.culture, m.`rank`
        FROM artifact_metadata md
        JOIN artifact_media m ON m.objectid = md.id
        WHERE md.period IS NOT NULL;
    """,
    "Q19. Top-10-ranked artifacts that include hue 'Grey'": """
        SELECT DISTINCT md.title, m.`rank`
        FROM artifact_metadata md
        JOIN artifact_media m ON m.objectid = md.id
        JOIN artifact_colors c ON c.objectid = md.id
        WHERE c.hue = 'Grey'
        ORDER BY m.`rank` ASC
        LIMIT 10;
    """,
    "Q20. Artifact count & avg media count per classification": """
        SELECT md.classification,
               COUNT(DISTINCT md.id) AS artifact_count,
               AVG(m.mediacount) AS avg_mediacount
        FROM artifact_metadata md
        JOIN artifact_media m ON m.objectid = md.id
        GROUP BY md.classification
        ORDER BY artifact_count DESC;
    """,
    "B1. Top 10 mediums by artifact count": """
        SELECT medium, COUNT(*) AS artifact_count
        FROM artifact_metadata
        WHERE medium IS NOT NULL
        GROUP BY medium
        ORDER BY artifact_count DESC
        LIMIT 10;
    """,
    "B2. Artifacts per century": """
        SELECT century, COUNT(*) AS artifact_count
        FROM artifact_metadata
        WHERE century IS NOT NULL
        GROUP BY century
        ORDER BY artifact_count DESC;
    """,
    "B3. Top 10 accession methods used": """
        SELECT accessionmethod, COUNT(*) AS artifact_count
        FROM artifact_metadata
        WHERE accessionmethod IS NOT NULL
        GROUP BY accessionmethod
        ORDER BY artifact_count DESC
        LIMIT 10;
    """,
    "B4. Average image count per department": """
        SELECT md.department, AVG(m.imagecount) AS avg_imagecount
        FROM artifact_metadata md
        JOIN artifact_media m ON m.objectid = md.id
        GROUP BY md.department
        ORDER BY avg_imagecount DESC;
    """,
    "B5. Artifacts with the widest date range": """
        SELECT md.title, m.datebegin, m.dateend,
               (m.dateend - m.datebegin) AS span_years
        FROM artifact_metadata md
        JOIN artifact_media m ON m.objectid = md.id
        WHERE m.datebegin IS NOT NULL AND m.dateend IS NOT NULL
        ORDER BY span_years DESC
        LIMIT 10;
    """,
    "B6. Most common CSS3 color overall": """
        SELECT css3, COUNT(*) AS frequency
        FROM artifact_colors
        WHERE css3 IS NOT NULL
        GROUP BY css3
        ORDER BY frequency DESC
        LIMIT 10;
    """,
}

# ============================================================================
# Session state
# ============================================================================
_DEFAULTS = {
    "fetched_records": [],
    "df_metadata": pd.DataFrame(),
    "df_media": pd.DataFrame(),
    "df_colors": pd.DataFrame(),
    "last_classification": None,
    "show_data": False,
    "query_result": None,
    "query_label": None,
}
for _key, _default in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

# ============================================================================
# Sidebar — connection settings
# ============================================================================
st.sidebar.header("⚙️ Configuration")

api_key = st.sidebar.text_input(
    "Harvard Art Museums API key",
    value="8c647885-c2b1-4554-9be1-7d8937e6ff53",
    type="password",
    help="Get a free key at harvardartmuseums.org/collections/api",
)

st.sidebar.subheader("MySQL connection")
mysql_host = st.sidebar.text_input("Host", value="localhost")
mysql_port = st.sidebar.number_input("Port", value=3306, step=1)
mysql_user = st.sidebar.text_input("User", value="root")
mysql_password = st.sidebar.text_input("Password", value="root", type="password")
mysql_db = st.sidebar.text_input("Database", value="harvard_artifacts")


# ============================================================================
# API helpers
# ============================================================================
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_classification_list(_api_key):
    """Pull the live list of classifications from the API; fall back to a
    fixed list if the call fails (e.g. bad/missing key)."""
    try:
        resp = requests.get(
            CLASSIFICATION_URL, params={"apikey": _api_key, "size": 100}, timeout=15
        )
        resp.raise_for_status()
        records = resp.json().get("records", [])
        names = sorted({r["name"] for r in records if r.get("name")})
        return names if names else FALLBACK_CLASSIFICATIONS
    except Exception:
        return FALLBACK_CLASSIFICATIONS


def fetch_objects(api_key, classification, target_count, page_size, progress_cb=None):
    """Paginate the /object endpoint until target_count records are collected
    for the chosen classification (same logic as the notebook)."""
    fetched = []
    page = 1
    while len(fetched) < target_count:
        params = {
            "apikey": api_key,
            "classification": classification,
            "size": page_size,
            "page": page,
            "fields": ",".join(OBJECT_FIELDS),
        }
        r = requests.get(OBJECT_URL, params=params, timeout=20)
        if r.status_code != 200:
            st.warning(f"API returned status {r.status_code} — stopping fetch.")
            break

        payload = r.json()
        records = payload.get("records", [])
        if not records:
            break

        fetched.extend(records)
        if progress_cb:
            progress_cb(len(fetched), target_count)

        page += 1
        info = payload.get("info", {})
        if info.get("pages") and page > info["pages"]:
            break
        time.sleep(0.12)  # be polite to the API

    return fetched[:target_count]


def transform_records(records):
    """Normalize raw JSON records into the 3 target tables."""
    metadata_rows, media_rows, colors_rows = [], [], []

    for rec in records:
        obj_id = rec.get("id") or rec.get("objectnumber")

        metadata_rows.append({
            "id": obj_id,
            "title": rec.get("title"),
            "culture": rec.get("culture"),
            "period": rec.get("period"),
            "century": rec.get("century") or rec.get("dated"),
            "medium": rec.get("medium"),
            "dimensions": rec.get("dimensions"),
            "description": rec.get("description"),
            "department": rec.get("department"),
            "classification": rec.get("classification"),
            "accessionyear": rec.get("accessionyear"),
            "accessionmethod": rec.get("accessionmethod"),
        })

        imagecount = rec.get("imagecount")
        if imagecount is None:
            imagecount = len(rec.get("images") or [])

        media_rows.append({
            "objectid": obj_id,
            "imagecount": imagecount,
            "mediacount": rec.get("mediacount"),
            "colorcount": rec.get("colorcount"),
            "rank": rec.get("rank"),
            "datebegin": rec.get("datebegin"),
            "dateend": rec.get("dateend"),
        })

        for c in (rec.get("colors") or []):
            colors_rows.append({
                "objectid": obj_id,
                "color": c.get("color"),
                "spectrum": c.get("spectrum"),
                "hue": c.get("hue"),
                "percent": c.get("percent"),
                "css3": c.get("css3"),
            })

    df_metadata = pd.DataFrame(metadata_rows).drop_duplicates(subset="id")
    df_media = pd.DataFrame(media_rows).drop_duplicates(subset="objectid")
    df_colors = pd.DataFrame(colors_rows)
    return df_metadata, df_media, df_colors


# ============================================================================
# MySQL helpers
# ============================================================================
def get_bootstrap_conn():
    return pymysql.connect(
        host=mysql_host, port=int(mysql_port), user=mysql_user,
        password=mysql_password, autocommit=True,
    )


def get_conn():
    return pymysql.connect(
        host=mysql_host, port=int(mysql_port), user=mysql_user,
        password=mysql_password, database=mysql_db,
        autocommit=False, charset="utf8mb4",
    )


def get_engine():
    return create_engine(
        f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
    )


def ensure_database_and_tables():
    bconn = get_bootstrap_conn()
    with bconn.cursor() as bcur:
        bcur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4;")
    bconn.close()

    conn = get_conn()
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()


def df_to_records(df, cols):
    """NaN/NaT -> None, whole-number floats -> int, so pymysql never chokes
    on a bare `nan` and INT columns don't receive values like 2011.0."""
    sub = df[cols].astype(object)
    sub = sub.where(pd.notnull(sub), None)
    records = []
    for row in sub.itertuples(index=False, name=None):
        clean = []
        for v in row:
            if isinstance(v, float) and v.is_integer():
                v = int(v)
            clean.append(v)
        records.append(tuple(clean))
    return records


def insert_into_mysql(df_metadata, df_media, df_colors):
    ensure_database_and_tables()
    conn = get_conn()
    cur = conn.cursor()

    metadata_cols = [
        "id", "title", "culture", "period", "century", "medium", "dimensions",
        "description", "department", "classification", "accessionyear", "accessionmethod",
    ]
    cur.executemany(
        """
        REPLACE INTO artifact_metadata
        (id,title,culture,period,century,medium,dimensions,description,department,classification,accessionyear,accessionmethod)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        df_to_records(df_metadata, metadata_cols),
    )

    media_cols = ["objectid", "imagecount", "mediacount", "colorcount", "rank", "datebegin", "dateend"]
    cur.executemany(
        """
        REPLACE INTO artifact_media
        (objectid,imagecount,mediacount,colorcount,`rank`,datebegin,dateend)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        df_to_records(df_media, media_cols),
    )

    n_colors = 0
    if not df_colors.empty:
        colors_cols = ["objectid", "color", "spectrum", "hue", "percent", "css3"]
        # avoid duplicate color rows piling up on repeated inserts of the
        # same classification
        cur.execute(
            "DELETE FROM artifact_colors WHERE objectid IN (%s)"
            % ",".join(str(int(i)) for i in df_metadata["id"].dropna().unique())
            if not df_metadata.empty else "SELECT 1"
        )
        cur.executemany(
            """
            INSERT INTO artifact_colors (objectid,color,spectrum,hue,percent,css3)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            df_to_records(df_colors, colors_cols),
        )
        n_colors = len(df_colors)

    conn.commit()
    cur.close()
    conn.close()
    return len(df_metadata), len(df_media), n_colors


def run_query(sql):
    engine = get_engine()
    try:
        df = pd.read_sql_query(sql, engine)
    finally:
        engine.dispose()
    return df


# ============================================================================
# UI — Header
# ============================================================================
st.title("🏛️ Harvard Art Museums — ETL & SQL Explorer")
st.markdown(
    """
Collect artifact records from the **Harvard Art Museums API**, load them into
**MySQL**, and explore the collection with pre-built SQL queries and charts.

**How to use this page:**
1. Pick a **classification** below (e.g. Coins, Paintings, Sculpture, Jewelry, Drawings) and a target record count (≥ 2,500 recommended).
2. Click **Collect Data** to fetch it from the API.
3. Click **Show Data** to preview what was fetched.
4. Click **Insert into SQL** to load it into your MySQL database.
5. Scroll down to **Query & Visualization** to run any of 26 pre-written analysis queries.
"""
)

st.divider()

# ============================================================================
# UI — Classification picker + fetch controls
# ============================================================================
col_a, col_b = st.columns([2, 1])

with col_a:
    classification_options = fetch_classification_list(api_key) if api_key else FALLBACK_CLASSIFICATIONS
    default_index = classification_options.index("Paintings") if "Paintings" in classification_options else 0
    classification = st.selectbox(
        "Select artifact classification",
        classification_options,
        index=default_index,
    )

with col_b:
    target_count = st.number_input(
        "Target record count",
        min_value=100,
        max_value=20000,
        value=MIN_TARGET_COUNT,
        step=100,
        help="Project spec requires a minimum of 2,500 records per classification.",
    )
    if target_count < MIN_TARGET_COUNT:
        st.caption(f"⚠️ Below the required minimum of {MIN_TARGET_COUNT}.")

btn_collect, btn_show, btn_insert = st.columns(3)

with btn_collect:
    collect_clicked = st.button("📥 Collect Data", use_container_width=True)

with btn_show:
    show_clicked = st.button("👀 Show Data", use_container_width=True)

with btn_insert:
    insert_clicked = st.button("💾 Insert into SQL", use_container_width=True)

# ---- Collect Data ----
if collect_clicked:
    if not api_key:
        st.error("Enter your Harvard Art Museums API key in the sidebar first.")
    else:
        progress_bar = st.progress(0, text="Starting fetch...")

        def _progress_cb(n, target):
            progress_bar.progress(min(n / target, 1.0), text=f"Fetched {n} / {target} records...")

        with st.spinner(f"Fetching '{classification}' records from the API..."):
            records = fetch_objects(api_key, classification, int(target_count), PAGE_SIZE, _progress_cb)

        progress_bar.empty()

        if not records:
            st.error("No records were returned — check your API key and classification, then try again.")
        else:
            df_m, df_med, df_c = transform_records(records)
            st.session_state.fetched_records = records
            st.session_state.df_metadata = df_m
            st.session_state.df_media = df_med
            st.session_state.df_colors = df_c
            st.session_state.last_classification = classification
            st.session_state.show_data = True
            st.success(f"Collected {len(records)} records for '{classification}'.")
            if len(records) < MIN_TARGET_COUNT:
                st.warning(
                    f"Only {len(records)} records were available from the API for this "
                    f"classification (fewer than the {MIN_TARGET_COUNT} target)."
                )

# ---- Show Data ----
if show_clicked:
    st.session_state.show_data = True

if st.session_state.show_data:
    if st.session_state.df_metadata.empty:
        st.info("No data collected yet — click **Collect Data** first.")
    else:
        st.subheader(f"Preview — {st.session_state.last_classification}")
        st.caption(
            f"metadata: {len(st.session_state.df_metadata)} rows | "
            f"media: {len(st.session_state.df_media)} rows | "
            f"colors: {len(st.session_state.df_colors)} rows"
        )
        tab1, tab2, tab3 = st.tabs(["📋 Metadata", "🖼️ Media", "🎨 Colors"])
        with tab1:
            st.dataframe(st.session_state.df_metadata, use_container_width=True)
        with tab2:
            st.dataframe(st.session_state.df_media, use_container_width=True)
        with tab3:
            st.dataframe(st.session_state.df_colors, use_container_width=True)

# ---- Insert into SQL ----
if insert_clicked:
    if st.session_state.df_metadata.empty:
        st.warning("Nothing to insert — click **Collect Data** first.")
    else:
        with st.spinner("Creating tables (if needed) and inserting into MySQL..."):
            try:
                n_meta, n_media, n_colors = insert_into_mysql(
                    st.session_state.df_metadata,
                    st.session_state.df_media,
                    st.session_state.df_colors,
                )
                st.success(
                    f"Inserted into MySQL database '{mysql_db}' — "
                    f"metadata: {n_meta}, media: {n_media}, colors: {n_colors}."
                )
            except Exception as e:
                st.error(f"Insert failed: {e}")

st.divider()

# ============================================================================
# UI — Query & Visualization
# ============================================================================
st.header("📊 Query & Visualization")

q_col, chart_col = st.columns([3, 1])
with q_col:
    query_label = st.selectbox("Choose a pre-written query", list(QUERIES.keys()))
with chart_col:
    st.write("")
    st.write("")
    show_chart = st.checkbox("Show chart (optional)")

if st.button("▶️ Run Query"):
    try:
        with st.spinner("Running query..."):
            df_result = run_query(QUERIES[query_label])
        st.session_state.query_result = df_result
        st.session_state.query_label = query_label
    except Exception as e:
        st.session_state.query_result = None
        st.error(f"Query failed: {e}")

if st.session_state.query_result is not None:
    st.subheader(st.session_state.query_label)
    st.dataframe(st.session_state.query_result, use_container_width=True)
    st.caption(f"{len(st.session_state.query_result)} row(s) returned.")

    if show_chart:
        df_r = st.session_state.query_result
        numeric_cols = df_r.select_dtypes(include="number").columns.tolist()
        other_cols = [c for c in df_r.columns if c not in numeric_cols]
        if numeric_cols and other_cols and not df_r.empty:
            x_col = other_cols[0]
            y_col = numeric_cols[0]
            chart_df = (
                df_r[[x_col, y_col]]
                .dropna()
                .head(20)
                .set_index(x_col)
            )
            st.bar_chart(chart_df)
        else:
            st.info("This result doesn't have a clear category/number pair to chart — showing table only.")

st.divider()
st.caption(
    "Data source: Harvard Art Museums API · "
    "Tables: artifact_metadata, artifact_media, artifact_colors"
)
