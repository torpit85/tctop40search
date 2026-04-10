from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DB_CANDIDATES = [
    BASE_DIR / 'db' / 'tctop40.sqlite',
    BASE_DIR / 'db' / 'torreys_corner_top40.sqlite',
    BASE_DIR / 'tctop40.sqlite',
    BASE_DIR / 'torreys_corner_top40.sqlite',
]
DB_PATH = next((path for path in DB_CANDIDATES if path.exists()), DB_CANDIDATES[0])

st.set_page_config(page_title="Torrey's Corner Top 40 Search Engine", layout="wide")


@st.cache_resource
def get_connection() -> sqlite3.Connection:
    if not Path(DB_PATH).exists():
        raise FileNotFoundError(f'Database not found: {DB_PATH}')
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def make_fts_query(user_text: str) -> str:
    user_text = (user_text or '').strip()
    if not user_text:
        return ''
    return '"' + user_text.replace('"', '""') + '"'


def artist_role_config(role_mode: str) -> dict[str, str]:
    mapping = {
        "Full credit": {
            "norm_col": "normalized_full_artist",
            "display_col": "full_artist_display",
            "label": "full credit",
        },
        "Lead artist": {
            "norm_col": "normalized_lead_artist",
            "display_col": "lead_artist_display",
            "label": "lead artist",
        },
        "Featured artist": {
            "norm_col": "normalized_featured_artist",
            "display_col": "featured_artist_display",
            "label": "featured artist",
        },
    }
    return mapping[role_mode]



ENTRY_STATS_CTE = """
WITH ordered_weeks AS (
    SELECT
        chart_week_id,
        chart_date,
        LAG(chart_week_id) OVER (ORDER BY chart_date, chart_week_id) AS prev_chart_week_id
    FROM chart_week
),
entry_stats AS (
    SELECT
        e.entry_id,
        cw.chart_date,
        cw.prev_chart_week_id,
        prev.position AS last_week_position,
        ROW_NUMBER() OVER (
            PARTITION BY e.canonical_song_id
            ORDER BY cw.chart_date, e.position, e.entry_id
        ) AS weeks_on_chart
    FROM entry e
    JOIN ordered_weeks cw ON cw.chart_week_id = e.chart_week_id
    LEFT JOIN entry prev
      ON prev.chart_week_id = cw.prev_chart_week_id
     AND prev.canonical_song_id = e.canonical_song_id
)
"""


@st.cache_data(show_spinner=False)
def load_chart_dates() -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT chart_date FROM chart_week ORDER BY chart_date DESC"
    ).fetchall()
    return [row[0] for row in rows]


def nearest_chart_date(selected_date: str, available_dates: list[str]) -> tuple[str | None, bool]:
    if not available_dates:
        return None, False
    ordered = sorted(available_dates)
    if selected_date in set(ordered):
        return selected_date, False
    prior = [d for d in ordered if d <= selected_date]
    if prior:
        return prior[-1], True
    return ordered[0], True


@st.cache_data(show_spinner=False)
def load_overview() -> dict[str, object]:
    conn = get_connection()
    min_date, max_date, weeks = conn.execute(
        "SELECT MIN(chart_date), MAX(chart_date), COUNT(*) FROM chart_week"
    ).fetchone()
    entries = conn.execute("SELECT COUNT(*) FROM entry").fetchone()[0]
    table_names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if 'canonical_song' in table_names:
        unique_songs = conn.execute("SELECT COUNT(*) FROM canonical_song").fetchone()[0]
    else:
        unique_songs = conn.execute(
            "SELECT COUNT(DISTINCT normalized_song_title || '||' || normalized_full_artist) FROM entry "
            "WHERE normalized_song_title <> '' AND normalized_full_artist <> ''"
        ).fetchone()[0]

    unique_full_artists = conn.execute(
        "SELECT COUNT(DISTINCT normalized_full_artist) FROM entry WHERE COALESCE(normalized_full_artist, '') <> ''"
    ).fetchone()[0]
    unique_lead_artists = conn.execute(
        "SELECT COUNT(DISTINCT normalized_lead_artist) FROM entry WHERE COALESCE(normalized_lead_artist, '') <> ''"
    ).fetchone()[0]

    return {
        "min_date": min_date,
        "max_date": max_date,
        "weeks": weeks,
        "entries": entries,
        "unique_songs": unique_songs,
        "unique_full_artists": unique_full_artists,
        "unique_lead_artists": unique_lead_artists,
    }


@st.cache_data(show_spinner=False)
def marker_counts() -> dict[str, int]:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN derived_is_debut = 1 THEN 1 ELSE 0 END) AS debuts,
            SUM(CASE WHEN derived_is_top_debut = 1 THEN 1 ELSE 0 END) AS top_debuts,
            SUM(CASE WHEN derived_is_reentry = 1 THEN 1 ELSE 0 END) AS reentries
        FROM entry
        """
    ).fetchone()
    return {
        "debuts": int(row["debuts"] or 0),
        "top_debuts": int(row["top_debuts"] or 0),
        "reentries": int(row["reentries"] or 0),
    }


@st.cache_data(show_spinner=False)
def run_search(query: str, limit: int, marker_filter: str) -> pd.DataFrame:
    conn = get_connection()
    where_sql = "WHERE entry_fts MATCH ?"
    params: list[object] = [make_fts_query(query)]

    if marker_filter == "DEBUT":
        where_sql += " AND e.derived_is_debut = 1"
    elif marker_filter == "TOP DEBUT":
        where_sql += " AND e.derived_is_top_debut = 1"
    elif marker_filter == "RE-ENTRY":
        where_sql += " AND e.derived_is_reentry = 1"

    params.append(limit)

    sql = ENTRY_STATS_CTE + f"""
        SELECT
            cw.chart_date,
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            e.canonical_song_id,
            cw.row_count,
            cw.source_file
        FROM entry_fts f
        JOIN entry e ON e.entry_id = f.rowid
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        {where_sql}
        ORDER BY cw.chart_date DESC, e.position ASC
        LIMIT ?
    """
    return pd.read_sql_query(sql, conn, params=params)


@st.cache_data(show_spinner=False)
def load_chart(chart_date: str) -> tuple[pd.DataFrame, dict[str, object] | None]:
    conn = get_connection()
    meta_row = conn.execute(
        "SELECT chart_date, chart_id, source_file, source_zip, row_count, notes FROM chart_week WHERE chart_date = ?",
        (chart_date,),
    ).fetchone()
    if meta_row is None:
        return pd.DataFrame(), None
    meta = dict(meta_row)
    sql = ENTRY_STATS_CTE + """
        SELECT
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            e.canonical_song_id,
            e.raw_slug AS slug
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        WHERE cw.chart_date = ?
        ORDER BY e.position
        """
    df = pd.read_sql_query(sql, conn, params=(chart_date,))
    return df, meta


@st.cache_data(show_spinner=False)
def canonical_song_matches(term: str, limit: int = 100) -> pd.DataFrame:
    conn = get_connection()
    like = f"%{term.strip().lower()}%"
    sql = """
        SELECT
            canonical_song_id,
            canonical_title,
            COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist,
            COALESCE(canonical_lead_artist, canonical_artist) AS canonical_lead_artist,
            COALESCE(canonical_featured_artist, '') AS canonical_featured_artist,
            entry_count AS chart_weeks,
            first_chart_date AS first_date,
            last_chart_date AS last_date
        FROM canonical_song
        WHERE LOWER(canonical_title) LIKE ?
           OR LOWER(COALESCE(canonical_full_artist, canonical_artist)) LIKE ?
           OR LOWER(COALESCE(canonical_lead_artist, canonical_artist)) LIKE ?
           OR LOWER(canonical_title || ' ' || COALESCE(canonical_full_artist, canonical_artist)) LIKE ?
        ORDER BY last_date DESC, chart_weeks DESC, canonical_title, canonical_artist
        LIMIT ?
    """
    df = pd.read_sql_query(sql, conn, params=(like, like, like, like, limit))
    if not df.empty:
        placeholders = ",".join("?" for _ in df["canonical_song_id"].tolist())
        peaks = pd.read_sql_query(
            f"""
            SELECT canonical_song_id, MIN(position) AS peak
            FROM entry
            WHERE canonical_song_id IN ({placeholders})
            GROUP BY canonical_song_id
            """,
            conn,
            params=df["canonical_song_id"].tolist(),
        )
        df = df.merge(peaks, on="canonical_song_id", how="left")
        df = df[
            [
                "canonical_song_id",
                "canonical_title",
                "canonical_artist",
                "canonical_lead_artist",
                "canonical_featured_artist",
                "chart_weeks",
                "peak",
                "first_date",
                "last_date",
            ]
        ]
    return df


@st.cache_data(show_spinner=False)
def canonical_song_history(canonical_song_id: int) -> tuple[pd.DataFrame, dict[str, object] | None, pd.DataFrame]:
    conn = get_connection()
    stats_row = conn.execute(
        """
        SELECT
            cs.canonical_song_id,
            cs.canonical_title AS song,
            COALESCE(cs.canonical_full_artist, cs.canonical_artist) AS artist,
            COALESCE(cs.canonical_lead_artist, cs.canonical_artist) AS lead_artist,
            COALESCE(cs.canonical_featured_artist, '') AS featured_artist,
            cs.entry_count AS chart_weeks,
            cs.first_chart_date AS first_date,
            cs.last_chart_date AS last_date,
            MIN(e.position) AS peak,
            COUNT(DISTINCT sa.alias_display_key) AS alias_count
        FROM canonical_song cs
        LEFT JOIN entry e ON e.canonical_song_id = cs.canonical_song_id
        LEFT JOIN song_alias sa ON sa.canonical_song_id = cs.canonical_song_id
        WHERE cs.canonical_song_id = ?
        GROUP BY
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.canonical_full_artist,
            cs.canonical_lead_artist,
            cs.canonical_featured_artist,
            cs.entry_count,
            cs.first_chart_date,
            cs.last_chart_date
        """,
        (canonical_song_id,),
    ).fetchone()
    if stats_row is None or stats_row["song"] is None:
        return pd.DataFrame(), None, pd.DataFrame()

    sql = ENTRY_STATS_CTE + """
        SELECT
            cw.chart_date,
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            cw.row_count,
            cw.source_file
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        WHERE e.canonical_song_id = ?
        ORDER BY cw.chart_date
        """
    history = pd.read_sql_query(sql, conn, params=(canonical_song_id,))

    aliases = pd.read_sql_query(
        """
        SELECT
            alias_song_title AS song,
            alias_artist AS artist,
            entry_count AS chart_weeks,
            week_count,
            first_chart_date AS first_date,
            last_chart_date AS last_date
        FROM song_alias
        WHERE canonical_song_id = ?
        ORDER BY last_date DESC, week_count DESC, song, artist
        """,
        conn,
        params=(canonical_song_id,),
    )
    return history, dict(stats_row), aliases


@st.cache_data(show_spinner=False)
def artist_matches(term: str, role_mode: str, limit: int = 100) -> pd.DataFrame:
    conn = get_connection()
    cfg = artist_role_config(role_mode)
    like = f"%{term.strip().lower()}%"
    norm_col = cfg["norm_col"]
    display_col = cfg["display_col"]

    sql = f"""
        SELECT
            {norm_col} AS normalized_artist,
            MIN({display_col}) AS display_artist,
            COUNT(*) AS chart_weeks,
            MIN(chart_week.chart_date) AS first_date,
            MAX(chart_week.chart_date) AS last_date,
            MIN(position) AS peak
        FROM entry
        JOIN chart_week USING(chart_week_id)
        WHERE COALESCE({norm_col}, '') LIKE ?
          AND COALESCE({norm_col}, '') <> ''
        GROUP BY {norm_col}
        ORDER BY chart_weeks DESC, last_date DESC, display_artist
        LIMIT ?
    """
    return pd.read_sql_query(sql, conn, params=(like, limit))


@st.cache_data(show_spinner=False)
def artist_history(normalized_artist: str, role_mode: str) -> tuple[pd.DataFrame, dict[str, object] | None, pd.DataFrame]:
    conn = get_connection()
    cfg = artist_role_config(role_mode)
    norm_col = cfg["norm_col"]
    display_col = cfg["display_col"]

    stats_row = conn.execute(
        f"""
        SELECT
            MIN({display_col}) AS artist,
            COUNT(*) AS chart_weeks,
            COUNT(DISTINCT COALESCE(canonical_song_id, normalized_song_title)) AS distinct_songs,
            MIN(position) AS peak,
            MIN(chart_week.chart_date) AS first_date,
            MAX(chart_week.chart_date) AS last_date
        FROM entry
        JOIN chart_week USING(chart_week_id)
        WHERE {norm_col} = ?
          AND COALESCE({norm_col}, '') <> ''
        """,
        (normalized_artist,),
    ).fetchone()
    if stats_row is None or stats_row[0] is None:
        return pd.DataFrame(), None, pd.DataFrame()

    sql = ENTRY_STATS_CTE + f"""
        SELECT
            cw.chart_date,
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            cw.row_count,
            cw.source_file
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        WHERE e.{norm_col} = ?
        ORDER BY cw.chart_date
        """
    history = pd.read_sql_query(sql, conn, params=(normalized_artist,))
    songs = pd.read_sql_query(
        f"""
        SELECT
            COALESCE(cs.canonical_title, entry.song_title_display) AS song,
            COUNT(*) AS chart_weeks,
            MIN(chart_week.chart_date) AS first_date,
            MAX(chart_week.chart_date) AS last_date,
            MIN(position) AS peak
        FROM entry
        JOIN chart_week USING(chart_week_id)
        LEFT JOIN canonical_song cs ON cs.canonical_song_id = entry.canonical_song_id
        WHERE entry.{norm_col} = ?
        GROUP BY COALESCE(entry.canonical_song_id, entry.normalized_song_title), COALESCE(cs.canonical_title, entry.song_title_display)
        ORDER BY peak ASC, chart_weeks DESC, last_date DESC, song
        """,
        conn,
        params=(normalized_artist,),
    )
    return history, dict(stats_row), songs


@st.cache_data(show_spinner=False)
def load_special_entries(kind: str, limit: int) -> pd.DataFrame:
    conn = get_connection()

    if kind == "Biggest climbers":
        sql = ENTRY_STATS_CTE + """
            SELECT
                cw.chart_date,
                e.position,
                es.last_week_position,
                es.weeks_on_chart,
                (es.last_week_position - e.position) AS improvement,
                e.song_title_display AS song,
                e.full_artist_display AS artist,
                e.lead_artist_display AS lead_artist,
                e.featured_artist_display AS featured_artist,
                e.derived_marker
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            JOIN entry_stats es ON es.entry_id = e.entry_id
            WHERE es.last_week_position IS NOT NULL
              AND es.last_week_position > e.position
            ORDER BY improvement DESC, cw.chart_date DESC, e.position ASC
            LIMIT ?
        """
        return pd.read_sql_query(sql, conn, params=(limit,))

    if kind == "Artists with most Top 10 weeks":
        sql = """
            SELECT
                lead_artist_display AS lead_artist,
                COUNT(*) AS top_10_weeks,
                COUNT(DISTINCT canonical_song_id) AS distinct_songs,
                MIN(chart_week.chart_date) AS first_date,
                MAX(chart_week.chart_date) AS last_date,
                MIN(position) AS best_peak
            FROM entry
            JOIN chart_week USING(chart_week_id)
            WHERE position <= 10
              AND COALESCE(normalized_lead_artist, '') <> ''
            GROUP BY normalized_lead_artist, lead_artist_display
            ORDER BY top_10_weeks DESC, best_peak ASC, last_date DESC, lead_artist
            LIMIT ?
        """
        return pd.read_sql_query(sql, conn, params=(limit,))

    if kind == "Artists with most appearances on a single chart":
        sql = """
            WITH artist_week_counts AS (
                SELECT
                    chart_week.chart_date,
                    lead_artist_display AS lead_artist,
                    normalized_lead_artist,
                    COUNT(*) AS appearances_on_chart
                FROM entry
                JOIN chart_week USING(chart_week_id)
                WHERE COALESCE(normalized_lead_artist, '') <> ''
                GROUP BY chart_week.chart_date, normalized_lead_artist, lead_artist_display
            )
            SELECT
                lead_artist,
                appearances_on_chart,
                chart_date
            FROM artist_week_counts
            ORDER BY appearances_on_chart DESC, chart_date DESC, lead_artist
            LIMIT ?
        """
        return pd.read_sql_query(sql, conn, params=(limit,))

    conditions = {
        "#1 hits": "e.position = 1",
        "Top 10 hits": "e.position <= 10",
        "Debut weeks": "e.derived_is_debut = 1",
        "Top 5 debuts": "e.derived_is_top_debut = 1 AND e.position <= 5",
        "Top debuts": "e.derived_is_top_debut = 1",
        "Re-entries": "e.derived_is_reentry = 1",
    }
    sql = ENTRY_STATS_CTE + f"""
        SELECT
            cw.chart_date,
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            e.canonical_song_id,
            cw.row_count
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        WHERE {conditions[kind]}
        ORDER BY cw.chart_date DESC, e.position ASC
        LIMIT ?
    """
    return pd.read_sql_query(sql, conn, params=(limit,))


def render_kpis(items: Iterable[tuple[str, object]]) -> None:
    items = list(items)
    cols = st.columns(len(items))
    for col, (label, value) in zip(cols, items):
        col.metric(label, value)




ANALYTICS_SECTIONS = [
    "Overview",
    "Movement",
    "Longevity",
    "Artists",
    "Years & Eras",
    "Records & Outliers",
]


def _safe_int(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _fmt_rank(value: object) -> str:
    iv = _safe_int(value)
    return f"#{iv}" if iv is not None else "—"


@st.cache_data(show_spinner=False)
def load_analytics_base() -> pd.DataFrame:
    conn = get_connection()
    sql = ENTRY_STATS_CTE + """
        SELECT
            cw.chart_date,
            e.chart_week_id,
            e.position,
            es.last_week_position,
            es.weeks_on_chart,
            e.song_title_display AS title,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            e.derived_is_debut,
            e.derived_is_reentry,
            e.canonical_song_id,
            e.normalized_song_title,
            e.normalized_full_artist,
            e.normalized_lead_artist,
            e.normalized_featured_artist
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        ORDER BY cw.chart_date, e.position
    """
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        return df
    df["chart_date"] = pd.to_datetime(df["chart_date"])
    df["year"] = df["chart_date"].dt.year
    df["month"] = df["chart_date"].dt.month
    df["position"] = pd.to_numeric(df["position"], errors="coerce")
    df["last_week_position"] = pd.to_numeric(df["last_week_position"], errors="coerce")
    df["weeks_on_chart"] = pd.to_numeric(df["weeks_on_chart"], errors="coerce")
    df["derived_is_debut"] = pd.to_numeric(df["derived_is_debut"], errors="coerce").fillna(0).astype(int)
    df["derived_is_reentry"] = pd.to_numeric(df["derived_is_reentry"], errors="coerce").fillna(0).astype(int)
    df["song_key"] = df["canonical_song_id"].apply(lambda x: f"cs_{int(x)}" if pd.notna(x) else None)
    fallback_song_key = (
        df["normalized_song_title"].fillna("").astype(str).str.strip().str.lower()
        + "||"
        + df["normalized_full_artist"].fillna("").astype(str).str.strip().str.lower()
    )
    df["song_key"] = df["song_key"].fillna(fallback_song_key)
    df["artist_key"] = df["normalized_lead_artist"].fillna("").astype(str).str.strip().str.lower()
    df.loc[df["artist_key"] == "", "artist_key"] = df["normalized_full_artist"].fillna("").astype(str).str.strip().str.lower()
    df["artist_key"] = df["artist_key"].replace("", pd.NA)
    df["is_debut"] = df["derived_is_debut"].eq(1)
    df["is_reentry"] = df["derived_is_reentry"].eq(1)
    df["has_prior_rank"] = df["last_week_position"].notna() & ~df["is_debut"] & ~df["is_reentry"]
    df["move"] = pd.NA
    df.loc[df["has_prior_rank"], "move"] = df.loc[df["has_prior_rank"], "last_week_position"] - df.loc[df["has_prior_rank"], "position"]
    df["move"] = pd.to_numeric(df["move"], errors="coerce")
    df["abs_move"] = df["move"].abs()
    df["is_up"] = df["move"] > 0
    df["is_down"] = df["move"] < 0
    df["is_hold"] = df["move"] == 0
    df["top20_flag"] = df["position"] <= 20
    df["top10_flag"] = df["position"] <= 10
    df["top5_flag"] = df["position"] <= 5
    df["num1_flag"] = df["position"] == 1

    ordered_dates = sorted(df["chart_date"].dropna().unique())
    next_map = {ordered_dates[i]: ordered_dates[i + 1] for i in range(len(ordered_dates) - 1)}
    df["next_chart_date"] = df["chart_date"].map(next_map)
    next_lookup = df[["song_key", "chart_date", "position", "top10_flag", "top20_flag", "top5_flag"]].rename(
        columns={
            "chart_date": "next_chart_date",
            "position": "next_position",
            "top10_flag": "next_top10_flag",
            "top20_flag": "next_top20_flag",
            "top5_flag": "next_top5_flag",
        }
    )
    df = df.merge(next_lookup, on=["song_key", "next_chart_date"], how="left")
    df["present_next_week"] = df["next_position"].notna()
    df["dropped_out_next_week"] = df["next_chart_date"].notna() & ~df["present_next_week"]
    df["entered_top10_this_week"] = df["top10_flag"] & df["has_prior_rank"] & (df["last_week_position"] > 10)
    df["entered_top20_this_week"] = df["top20_flag"] & df["has_prior_rank"] & (df["last_week_position"] > 20)
    df["entered_top5_this_week"] = df["top5_flag"] & df["has_prior_rank"] & (df["last_week_position"] > 5)
    df["exited_top10_this_week"] = df["has_prior_rank"] & (df["last_week_position"] <= 10) & (df["position"] > 10)
    return df


@st.cache_data(show_spinner=False)
def build_weekly_summary(df_chart: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()
    rows = []
    for chart_date, g in df_chart.groupby("chart_date", sort=True):
        valid_moves = g.loc[g["move"].notna(), "move"]
        abs_moves = g.loc[g["abs_move"].notna(), "abs_move"]
        top10 = g.loc[g["position"] <= 10, "weeks_on_chart"]
        bottom10 = g.loc[g["position"] >= 31, "weeks_on_chart"]
        rows.append({
            "chart_date": chart_date,
            "year": int(g["year"].iloc[0]),
            "month": int(g["month"].iloc[0]),
            "unique_titles": int(g["song_key"].nunique()),
            "unique_artists": int(g["artist_key"].dropna().nunique()),
            "debuts": int(g["is_debut"].sum()),
            "reentries": int(g["is_reentry"].sum()),
            "dropouts": int(g["dropped_out_next_week"].sum()),
            "turnover_total": int(g["is_debut"].sum() + g["is_reentry"].sum() + g["dropped_out_next_week"].sum()),
            "upward_movers": int(g["is_up"].sum()),
            "downward_movers": int(g["is_down"].sum()),
            "holds": int(g["is_hold"].sum()),
            "avg_abs_move": float(abs_moves.mean()) if not abs_moves.empty else float('nan'),
            "median_abs_move": float(abs_moves.median()) if not abs_moves.empty else float('nan'),
            "max_climb": float(valid_moves.max()) if not valid_moves.empty else float('nan'),
            "max_fall": float(valid_moves.min()) if not valid_moves.empty else float('nan'),
            "avg_chart_age": float(g["weeks_on_chart"].mean()) if not g["weeks_on_chart"].empty else float('nan'),
            "median_chart_age": float(g["weeks_on_chart"].median()) if not g["weeks_on_chart"].empty else float('nan'),
            "avg_top10_age": float(top10.mean()) if not top10.empty else float('nan'),
            "median_top10_age": float(top10.median()) if not top10.empty else float('nan'),
            "avg_bottom10_age": float(bottom10.mean()) if not bottom10.empty else float('nan'),
            "top10_churn": int(g["entered_top10_this_week"].sum()),
            "top20_churn": int(g["entered_top20_this_week"].sum()),
            "top5_churn": int(g["entered_top5_this_week"].sum()),
        })
    return pd.DataFrame(rows).sort_values("chart_date")


def _longest_true_run(mask: pd.Series) -> int:
    best = cur = 0
    for flag in mask.astype(bool).tolist():
        if flag:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _longest_presence_run(chart_dates: pd.Series, ordered_dates: list[pd.Timestamp]) -> int:
    date_to_idx = {d: i for i, d in enumerate(ordered_dates)}
    idxs = sorted(date_to_idx[d] for d in chart_dates.tolist() if d in date_to_idx)
    if not idxs:
        return 0
    best = cur = 1
    for prev, cur_idx in zip(idxs, idxs[1:]):
        if cur_idx == prev + 1:
            cur += 1
        else:
            best = max(best, cur)
            cur = 1
    return max(best, cur)


@st.cache_data(show_spinner=False)
def build_song_summary(df_chart: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()
    ordered_dates = sorted(df_chart["chart_date"].dropna().unique())
    rows = []
    for song_key, g in df_chart.groupby("song_key", sort=False):
        g = g.sort_values(["chart_date", "position"])
        peak_position = int(g["position"].min())
        peak_date = g.loc[g["position"] == peak_position, "chart_date"].iloc[0]
        dates = g["chart_date"].tolist()
        first_date = dates[0]
        last_date = dates[-1]
        ordered_index = {d: i for i, d in enumerate(ordered_dates)}
        chart_span_weeks = ordered_index[last_date] - ordered_index[first_date] + 1 if first_date in ordered_index and last_date in ordered_index else len(g)
        peak_loc = g.index[g["chart_date"] == peak_date][0]
        peak_seq = g.index.get_loc(peak_loc)
        move_vals = g["move"].dropna()
        rows.append({
            "song_key": song_key,
            "title": g["title"].iloc[0],
            "artist": g["artist"].iloc[0],
            "first_chart_date": first_date,
            "last_chart_date": last_date,
            "chart_span_weeks": int(chart_span_weeks),
            "total_chart_weeks": int(len(g)),
            "debut_position": int(g["position"].iloc[0]),
            "peak_position": peak_position,
            "peak_date": peak_date,
            "final_position": int(g["position"].iloc[-1]),
            "top20_weeks": int((g["position"] <= 20).sum()),
            "top10_weeks": int((g["position"] <= 10).sum()),
            "top5_weeks": int((g["position"] <= 5).sum()),
            "num1_weeks": int((g["position"] == 1).sum()),
            "longest_consecutive_chart_run": int(_longest_presence_run(g["chart_date"], ordered_dates)),
            "longest_consecutive_top10_run": int(_longest_true_run(g["position"] <= 10)),
            "longest_consecutive_top5_run": int(_longest_true_run(g["position"] <= 5)),
            "longest_consecutive_num1_run": int(_longest_true_run(g["position"] == 1)),
            "biggest_climb": float(move_vals.max()) if not move_vals.empty else float('nan'),
            "biggest_fall": float(move_vals.min()) if not move_vals.empty else float('nan'),
            "avg_abs_move": float(g["abs_move"].dropna().mean()) if not g["abs_move"].dropna().empty else float('nan'),
            "reentry_count": int(g["is_reentry"].sum()),
            "weeks_to_peak": int(peak_seq),
            "post_peak_weeks": int(len(g) - peak_seq - 1),
            "peaked_on_debut": bool(int(g["position"].iloc[0]) == peak_position),
            "returned_after_exit": bool(g["is_reentry"].sum() > 0),
        })
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def build_artist_weekly_presence(df_chart: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()
    grouped = df_chart.groupby(["chart_date", "artist_key", "lead_artist"], dropna=True)
    rows = []
    for (chart_date, artist_key, artist), g in grouped:
        rows.append({
            "chart_date": chart_date,
            "artist_key": artist_key,
            "artist": artist,
            "entries_on_chart": int(g["song_key"].nunique()),
            "entries_top20": int(g.loc[g["position"] <= 20, "song_key"].nunique()),
            "entries_top10": int(g.loc[g["position"] <= 10, "song_key"].nunique()),
            "entries_top5": int(g.loc[g["position"] <= 5, "song_key"].nunique()),
            "entries_num1": int(g.loc[g["position"] == 1, "song_key"].nunique()),
        })
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def build_artist_summary(df_chart: pd.DataFrame, df_song: pd.DataFrame, df_artist_presence: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()

    song_artist = df_chart.groupby("song_key", as_index=False).agg(
        artist_key=("artist_key", "first"),
        lead_artist_name=("lead_artist", "first"),
    )
    song_with_artist = df_song.merge(song_artist, on="song_key", how="left")
    song_with_artist["artist"] = song_with_artist["lead_artist_name"]

    song_agg = song_with_artist.groupby(["artist_key", "artist"], dropna=True).agg(
        distinct_songs=("song_key", "nunique"),
        top20_hits=("peak_position", lambda s: int((s <= 20).sum())),
        top10_hits=("peak_position", lambda s: int((s <= 10).sum())),
        top5_hits=("peak_position", lambda s: int((s <= 5).sum())),
        num1_hits=("peak_position", lambda s: int((s == 1).sum())),
        best_peak=("peak_position", "min"),
        avg_peak=("peak_position", "mean"),
        median_peak=("peak_position", "median"),
        first_chart_date=("first_chart_date", "min"),
        last_chart_date=("last_chart_date", "max"),
    ).reset_index()

    week_agg = df_chart.groupby(["artist_key", "lead_artist"], dropna=True).agg(
        total_chart_entries=("song_key", "nunique"),
        total_chart_weeks=("song_key", "size"),
        total_top20_weeks=("top20_flag", "sum"),
        total_top10_weeks=("top10_flag", "sum"),
        total_top5_weeks=("top5_flag", "sum"),
        total_num1_weeks=("num1_flag", "sum"),
    ).reset_index().rename(columns={"lead_artist": "artist"})

    max_presence = df_artist_presence.sort_values(["entries_on_chart", "chart_date"], ascending=[False, True]).drop_duplicates("artist_key")
    max_presence = max_presence[["artist_key", "entries_on_chart", "chart_date"]].rename(columns={
        "entries_on_chart": "max_simultaneous_entries",
        "chart_date": "week_of_max_simultaneous_entries",
    })

    out = week_agg.merge(song_agg, on=["artist_key", "artist"], how="outer")
    out = out.merge(max_presence, on="artist_key", how="left")
    out["active_span_weeks"] = (
        (pd.to_datetime(out["last_chart_date"]) - pd.to_datetime(out["first_chart_date"])) / pd.Timedelta(days=7)
    ).fillna(0).round().astype(int) + 1
    return out


@st.cache_data(show_spinner=False)
def build_yearly_summary(df_chart: pd.DataFrame, df_weekly: pd.DataFrame, df_song: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty or df_weekly.empty:
        return pd.DataFrame()
    year_base = df_chart.groupby("year").agg(
        unique_songs=("song_key", "nunique"),
        unique_artists=("artist_key", lambda s: s.dropna().nunique()),
    ).reset_index()
    weekly_year = df_weekly.groupby("year").agg(
        debuts=("debuts", "sum"),
        reentries=("reentries", "sum"),
        dropouts=("dropouts", "sum"),
        avg_turnover=("turnover_total", "mean"),
        avg_abs_move=("avg_abs_move", "mean"),
        avg_top10_churn=("top10_churn", "mean"),
        avg_chart_age=("avg_chart_age", "mean"),
        avg_top10_age=("avg_top10_age", "mean"),
    ).reset_index()
    num1_songs = df_chart.loc[df_chart["position"] == 1].groupby("year")["song_key"].nunique().reset_index(name="num1_songs")
    num1_runs = []
    num1_rows = df_chart.loc[df_chart["position"] == 1, ["song_key", "chart_date", "year"]].sort_values(["song_key", "chart_date"])
    for year, g in num1_rows.groupby("year"):
        longest = 0
        for _, sg in g.groupby("song_key"):
            cur = _longest_presence_run(sg["chart_date"], sorted(df_chart.loc[df_chart["year"] == year, "chart_date"].unique()))
            longest = max(longest, cur)
        num1_runs.append({"year": year, "longest_num1_run": int(longest)})
    num1_runs_df = pd.DataFrame(num1_runs)
    out = year_base.merge(weekly_year, on="year", how="left")
    out = out.merge(num1_songs, on="year", how="left")
    out = out.merge(num1_runs_df, on="year", how="left")
    out["num1_songs"] = out["num1_songs"].fillna(0).astype(int)
    out["longest_num1_run"] = out["longest_num1_run"].fillna(0).astype(int)
    return out.sort_values("year")


@st.cache_data(show_spinner=False)
def build_analytics_package() -> dict[str, pd.DataFrame]:
    chart = load_analytics_base()
    weekly = build_weekly_summary(chart)
    songs = build_song_summary(chart)
    artist_presence = build_artist_weekly_presence(chart)
    artists = build_artist_summary(chart, songs, artist_presence)
    years = build_yearly_summary(chart, weekly, songs)
    return {
        "chart": chart,
        "weekly": weekly,
        "songs": songs,
        "artist_presence": artist_presence,
        "artists": artists,
        "years": years,
    }


def _apply_analytics_filters(pkg: dict[str, pd.DataFrame], start_date: dt.date, end_date: dt.date, include_reentries: bool, min_weeks_on_chart: int) -> dict[str, pd.DataFrame]:
    chart = pkg["chart"].copy()
    if not chart.empty:
        mask = (chart["chart_date"].dt.date >= start_date) & (chart["chart_date"].dt.date <= end_date)
        chart = chart.loc[mask].copy()
        if not include_reentries:
            chart = chart.loc[~chart["is_reentry"]].copy()
    weekly = build_weekly_summary(chart)
    songs = build_song_summary(chart)
    if not songs.empty:
        songs = songs.loc[songs["total_chart_weeks"] >= min_weeks_on_chart].copy()
    artist_presence = build_artist_weekly_presence(chart)
    artists = build_artist_summary(chart, songs, artist_presence) if not chart.empty else pd.DataFrame()
    years = build_yearly_summary(chart, weekly, songs) if not chart.empty else pd.DataFrame()
    return {
        "chart": chart,
        "weekly": weekly,
        "songs": songs,
        "artist_presence": artist_presence,
        "artists": artists,
        "years": years,
    }


def _display_df(df: pd.DataFrame, columns: list[str] | None = None, hide_index: bool = True):
    if columns is not None and not df.empty:
        cols = [c for c in columns if c in df.columns]
        df = df[cols]

    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    st.dataframe(df, width="stretch", hide_index=hide_index)


def _render_overview(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    weekly = pkg["weekly"]
    chart = pkg["chart"]
    if chart.empty or weekly.empty:
        st.info("No analytics rows available for the selected filters.")
        return
    render_kpis([
        ("Unique songs", int(chart["song_key"].nunique())),
        ("Unique artists", int(chart["artist_key"].dropna().nunique())),
        ("Avg debuts/week", f"{weekly['debuts'].mean():.2f}"),
        ("Avg re-entries/week", f"{weekly['reentries'].mean():.2f}"),
        ("Avg dropouts/week", f"{weekly['dropouts'].mean():.2f}"),
        ("Avg movement", f"{weekly['avg_abs_move'].mean():.2f}"),
        ("Avg chart age", f"{weekly['avg_chart_age'].mean():.2f}"),
        ("Avg Top 10 age", f"{weekly['avg_top10_age'].mean():.2f}"),
    ])
    st.markdown("**Turnover over time**")
    st.line_chart(weekly.set_index("chart_date")[["debuts", "reentries", "dropouts"]], width="stretch")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Average movement over time**")
        st.line_chart(weekly.set_index("chart_date")[["avg_abs_move"]], width="stretch")
        st.markdown("**Average chart age over time**")
        st.line_chart(weekly.set_index("chart_date")[["avg_chart_age"]], width="stretch")
    with c2:
        st.markdown("**Average Top 10 age over time**")
        st.line_chart(weekly.set_index("chart_date")[["avg_top10_age"]], width="stretch")
        st.markdown("**Top 10 churn over time**")
        st.line_chart(weekly.set_index("chart_date")[["top10_churn"]], width="stretch")
    t1, t2, t3 = st.columns(3)
    with t1:
        st.markdown("**Highest-turnover weeks**")
        _display_df(weekly.sort_values(["turnover_total", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "debuts", "reentries", "dropouts", "turnover_total", "avg_abs_move"])
    with t2:
        st.markdown("**Oldest chart weeks**")
        _display_df(weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_chart_age", "median_chart_age", "avg_top10_age"])
    with t3:
        st.markdown("**Freshest chart weeks**")
        _display_df(weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[True, False]).head(top_n), ["chart_date", "avg_chart_age", "median_chart_age", "avg_top10_age"])


def _render_movement(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    chart = pkg["chart"]
    weekly = pkg["weekly"]
    valid_moves = chart.loc[chart["move"].notna()].copy()
    if weekly.empty:
        st.info("No movement data available for the selected filters.")
        return
    biggest_jump = valid_moves.sort_values(["move", "chart_date"], ascending=[False, False]).head(1)
    biggest_fall = valid_moves.sort_values(["move", "chart_date"], ascending=[True, False]).head(1)
    render_kpis([
        ("Biggest jump", f"{int(biggest_jump['move'].iloc[0]):+d}" if not biggest_jump.empty else "—"),
        ("Biggest fall", f"{int(biggest_fall['move'].iloc[0]):+d}" if not biggest_fall.empty else "—"),
        ("Avg up movers/week", f"{weekly['upward_movers'].mean():.2f}"),
        ("Avg down movers/week", f"{weekly['downward_movers'].mean():.2f}"),
        ("Avg holds/week", f"{weekly['holds'].mean():.2f}"),
        ("Avg Top 10 churn", f"{weekly['top10_churn'].mean():.2f}"),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Average absolute movement by week**")
        st.line_chart(weekly.set_index("chart_date")[["avg_abs_move"]], width="stretch")
        st.markdown("**Debuts / re-entries / dropouts over time**")
        st.line_chart(weekly.set_index("chart_date")[["debuts", "reentries", "dropouts"]], width="stretch")
    with c2:
        st.markdown("**Movement size distribution**")
        hist = valid_moves["abs_move"].value_counts().sort_index().rename_axis("abs_move").reset_index(name="count")
        if not hist.empty:
            st.bar_chart(hist.set_index("abs_move")[["count"]], width="stretch")
        st.markdown("**Top 10 churn over time**")
        st.line_chart(weekly.set_index("chart_date")[["top10_churn"]], width="stretch")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Biggest climbs**")
        _display_df(valid_moves.sort_values(["move", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "title", "artist", "last_week_position", "position", "move", "weeks_on_chart"])
        st.markdown("**Most chaotic weeks**")
        _display_df(weekly.sort_values(["avg_abs_move", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_abs_move", "median_abs_move", "debuts", "reentries", "dropouts", "top10_churn"])
    with t2:
        st.markdown("**Biggest falls**")
        _display_df(valid_moves.sort_values(["move", "chart_date"], ascending=[True, False]).head(top_n), ["chart_date", "title", "artist", "last_week_position", "position", "move", "weeks_on_chart"])
        st.markdown("**Most stable weeks**")
        _display_df(weekly.sort_values(["avg_abs_move", "chart_date"], ascending=[True, False]).head(top_n), ["chart_date", "avg_abs_move", "median_abs_move", "debuts", "reentries", "dropouts", "top10_churn"])


def _render_longevity(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    songs = pkg["songs"]
    chart = pkg["chart"]
    if songs.empty:
        st.info("No song-summary rows available for the selected filters.")
        return
    render_kpis([
        ("Avg run length", f"{songs['total_chart_weeks'].mean():.2f}"),
        ("Median run length", f"{songs['total_chart_weeks'].median():.2f}"),
        ("Avg weeks to peak", f"{songs['weeks_to_peak'].mean():.2f}"),
        ("Longest chart run", int(songs['total_chart_weeks'].max())),
        ("Longest Top 10 run", int(songs['longest_consecutive_top10_run'].max())),
        ("Longest #1 run", int(songs['longest_consecutive_num1_run'].max())),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Run-length distribution**")
        run_hist = songs["total_chart_weeks"].value_counts().sort_index().rename_axis("weeks").reset_index(name="count")
        st.bar_chart(run_hist.set_index("weeks")[["count"]], width="stretch")
        st.markdown("**Debut position vs eventual peak**")
        scatter = songs[["debut_position", "peak_position"]].dropna().copy()
        st.scatter_chart(scatter, x="debut_position", y="peak_position", width="stretch")
    with c2:
        st.markdown("**Weeks-to-peak distribution**")
        peak_hist = songs["weeks_to_peak"].value_counts().sort_index().rename_axis("weeks_to_peak").reset_index(name="count")
        st.bar_chart(peak_hist.set_index("weeks_to_peak")[["count"]], width="stretch")
        st.markdown("**Peak position vs total run length**")
        scatter2 = songs[["peak_position", "total_chart_weeks"]].dropna().copy()
        st.scatter_chart(scatter2, x="peak_position", y="total_chart_weeks", width="stretch")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Longest chart runs**")
        _display_df(songs.sort_values(["total_chart_weeks", "peak_position"], ascending=[False, True]).head(top_n), ["title", "artist", "debut_position", "peak_position", "total_chart_weeks", "first_chart_date", "last_chart_date"])
        st.markdown("**Longest Top 10 runs**")
        _display_df(songs.sort_values(["longest_consecutive_top10_run", "top10_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "peak_position", "top10_weeks", "longest_consecutive_top10_run"])
        st.markdown("**Longest #1 runs**")
        _display_df(songs.sort_values(["longest_consecutive_num1_run", "num1_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "num1_weeks", "longest_consecutive_num1_run", "peak_date"])
    with t2:
        st.markdown("**Most re-entries**")
        _display_df(songs.sort_values(["reentry_count", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "reentry_count", "total_chart_weeks", "peak_position"])
        st.markdown("**Longest climb to peak**")
        _display_df(songs.sort_values(["weeks_to_peak", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "debut_position", "peak_position", "weeks_to_peak", "total_chart_weeks"])
        st.markdown("**Longest post-peak survival**")
        _display_df(songs.sort_values(["post_peak_weeks", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "peak_position", "peak_date", "post_peak_weeks", "total_chart_weeks"])
    song_options = songs.sort_values(["artist", "title"])
    labels = {f"{row.title} — {row.artist}": row.song_key for row in song_options.itertuples(index=False)}
    if labels:
        st.markdown("**Song profile explorer**")
        selected = st.selectbox("Choose a song", list(labels.keys()), key="analytics_song_profile")
        key = labels[selected]
        row = songs.loc[songs["song_key"] == key].iloc[0]
        render_kpis([
            ("Debut", _fmt_rank(row["debut_position"])),
            ("Peak", _fmt_rank(row["peak_position"])),
            ("Weeks", int(row["total_chart_weeks"])),
            ("Top 10 weeks", int(row["top10_weeks"])),
            ("#1 weeks", int(row["num1_weeks"])),
            ("Re-entries", int(row["reentry_count"])),
        ])
        history = chart.loc[chart["song_key"] == key, ["chart_date", "position", "last_week_position", "move", "weeks_on_chart", "derived_marker"]].copy().sort_values("chart_date")
        if not history.empty:
            st.line_chart((-history.set_index("chart_date")[["position"]]).rename(columns={"position": "inverted_position"}), width="stretch")
            _display_df(history, ["chart_date", "position", "last_week_position", "move", "weeks_on_chart", "derived_marker"])


def _render_artists(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    artists = pkg["artists"]
    songs = pkg["songs"]
    artist_presence = pkg["artist_presence"]
    if artists.empty:
        st.info("No artist-summary rows available for the selected filters.")
        return
    top_chart_weeks = artists.sort_values(["total_chart_weeks", "artist"], ascending=[False, True]).iloc[0]
    top_top10 = artists.sort_values(["total_top10_weeks", "artist"], ascending=[False, True]).iloc[0]
    top_num1 = artists.sort_values(["total_num1_weeks", "artist"], ascending=[False, True]).iloc[0]
    render_kpis([
        ("Unique artists", int(artists["artist_key"].nunique())),
        ("Avg songs/artist", f"{artists['distinct_songs'].mean():.2f}"),
        ("Max simultaneous entries", int(artists["max_simultaneous_entries"].fillna(0).max())),
        ("Most chart weeks", top_chart_weeks["artist"]),
        ("Most Top 10 weeks", top_top10["artist"]),
        ("Most #1 weeks", top_num1["artist"]),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Top artists by chart weeks**")
        st.bar_chart(artists.sort_values("total_chart_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_chart_weeks"]], width="stretch")
        st.markdown("**Top artists by distinct songs**")
        st.bar_chart(artists.sort_values("distinct_songs", ascending=False).head(min(top_n, 20)).set_index("artist")[["distinct_songs"]], width="stretch")
    with c2:
        st.markdown("**Top artists by Top 10 weeks**")
        st.bar_chart(artists.sort_values("total_top10_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_top10_weeks"]], width="stretch")
        st.markdown("**Top artists by #1 weeks**")
        st.bar_chart(artists.sort_values("total_num1_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_num1_weeks"]], width="stretch")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Most chart weeks**")
        _display_df(artists.sort_values(["total_chart_weeks", "artist"], ascending=[False, True]).head(top_n), ["artist", "total_chart_weeks", "distinct_songs", "total_top10_weeks", "total_num1_weeks"])
        st.markdown("**Most Top 10 hits**")
        _display_df(artists.sort_values(["top10_hits", "top5_hits", "num1_hits"], ascending=[False, False, False]).head(top_n), ["artist", "top10_hits", "top5_hits", "num1_hits"])
        st.markdown("**Best average peak (min 3 songs)**")
        _display_df(artists.loc[artists["distinct_songs"] >= 3].sort_values(["avg_peak", "distinct_songs"], ascending=[True, False]).head(top_n), ["artist", "avg_peak", "median_peak", "distinct_songs", "num1_hits"])
    with t2:
        st.markdown("**Most distinct songs charted**")
        _display_df(artists.sort_values(["distinct_songs", "artist"], ascending=[False, True]).head(top_n), ["artist", "distinct_songs", "total_chart_weeks", "best_peak"])
        st.markdown("**Most #1 hits**")
        _display_df(artists.sort_values(["num1_hits", "total_num1_weeks"], ascending=[False, False]).head(top_n), ["artist", "num1_hits", "total_num1_weeks", "best_peak"])
        st.markdown("**Most simultaneous entries**")
        _display_df(artists.sort_values(["max_simultaneous_entries", "week_of_max_simultaneous_entries"], ascending=[False, False]).head(top_n), ["artist", "max_simultaneous_entries", "week_of_max_simultaneous_entries", "distinct_songs", "total_chart_weeks"])
    labels = {row.artist: row.artist_key for row in artists.sort_values("artist").itertuples(index=False) if pd.notna(row.artist_key)}
    if labels:
        st.markdown("**Artist profile explorer**")
        selected = st.selectbox("Choose an artist", list(labels.keys()), key="analytics_artist_profile")
        key = labels[selected]
        row = artists.loc[artists["artist_key"] == key].iloc[0]
        render_kpis([
            ("Distinct songs", int(row["distinct_songs"])),
            ("Chart weeks", int(row["total_chart_weeks"])),
            ("Top 10 hits", int(row["top10_hits"])),
            ("#1 hits", int(row["num1_hits"])),
            ("Best peak", _fmt_rank(row["best_peak"])),
            ("Max simultaneous", int(row["max_simultaneous_entries"] or 0)),
        ])
        songs_for_artist = songs.merge(artists[["artist_key", "artist"]], on=["artist_key", "artist"], how="left") if "artist_key" in songs.columns else songs
        # safer route: infer from chart rows
        artist_song_keys = pkg["chart"].loc[pkg["chart"]["artist_key"] == key, "song_key"].unique().tolist()
        artist_songs = songs.loc[songs["song_key"].isin(artist_song_keys)].sort_values(["peak_position", "total_chart_weeks", "title"], ascending=[True, False, True])
        _display_df(artist_songs.head(top_n), ["title", "peak_position", "total_chart_weeks", "top10_weeks", "num1_weeks", "reentry_count"])
        presence = artist_presence.loc[artist_presence["artist_key"] == key].sort_values("chart_date")
        if not presence.empty:
            st.line_chart(presence.set_index("chart_date")[["entries_on_chart"]], width="stretch")


def _render_years_eras(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    years = pkg["years"]
    if years.empty:
        st.info("No yearly rows available for the selected filters.")
        return
    render_kpis([
        ("Avg debuts/year", f"{years['debuts'].mean():.2f}"),
        ("Avg turnover/year", f"{years['avg_turnover'].mean():.2f}"),
        ("Avg chart age/year", f"{years['avg_chart_age'].mean():.2f}"),
        ("Avg Top 10 age/year", f"{years['avg_top10_age'].mean():.2f}"),
        ("Avg artists/year", f"{years['unique_artists'].mean():.2f}"),
        ("Avg #1 songs/year", f"{years['num1_songs'].mean():.2f}"),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Debuts by year**")
        st.line_chart(years.set_index("year")[["debuts"]], width="stretch")
        st.markdown("**Average chart age by year**")
        st.line_chart(years.set_index("year")[["avg_chart_age"]], width="stretch")
        st.markdown("**Unique artists by year**")
        st.line_chart(years.set_index("year")[["unique_artists"]], width="stretch")
    with c2:
        st.markdown("**Average Top 10 age by year**")
        st.line_chart(years.set_index("year")[["avg_top10_age"]], width="stretch")
        st.markdown("**Average turnover by year**")
        st.line_chart(years.set_index("year")[["avg_turnover"]], width="stretch")
        st.markdown("**#1 songs by year**")
        st.bar_chart(years.set_index("year")[["num1_songs"]], width="stretch")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Yearly summary**")
        _display_df(years.sort_values("year", ascending=False), ["year", "unique_songs", "unique_artists", "debuts", "reentries", "dropouts", "num1_songs", "avg_chart_age", "avg_top10_age", "avg_abs_move", "avg_turnover"])
        st.markdown("**Most stable years**")
        _display_df(years.sort_values(["avg_abs_move", "year"], ascending=[True, False]).head(top_n), ["year", "avg_abs_move", "avg_turnover", "avg_top10_churn", "avg_chart_age"])
    with t2:
        st.markdown("**Most chaotic years**")
        _display_df(years.sort_values(["avg_abs_move", "year"], ascending=[False, False]).head(top_n), ["year", "avg_abs_move", "avg_turnover", "avg_top10_churn", "avg_chart_age"])
        st.markdown("**Freshest / oldest years**")
        col_a, col_b = st.columns(2)
        with col_a:
            _display_df(years.sort_values(["avg_chart_age", "year"], ascending=[True, False]).head(top_n), ["year", "avg_chart_age", "avg_top10_age", "debuts", "avg_turnover"])
        with col_b:
            _display_df(years.sort_values(["avg_chart_age", "year"], ascending=[False, False]).head(top_n), ["year", "avg_chart_age", "avg_top10_age", "debuts", "avg_turnover"])


def _render_records_outliers(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    songs = pkg["songs"]
    artists = pkg["artists"]
    weekly = pkg["weekly"]
    chart = pkg["chart"]
    if songs.empty or artists.empty or weekly.empty:
        st.info("No records data available for the selected filters.")
        return
    valid_moves = chart.loc[chart["move"].notna()].copy()
    render_kpis([
        ("All-time biggest climb", f"{int(valid_moves['move'].max()):+d}" if not valid_moves.empty else "—"),
        ("All-time biggest fall", f"{int(valid_moves['move'].min()):+d}" if not valid_moves.empty else "—"),
        ("Longest chart run", int(songs['total_chart_weeks'].max())),
        ("Longest Top 10 run", int(songs['longest_consecutive_top10_run'].max())),
        ("Longest #1 run", int(songs['longest_consecutive_num1_run'].max())),
        ("Highest debut", _fmt_rank(songs['debut_position'].min())),
        ("Most re-entries", int(songs['reentry_count'].max())),
        ("Most simultaneous entries", int(artists['max_simultaneous_entries'].fillna(0).max())),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Peak position vs total run length**")
        st.scatter_chart(songs[["peak_position", "total_chart_weeks"]].dropna(), x="peak_position", y="total_chart_weeks", width="stretch")
    with c2:
        st.markdown("**Artist depth scatter**")
        st.scatter_chart(artists[["distinct_songs", "total_chart_weeks"]].dropna(), x="distinct_songs", y="total_chart_weeks", width="stretch")
    st.markdown("**Song records**")
    r1, r2, r3 = st.columns(3)
    with r1:
        _display_df(songs.sort_values(["total_chart_weeks", "peak_position"], ascending=[False, True]).head(top_n), ["title", "artist", "total_chart_weeks", "peak_position", "debut_position"])
        _display_df(songs.sort_values(["top10_weeks", "peak_position"], ascending=[False, True]).head(top_n), ["title", "artist", "top10_weeks", "peak_position", "total_chart_weeks"])
    with r2:
        _display_df(songs.sort_values(["num1_weeks", "longest_consecutive_num1_run"], ascending=[False, False]).head(top_n), ["title", "artist", "num1_weeks", "longest_consecutive_num1_run"])
        _display_df(songs.sort_values(["debut_position", "peak_position"], ascending=[True, True]).head(top_n), ["title", "artist", "debut_position", "peak_position", "total_chart_weeks"])
    with r3:
        _display_df(songs.sort_values(["biggest_climb", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "biggest_climb", "peak_position", "total_chart_weeks"])
        _display_df(songs.sort_values(["reentry_count", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "reentry_count", "total_chart_weeks"])
    st.markdown("**Artist records**")
    a1, a2 = st.columns(2)
    with a1:
        _display_df(artists.sort_values(["total_chart_weeks", "distinct_songs"], ascending=[False, False]).head(top_n), ["artist", "total_chart_weeks", "distinct_songs", "top10_hits", "num1_hits"])
        _display_df(artists.sort_values(["top10_hits", "num1_hits"], ascending=[False, False]).head(top_n), ["artist", "top10_hits", "num1_hits", "distinct_songs"])
    with a2:
        _display_df(artists.sort_values(["distinct_songs", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["artist", "distinct_songs", "total_chart_weeks", "best_peak"])
        _display_df(artists.sort_values(["max_simultaneous_entries", "week_of_max_simultaneous_entries"], ascending=[False, False]).head(top_n), ["artist", "max_simultaneous_entries", "week_of_max_simultaneous_entries"])
    st.markdown("**Weekly records**")
    w1, w2 = st.columns(2)
    with w1:
        _display_df(weekly.sort_values(["debuts", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "debuts", "reentries", "dropouts", "turnover_total"])
        _display_df(weekly.sort_values(["top10_churn", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "top10_churn", "avg_abs_move", "turnover_total"])
        _display_df(weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_chart_age", "avg_top10_age"])
    with w2:
        _display_df(weekly.sort_values(["reentries", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "reentries", "debuts", "turnover_total"])
        _display_df(weekly.sort_values(["avg_abs_move", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_abs_move", "debuts", "reentries", "dropouts"])
        _display_df(weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[True, False]).head(top_n), ["chart_date", "avg_chart_age", "avg_top10_age"])


def render_analytics_tab() -> None:
    st.subheader("Analytics")
    base = load_analytics_base()
    if base.empty:
        st.info("No analytics data is available in the database yet.")
        return
    min_date = base["chart_date"].min().date()
    max_date = base["chart_date"].max().date()
    controls = st.columns([1.4, 1.4, 1.0, 1.0])
    start_date = controls[0].date_input("Start date", value=min_date, min_value=min_date, max_value=max_date, key="analytics_start")
    end_date = controls[1].date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="analytics_end")
    include_reentries = controls[2].checkbox("Include re-entries", value=True, key="analytics_include_reentries")
    min_weeks = int(controls[3].number_input("Min weeks on chart", min_value=1, max_value=500, value=1, step=1, key="analytics_min_weeks"))
    section_cols = st.columns([2, 1])
    section = section_cols[0].selectbox("Analytics section", ANALYTICS_SECTIONS, key="analytics_section")
    top_n = int(section_cols[1].slider("Top N rows", 5, 100, 25, 5, key="analytics_top_n"))
    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return
    pkg = _apply_analytics_filters(build_analytics_package(), start_date, end_date, include_reentries, min_weeks)
    if section == "Overview":
        _render_overview(pkg, top_n)
    elif section == "Movement":
        _render_movement(pkg, top_n)
    elif section == "Longevity":
        _render_longevity(pkg, top_n)
    elif section == "Artists":
        _render_artists(pkg, top_n)
    elif section == "Years & Eras":
        _render_years_eras(pkg, top_n)
    elif section == "Records & Outliers":
        _render_records_outliers(pkg, top_n)


def main() -> None:
    st.title("Torrey's Corner Top 40 Search Engine")
    st.caption("SQLite + FTS5 chart browser for the Torrey's Corner Top 40 database")

    overview = load_overview()
    derived = marker_counts()
    render_kpis([
        ("Chart weeks", overview["weeks"]),
        ("Entries", overview["entries"]),
        ("Canonical songs", overview["unique_songs"]),
        ("Lead artists", overview["unique_lead_artists"]),
        ("Full-credit artists", overview["unique_full_artists"]),
    ])
    st.caption(f"Coverage: {overview['min_date']} through {overview['max_date']}")
    render_kpis([
        ("Debuts", derived["debuts"]),
        ("Top debuts", derived["top_debuts"]),
        ("Re-entries", derived["reentries"]),
    ])

    tab_search, tab_week, tab_song, tab_artist, tab_special, tab_analytics = st.tabs(
        ["Full-text search", "Week browser", "Canonical song history", "Artist history", "Quick tables", "Analytics"]
    )

    with tab_search:
        st.subheader("Full-text search")
        query = st.text_input(
            "Search songs, artists, slugs, or mixed text",
            placeholder='Examples: "slow jamz", janet jackson, prof "big dog"',
        )
        filter_cols = st.columns(2)
        marker_filter = filter_cols[0].selectbox("Marker filter", ["All", "DEBUT", "TOP DEBUT", "RE-ENTRY"])
        limit = filter_cols[1].slider("Result limit", 10, 200, 50, 10, key="fts_limit")
        if query.strip():
            try:
                results = run_search(query.strip(), limit, marker_filter)
                st.write(f"{len(results):,} result(s)")
                _display_df(results)
            except Exception as exc:
                st.error(f"Search query could not be run: {exc}")
        else:
            st.info("Enter a search query to browse the database.")

    with tab_week:
        st.subheader("Browse a chart week")
        dates = load_chart_dates()
        if dates:
            valid_dates = sorted(dates)
            min_date = dt.date.fromisoformat(valid_dates[0])
            max_date = dt.date.fromisoformat(valid_dates[-1])
            selected_date_obj = st.date_input(
                "Chart date",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                format="YYYY-MM-DD",
            )
            selected_date, snapped = nearest_chart_date(selected_date_obj.isoformat(), valid_dates)
            if selected_date:
                if snapped:
                    st.info(f"No chart exists for {selected_date_obj.isoformat()}. Showing nearest prior chart week: {selected_date}.")
                df, meta = load_chart(selected_date)
                if meta:
                    k1, k2, k3 = st.columns(3)
                    k1.metric("Rows stored", meta["row_count"])
                    k2.metric("Chart ID", meta["chart_id"] or "—")
                    k3.metric("Source ZIP", meta["source_zip"] or "—")
                    st.caption(f"Source file: {meta['source_file']}")
                    if meta.get("notes"):
                        st.warning(meta["notes"])
                _display_df(df)
        else:
            st.info("No chart weeks are available in the database.")

    with tab_song:
        st.subheader("Canonical song history")
        song_term = st.text_input("Find song or artist", placeholder="Type part of a title or artist", key="song_term")
        if song_term.strip():
            candidates = canonical_song_matches(song_term)
            if candidates.empty:
                st.info("No canonical songs matched that search.")
            else:
                display_options = {
                    f"{row.canonical_title} — {row.canonical_artist} | peak #{int(row.peak)} | {int(row.chart_weeks)} weeks | {row.first_date} to {row.last_date}": int(row.canonical_song_id)
                    for row in candidates.itertuples(index=False)
                }
                selected_label = st.selectbox("Choose a canonical song", list(display_options.keys()))
                selected_song_id = display_options[selected_label]
                history, stats, aliases = canonical_song_history(selected_song_id)
                if stats:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Peak", f"#{int(stats['peak'])}")
                    c2.metric("Chart weeks", int(stats["chart_weeks"]))
                    c3.metric("First week", stats["first_date"])
                    c4.metric("Last week", stats["last_date"])
                    st.caption(
                        f"Canonical full credit: {stats['artist']} | "
                        f"Lead: {stats['lead_artist']} | "
                        f"Featured: {stats['featured_artist'] or '—'} | "
                        f"Alias variants: {int(stats['alias_count'])}"
                    )
                    chart_df = history.set_index("chart_date")["position"].sort_index()
                    st.line_chart((-chart_df).rename("inverted_position"))
                    st.caption("Line chart uses inverted positions so higher placements plot higher.")
                    st.markdown("**Week-by-week history**")
                    _display_df(history)
                    st.markdown("**Alias variants in this canonical song**")
                    _display_df(aliases)
        else:
            st.info("Type part of a title or artist to load a canonical song history.")

    with tab_artist:
        st.subheader("Artist history")
        artist_cols = st.columns([2, 1])
        artist_term = artist_cols[0].text_input("Find artist", placeholder="Type part of an artist name", key="artist_term")
        role_mode = artist_cols[1].selectbox("Browse by", ["Full credit", "Lead artist", "Featured artist"])
        if artist_term.strip():
            candidates = artist_matches(artist_term, role_mode)
            if candidates.empty:
                st.info(f"No {artist_role_config(role_mode)['label']} matches found.")
            else:
                display_options = {
                    f"{row.display_artist} | peak #{int(row.peak)} | {int(row.chart_weeks)} weeks | {row.first_date} to {row.last_date}": row.normalized_artist
                    for row in candidates.itertuples(index=False)
                }
                selected_label = st.selectbox("Choose an artist", list(display_options.keys()), key="artist_pick")
                selected_artist = display_options[selected_label]
                history, stats, songs = artist_history(selected_artist, role_mode)
                if stats:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Peak", f"#{int(stats['peak'])}")
                    c2.metric("Chart weeks", int(stats["chart_weeks"]))
                    c3.metric("Distinct songs", int(stats["distinct_songs"]))
                    c4.metric("Span", f"{stats['first_date']} to {stats['last_date']}")
                    st.caption(f"Mode: {artist_role_config(role_mode)['label']}")
                    st.markdown("**Song summary**")
                    _display_df(songs)
                    st.markdown("**Full week-by-week history**")
                    _display_df(history)
        else:
            st.info("Type part of an artist name to load an artist history.")

    with tab_special:
        st.subheader("Quick tables")
        table_kind = st.selectbox(
            "View",
            [
                "#1 hits",
                "Top 10 hits",
                "Top debuts",
                "Top 5 debuts",
                "Debut weeks",
                "Re-entries",
                "Biggest climbers",
                "Artists with most Top 10 weeks",
                "Artists with most appearances on a single chart",
            ],
        )
        limit = st.slider("Rows", 10, 500, 100, 10, key="special_limit")
        table = load_special_entries(table_kind, limit)
        _display_df(table)

    with tab_analytics:
        render_analytics_tab()


if __name__ == "__main__":
    main()
