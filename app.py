from __future__ import annotations

import datetime as dt
import sqlite3
import re
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st

def _fold_quotes(text: str) -> str:
    text = (text or "")
    return (
        text.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def normalize_search_text(text: object) -> str:
    text = _fold_quotes("" if text is None else str(text))
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


ARTIST_KEY_ALIASES = {
    "dino of h-town": "dino conner",
    "dino of conner": "dino conner",
    "jake": "jake&papa",
    "papa": "jake&papa",
    "jake & papa": "jake&papa",
    "jake and papa": "jake&papa",
    "jake+papa": "jake&papa",
    "jake/papa": "jake&papa",
    "jake; papa": "jake&papa",
}

PREFERRED_ARTIST_DISPLAY = {
    "dino conner": "Dino Conner",
    "jake&papa": "Jake&Papa",
}


@st.cache_data(show_spinner=False)
def load_artist_key_override_map() -> dict[str, str]:
    if not Path(DB_PATH).exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "artist_key_override" not in tables:
            return {}
        rows = conn.execute(
            "SELECT source_artist_key, target_artist_key FROM artist_key_override WHERE COALESCE(source_artist_key, '') <> '' AND COALESCE(target_artist_key, '') <> ''"
        ).fetchall()
        return {normalize_search_text(src): normalize_search_text(dst) for src, dst in rows if src and dst}
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_artist_display_override_map() -> dict[str, str]:
    if not Path(DB_PATH).exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "artist_key_override" not in tables:
            return {}
        rows = conn.execute(
            "SELECT target_artist_key, preferred_display FROM artist_key_override WHERE COALESCE(target_artist_key, '') <> '' AND COALESCE(preferred_display, '') <> ''"
        ).fetchall()
        out: dict[str, str] = {}
        for target, display in rows:
            key = normalize_search_text(target)
            if key and display:
                out[key] = str(display)
        return out
    finally:
        conn.close()


def _follow_artist_key_override_chain(key: str) -> str:
    overrides = load_artist_key_override_map()
    seen: set[str] = set()
    cur = normalize_search_text(key)
    while cur in overrides and cur not in seen:
        seen.add(cur)
        nxt = normalize_search_text(overrides[cur])
        if not nxt or nxt == cur:
            break
        cur = nxt
    return cur


def resolve_artist_key_alias(artist_key: object) -> object:
    if artist_key is None or (isinstance(artist_key, float) and pd.isna(artist_key)):
        return artist_key
    key = normalize_search_text(artist_key)
    key = ARTIST_KEY_ALIASES.get(key, key)
    key = _follow_artist_key_override_chain(key)
    return key


def preferred_artist_display(artist_key: object, fallback: object = "") -> str:
    key = resolve_artist_key_alias(artist_key)
    if key is None or (isinstance(key, float) and pd.isna(key)):
        return "" if fallback is None else str(fallback)
    key = normalize_search_text(key)
    display_overrides = load_artist_display_override_map()
    preferred = display_overrides.get(key) or PREFERRED_ARTIST_DISPLAY.get(key)
    if preferred:
        return preferred
    return "" if fallback is None else str(fallback)


BASE_DIR = Path(__file__).resolve().parent
DB_CANDIDATES = [
    BASE_DIR / 'db' / 'tctop40.sqlite',
    BASE_DIR / 'db' / 'torreys_corner_top40.sqlite',
    BASE_DIR / 'tctop40.sqlite',
    BASE_DIR / 'torreys_corner_top40.sqlite',
]
DB_PATH = next((path for path in DB_CANDIDATES if path.exists()), DB_CANDIDATES[0])

st.set_page_config(page_title="Torrey's Corner Top 40 Search Engine", layout="wide")
st.caption(f"Using database: {DB_PATH}")

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

    # Be tolerant of older deployed DBs that may not have every chart_week metadata column.
    chart_week_cols = {row[1] for row in conn.execute("PRAGMA table_info(chart_week)").fetchall()}
    select_cols = ["chart_date"]
    for col in ["chart_id", "source_file", "source_zip", "row_count", "notes"]:
        if col in chart_week_cols:
            select_cols.append(col)

    meta_row = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM chart_week WHERE chart_date = ?",
        (chart_date,),
    ).fetchone()
    if meta_row is None:
        return pd.DataFrame(), None

    meta = dict(meta_row)
    meta.setdefault("chart_id", meta.get("chart_week_id", ""))
    meta.setdefault("source_file", "")
    meta.setdefault("source_zip", "")
    meta.setdefault("row_count", None)
    meta.setdefault("notes", "")

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
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["normalized_artist", "display_artist", "chart_weeks", "first_date", "last_date", "peak"])

    credits = build_artist_credit_rows(chart)
    if role_mode == "Lead artist":
        credits = credits.loc[credits["artist_role_mode"] == "Lead"].copy()
    elif role_mode == "Featured artist":
        credits = credits.loc[credits["artist_role_mode"] == "Featured"].copy()

    if credits.empty:
        return pd.DataFrame(columns=["normalized_artist", "display_artist", "chart_weeks", "first_date", "last_date", "peak"])

    term_norm = normalize_search_text(term)
    if term_norm:
        credits = credits.loc[
            credits["artist_key"].fillna("").astype(str).str.contains(re.escape(term_norm), regex=True)
            | credits["artist"].fillna("").astype(str).str.lower().str.contains(re.escape(term_norm), regex=True)
        ].copy()

    if credits.empty:
        return pd.DataFrame(columns=["normalized_artist", "display_artist", "chart_weeks", "first_date", "last_date", "peak"])

    out = (
        credits.groupby(["artist_key"], dropna=True)
        .agg(
            chart_weeks=("entry_id", "count"),
            first_date=("chart_date", "min"),
            last_date=("chart_date", "max"),
            peak=("position", "min"),
            display_artist=("artist", lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else ""),
        )
        .reset_index()
        .rename(columns={"artist_key": "normalized_artist"})
    )
    out["display_artist"] = out.apply(
        lambda r: preferred_artist_display(r["normalized_artist"], r["display_artist"]),
        axis=1,
    )
    return out.sort_values(["chart_weeks", "last_date", "display_artist"], ascending=[False, False, True]).head(limit)



@st.cache_data(show_spinner=False)
def artist_history(normalized_artist: str, role_mode: str) -> tuple[pd.DataFrame, dict[str, object] | None, pd.DataFrame]:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(), None, pd.DataFrame()

    credits = build_artist_credit_rows(chart)
    if role_mode == "Lead artist":
        credits = credits.loc[credits["artist_role_mode"] == "Lead"].copy()
    elif role_mode == "Featured artist":
        credits = credits.loc[credits["artist_role_mode"] == "Featured"].copy()

    if credits.empty:
        return pd.DataFrame(), None, pd.DataFrame()

    credits = credits.loc[credits["artist_key"] == normalized_artist].copy()
    if credits.empty:
        return pd.DataFrame(), None, pd.DataFrame()

    artist_name = credits["artist"].dropna().astype(str)
    fallback_artist = artist_name.mode().iloc[0] if not artist_name.empty else normalized_artist
    display_artist = preferred_artist_display(normalized_artist, fallback_artist)

    stats = {
        "artist": display_artist,
        "chart_weeks": int(len(credits)),
        "distinct_songs": int(credits["song_key"].nunique()),
        "peak": int(credits["position"].min()),
        "first_date": pd.to_datetime(credits["chart_date"]).min().strftime("%Y-%m-%d"),
        "last_date": pd.to_datetime(credits["chart_date"]).max().strftime("%Y-%m-%d"),
    }

    history = (
        credits.sort_values(["chart_date", "position", "entry_id"])
        .drop_duplicates(subset=["entry_id"])
        [["chart_date", "position", "last_week_position", "weeks_on_chart", "title", "artist", "lead_artist", "featured_artist", "derived_marker"]]
        .rename(columns={"title": "song"})
        .copy()
    )

    songs = (
        credits.groupby(["song_key", "title"], dropna=True)
        .agg(
            chart_weeks=("entry_id", "count"),
            first_date=("chart_date", "min"),
            last_date=("chart_date", "max"),
            peak=("position", "min"),
        )
        .reset_index()
        .rename(columns={"title": "song"})
        .sort_values(["peak", "chart_weeks", "last_date", "song"], ascending=[True, False, False, True])
        [["song", "chart_weeks", "first_date", "last_date", "peak"]]
    )

    return history, stats, songs



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
            e.entry_id,
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
    df["artist_key"] = df["artist_key"].map(resolve_artist_key_alias)
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




def _split_credit_people(norm_value: object, display_value: object) -> list[tuple[str, str]]:
    norm_text = "" if norm_value is None else str(norm_value).strip()
    display_text = "" if display_value is None else str(display_value).strip()
    if not norm_text and not display_text:
        return []

    norm_parts = [p.strip() for p in re.split(r"\s*;\s*", norm_text) if p.strip()] if norm_text else []
    display_parts = [p.strip() for p in re.split(r"\s*;\s*", display_text) if p.strip()] if display_text else []

    if not norm_parts and display_parts:
        norm_parts = [p.lower() for p in display_parts]
    if not display_parts and norm_parts:
        display_parts = norm_parts[:]

    n = max(len(norm_parts), len(display_parts))
    if len(norm_parts) < n:
        norm_parts.extend([""] * (n - len(norm_parts)))
    if len(display_parts) < n:
        display_parts.extend([""] * (n - len(display_parts)))

    pairs: list[tuple[str, str]] = []
    for norm_part, display_part in zip(norm_parts, display_parts):
        norm_part = (norm_part or "").strip().lower()
        display_part = (display_part or "").strip()
        if norm_part and display_part:
            pairs.append((norm_part, display_part))
        elif norm_part:
            pairs.append((norm_part, norm_part))
        elif display_part:
            pairs.append((display_part.lower(), display_part))
    return pairs


@st.cache_data(show_spinner=False)
def build_artist_credit_rows(df_chart: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    for row in df_chart.to_dict("records"):
        lead_people = _split_credit_people(row.get("normalized_lead_artist"), row.get("lead_artist"))
        featured_people = _split_credit_people(row.get("normalized_featured_artist"), row.get("featured_artist"))

        for artist_key, artist in lead_people:
            credit_row = dict(row)
            credit_row["artist_key"] = artist_key
            credit_row["artist"] = artist
            credit_row["artist_role_mode"] = "Lead"
            rows.append(credit_row)

        for artist_key, artist in featured_people:
            credit_row = dict(row)
            credit_row["artist_key"] = artist_key
            credit_row["artist"] = artist
            credit_row["artist_role_mode"] = "Featured"
            rows.append(credit_row)

    if not rows:
        return pd.DataFrame()

    credits = pd.DataFrame(rows)
    credits["artist_key"] = credits["artist_key"].map(resolve_artist_key_alias)
    credits["artist_key"] = credits["artist_key"].replace("", pd.NA)
    credits["artist"] = credits["artist"].replace("", pd.NA)
    credits = credits.loc[credits["artist_key"].notna() & credits["artist"].notna()].copy()

    if credits.empty:
        return credits

    credits = credits.drop_duplicates(
        subset=["entry_id", "song_key", "chart_date", "artist_key", "artist_role_mode"]
    ).copy()
    return credits


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
def build_artist_weekly_presence(df_artist_credits: pd.DataFrame) -> pd.DataFrame:
    if df_artist_credits.empty:
        return pd.DataFrame()

    grouped = df_artist_credits.groupby(["chart_date", "artist_key"], dropna=True)
    rows = []
    for (chart_date, artist_key), g in grouped:
        artist_vals = g["artist"].dropna().astype(str)
        fallback_artist = artist_vals.mode().iloc[0] if not artist_vals.empty else str(artist_key)
        artist_display = preferred_artist_display(artist_key, fallback_artist)
        rows.append({
            "chart_date": chart_date,
            "artist_key": artist_key,
            "artist": artist_display,
            "entries_on_chart": int(g["song_key"].nunique()),
            "entries_top20": int(g.loc[g["position"] <= 20, "song_key"].nunique()),
            "entries_top10": int(g.loc[g["position"] <= 10, "song_key"].nunique()),
            "entries_top5": int(g.loc[g["position"] <= 5, "song_key"].nunique()),
            "entries_num1": int(g.loc[g["position"] == 1, "song_key"].nunique()),
        })
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def build_artist_summary(df_artist_credits: pd.DataFrame, df_song: pd.DataFrame, df_artist_presence: pd.DataFrame) -> pd.DataFrame:
    if df_artist_credits.empty:
        return pd.DataFrame()

    artist_name_map = (
        df_artist_credits.groupby("artist_key", dropna=True)["artist"]
        .agg(lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else "")
        .reset_index()
    )
    artist_name_map["artist"] = artist_name_map.apply(
        lambda r: preferred_artist_display(r["artist_key"], r["artist"]),
        axis=1,
    )

    song_level = (
        df_artist_credits.groupby(["artist_key", "song_key"], dropna=True)
        .agg(
            peak_position=("position", "min"),
            first_chart_date=("chart_date", "min"),
            last_chart_date=("chart_date", "max"),
        )
        .reset_index()
    )

    song_agg = song_level.groupby(["artist_key"], dropna=True).agg(
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

    week_agg = df_artist_credits.groupby(["artist_key"], dropna=True).agg(
        total_chart_entries=("song_key", "nunique"),
        total_chart_weeks=("song_key", "size"),
        total_top20_weeks=("top20_flag", "sum"),
        total_top10_weeks=("top10_flag", "sum"),
        total_top5_weeks=("top5_flag", "sum"),
        total_num1_weeks=("num1_flag", "sum"),
        lead_chart_weeks=("artist_role_mode", lambda s: int((s == "Lead").sum())),
        featured_chart_weeks=("artist_role_mode", lambda s: int((s == "Featured").sum())),
    ).reset_index()

    role_song_agg = (
        df_artist_credits.groupby(["artist_key", "artist_role_mode"], dropna=True)["song_key"]
        .nunique()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"Lead": "lead_distinct_songs", "Featured": "featured_distinct_songs"})
    )

    max_presence = df_artist_presence.sort_values(["entries_on_chart", "chart_date"], ascending=[False, True]).drop_duplicates("artist_key")
    max_presence = max_presence[["artist_key", "entries_on_chart", "chart_date"]].rename(columns={
        "entries_on_chart": "max_simultaneous_entries",
        "chart_date": "week_of_max_simultaneous_entries",
    })

    out = week_agg.merge(song_agg, on=["artist_key"], how="outer")
    out = out.merge(role_song_agg, on=["artist_key"], how="left")
    out = out.merge(max_presence, on="artist_key", how="left")
    out = out.merge(artist_name_map, on="artist_key", how="left")
    out["artist"] = out.apply(
        lambda r: preferred_artist_display(r["artist_key"], r.get("artist", "")),
        axis=1,
    )
    out["lead_distinct_songs"] = out["lead_distinct_songs"].fillna(0).astype(int)
    out["featured_distinct_songs"] = out["featured_distinct_songs"].fillna(0).astype(int)
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
    artist_credits = build_artist_credit_rows(chart)
    artist_presence = build_artist_weekly_presence(artist_credits)
    artists = build_artist_summary(artist_credits, songs, artist_presence)
    years = build_yearly_summary(chart, weekly, songs)
    return {
        "chart": chart,
        "weekly": weekly,
        "songs": songs,
        "artist_credits": artist_credits,
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
    artist_credits = build_artist_credit_rows(chart)
    if not songs.empty:
        valid_song_keys = set(songs["song_key"].tolist())
        artist_credits = artist_credits.loc[artist_credits["song_key"].isin(valid_song_keys)].copy()
    artist_presence = build_artist_weekly_presence(artist_credits)
    artists = build_artist_summary(artist_credits, songs, artist_presence) if not artist_credits.empty else pd.DataFrame()
    years = build_yearly_summary(chart, weekly, songs) if not chart.empty else pd.DataFrame()
    return {
        "chart": chart,
        "weekly": weekly,
        "songs": songs,
        "artist_credits": artist_credits,
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
            st.markdown("**Song week-by-week history**")
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
        ("Avg featured weeks/artist", f"{artists['featured_chart_weeks'].mean():.2f}"),
        ("Max simultaneous entries", int(artists["max_simultaneous_entries"].fillna(0).max())),
        ("Most chart weeks", top_chart_weeks["artist"]),
        ("Most Top 10 weeks", top_top10["artist"]),
        ("Most #1 weeks", top_num1["artist"]),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Top artists by chart weeks**")
        st.bar_chart(artists.sort_values("total_chart_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_chart_weeks"]], width="stretch")
        st.markdown("**Top artists by distinct songs (lead + featured)**")
        st.bar_chart(artists.sort_values("distinct_songs", ascending=False).head(min(top_n, 20)).set_index("artist")[["distinct_songs"]], width="stretch")
    with c2:
        st.markdown("**Top artists by Top 10 weeks**")
        st.bar_chart(artists.sort_values("total_top10_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_top10_weeks"]], width="stretch")
        st.markdown("**Top artists by #1 weeks**")
        st.bar_chart(artists.sort_values("total_num1_weeks", ascending=False).head(min(top_n, 20)).set_index("artist")[["total_num1_weeks"]], width="stretch")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Most chart weeks**")
        _display_df(artists.sort_values(["total_chart_weeks", "artist"], ascending=[False, True]).head(top_n), ["artist", "total_chart_weeks", "distinct_songs", "lead_chart_weeks", "featured_chart_weeks", "total_top10_weeks", "total_num1_weeks"])
        st.markdown("**Most Top 10 hits**")
        _display_df(artists.sort_values(["top10_hits", "top5_hits", "num1_hits"], ascending=[False, False, False]).head(top_n), ["artist", "top10_hits", "top5_hits", "num1_hits"])
        st.markdown("**Best average peak (min 3 songs)**")
        _display_df(artists.loc[artists["distinct_songs"] >= 3].sort_values(["avg_peak", "distinct_songs"], ascending=[True, False]).head(top_n), ["artist", "avg_peak", "median_peak", "distinct_songs", "num1_hits"])
    with t2:
        st.markdown("**Most distinct songs charted**")
        _display_df(artists.sort_values(["distinct_songs", "artist"], ascending=[False, True]).head(top_n), ["artist", "distinct_songs", "lead_distinct_songs", "featured_distinct_songs", "total_chart_weeks", "best_peak"])
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
            ("Featured weeks", int(row["featured_chart_weeks"])),
            ("Top 10 hits", int(row["top10_hits"])),
            ("#1 hits", int(row["num1_hits"])),
            ("Best peak", _fmt_rank(row["best_peak"])),
            ("Max simultaneous", int(row["max_simultaneous_entries"] or 0)),
        ])
        artist_song_keys = pkg["artist_credits"].loc[pkg["artist_credits"]["artist_key"] == key, "song_key"].unique().tolist()
        artist_songs = songs.loc[songs["song_key"].isin(artist_song_keys)].sort_values(["peak_position", "total_chart_weeks", "title"], ascending=[True, False, True])
        st.markdown("**Artist song summary**")
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
            st.markdown("**Freshest years**")
            _display_df(years.sort_values(["avg_chart_age", "year"], ascending=[True, False]).head(top_n), ["year", "avg_chart_age", "avg_top10_age", "debuts", "avg_turnover"])
        with col_b:
            st.markdown("**Oldest years**")
            _display_df(years.sort_values(["avg_chart_age", "year"], ascending=[False, False]).head(top_n), ["year", "avg_chart_age", "avg_top10_age", "debuts", "avg_turnover"])


def _render_records_outliers(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    songs = pkg["songs"]
    artists = pkg["artists"]
    weekly = pkg["weekly"]
    chart = pkg["chart"]
    if songs.empty or artists.empty or weekly.empty:
        st.info("No records data available for the selected filters.")
        return

    def _analytics_table(title: str, df: pd.DataFrame, columns: list[str] | None = None) -> None:
        st.markdown(f"**{title}**")
        _display_df(df, columns)

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

    st.markdown("### Song records")
    r1, r2, r3 = st.columns(3)
    with r1:
        _analytics_table("Longest chart runs", songs.sort_values(["total_chart_weeks", "peak_position"], ascending=[False, True]).head(top_n), ["title", "artist", "total_chart_weeks", "peak_position", "debut_position"])
        _analytics_table("Most Top 10 weeks", songs.sort_values(["top10_weeks", "peak_position"], ascending=[False, True]).head(top_n), ["title", "artist", "top10_weeks", "peak_position", "total_chart_weeks"])
    with r2:
        _analytics_table("Most #1 weeks", songs.sort_values(["num1_weeks", "longest_consecutive_num1_run"], ascending=[False, False]).head(top_n), ["title", "artist", "num1_weeks", "longest_consecutive_num1_run"])
        _analytics_table("Highest debuts", songs.sort_values(["debut_position", "peak_position"], ascending=[True, True]).head(top_n), ["title", "artist", "debut_position", "peak_position", "total_chart_weeks"])
    with r3:
        _analytics_table("Biggest climbers", songs.sort_values(["biggest_climb", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "biggest_climb", "peak_position", "total_chart_weeks"])
        _analytics_table("Most re-entries", songs.sort_values(["reentry_count", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["title", "artist", "reentry_count", "total_chart_weeks"])

    st.markdown("### Artist records")
    a1, a2 = st.columns(2)
    with a1:
        _analytics_table("Most artist chart weeks", artists.sort_values(["total_chart_weeks", "distinct_songs"], ascending=[False, False]).head(top_n), ["artist", "total_chart_weeks", "distinct_songs", "top10_hits", "num1_hits"])
        _analytics_table("Artists with most Top 10 hits", artists.sort_values(["top10_hits", "num1_hits"], ascending=[False, False]).head(top_n), ["artist", "top10_hits", "num1_hits", "distinct_songs"])
    with a2:
        _analytics_table("Artists with most distinct songs", artists.sort_values(["distinct_songs", "total_chart_weeks"], ascending=[False, False]).head(top_n), ["artist", "distinct_songs", "total_chart_weeks", "best_peak"])
        _analytics_table("Most simultaneous entries", artists.sort_values(["max_simultaneous_entries", "week_of_max_simultaneous_entries"], ascending=[False, False]).head(top_n), ["artist", "max_simultaneous_entries", "week_of_max_simultaneous_entries"])

    st.markdown("### Weekly records")
    w1, w2 = st.columns(2)
    with w1:
        _analytics_table("Most debuts in one week", weekly.sort_values(["debuts", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "debuts", "reentries", "dropouts", "turnover_total"])
        _analytics_table("Highest Top 10 churn", weekly.sort_values(["top10_churn", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "top10_churn", "avg_abs_move", "turnover_total"])
        _analytics_table("Oldest chart weeks", weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_chart_age", "avg_top10_age"])
    with w2:
        _analytics_table("Most re-entries in one week", weekly.sort_values(["reentries", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "reentries", "debuts", "turnover_total"])
        _analytics_table("Most movement-heavy weeks", weekly.sort_values(["avg_abs_move", "chart_date"], ascending=[False, False]).head(top_n), ["chart_date", "avg_abs_move", "debuts", "reentries", "dropouts"])
        _analytics_table("Freshest chart weeks", weekly.sort_values(["avg_chart_age", "chart_date"], ascending=[True, False]).head(top_n), ["chart_date", "avg_chart_age", "avg_top10_age"])



def _admin_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


@st.cache_data(show_spinner=False)
def _admin_table_columns(table_name: str) -> list[str]:
    conn = get_connection()
    if not _admin_table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _reset_app_caches() -> None:
    st.cache_data.clear()
    try:
        get_connection.clear()
    except Exception:
        pass


@st.cache_data(show_spinner=False)
def admin_song_options() -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "canonical_song"):
        return pd.DataFrame(columns=["canonical_song_id", "canonical_title", "canonical_artist", "chart_weeks"])
    return pd.read_sql_query(
        """
        SELECT
            canonical_song_id,
            canonical_title,
            COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist,
            entry_count AS chart_weeks,
            first_chart_date,
            last_chart_date
        FROM canonical_song
        ORDER BY LOWER(canonical_title), LOWER(COALESCE(canonical_full_artist, canonical_artist)), canonical_song_id
        """,
        conn,
    )


@st.cache_data(show_spinner=False)
def admin_song_aliases(canonical_song_id: int) -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "song_alias"):
        return pd.DataFrame(columns=["song", "artist", "chart_weeks", "week_count", "first_date", "last_date"])
    return pd.read_sql_query(
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


def _insert_song_alias_row(cur: sqlite3.Cursor, canonical_song_id: int, old_title: str, old_artist: str) -> None:
    cols = _admin_table_columns("song_alias")
    if not cols:
        return

    payload: dict[str, object] = {}
    if "canonical_song_id" in cols:
        payload["canonical_song_id"] = canonical_song_id
    if "alias_song_title" in cols:
        payload["alias_song_title"] = old_title
    if "alias_artist" in cols:
        payload["alias_artist"] = old_artist
    if "week_count" in cols:
        payload["week_count"] = 0
    if "entry_count" in cols:
        payload["entry_count"] = 0
    if "first_chart_date" in cols:
        payload["first_chart_date"] = None
    if "last_chart_date" in cols:
        payload["last_chart_date"] = None
    if "alias_display_key" in cols:
        payload["alias_display_key"] = normalize_search_text(f"{old_title}||{old_artist}")

    if not payload or "canonical_song_id" not in payload or "alias_song_title" not in payload:
        return

    where_bits = []
    where_vals: list[object] = []
    for key in payload:
        if payload[key] is None:
            where_bits.append(f"{key} IS NULL")
        else:
            where_bits.append(f"{key} = ?")
            where_vals.append(payload[key])
    exists_sql = f"SELECT 1 FROM song_alias WHERE {' AND '.join(where_bits)} LIMIT 1"
    exists = cur.execute(exists_sql, tuple(where_vals)).fetchone()
    if exists:
        return

    insert_cols = list(payload.keys())
    insert_vals = [payload[c] for c in insert_cols]
    placeholders = ", ".join(["?"] * len(insert_cols))
    cur.execute(
        f"INSERT INTO song_alias ({', '.join(insert_cols)}) VALUES ({placeholders})",
        insert_vals,
    )


def admin_rename_canonical_song(canonical_song_id: int, new_title: str) -> tuple[bool, str]:
    new_title = (new_title or "").strip()
    if not new_title:
        return False, "New canonical song title cannot be blank."

    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"

    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        row = cur.execute(
            """
            SELECT
                canonical_song_id,
                canonical_title,
                COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (canonical_song_id,),
        ).fetchone()
        if row is None:
            return False, "Selected canonical song was not found."

        old_title = (row["canonical_title"] or "").strip()
        old_artist = (row["canonical_artist"] or "").strip()
        if normalize_search_text(old_title) == normalize_search_text(new_title):
            return False, "The new title matches the current canonical title."

        collision = cur.execute(
            """
            SELECT canonical_song_id
            FROM canonical_song
            WHERE LOWER(TRIM(canonical_title)) = LOWER(TRIM(?))
              AND canonical_song_id <> ?
            LIMIT 1
            """,
            (new_title, canonical_song_id),
        ).fetchone()
        if collision is not None:
            return False, "Another canonical song already uses that exact title."

        cur.execute("BEGIN")
        cur.execute(
            "UPDATE canonical_song SET canonical_title = ? WHERE canonical_song_id = ?",
            (new_title, canonical_song_id),
        )
        try:
            _insert_song_alias_row(cur, canonical_song_id, old_title, old_artist)
        except Exception:
            pass
        conn.commit()
        _reset_app_caches()
        return True, f'Renamed "{old_title}" to "{new_title}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Song rename failed: {exc}"
    finally:
        conn.close()


def _refresh_canonical_song_rollup(cur: sqlite3.Cursor, canonical_song_id: int) -> None:
    row = cur.execute(
        """
        SELECT
            COUNT(*) AS entry_count,
            MIN(cw.chart_date) AS first_chart_date,
            MAX(cw.chart_date) AS last_chart_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE e.canonical_song_id = ?
        """,
        (canonical_song_id,),
    ).fetchone()
    entry_count = int(row[0] or 0) if row is not None else 0
    first_chart_date = row[1] if row is not None else None
    last_chart_date = row[2] if row is not None else None
    alias_count_row = cur.execute(
        "SELECT COUNT(*) FROM song_alias WHERE canonical_song_id = ?",
        (canonical_song_id,),
    ).fetchone()
    alias_count = int(alias_count_row[0] or 0) if alias_count_row is not None else 0
    cur.execute(
        """
        UPDATE canonical_song
        SET entry_count = ?, alias_count = ?, first_chart_date = ?, last_chart_date = ?
        WHERE canonical_song_id = ?
        """,
        (entry_count, alias_count, first_chart_date, last_chart_date, canonical_song_id),
    )


def admin_merge_canonical_songs(source_canonical_song_id: int, target_canonical_song_id: int) -> tuple[bool, str]:
    if source_canonical_song_id == target_canonical_song_id:
        return False, "Choose two different canonical songs."

    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"

    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        src = cur.execute(
            """
            SELECT canonical_song_id, canonical_title, COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist,
                   canonical_title_key, canonical_artist_key, canonical_group_key
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (source_canonical_song_id,),
        ).fetchone()
        tgt = cur.execute(
            """
            SELECT canonical_song_id, canonical_title, COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist,
                   canonical_title_key, canonical_artist_key, canonical_group_key
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (target_canonical_song_id,),
        ).fetchone()
        if src is None or tgt is None:
            return False, "Source or target canonical song was not found."

        cur.execute("BEGIN")

        cur.execute(
            """
            UPDATE entry
            SET canonical_song_id = ?,
                canonical_title_key = ?,
                canonical_artist_key = ?,
                canonical_group_key = ?
            WHERE canonical_song_id = ?
            """,
            (
                int(tgt["canonical_song_id"]),
                tgt["canonical_title_key"],
                tgt["canonical_artist_key"],
                tgt["canonical_group_key"],
                int(src["canonical_song_id"]),
            ),
        )

        try:
            _insert_song_alias_row(cur, int(tgt["canonical_song_id"]), (src["canonical_title"] or "").strip(), (src["canonical_artist"] or "").strip())
        except Exception:
            pass

        if "song_alias" in {row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}:
            alias_rows = cur.execute(
                """
                SELECT alias_song_title, alias_artist
                FROM song_alias
                WHERE canonical_song_id = ?
                """,
                (int(src["canonical_song_id"]),),
            ).fetchall()
            for alias_song_title, alias_artist in alias_rows:
                try:
                    _insert_song_alias_row(cur, int(tgt["canonical_song_id"]), alias_song_title, alias_artist)
                except Exception:
                    pass
            cur.execute("DELETE FROM song_alias WHERE canonical_song_id = ?", (int(src["canonical_song_id"]),))

        cur.execute("DELETE FROM canonical_song WHERE canonical_song_id = ?", (int(src["canonical_song_id"]),))
        _refresh_canonical_song_rollup(cur, int(tgt["canonical_song_id"]))

        conn.commit()
        _reset_app_caches()
        return True, f'Merged "{src["canonical_title"]}" into "{tgt["canonical_title"]}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Song merge failed: {exc}"
    finally:
        conn.close()


def _ensure_artist_key_override_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS artist_key_override (
            source_artist_key TEXT PRIMARY KEY,
            target_artist_key TEXT NOT NULL,
            preferred_display TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def admin_save_artist_key_merge(source_artist_key: str, target_artist_key: str, preferred_display: str) -> tuple[bool, str]:
    source_key = normalize_search_text(source_artist_key)
    target_key = normalize_search_text(target_artist_key)
    preferred_display = (preferred_display or "").strip()
    if not source_key:
        return False, "Source artist key is blank."
    if not target_key:
        return False, "Target artist key is blank."

    conn = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_artist_key_override_table(conn)
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO artist_key_override (source_artist_key, target_artist_key, preferred_display, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(source_artist_key) DO UPDATE SET
                target_artist_key = excluded.target_artist_key,
                preferred_display = CASE
                    WHEN excluded.preferred_display IS NULL OR excluded.preferred_display = '' THEN artist_key_override.preferred_display
                    ELSE excluded.preferred_display
                END,
                updated_at = CURRENT_TIMESTAMP
            """,
            (source_key, target_key, preferred_display or None),
        )
        if preferred_display:
            conn.execute(
                """
                INSERT INTO artist_key_override (source_artist_key, target_artist_key, preferred_display, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(source_artist_key) DO UPDATE SET
                    target_artist_key = excluded.target_artist_key,
                    preferred_display = excluded.preferred_display,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (target_key, target_key, preferred_display),
            )
        conn.commit()
        _reset_app_caches()
        if source_key == target_key:
            return True, f'Saved preferred display for "{target_key}".'
        return True, f'Merged "{source_key}" into "{target_key}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Artist merge failed: {exc}"
    finally:
        conn.close()


def admin_delete_artist_key_override(source_artist_key: str) -> tuple[bool, str]:
    source_key = normalize_search_text(source_artist_key)
    if not source_key:
        return False, "Source artist key is blank."

    conn = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_artist_key_override_table(conn)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM artist_key_override WHERE source_artist_key = ?", (source_key,))
        deleted = cur.rowcount
        conn.commit()
        _reset_app_caches()
        if deleted:
            return True, f'Deleted saved override for "{source_key}".'
        return False, f'No saved override exists for "{source_key}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Delete override failed: {exc}"
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def admin_artist_key_override_rows() -> pd.DataFrame:
    if not Path(DB_PATH).exists():
        return pd.DataFrame(columns=["source_artist_key", "target_artist_key", "preferred_display", "updated_at"])
    conn = sqlite3.connect(str(DB_PATH))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "artist_key_override" not in tables:
            return pd.DataFrame(columns=["source_artist_key", "target_artist_key", "preferred_display", "updated_at"])
        return pd.read_sql_query(
            "SELECT source_artist_key, target_artist_key, COALESCE(preferred_display, '') AS preferred_display, updated_at FROM artist_key_override ORDER BY target_artist_key, source_artist_key",
            conn,
        )
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def admin_artist_alias_audit() -> pd.DataFrame:
    chart = load_analytics_base()
    credits = build_artist_credit_rows(chart)
    if credits.empty:
        return pd.DataFrame(columns=["artist_key", "display_artist", "raw_artist_variants", "lead_weeks", "featured_weeks"])

    grouped = (
        credits.groupby(["artist_key"], dropna=True)
        .agg(
            raw_artist_variants=("artist", lambda s: int(s.dropna().astype(str).nunique())),
            lead_weeks=("artist_role_mode", lambda s: int((s == "Lead").sum())),
            featured_weeks=("artist_role_mode", lambda s: int((s == "Featured").sum())),
        )
        .reset_index()
    )
    name_map = (
        credits.groupby("artist_key", dropna=True)["artist"]
        .agg(lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else "")
        .reset_index()
        .rename(columns={"artist": "display_artist"})
    )
    out = grouped.merge(name_map, on="artist_key", how="left")
    out["display_artist"] = out.apply(
        lambda r: preferred_artist_display(r["artist_key"], r["display_artist"]),
        axis=1,
    )
    return out.sort_values(["raw_artist_variants", "lead_weeks", "display_artist"], ascending=[False, False, True])


@st.cache_data(show_spinner=False)
def admin_artist_variants_for_key(artist_key: str) -> pd.DataFrame:
    chart = load_analytics_base()
    credits = build_artist_credit_rows(chart)
    if credits.empty:
        return pd.DataFrame(columns=["artist_variant", "role_mode", "chart_weeks", "first_date", "last_date", "best_peak"])
    subset = credits.loc[credits["artist_key"] == artist_key].copy()
    if subset.empty:
        return pd.DataFrame(columns=["artist_variant", "role_mode", "chart_weeks", "first_date", "last_date", "best_peak"])
    return (
        subset.groupby(["artist", "artist_role_mode"], dropna=True)
        .agg(
            chart_weeks=("entry_id", "count"),
            first_date=("chart_date", "min"),
            last_date=("chart_date", "max"),
            best_peak=("position", "min"),
        )
        .reset_index()
        .rename(columns={"artist": "artist_variant", "artist_role_mode": "role_mode"})
        .sort_values(["chart_weeks", "best_peak", "artist_variant"], ascending=[False, True, True])
    )


@st.cache_data(show_spinner=False)
def admin_db_stats() -> dict[str, object]:
    conn = get_connection()

    def scalar(sql: str) -> object:
        row = conn.execute(sql).fetchone()
        return row[0] if row is not None else None

    stats = {
        "chart_weeks": scalar("SELECT COUNT(*) FROM chart_week") if _admin_table_exists(conn, "chart_week") else 0,
        "entries": scalar("SELECT COUNT(*) FROM entry") if _admin_table_exists(conn, "entry") else 0,
        "canonical_songs": scalar("SELECT COUNT(*) FROM canonical_song") if _admin_table_exists(conn, "canonical_song") else 0,
        "song_aliases": scalar("SELECT COUNT(*) FROM song_alias") if _admin_table_exists(conn, "song_alias") else 0,
        "first_chart_date": scalar("SELECT MIN(chart_date) FROM chart_week") if _admin_table_exists(conn, "chart_week") else None,
        "last_chart_date": scalar("SELECT MAX(chart_date) FROM chart_week") if _admin_table_exists(conn, "chart_week") else None,
    }
    artist_audit = admin_artist_alias_audit()
    stats["artist_keys"] = int(artist_audit["artist_key"].nunique()) if not artist_audit.empty else 0
    stats["artist_variants"] = int(artist_audit["raw_artist_variants"].sum()) if not artist_audit.empty else 0
    return stats




@st.cache_data(show_spinner=False)
def admin_known_anomalies() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "chart_date": "2004-02-24",
                "issue_type": "Source anomaly",
                "severity": "Info",
                "summary": "Original written chart duplicates Janet Jackson's 'Anyplace, Anyplace' at #19 and #23.",
                "status": "Preserve as written",
                "notes": "Treat as a documented historical source anomaly rather than a DB/import error.",
            },
        ]
    )

@st.cache_data(show_spinner=False)
def admin_data_quality_checks() -> dict[str, pd.DataFrame]:
    conn = get_connection()
    checks: dict[str, pd.DataFrame] = {}

    if _admin_table_exists(conn, "chart_week"):
        checks["duplicate_chart_dates"] = pd.read_sql_query(
            """
            SELECT chart_date, COUNT(*) AS chart_count
            FROM chart_week
            GROUP BY chart_date
            HAVING COUNT(*) > 1
            ORDER BY chart_date
            """,
            conn,
        )
    else:
        checks["duplicate_chart_dates"] = pd.DataFrame()

    if _admin_table_exists(conn, "entry") and _admin_table_exists(conn, "chart_week"):
        checks["entry_count_issues"] = pd.read_sql_query(
            """
            SELECT
                cw.chart_date,
                COUNT(*) AS entry_count
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            GROUP BY e.chart_week_id, cw.chart_date
            HAVING COUNT(*) <> 40
            ORDER BY cw.chart_date
            """,
            conn,
        )
        checks["duplicate_positions"] = pd.read_sql_query(
            """
            SELECT
                cw.chart_date,
                e.position,
                COUNT(*) AS row_count
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            GROUP BY e.chart_week_id, cw.chart_date, e.position
            HAVING COUNT(*) > 1
            ORDER BY cw.chart_date, e.position
            """,
            conn,
        )
        checks["duplicate_canonical_song_rows"] = pd.read_sql_query(
            """
            SELECT
                cw.chart_date,
                e.canonical_song_id,
                COALESCE(cs.canonical_title, e.song_title_display) AS song,
                COUNT(*) AS appearances
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            LEFT JOIN canonical_song cs ON cs.canonical_song_id = e.canonical_song_id
            WHERE e.canonical_song_id IS NOT NULL
            GROUP BY e.chart_week_id, cw.chart_date, e.canonical_song_id, song
            HAVING COUNT(*) > 1
            ORDER BY cw.chart_date, song
            """,
            conn,
        )
        checks["missing_song_mappings"] = pd.read_sql_query(
            """
            SELECT
                cw.chart_date,
                e.position,
                e.song_title_display AS song,
                e.full_artist_display AS artist
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id IS NULL
            ORDER BY cw.chart_date, e.position
            """,
            conn,
        )
    else:
        checks["entry_count_issues"] = pd.DataFrame()
        checks["duplicate_positions"] = pd.DataFrame()
        checks["duplicate_canonical_song_rows"] = pd.DataFrame()
        checks["missing_song_mappings"] = pd.DataFrame()

    return checks


def render_admin_tab() -> None:
    st.subheader("Admin")
    admin_section = st.selectbox(
        "Admin section",
        ["Songs", "Artists", "Data Quality", "Maintenance"],
        key="admin_section_selector",
    )

    if admin_section == "Songs":
        st.markdown("### Canonical songs")
        songs = admin_song_options()
        if songs.empty:
            st.info("No canonical_song rows are available in the database.")
        else:
            song_options = {
                f"{row.canonical_title} — {row.canonical_artist} | {int(row.chart_weeks or 0)} weeks | {row.first_chart_date} to {row.last_chart_date}": int(row.canonical_song_id)
                for row in songs.itertuples(index=False)
            }

            st.markdown("#### Rename canonical song")
            selected_label = st.selectbox("Choose a canonical song to manage", list(song_options.keys()), key="admin_song_pick")
            selected_song_id = song_options[selected_label]
            selected_row = songs.loc[songs["canonical_song_id"] == selected_song_id].iloc[0]
            st.caption(f"Current canonical title: {selected_row['canonical_title']}")
            new_title = st.text_input("New canonical song title", value="", key="admin_song_new_title")
            if st.button("Rename canonical song", key="admin_song_rename_btn"):
                ok, msg = admin_rename_canonical_song(selected_song_id, new_title)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
            st.markdown("**Alias variants for this canonical song**")
            _display_df(admin_song_aliases(selected_song_id))

            st.markdown("#### Merge duplicate canonical songs")
            merge_cols = st.columns([1.2, 1.2])
            merge_source_label = merge_cols[0].selectbox(
                "Duplicate/source canonical song",
                list(song_options.keys()),
                key="admin_song_merge_source",
            )
            merge_source_id = song_options[merge_source_label]
            target_labels = [label for label, cid in song_options.items() if cid != merge_source_id]
            if not target_labels:
                st.caption("No second canonical song is available to merge into.")
            else:
                merge_target_label = merge_cols[1].selectbox(
                    "Keep/target canonical song",
                    target_labels,
                    key="admin_song_merge_target",
                )
                merge_target_id = song_options[merge_target_label]
                st.caption("This rewrites entry rows in the DB to the kept canonical song, migrates aliases where possible, and deletes the duplicate canonical_song row.")
                if st.button("Merge canonical songs", key="admin_song_merge_btn"):
                    ok, msg = admin_merge_canonical_songs(merge_source_id, merge_target_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

    elif admin_section == "Artists":
        st.markdown("### Artist alias audit")
        st.caption("Use this section to pick the correct display name for an artist key or merge one artist key into another. These saved DB overrides are used throughout artist analytics.")
        artist_audit = admin_artist_alias_audit()
        if artist_audit.empty:
            st.info("No artist-key rows are available to audit.")
        else:
            display_map = {
                f"{row.display_artist} | {int(row.raw_artist_variants)} raw variants | {int(row.lead_weeks)} lead weeks | {int(row.featured_weeks)} featured weeks": row.artist_key
                for row in artist_audit.head(3000).itertuples(index=False)
            }
            selected_artist_label = st.selectbox(
                "Choose an artist key",
                list(display_map.keys()),
                key="admin_artist_audit_pick",
            )
            selected_artist_key = display_map[selected_artist_label]
            selected_row = artist_audit.loc[artist_audit["artist_key"] == selected_artist_key].iloc[0]
            variants_df = admin_artist_variants_for_key(selected_artist_key)
            st.markdown("**Raw artist variants mapped to this artist key**")
            _display_df(variants_df)

            merge_cols = st.columns([1.2, 1.2])
            target_options = {row.artist_key: row.display_artist for row in artist_audit.itertuples(index=False)}
            default_target_index = list(target_options.keys()).index(selected_artist_key)
            target_artist_key = merge_cols[0].selectbox(
                "Merge selected key into",
                list(target_options.keys()),
                index=default_target_index,
                format_func=lambda k: f"{target_options[k]} [{k}]",
                key="admin_artist_merge_target",
            )
            preferred_choices = []
            if not variants_df.empty and "artist_variant" in variants_df.columns:
                preferred_choices = sorted({str(v) for v in variants_df["artist_variant"].dropna().tolist()})
            default_preferred = selected_row["display_artist"] if "display_artist" in selected_row else ""
            if preferred_choices:
                if default_preferred not in preferred_choices:
                    preferred_choices = [default_preferred] + preferred_choices
                preferred_display = merge_cols[1].selectbox(
                    "Correct display name",
                    preferred_choices,
                    index=preferred_choices.index(default_preferred) if default_preferred in preferred_choices else 0,
                    key="admin_artist_preferred_display",
                )
            else:
                preferred_display = merge_cols[1].text_input(
                    "Correct display name",
                    value=default_preferred,
                    key="admin_artist_preferred_display_text",
                )

            action_cols = st.columns([1, 1])
            if action_cols[0].button("Save artist merge", key="admin_artist_merge_btn"):
                ok, msg = admin_save_artist_key_merge(selected_artist_key, target_artist_key, preferred_display)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
            if action_cols[1].button("Delete saved override for selected key", key="admin_artist_delete_override_btn"):
                ok, msg = admin_delete_artist_key_override(selected_artist_key)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

            st.markdown("**Saved artist-key merges / display overrides**")
            overrides_df = admin_artist_key_override_rows()
            _display_df(overrides_df)

            if not overrides_df.empty:
                delete_label_map = {
                    f"{row.source_artist_key} → {row.target_artist_key} | {row.preferred_display}": row.source_artist_key
                    for row in overrides_df.itertuples(index=False)
                }
                delete_label = st.selectbox(
                    "Delete a specific saved override",
                    list(delete_label_map.keys()),
                    key="admin_artist_saved_override_pick",
                )
                if st.button("Delete selected saved override", key="admin_artist_delete_saved_override_btn"):
                    ok, msg = admin_delete_artist_key_override(delete_label_map[delete_label])
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            with st.expander("Artist-key audit summary", expanded=False):
                _display_df(artist_audit.head(400), ["display_artist", "artist_key", "raw_artist_variants", "lead_weeks", "featured_weeks"])

    elif admin_section == "Data Quality":
        st.markdown("### Data quality checks")
        checks = admin_data_quality_checks()
        sections = [
            ("Duplicate chart dates", "duplicate_chart_dates"),
            ("Charts with non-40 entry counts", "entry_count_issues"),
            ("Duplicate positions within a chart", "duplicate_positions"),
            ("Duplicate canonical-song rows within a chart", "duplicate_canonical_song_rows"),
            ("Rows missing canonical song mapping", "missing_song_mappings"),
        ]
        for title, key in sections:
            df = checks.get(key, pd.DataFrame())
            with st.expander(f"{title} ({len(df)})", expanded=False):
                if df.empty:
                    st.caption("No issues found.")
                else:
                    _display_df(df)

        st.markdown("### Known anomalies")
        st.caption("These are documented source-chart issues that should not be treated like ordinary import or database errors.")
        known_anomalies = admin_known_anomalies()
        with st.expander(f"Known anomalies ({len(known_anomalies)})", expanded=True):
            if known_anomalies.empty:
                st.caption("No known anomalies have been documented yet.")
            else:
                _display_df(known_anomalies, ["chart_date", "issue_type", "severity", "summary", "status", "notes"])

    else:
        st.markdown("### Maintenance")
        stats = admin_db_stats()
        a1, a2, a3 = st.columns(3)
        a1.metric("Chart weeks", stats["chart_weeks"])
        a2.metric("Entries", stats["entries"])
        a3.metric("Canonical songs", stats["canonical_songs"])
        b1, b2, b3 = st.columns(3)
        b1.metric("Song aliases", stats["song_aliases"])
        b2.metric("Artist keys", stats["artist_keys"])
        b3.metric("Artist variants", stats["artist_variants"])
        st.caption(f"Coverage: {stats['first_chart_date'] or '—'} through {stats['last_chart_date'] or '—'}")
        if st.button("Clear cached data and reconnect", key="admin_clear_cache_btn"):
            _reset_app_caches()
            st.success("Cached data cleared.")
            st.rerun()

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
    section_cols = st.columns([2, 1, 1])
    section = section_cols[0].selectbox("Analytics section", ANALYTICS_SECTIONS, key="analytics_section")
    top_n = int(section_cols[1].slider("Top N rows", 5, 100, 25, 5, key="analytics_top_n"))
    chart_key = "analytics_show_charts_" + re.sub(r"[^a-z0-9]+", "_", section.lower()).strip("_")
    show_charts = section_cols[2].checkbox("Load charts", value=False, key=chart_key)
    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return
    pkg = _apply_analytics_filters(build_analytics_package(), start_date, end_date, include_reentries, min_weeks)

    def _render_selected_section() -> None:
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

    if show_charts:
        _render_selected_section()
    else:
        st.caption("Charts are hidden for this Analytics section. Turn on 'Load charts' to render the visualizations.")
        original_line_chart = st.line_chart
        original_bar_chart = st.bar_chart
        original_scatter_chart = st.scatter_chart
        original_altair_chart = st.altair_chart

        def _skip_chart(*args, **kwargs):
            return None

        try:
            st.line_chart = _skip_chart
            st.bar_chart = _skip_chart
            st.scatter_chart = _skip_chart
            st.altair_chart = _skip_chart
            _render_selected_section()
        finally:
            st.line_chart = original_line_chart
            st.bar_chart = original_bar_chart
            st.scatter_chart = original_scatter_chart
            st.altair_chart = original_altair_chart


def render_search_tab() -> None:
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

def render_week_browser_tab() -> None:
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

def render_song_history_tab() -> None:
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

def render_artist_history_tab() -> None:
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

def render_special_tables_tab() -> None:
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

    if table_kind == "#1 hits":
        conn = get_connection()
        year_rows = conn.execute(
            "SELECT DISTINCT SUBSTR(chart_date, 1, 4) AS year FROM chart_week ORDER BY year DESC"
        ).fetchall()
        year_options = ["All years"] + [row[0] for row in year_rows if row[0]]
        selected_year = st.selectbox("Year", year_options, key="special_num1_year")

        table = load_special_entries(table_kind, 1000000)
        if selected_year != "All years" and not table.empty and "chart_date" in table.columns:
            table = table.loc[table["chart_date"].astype(str).str.startswith(selected_year)].copy()
        _display_df(table)
    else:
        limit = st.slider("Rows", 10, 500, 100, 10, key="special_limit")
        table = load_special_entries(table_kind, limit)
        _display_df(table)

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

    main_section = st.selectbox(
        "Section",
        [
            "Full-text search",
            "Week browser",
            "Canonical song history",
            "Artist history",
            "Quick tables",
            "Analytics",
            "Admin",
        ],
        key="main_section_selector",
    )

    if main_section == "Full-text search":
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

    elif main_section == "Week browser":
        render_week_browser_tab()
    elif main_section == "Canonical song history":
        render_song_history_tab()
    elif main_section == "Artist history":
        render_artist_history_tab()
    elif main_section == "Quick tables":
        render_special_tables_tab()
    elif main_section == "Analytics":
        render_analytics_tab()
    else:
        render_admin_tab()


if __name__ == "__main__":
    main()
