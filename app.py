from __future__ import annotations

import datetime as dt
import sqlite3
import re
import math
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st

# Keep Streamlit Community Cloud from retaining too many large pandas objects.
# The app can always recompute these from SQLite, so bounded caches are safer than
# unlimited in-memory caches for long-running/mobile sessions.
CACHE_TTL_SECONDS = 60 * 60
CACHE_MAX_ENTRIES = 24

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
    "epic rap battles": "epic rap battles of history",
    "epic battles of history": "epic rap battles of history",
    "erb": "epic rap battles of history",
}

PREFERRED_ARTIST_DISPLAY = {
    "dino conner": "Dino Conner",
    "jake&papa": "Jake&Papa",
}


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


def artist_key_and_display(artist_key: object, fallback_display: object = "") -> tuple[object, str]:
    """Resolve an artist key and preferred display name in one place."""
    resolved_key = resolve_artist_key_alias(artist_key)
    if resolved_key is None or (isinstance(resolved_key, float) and pd.isna(resolved_key)):
        return resolved_key, "" if fallback_display is None else str(fallback_display)
    resolved_key = normalize_search_text(resolved_key)
    return resolved_key, preferred_artist_display(resolved_key, fallback_display)


def apply_artist_display_overrides(df: pd.DataFrame, key_col: str = "artist_key", display_col: str = "artist") -> pd.DataFrame:
    """Return a copy with resolved artist keys and preferred display names."""
    if df.empty or key_col not in df.columns:
        return df
    out = df.copy()
    out[key_col] = out[key_col].map(resolve_artist_key_alias).replace("", pd.NA)
    if display_col in out.columns:
        display_overrides = load_artist_display_override_map()

        def _display_for_row(r: pd.Series) -> str:
            key = r.get(key_col)
            fallback = r.get(display_col, "")
            if key is None or (isinstance(key, float) and pd.isna(key)):
                return "" if fallback is None else str(fallback)
            key = normalize_search_text(key)
            preferred = display_overrides.get(key) or PREFERRED_ARTIST_DISPLAY.get(key)
            return preferred or ("" if fallback is None else str(fallback))

        out[display_col] = out.apply(_display_for_row, axis=1)
    return out


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
entry_keyed AS (
    SELECT
        e.entry_id,
        e.chart_week_id,
        e.position,
        e.canonical_song_id,
        ow.chart_date,
        ow.prev_chart_week_id,
        CASE
            WHEN COALESCE(TRIM(e.normalized_song_title), '') <> ''
             AND COALESCE(TRIM(e.normalized_full_artist), '') <> ''
            THEN LOWER(TRIM(REPLACE(COALESCE(e.normalized_song_title, ''), '.', '')))
              || '||'
              || LOWER(TRIM(REPLACE(COALESCE(e.normalized_full_artist, ''), '.', '')))
            ELSE 'entry:' || e.entry_id
        END AS fallback_song_key,
        ROW_NUMBER() OVER (
            PARTITION BY e.canonical_song_id
            ORDER BY ow.chart_date, e.position, e.entry_id
        ) AS canonical_weeks_on_chart,
        ROW_NUMBER() OVER (
            PARTITION BY
                CASE
                    WHEN COALESCE(TRIM(e.normalized_song_title), '') <> ''
                     AND COALESCE(TRIM(e.normalized_full_artist), '') <> ''
                    THEN LOWER(TRIM(REPLACE(COALESCE(e.normalized_song_title, ''), '.', '')))
                      || '||'
                      || LOWER(TRIM(REPLACE(COALESCE(e.normalized_full_artist, ''), '.', '')))
                    ELSE 'entry:' || e.entry_id
                END
            ORDER BY ow.chart_date, e.position, e.entry_id
        ) AS fallback_weeks_on_chart
    FROM entry e
    JOIN ordered_weeks ow ON ow.chart_week_id = e.chart_week_id
),
entry_stats AS (
    SELECT
        cur.entry_id,
        cur.chart_date,
        cur.prev_chart_week_id,
        CASE
            WHEN cur.canonical_song_id IS NOT NULL THEN COALESCE(MIN(prev_canon.position), MIN(prev_fallback.position))
            ELSE MIN(prev_fallback.position)
        END AS last_week_position,
        CASE
            WHEN cur.canonical_song_id IS NOT NULL THEN cur.canonical_weeks_on_chart
            ELSE cur.fallback_weeks_on_chart
        END AS weeks_on_chart
    FROM entry_keyed cur
    LEFT JOIN entry_keyed prev_canon
      ON cur.canonical_song_id IS NOT NULL
     AND prev_canon.chart_week_id = cur.prev_chart_week_id
     AND prev_canon.canonical_song_id = cur.canonical_song_id
    LEFT JOIN entry_keyed prev_fallback
      ON prev_fallback.chart_week_id = cur.prev_chart_week_id
     AND prev_fallback.fallback_song_key = cur.fallback_song_key
    GROUP BY
        cur.entry_id,
        cur.chart_date,
        cur.prev_chart_week_id,
        cur.canonical_song_id,
        cur.canonical_weeks_on_chart,
        cur.fallback_weeks_on_chart
)
"""


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
        ,
        keyed_entries AS (
            SELECT
                entry_id,
                chart_week_id,
                chart_date,
                position,
                canonical_song_id,
                CASE
                    WHEN canonical_song_id IS NOT NULL THEN 'cs:' || canonical_song_id
                    ELSE 'fb:' || fallback_song_key
                END AS song_identity
            FROM entry_keyed
        ),
        keyed_entries_to_selected_week AS (
            SELECT *
            FROM keyed_entries
            WHERE chart_date <= ?
        ),
        song_peak AS (
            SELECT
                song_identity,
                MIN(position) AS peak_position
            FROM keyed_entries_to_selected_week
            GROUP BY song_identity
        ),
        peak_first AS (
            SELECT
                ke.song_identity,
                MIN(ke.chart_date) AS week_hit_peak
            FROM keyed_entries_to_selected_week ke
            JOIN song_peak sp
              ON sp.song_identity = ke.song_identity
             AND sp.peak_position = ke.position
            GROUP BY ke.song_identity
        )
        SELECT
            e.position,
            es.last_week_position,
            CASE
                WHEN es.last_week_position IS NOT NULL THEN es.last_week_position - e.position
                ELSE NULL
            END AS movement,
            es.weeks_on_chart,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker,
            sp.peak_position,
            pf.week_hit_peak,
            e.canonical_song_id,
            cur_keyed.song_identity,
            e.raw_slug AS slug
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN entry_stats es ON es.entry_id = e.entry_id
        LEFT JOIN keyed_entries cur_keyed ON cur_keyed.entry_id = e.entry_id
        LEFT JOIN song_peak sp ON sp.song_identity = cur_keyed.song_identity
        LEFT JOIN peak_first pf ON pf.song_identity = cur_keyed.song_identity
        WHERE cw.chart_date = ?
        ORDER BY e.position
        """
    df = pd.read_sql_query(sql, conn, params=(chart_date, chart_date))
    return df, meta



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def canonical_song_matches(term: str, limit: int = 100) -> pd.DataFrame:
    conn = get_connection()
    like = f"%{term.strip().lower()}%"
    sql = """
        SELECT
            cs.canonical_song_id,
            cs.canonical_title,
            COALESCE(cs.canonical_full_artist, cs.canonical_artist) AS canonical_artist,
            COALESCE(cs.canonical_lead_artist, cs.canonical_artist) AS canonical_lead_artist,
            COALESCE(cs.canonical_featured_artist, '') AS canonical_featured_artist,
            COUNT(DISTINCT e.entry_id) AS chart_weeks,
            MIN(cw.chart_date) AS first_date,
            MAX(cw.chart_date) AS last_date
        FROM canonical_song cs
        LEFT JOIN entry e ON e.canonical_song_id = cs.canonical_song_id
        LEFT JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE LOWER(cs.canonical_title) LIKE ?
           OR LOWER(COALESCE(cs.canonical_full_artist, cs.canonical_artist)) LIKE ?
           OR LOWER(COALESCE(cs.canonical_lead_artist, cs.canonical_artist)) LIKE ?
           OR LOWER(cs.canonical_title || ' ' || COALESCE(cs.canonical_full_artist, cs.canonical_artist)) LIKE ?
        GROUP BY
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.canonical_full_artist,
            cs.canonical_lead_artist,
            cs.canonical_featured_artist
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
            COUNT(DISTINCT e.entry_id) AS chart_weeks,
            MIN(cw.chart_date) AS first_date,
            MAX(cw.chart_date) AS last_date,
            MIN(e.position) AS peak,
            COUNT(DISTINCT sa.alias_display_key) AS alias_count
        FROM canonical_song cs
        LEFT JOIN entry e ON e.canonical_song_id = cs.canonical_song_id
        LEFT JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        LEFT JOIN song_alias sa ON sa.canonical_song_id = cs.canonical_song_id
        WHERE cs.canonical_song_id = ?
        GROUP BY
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.canonical_full_artist,
            cs.canonical_lead_artist,
            cs.canonical_featured_artist
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

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
            chart_weeks=("entry_id", "nunique"),
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



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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

    normalized_artist = resolve_artist_key_alias(normalized_artist)
    credits = credits.loc[credits["artist_key"] == normalized_artist].copy()
    if credits.empty:
        return pd.DataFrame(), None, pd.DataFrame()

    artist_name = credits["artist"].dropna().astype(str)
    fallback_artist = artist_name.mode().iloc[0] if not artist_name.empty else normalized_artist
    display_artist = preferred_artist_display(normalized_artist, fallback_artist)

    stats = {
        "artist": display_artist,
        "chart_weeks": int(credits["entry_id"].nunique()),
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

    # Group by song_key only so already-merged canonical songs do not split back out
    # just because an old entry used different title casing/punctuation.
    songs = (
        credits.groupby(["song_key"], dropna=True)
        .agg(
            song=("title", lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else ""),
            chart_weeks=("entry_id", "count"),
            first_date=("chart_date", "min"),
            last_date=("chart_date", "max"),
            peak=("position", "min"),
        )
        .reset_index()
        .sort_values(["peak", "chart_weeks", "last_date", "song"], ascending=[True, False, False, True])
        [["song", "chart_weeks", "first_date", "last_date", "peak"]]
    )

    return history, stats, songs



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
        chart = load_analytics_base()
        credits = build_artist_credit_rows(chart)
        if credits.empty:
            return pd.DataFrame(columns=["lead_artist", "top_10_weeks", "distinct_songs", "first_date", "last_date", "best_peak"])
        top10 = credits.loc[(credits["artist_role_mode"] == "Lead") & (credits["position"] <= 10)].copy()
        if top10.empty:
            return pd.DataFrame(columns=["lead_artist", "top_10_weeks", "distinct_songs", "first_date", "last_date", "best_peak"])
        out = (
            top10.groupby("artist_key", dropna=True)
            .agg(
                lead_artist=("artist", lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else ""),
                top_10_weeks=("entry_id", "count"),
                distinct_songs=("song_key", "nunique"),
                first_date=("chart_date", "min"),
                last_date=("chart_date", "max"),
                best_peak=("position", "min"),
            )
            .reset_index()
        )
        out["lead_artist"] = out.apply(lambda r: preferred_artist_display(r["artist_key"], r["lead_artist"]), axis=1)
        out = (
            out.drop(columns=["artist_key"])
            .sort_values(["top_10_weeks", "best_peak", "last_date", "lead_artist"], ascending=[False, True, False, True])
            .head(limit)
        )
        return out

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


def _escape_streamlit_caption_text(value: object) -> str:
    """Escape markdown/KaTeX trigger characters in short Streamlit captions."""
    text = "" if value is None else str(value)
    return text.replace("\\", "\\\\").replace("$", "\\$").replace("`", "\\`")


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
        norm_part = normalize_search_text(norm_part)
        display_part = (display_part or "").strip()
        raw_key = norm_part or normalize_search_text(display_part)
        if not raw_key:
            continue
        resolved_key = resolve_artist_key_alias(raw_key)
        if resolved_key is None or (isinstance(resolved_key, float) and pd.isna(resolved_key)):
            continue
        resolved_key = normalize_search_text(resolved_key)
        if resolved_key:
            pairs.append((resolved_key, display_part or raw_key))
    return pairs


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
    credits = apply_artist_display_overrides(credits, "artist_key", "artist")
    credits["artist_key"] = credits["artist_key"].replace("", pd.NA)
    credits["artist"] = credits["artist"].replace("", pd.NA)
    credits = credits.loc[credits["artist_key"].notna() & credits["artist"].notna()].copy()

    if credits.empty:
        return credits

    credits = credits.drop_duplicates(
        subset=["entry_id", "song_key", "chart_date", "artist_key", "artist_role_mode"]
    ).copy()
    return credits


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_weekly_summary(df_chart: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty:
        return pd.DataFrame()
    rows = []
    for chart_date, g in df_chart.groupby("chart_date", sort=True):
        valid_moves = g.loc[g["move"].notna(), "move"]
        abs_moves = g.loc[g["abs_move"].notna(), "abs_move"]
        top10 = g.loc[g["position"] <= 10, "weeks_on_chart"]
        bottom10 = g.loc[g["position"] >= 31, "weeks_on_chart"]
        week_credits = build_artist_credit_rows(g)
        unique_artist_count = (
            int(week_credits["artist_key"].dropna().nunique())
            if not week_credits.empty
            else int(g["artist_key"].dropna().nunique())
        )
        rows.append({
            "chart_date": chart_date,
            "year": int(g["year"].iloc[0]),
            "month": int(g["month"].iloc[0]),
            "unique_titles": int(g["song_key"].nunique()),
            "unique_artists": unique_artist_count,
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_yearly_summary(df_chart: pd.DataFrame, df_weekly: pd.DataFrame, df_song: pd.DataFrame) -> pd.DataFrame:
    if df_chart.empty or df_weekly.empty:
        return pd.DataFrame()
    year_rows = []
    for year, g in df_chart.groupby("year", sort=True):
        year_credits = build_artist_credit_rows(g)
        unique_artists = (
            int(year_credits["artist_key"].dropna().nunique())
            if not year_credits.empty
            else int(g["artist_key"].dropna().nunique())
        )
        year_rows.append({
            "year": year,
            "unique_songs": int(g["song_key"].nunique()),
            "unique_artists": unique_artists,
        })
    year_base = pd.DataFrame(year_rows)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def load_analytics_date_bounds() -> tuple[dt.date | None, dt.date | None]:
    conn = get_connection()
    row = conn.execute("SELECT MIN(chart_date) AS min_date, MAX(chart_date) AS max_date FROM chart_week").fetchone()
    if row is None or not row[0] or not row[1]:
        return None, None
    return dt.date.fromisoformat(str(row[0])), dt.date.fromisoformat(str(row[1]))


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def _analytics_filtered_chart(start_date: dt.date, end_date: dt.date, include_reentries: bool) -> pd.DataFrame:
    chart = load_analytics_base().copy()
    if chart.empty:
        return chart
    mask = (chart["chart_date"].dt.date >= start_date) & (chart["chart_date"].dt.date <= end_date)
    chart = chart.loc[mask].copy()
    if not include_reentries:
        chart = chart.loc[~chart["is_reentry"]].copy()
    return chart


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def _analytics_pkg_for_section(section: str, start_date: dt.date, end_date: dt.date, include_reentries: bool, min_weeks_on_chart: int) -> dict[str, pd.DataFrame]:
    chart = _analytics_filtered_chart(start_date, end_date, include_reentries)
    empty = pd.DataFrame()
    pkg: dict[str, pd.DataFrame] = {
        "chart": chart,
        "weekly": empty,
        "songs": empty,
        "artist_credits": empty,
        "artist_presence": empty,
        "artists": empty,
        "years": empty,
    }
    if chart.empty:
        return pkg

    needs_weekly = section in {"Overview", "Movement", "Years & Eras", "Records & Outliers"}
    needs_songs = section in {"Longevity", "Artists", "Years & Eras", "Records & Outliers"}
    needs_artist_stack = section in {"Artists", "Records & Outliers"}
    needs_years = section == "Years & Eras"

    weekly = build_weekly_summary(chart) if needs_weekly else empty
    pkg["weekly"] = weekly

    songs = build_song_summary(chart) if needs_songs else empty
    if needs_songs and not songs.empty:
        songs = songs.loc[songs["total_chart_weeks"] >= min_weeks_on_chart].copy()
    pkg["songs"] = songs

    if needs_artist_stack:
        artist_credits = build_artist_credit_rows(chart)
        if not songs.empty:
            valid_song_keys = set(songs["song_key"].tolist())
            artist_credits = artist_credits.loc[artist_credits["song_key"].isin(valid_song_keys)].copy()
        artist_presence = build_artist_weekly_presence(artist_credits)
        artists = build_artist_summary(artist_credits, songs, artist_presence) if not artist_credits.empty else empty
        pkg["artist_credits"] = artist_credits
        pkg["artist_presence"] = artist_presence
        pkg["artists"] = artists

    if needs_years:
        if weekly.empty:
            weekly = build_weekly_summary(chart)
            pkg["weekly"] = weekly
        if songs.empty:
            songs = build_song_summary(chart)
            if not songs.empty:
                songs = songs.loc[songs["total_chart_weeks"] >= min_weeks_on_chart].copy()
            pkg["songs"] = songs
        pkg["years"] = build_yearly_summary(chart, weekly, songs)

    return pkg


def _display_df(df: pd.DataFrame, columns: list[str] | None = None, hide_index: bool = True):
    if columns is not None and not df.empty:
        cols = [c for c in columns if c in df.columns]
        df = df[cols]

    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    st.dataframe(df, width="stretch", hide_index=hide_index)




def _forecast_tier(position: object) -> str:
    pos = _safe_int(position)
    if pos is None:
        return "Unknown"
    if pos == 1:
        return "#1"
    if pos <= 5:
        return "Top 5"
    if pos <= 10:
        return "Top 10"
    if pos <= 20:
        return "Top 20"
    if pos <= 30:
        return "21-30"
    return "31-40"


def _forecast_direction(move: object) -> str:
    if move is None or (isinstance(move, float) and pd.isna(move)):
        return "New/returning"
    try:
        value = float(move)
    except Exception:
        return "New/returning"
    if value > 0:
        return "Up"
    if value < 0:
        return "Down"
    return "Hold"


def _clip_probability(value: object) -> float:
    try:
        val = float(value)
    except Exception:
        return 0.0
    if pd.isna(val):
        return 0.0
    return max(0.0, min(1.0, val))


def _reign_inertia_bonus(current_weeks_at_1: object) -> float:
    """Small next-week #1 hold boost. Long #1 reigns are rare, so this stays capped."""
    weeks = _safe_int(current_weeks_at_1) or 0
    if weeks <= 0:
        return 0.0
    if weeks == 1:
        return 0.03
    if weeks == 2:
        return 0.07
    if weeks == 3:
        return 0.12
    if weeks == 4:
        return 0.18
    return 0.22


def _long_reign_rarity_penalty(current_weeks_at_1: object, long_reigns_started_this_year: int = 0) -> float:
    """Penalty for treating 5+ week #1 reigns as normal outcomes."""
    weeks = _safe_int(current_weeks_at_1) or 0
    if weeks >= 5:
        base = 0.0
    elif weeks == 4:
        base = 0.02
    elif weeks == 3:
        base = 0.06
    elif weeks == 2:
        base = 0.12
    elif weeks == 1:
        base = 0.18
    else:
        base = 0.0

    # The DB check showed 5+ week #1 reigns are rare. If a year already has one,
    # require a little more proof before forecasting another early-stage dynasty.
    if long_reigns_started_this_year >= 1 and 0 < weeks < 3:
        base += 0.05
    return base


def _long_reign_probability(current_weeks_at_1: object, next_week_num1_probability: object, long_reigns_started_this_year: int = 0) -> float:
    """Approximate chance that the current #1 reaches 5+ consecutive weeks."""
    weeks = _safe_int(current_weeks_at_1) or 0
    if weeks <= 0:
        return 0.0
    if weeks >= 5:
        return 1.0
    hold = _clip_probability(next_week_num1_probability)
    needed_holds = max(1, 5 - weeks)
    raw = hold ** needed_holds
    raw += _reign_inertia_bonus(weeks) * 0.35
    raw -= _long_reign_rarity_penalty(weeks, long_reigns_started_this_year)
    return _clip_probability(raw)


def _long_reign_watch_label(current_weeks_at_1: object, long_reign_probability: object) -> str:
    weeks = _safe_int(current_weeks_at_1) or 0
    prob = _clip_probability(long_reign_probability)
    if weeks >= 5:
        return "Confirmed long reign"
    if weeks == 4:
        return "Strong watch"
    if weeks == 3:
        return "Active watch"
    if weeks == 2 or prob >= 0.20:
        return "Possible"
    if weeks == 1:
        return "Early / unlikely"
    return "No"


def _add_num1_reign_features(df_chart: pd.DataFrame) -> pd.DataFrame:
    """Add current consecutive #1 streak length to each row, using available chart weeks."""
    if df_chart.empty:
        return df_chart
    out = df_chart.copy()
    out["current_num1_streak"] = 0
    out["num1_run_start_date"] = pd.NaT

    needed_cols = {"chart_date", "position", "song_key"}
    if not needed_cols.issubset(out.columns):
        return out

    num1 = out.loc[out["position"].eq(1), ["chart_date", "song_key"]].copy().sort_values("chart_date")
    if num1.empty:
        return out

    streak_rows: list[dict[str, object]] = []
    prev_song = None
    streak = 0
    run_start = None
    for rec in num1.to_dict("records"):
        song_key = rec["song_key"]
        chart_dt = rec["chart_date"]
        if song_key == prev_song:
            streak += 1
        else:
            streak = 1
            run_start = chart_dt
        streak_rows.append({
            "chart_date": chart_dt,
            "song_key": song_key,
            "current_num1_streak": int(streak),
            "num1_run_start_date": run_start,
        })
        prev_song = song_key

    streak_df = pd.DataFrame(streak_rows)
    out = out.drop(columns=["current_num1_streak", "num1_run_start_date"], errors="ignore")
    out = out.merge(streak_df, on=["chart_date", "song_key"], how="left")
    out["current_num1_streak"] = pd.to_numeric(out["current_num1_streak"], errors="coerce").fillna(0).astype(int)
    return out


def _count_prior_long_reigns_started_this_year(df_chart: pd.DataFrame, chart_date: pd.Timestamp) -> int:
    """Count #1 runs that already reached 5+ weeks and started earlier in the same year."""
    if df_chart.empty or "current_num1_streak" not in df_chart.columns:
        return 0
    chart_date = pd.to_datetime(chart_date)
    year = chart_date.year
    num1 = df_chart.loc[(df_chart["position"] == 1) & (df_chart["chart_date"] < chart_date)].copy()
    if num1.empty or "num1_run_start_date" not in num1.columns:
        return 0
    reached = num1.loc[pd.to_numeric(num1["current_num1_streak"], errors="coerce").fillna(0) >= 5].copy()
    if reached.empty:
        return 0
    starts = pd.to_datetime(reached["num1_run_start_date"], errors="coerce").dropna().drop_duplicates()
    return int(((starts.dt.year == year) & (starts < chart_date)).sum())


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_forecast_neighbor_table(df_chart: pd.DataFrame) -> pd.DataFrame:
    """Prepare historical rows that have a known next-week outcome."""
    if df_chart.empty:
        return pd.DataFrame()
    hist = df_chart.loc[df_chart["next_chart_date"].notna()].copy()
    if hist.empty:
        return hist
    hist["position"] = pd.to_numeric(hist["position"], errors="coerce")
    hist["next_position"] = pd.to_numeric(hist["next_position"], errors="coerce")
    hist["weeks_on_chart"] = pd.to_numeric(hist["weeks_on_chart"], errors="coerce").fillna(1)
    hist["move"] = pd.to_numeric(hist["move"], errors="coerce")
    hist["move_filled"] = hist["move"].fillna(0)
    hist["next_move"] = hist["position"] - hist["next_position"]
    hist["forecast_tier"] = hist["position"].apply(_forecast_tier)
    hist["forecast_direction"] = hist["move"].apply(_forecast_direction)
    if "current_num1_streak" not in hist.columns:
        hist["current_num1_streak"] = 0
    hist["current_num1_streak"] = pd.to_numeric(hist["current_num1_streak"], errors="coerce").fillna(0).astype(int)
    return hist


def _similar_cases_for_row(history: pd.DataFrame, row: pd.Series, max_neighbors: int) -> pd.DataFrame:
    if history.empty:
        return history
    pos = float(row.get("position", 40) or 40)
    weeks = float(row.get("weeks_on_chart", 1) or 1)
    move = row.get("move")
    move_val = 0.0 if pd.isna(move) else float(move)
    tier = _forecast_tier(row.get("position"))
    direction = _forecast_direction(row.get("move"))
    is_debut = bool(row.get("is_debut", False))
    is_reentry = bool(row.get("is_reentry", False))
    current_num1_streak = float(row.get("current_num1_streak", 0) or 0)

    scored = history.copy()
    scored_streak = pd.to_numeric(scored.get("current_num1_streak", 0), errors="coerce").fillna(0)
    num1_streak_distance = (scored_streak - current_num1_streak).abs().clip(upper=6)
    num1_streak_weight = 1.4 if pos == 1 else 0.0
    scored["similarity_score"] = (
        (scored["position"] - pos).abs() * 1.15
        + (scored["weeks_on_chart"].fillna(1) - weeks).abs().clip(upper=20) * 0.28
        + (scored["move_filled"].fillna(0) - move_val).abs().clip(upper=25) * 0.55
        + (num1_streak_distance * num1_streak_weight)
        + (scored["forecast_tier"].ne(tier).astype(int) * 3.5)
        + (scored["forecast_direction"].ne(direction).astype(int) * 2.0)
        + (scored["is_debut"].ne(is_debut).astype(int) * 5.0)
        + (scored["is_reentry"].ne(is_reentry).astype(int) * 4.0)
    )
    return scored.sort_values(["similarity_score", "chart_date"], ascending=[True, False]).head(max_neighbors)


def _forecast_for_chart_date(df_chart: pd.DataFrame, chart_date: pd.Timestamp, max_neighbors: int = 125) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_chart.empty:
        return pd.DataFrame(), pd.DataFrame()
    chart_date = pd.to_datetime(chart_date)
    df_chart = _add_num1_reign_features(df_chart)
    current = df_chart.loc[df_chart["chart_date"] == chart_date].copy().sort_values("position")
    history = build_forecast_neighbor_table(df_chart.loc[df_chart["chart_date"] < chart_date].copy())
    if current.empty or history.empty:
        return pd.DataFrame(), pd.DataFrame()

    long_reigns_started_this_year = _count_prior_long_reigns_started_this_year(df_chart, chart_date)

    rows: list[dict[str, object]] = []
    similar_rows: list[pd.DataFrame] = []
    for _, row in current.iterrows():
        cases = _similar_cases_for_row(history, row, max_neighbors=max_neighbors)
        if cases.empty:
            continue
        present = cases.loc[cases["next_position"].notna()].copy()
        p_stay = float(cases["present_next_week"].mean()) if "present_next_week" in cases else float(len(present) / len(cases))
        p_dropout = 1.0 - p_stay
        if present.empty:
            exp_next_position = 41.0
            median_next_position = 41.0
            avg_next_move = float("nan")
            p_up = p_down = p_hold = 0.0
            p_top20 = p_top10 = p_top5 = p_num1 = 0.0
        else:
            exp_next_position = float(present["next_position"].mean())
            median_next_position = float(present["next_position"].median())
            avg_next_move = float(present["next_move"].mean())
            p_up = float((present["next_position"] < row["position"]).mean())
            p_down = float((present["next_position"] > row["position"]).mean())
            p_hold = float((present["next_position"] == row["position"]).mean())
            p_top20 = float((present["next_position"] <= 20).mean())
            p_top10 = float((present["next_position"] <= 10).mean())
            p_top5 = float((present["next_position"] <= 5).mean())
            p_num1 = float((present["next_position"] == 1).mean())

        current_num1_streak = _safe_int(row.get("current_num1_streak")) or 0
        reign_bonus = _reign_inertia_bonus(current_num1_streak)
        rarity_penalty = _long_reign_rarity_penalty(current_num1_streak, long_reigns_started_this_year)
        num1_hold_score = _clip_probability(p_num1 + reign_bonus - (rarity_penalty * 0.35))
        long_reign_prob = _long_reign_probability(current_num1_streak, p_num1, long_reigns_started_this_year)
        long_reign_watch = _long_reign_watch_label(current_num1_streak, long_reign_prob)

        momentum_score = (
            max(0.0, 41.0 - float(row["position"]))
            + max(0.0, float(row["move"] if pd.notna(row.get("move")) else 0.0)) * 1.7
            + p_top10 * 18.0
            + p_top5 * 12.0
            + p_num1 * 20.0
            + reign_bonus * 40.0
            - rarity_penalty * 12.0
            - p_dropout * 26.0
            - min(float(row.get("weeks_on_chart", 1) or 1), 40.0) * 0.18
        )

        rows.append({
            "chart_date": chart_date,
            "position": int(row["position"]),
            "title": row.get("title", ""),
            "artist": row.get("artist", ""),
            "last_week_position": row.get("last_week_position"),
            "move": row.get("move"),
            "weeks_on_chart": row.get("weeks_on_chart"),
            "current_num1_streak": current_num1_streak,
            "derived_marker": row.get("derived_marker", ""),
            "similar_cases": int(len(cases)),
            "stay_probability": p_stay,
            "dropout_risk": p_dropout,
            "up_probability": p_up,
            "down_probability": p_down,
            "hold_probability": p_hold,
            "top20_probability": p_top20,
            "top10_probability": p_top10,
            "top5_probability": p_top5,
            "num1_probability": p_num1,
            "num1_hold_score": num1_hold_score,
            "long_reign_probability": long_reign_prob,
            "long_reign_watch": long_reign_watch,
            "reign_inertia_bonus": reign_bonus,
            "long_reign_rarity_penalty": rarity_penalty,
            "long_reigns_started_this_year": long_reigns_started_this_year,
            "expected_next_position": exp_next_position,
            "median_next_position": median_next_position,
            "expected_next_move": avg_next_move,
            "momentum_score": momentum_score,
        })
        keep = cases.head(10).copy()
        keep["source_current_position"] = int(row["position"])
        keep["source_title"] = row.get("title", "")
        keep["source_artist"] = row.get("artist", "")
        similar_rows.append(keep)

    forecast = pd.DataFrame(rows)
    similar = pd.concat(similar_rows, ignore_index=True) if similar_rows else pd.DataFrame()
    if forecast.empty:
        return forecast, similar

    forecast = forecast.sort_values(["expected_next_position", "dropout_risk", "position"], ascending=[True, True, True]).reset_index(drop=True)
    forecast["projected_rank"] = forecast.index + 1
    forecast["forecast_note"] = ""
    forecast.loc[forecast["dropout_risk"] >= 0.55, "forecast_note"] = "High dropout risk"
    forecast.loc[(forecast["num1_hold_score"] >= 0.08) | (forecast["projected_rank"] <= 3), "forecast_note"] = "#1 contender"
    forecast.loc[forecast["long_reign_watch"].isin(["Active watch", "Strong watch", "Confirmed long reign"]), "forecast_note"] = forecast["long_reign_watch"]
    forecast.loc[(forecast["top10_probability"] >= 0.45) & (forecast["position"] > 10), "forecast_note"] = "Top 10 watch"
    forecast.loc[(forecast["top10_probability"] < 0.45) & (forecast["position"] <= 10), "forecast_note"] = "Top 10 danger"
    return forecast.sort_values("projected_rank"), similar


def _format_probability_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "stay_probability", "dropout_risk", "up_probability", "down_probability", "hold_probability",
        "top20_probability", "top10_probability", "top5_probability", "num1_probability",
        "num1_hold_score", "long_reign_probability",
    ]:
        if col in out.columns:
            out[col] = (pd.to_numeric(out[col], errors="coerce") * 100).round(1).astype(str) + "%"
    for col in ["expected_next_position", "median_next_position", "expected_next_move", "momentum_score", "rank_error", "reign_inertia_bonus", "long_reign_rarity_penalty"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)
    return out



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def load_lastfm_forecast_weeks() -> pd.DataFrame:
    """Chart weeks that have imported Last.fm play data."""
    conn = get_connection()
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "lastfm_weekly_track_play" not in tables:
        return pd.DataFrame(columns=["chart_week_id", "chart_date", "period_from_local", "period_to_local", "lastfm_tracks", "matched_tracks", "total_plays"])

    sql = """
        SELECT
            cw.chart_week_id,
            cw.chart_date,
            MIN(l.period_from_local) AS period_from_local,
            MAX(l.period_to_local) AS period_to_local,
            COUNT(*) AS lastfm_tracks,
            SUM(CASE WHEN l.canonical_song_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_tracks,
            SUM(COALESCE(l.playcount, 0)) AS total_plays
        FROM lastfm_weekly_track_play l
        JOIN chart_week cw ON cw.chart_week_id = l.chart_week_id
        GROUP BY cw.chart_week_id, cw.chart_date
        ORDER BY cw.chart_date DESC
    """
    return pd.read_sql_query(sql, conn)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def load_lastfm_forecast_signals(chart_week_id: int) -> pd.DataFrame:
    """Matched chart songs with imported Last.fm play-rank pressure for one chart week."""
    conn = get_connection()
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "lastfm_weekly_track_play" not in tables:
        return pd.DataFrame()

    sql = """
        SELECT
            e.position AS chart_rank,
            l.lastfm_rank,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            l.playcount AS plays,
            (e.position - l.lastfm_rank) AS play_pressure,
            l.lastfm_track_name,
            l.lastfm_artist_name,
            e.canonical_song_id
        FROM entry e
        JOIN lastfm_weekly_track_play l
          ON l.chart_week_id = e.chart_week_id
         AND l.canonical_song_id = e.canonical_song_id
        WHERE e.chart_week_id = ?
        ORDER BY e.position
    """
    df = pd.read_sql_query(sql, conn, params=(chart_week_id,))
    if df.empty:
        return df
    df["chart_rank"] = pd.to_numeric(df["chart_rank"], errors="coerce")
    df["lastfm_rank"] = pd.to_numeric(df["lastfm_rank"], errors="coerce")
    df["plays"] = pd.to_numeric(df["plays"], errors="coerce").fillna(0).astype(int)
    df["play_pressure"] = pd.to_numeric(df["play_pressure"], errors="coerce")
    total_plays = float(df["plays"].sum())
    df["matched_play_share"] = (df["plays"] / total_plays) if total_plays > 0 else 0.0
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def load_lastfm_off_chart_watch(chart_week_id: int) -> pd.DataFrame:
    """Imported Last.fm rows that are not currently matched to a song on the selected chart week."""
    conn = get_connection()
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "lastfm_weekly_track_play" not in tables:
        return pd.DataFrame()

    sql = """
        SELECT
            l.lastfm_rank,
            l.lastfm_track_name AS song,
            l.lastfm_artist_name AS artist,
            l.playcount AS plays,
            l.canonical_song_id
        FROM lastfm_weekly_track_play l
        LEFT JOIN entry e
          ON e.chart_week_id = l.chart_week_id
         AND e.canonical_song_id = l.canonical_song_id
        WHERE l.chart_week_id = ?
          AND e.entry_id IS NULL
        ORDER BY l.lastfm_rank
    """
    df = pd.read_sql_query(sql, conn, params=(chart_week_id,))
    if df.empty:
        return df
    df["lastfm_rank"] = pd.to_numeric(df["lastfm_rank"], errors="coerce")
    df["plays"] = pd.to_numeric(df["plays"], errors="coerce").fillna(0).astype(int)
    return df


def _format_lastfm_signal_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "matched_play_share" in out.columns:
        out["matched_play_share"] = (pd.to_numeric(out["matched_play_share"], errors="coerce") * 100).round(1).astype(str) + "%"
    if "play_pressure" in out.columns:
        out["play_pressure"] = pd.to_numeric(out["play_pressure"], errors="coerce").round(0).astype("Int64")
    for col in ["chart_rank", "lastfm_rank", "plays"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out


def _render_lastfm_forecast_lab(top_n: int) -> None:
    st.markdown("### Last.fm Play Data Model")
    st.caption(
        "This model uses imported Last.fm play counts for the selected chart week. "
        "Positive play pressure means a song ranked higher in Last.fm plays than it did on the chart; "
        "negative pressure means the chart rank was stronger than the Last.fm play rank."
    )

    weeks = load_lastfm_forecast_weeks()
    if weeks.empty:
        st.info("No imported Last.fm play data was found. Run scripts/import_lastfm_weekly.py for a chart date first.")
        return

    weeks = weeks.copy()
    weeks["label"] = weeks.apply(
        lambda r: f"{r['chart_date']} | {int(r['lastfm_tracks'])} Last.fm rows | {int(r['matched_tracks'] or 0)} matched",
        axis=1,
    )
    selected_label = st.selectbox("Last.fm forecast week", weeks["label"].tolist(), key="forecast_lab_lastfm_chart_date")
    selected_week = weeks.loc[weeks["label"] == selected_label].iloc[0]
    chart_week_id = int(selected_week["chart_week_id"])

    period_from = selected_week.get("period_from_local") or "—"
    period_to = selected_week.get("period_to_local") or "—"
    st.caption(f"Forecast listening window: {period_from} through {period_to}")

    signals = load_lastfm_forecast_signals(chart_week_id)
    off_chart = load_lastfm_off_chart_watch(chart_week_id)

    imported_tracks = int(selected_week.get("lastfm_tracks") or 0)
    matched_tracks = int(selected_week.get("matched_tracks") or 0)
    total_plays = int(selected_week.get("total_plays") or 0)
    matched_chart_songs = int(len(signals))

    strongest = signals.sort_values(["play_pressure", "plays"], ascending=[False, False]).head(1) if not signals.empty else pd.DataFrame()
    softest = signals.sort_values(["play_pressure", "plays"], ascending=[True, False]).head(1) if not signals.empty else pd.DataFrame()

    render_kpis([
        ("Imported Last.fm rows", f"{imported_tracks:,}"),
        ("Matched Last.fm rows", f"{matched_tracks:,}"),
        ("Matched chart songs", f"{matched_chart_songs:,}"),
        ("Imported plays", f"{total_plays:,}"),
        ("Strongest upward pressure", strongest["song"].iloc[0] if not strongest.empty else "—"),
        ("Strongest softening signal", softest["song"].iloc[0] if not softest.empty else "—"),
    ])

    if signals.empty:
        st.info("Last.fm rows exist for this week, but none are matched to songs on this chart week yet. Run the matcher script or add aliases.")
        if not off_chart.empty:
            st.markdown("**Imported Last.fm rows**")
            _display_df(off_chart.head(top_n), ["lastfm_rank", "song", "artist", "plays", "canonical_song_id"])
        return

    st.markdown("**Last.fm play pressure by chart rank**")
    _display_df(
        _format_lastfm_signal_columns(signals),
        ["chart_rank", "lastfm_rank", "song", "artist", "plays", "play_pressure", "matched_play_share", "lastfm_track_name", "lastfm_artist_name"],
    )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Upward pressure watch**")
        upward = signals.loc[pd.to_numeric(signals["play_pressure"], errors="coerce") > 0].sort_values(
            ["play_pressure", "plays"], ascending=[False, False]
        ).head(top_n)
        if upward.empty:
            st.caption("No matched chart songs had positive Last.fm play pressure.")
        else:
            _display_df(
                _format_lastfm_signal_columns(upward),
                ["chart_rank", "lastfm_rank", "song", "artist", "plays", "play_pressure", "matched_play_share"],
            )

    with c2:
        st.markdown("**Softening watch**")
        softening = signals.loc[pd.to_numeric(signals["play_pressure"], errors="coerce") < 0].sort_values(
            ["play_pressure", "plays"], ascending=[True, False]
        ).head(top_n)
        if softening.empty:
            st.caption("No matched chart songs had negative Last.fm play pressure.")
        else:
            _display_df(
                _format_lastfm_signal_columns(softening),
                ["chart_rank", "lastfm_rank", "song", "artist", "plays", "play_pressure", "matched_play_share"],
            )

    st.markdown("**Off-chart / unmatched Last.fm watch**")
    st.caption("These imported Last.fm rows are not currently matched to songs on the selected chart week. Some may be future debuts/re-entries; others may be old catalog tracks, podcasts, or unmatched aliases.")
    if off_chart.empty:
        st.caption("No off-chart or unmatched Last.fm rows for this week.")
    else:
        _display_df(off_chart.head(top_n), ["lastfm_rank", "song", "artist", "plays", "canonical_song_id"])

def _render_forecast_lab(pkg: dict[str, pd.DataFrame], top_n: int) -> None:
    chart = pkg["chart"]
    if chart.empty:
        st.info("No chart rows available for forecasting.")
        return

    chart_dates = sorted(chart["chart_date"].dropna().unique())
    if len(chart_dates) < 8:
        st.info("Forecast Lab needs several historical chart weeks before it can compare similar past cases.")
        return

    latest_date = chart_dates[-1]
    st.markdown("### Next Week Predictor")
    st.caption(
        "This forecast compares each song on the selected chart with similar historical chart situations. "
        "It estimates movement and dropout risk for current songs, but it does not guess brand-new debuts because play-count inputs are not in the database."
    )

    controls = st.columns([1.4, 1.0, 1.0])
    date_labels = [pd.to_datetime(d).strftime("%Y-%m-%d") for d in chart_dates]
    selected_label = controls[0].selectbox("Forecast from chart week", date_labels, index=len(date_labels) - 1, key="forecast_lab_chart_date")
    max_neighbors = int(controls[1].slider("Similar cases per song", 25, 250, 125, 25, key="forecast_lab_neighbors"))
    show_similar = controls[2].checkbox("Show similar-case samples", value=False, key="forecast_lab_show_similar")
    selected_date = pd.to_datetime(selected_label)

    forecast, similar = _forecast_for_chart_date(chart, selected_date, max_neighbors=max_neighbors)
    if forecast.empty:
        st.info("Not enough earlier chart history exists before this selected week to build a forecast.")
        return

    current_rows = chart.loc[chart["chart_date"] == selected_date]
    avg_dropout = forecast["dropout_risk"].mean()
    expected_unknown_slots = int(round(forecast["dropout_risk"].sum()))
    top_contender = forecast.sort_values(["num1_hold_score", "num1_probability", "expected_next_position"], ascending=[False, False, True]).head(1)
    long_watch = forecast.loc[forecast["long_reign_watch"].ne("No")].sort_values(
        ["long_reign_probability", "current_num1_streak"], ascending=[False, False]
    ).head(1)
    render_kpis([
        ("Forecast week", selected_date.strftime("%Y-%m-%d")),
        ("Songs evaluated", int(len(current_rows))),
        ("Expected unknown slots", expected_unknown_slots),
        ("Avg dropout risk", f"{avg_dropout * 100:.1f}%"),
        ("Top #1 contender", top_contender["title"].iloc[0] if not top_contender.empty else "—"),
        ("Long-reign watch", long_watch["long_reign_watch"].iloc[0] if not long_watch.empty else "No"),
    ])

    st.markdown("**Projected current-song order for next week**")
    projection = forecast.sort_values("projected_rank").copy()
    _display_df(
        _format_probability_columns(projection),
        [
            "projected_rank", "title", "artist", "position", "last_week_position", "move", "weeks_on_chart", "current_num1_streak",
            "expected_next_position", "dropout_risk", "up_probability", "top10_probability", "num1_probability", "num1_hold_score", "long_reign_probability", "long_reign_watch", "forecast_note", "similar_cases",
        ],
    )
    st.caption(f"Expected unknown debut/re-entry/dropout replacement slots next week: about {expected_unknown_slots}. Those titles are intentionally not guessed.")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**#1 contenders**")
        contenders = forecast.sort_values(["num1_hold_score", "num1_probability", "top5_probability", "expected_next_position"], ascending=[False, False, False, True]).head(top_n)
        _display_df(
            _format_probability_columns(contenders),
            ["title", "artist", "position", "current_num1_streak", "expected_next_position", "num1_probability", "num1_hold_score", "long_reign_probability", "long_reign_watch", "top5_probability", "momentum_score", "similar_cases"],
        )
        st.markdown("**Long-reign watch**")
        reign_watch = forecast.loc[forecast["current_num1_streak"] > 0].sort_values(
            ["long_reign_probability", "current_num1_streak", "num1_hold_score"],
            ascending=[False, False, False],
        ).head(top_n)
        _display_df(
            _format_probability_columns(reign_watch),
            ["title", "artist", "position", "current_num1_streak", "num1_probability", "num1_hold_score", "long_reign_probability", "long_reign_watch", "long_reigns_started_this_year"],
        )
        st.markdown("**Top 10 watch**")
        top10_watch = forecast.loc[(forecast["position"] > 10) | (forecast["top10_probability"] < 0.75)].sort_values("top10_probability", ascending=False).head(top_n)
        _display_df(
            _format_probability_columns(top10_watch),
            ["title", "artist", "position", "expected_next_position", "top10_probability", "dropout_risk", "forecast_note"],
        )
    with c2:
        st.markdown("**Dropout watch**")
        dropouts = forecast.sort_values(["dropout_risk", "position"], ascending=[False, False]).head(top_n)
        _display_df(
            _format_probability_columns(dropouts),
            ["title", "artist", "position", "weeks_on_chart", "move", "dropout_risk", "stay_probability", "similar_cases"],
        )
        st.markdown("**Momentum scores**")
        momentum = forecast.sort_values("momentum_score", ascending=False).head(top_n)
        _display_df(
            _format_probability_columns(momentum),
            ["title", "artist", "position", "move", "weeks_on_chart", "momentum_score", "up_probability", "top10_probability", "dropout_risk"],
        )

    if selected_date < latest_date:
        compare = forecast.merge(
            chart.loc[chart["chart_date"] == selected_date, ["title", "artist", "position", "next_position", "dropped_out_next_week"]],
            on=["title", "artist", "position"],
            how="left",
        )
        present_actual = compare.loc[compare["next_position"].notna()].copy()
        avg_error = float((present_actual["expected_next_position"] - present_actual["next_position"]).abs().mean()) if not present_actual.empty else float("nan")
        actual_num1 = compare.loc[compare["next_position"] == 1, "title"].head(1)
        predicted_num1 = contenders["title"].head(1)
        top3_titles = set(contenders["title"].head(3).tolist())
        st.markdown("### Backtest against the actual next week")
        render_kpis([
            ("Avg rank error", f"{avg_error:.2f}" if pd.notna(avg_error) else "—"),
            ("Predicted #1", predicted_num1.iloc[0] if not predicted_num1.empty else "—"),
            ("Actual #1", actual_num1.iloc[0] if not actual_num1.empty else "—"),
            ("Actual #1 in top 3 contenders", "Yes" if (not actual_num1.empty and actual_num1.iloc[0] in top3_titles) else "No"),
        ])
        compare["rank_error"] = (compare["expected_next_position"] - compare["next_position"]).abs()
        st.markdown("**Backtest detail**")
        _display_df(
            _format_probability_columns(compare.sort_values(["projected_rank", "position"])),
            ["projected_rank", "title", "artist", "position", "expected_next_position", "next_position", "rank_error", "dropout_risk", "dropped_out_next_week"],
        )

    if show_similar and not similar.empty:
        st.markdown("### Similar-case samples")
        options_df = forecast.sort_values("position").copy()
        options_df["label"] = options_df["title"].astype(str) + " — " + options_df["artist"].astype(str)
        sample_source = st.selectbox("Choose current song", options_df["label"].tolist(), key="forecast_lab_similar_song")
        sample_title = sample_source.split(" — ", 1)[0]
        sample = similar.loc[similar["source_title"] == sample_title].copy()
        _display_df(
            sample,
            ["source_current_position", "source_title", "chart_date", "title", "artist", "position", "move", "weeks_on_chart", "next_position", "dropped_out_next_week", "similarity_score"],
        )


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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def admin_song_options() -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "canonical_song"):
        return pd.DataFrame(columns=["canonical_song_id", "canonical_title", "canonical_artist", "chart_weeks"])
    return pd.read_sql_query(
        """
        SELECT
            cs.canonical_song_id,
            cs.canonical_title,
            COALESCE(cs.canonical_full_artist, cs.canonical_artist) AS canonical_artist,
            COUNT(DISTINCT e.entry_id) AS chart_weeks,
            MIN(cw.chart_date) AS first_chart_date,
            MAX(cw.chart_date) AS last_chart_date
        FROM canonical_song cs
        LEFT JOIN entry e ON e.canonical_song_id = cs.canonical_song_id
        LEFT JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        GROUP BY
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.canonical_full_artist
        ORDER BY LOWER(cs.canonical_title), LOWER(COALESCE(cs.canonical_full_artist, cs.canonical_artist)), cs.canonical_song_id
        """,
        conn,
    )

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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



@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def admin_song_artist_credit_defaults(canonical_song_id: int) -> dict[str, str]:
    conn = get_connection()
    out = {
        "canonical_title": "",
        "canonical_full_artist": "",
        "canonical_lead_artist": "",
        "canonical_featured_artist": "",
        "entry_full_artist": "",
        "entry_lead_artist": "",
        "entry_featured_artist": "",
    }
    if not _admin_table_exists(conn, "canonical_song"):
        return out
    row = conn.execute(
        """
        SELECT
            canonical_title,
            COALESCE(canonical_full_artist, canonical_artist, '') AS canonical_full_artist,
            COALESCE(canonical_lead_artist, canonical_artist, '') AS canonical_lead_artist,
            COALESCE(canonical_featured_artist, '') AS canonical_featured_artist
        FROM canonical_song
        WHERE canonical_song_id = ?
        """,
        (canonical_song_id,),
    ).fetchone()
    if row is not None:
        out.update({k: "" if row[k] is None else str(row[k]) for k in row.keys()})

    if _admin_table_exists(conn, "entry"):
        entry_rows = pd.read_sql_query(
            """
            SELECT
                COALESCE(full_artist_display, '') AS full_artist,
                COALESCE(lead_artist_display, artist_display, '') AS lead_artist,
                COALESCE(featured_artist_display, featured_display, '') AS featured_artist
            FROM entry
            WHERE canonical_song_id = ?
            """,
            conn,
            params=(canonical_song_id,),
        )
        for src_col, dest_key in [
            ("full_artist", "entry_full_artist"),
            ("lead_artist", "entry_lead_artist"),
            ("featured_artist", "entry_featured_artist"),
        ]:
            vals = entry_rows[src_col].dropna().astype(str).str.strip() if not entry_rows.empty else pd.Series(dtype=str)
            vals = vals[vals != ""]
            if not vals.empty:
                out[dest_key] = vals.mode().iloc[0]
    return out


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def admin_song_artist_credit_summary(canonical_song_id: int) -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "entry"):
        return pd.DataFrame(columns=["full_artist", "lead_artist", "featured_artist", "entry_count", "first_date", "last_date"])
    return pd.read_sql_query(
        """
        SELECT
            COALESCE(e.full_artist_display, '') AS full_artist,
            COALESCE(e.lead_artist_display, e.artist_display, '') AS lead_artist,
            COALESCE(e.featured_artist_display, e.featured_display, '') AS featured_artist,
            COUNT(*) AS entry_count,
            MIN(cw.chart_date) AS first_date,
            MAX(cw.chart_date) AS last_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE e.canonical_song_id = ?
        GROUP BY full_artist, lead_artist, featured_artist
        ORDER BY entry_count DESC, last_date DESC, full_artist, lead_artist, featured_artist
        """,
        conn,
        params=(canonical_song_id,),
    )


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def admin_song_title_summary(canonical_song_id: int) -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "entry"):
        return pd.DataFrame(columns=["song_title", "entry_count", "first_date", "last_date"])
    return pd.read_sql_query(
        """
        SELECT
            COALESCE(e.song_title_display, '') AS song_title,
            COUNT(*) AS entry_count,
            MIN(cw.chart_date) AS first_date,
            MAX(cw.chart_date) AS last_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE e.canonical_song_id = ?
        GROUP BY song_title
        ORDER BY entry_count DESC, last_date DESC, song_title
        """,
        conn,
        params=(canonical_song_id,),
    )


def _clean_song_title_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = _fold_quotes(text).strip()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _refresh_entry_fts_rows_for_title_artist(
    cur: sqlite3.Cursor,
    rows: list[sqlite3.Row],
    new_title: str,
    new_full_artist: str,
) -> None:
    # entry_fts is a contentless FTS5 table in this app. A normal DELETE is not allowed,
    # so remove old tokens via the special 'delete' command and then insert fresh tokens.
    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO entry_fts(entry_fts, rowid, song_title_display, full_artist_display, normalized_display, raw_slug, source_file)
                VALUES('delete', ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["entry_id"]),
                    row["old_song_title_display"] or "",
                    row["old_full_artist_display"] or "",
                    row["old_normalized_display"] or "",
                    row["raw_slug"] or "",
                    row["source_file"] or "",
                ),
            )
            new_normalized_display = normalize_search_text(
                f"{new_title or ''} {new_full_artist or ''} {row['raw_slug'] or ''}"
            )
            cur.execute(
                """
                INSERT INTO entry_fts(rowid, song_title_display, full_artist_display, normalized_display, raw_slug, source_file)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["entry_id"]),
                    new_title,
                    new_full_artist,
                    new_normalized_display,
                    row["raw_slug"] or "",
                    row["source_file"] or "",
                ),
            )
    except Exception:
        # Search indexing should never make the primary DB cleanup fail.
        pass


def admin_update_song_title_everywhere(
    canonical_song_id: int,
    new_title: str,
    update_alias_titles: bool = False,
) -> tuple[bool, str]:
    new_title = _clean_song_title_text(new_title)
    if not new_title:
        return False, "New song title cannot be blank."
    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"

    new_title_key = normalize_search_text(new_title)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        src = cur.execute(
            """
            SELECT
                canonical_song_id,
                canonical_title,
                COALESCE(canonical_full_artist, canonical_artist, '') AS canonical_artist,
                COALESCE(canonical_artist_key, '') AS canonical_artist_key
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (canonical_song_id,),
        ).fetchone()
        if src is None:
            return False, "Selected canonical song was not found."

        old_title = (src["canonical_title"] or "").strip()
        canonical_artist = (src["canonical_artist"] or "").strip()
        canonical_artist_key = normalize_search_text(src["canonical_artist_key"] or canonical_artist)
        old_title_key = normalize_search_text(old_title)
        title_key_changed = old_title_key != new_title_key

        new_group_key = f"{new_title_key}||{canonical_artist_key}"
        collision = cur.execute(
            """
            SELECT canonical_song_id
            FROM canonical_song
            WHERE canonical_group_key = ?
              AND canonical_song_id <> ?
            LIMIT 1
            """,
            (new_group_key, canonical_song_id),
        ).fetchone()
        if collision is not None:
            return False, "Another canonical song already uses this title + artist key. Merge those songs first, or choose a different title."

        entry_rows = cur.execute(
            """
            SELECT
                e.entry_id,
                e.song_title_display AS old_song_title_display,
                e.full_artist_display AS old_full_artist_display,
                e.normalized_display AS old_normalized_display,
                e.raw_slug,
                cw.source_file
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id = ?
            ORDER BY cw.chart_date, e.position, e.entry_id
            """,
            (canonical_song_id,),
        ).fetchall()
        if not entry_rows:
            return False, "No entry rows are attached to this canonical song."

        cur.execute("BEGIN")
        cur.execute(
            """
            UPDATE canonical_song
            SET canonical_title = ?,
                canonical_title_key = ?,
                canonical_group_key = ?
            WHERE canonical_song_id = ?
            """,
            (new_title, new_title_key, new_group_key, canonical_song_id),
        )
        cur.execute(
            """
            UPDATE entry
            SET song_title_display = ?,
                normalized_song_title = ?,
                canonical_title_key = ?,
                canonical_group_key = ?,
                normalized_display = LOWER(TRIM(? || ' ' || COALESCE(full_artist_display, '') || ' ' || COALESCE(raw_slug, '')))
            WHERE canonical_song_id = ?
            """,
            (new_title, new_title_key, new_title_key, new_group_key, new_title, canonical_song_id),
        )

        if title_key_changed:
            try:
                _insert_song_alias_row(cur, canonical_song_id, old_title, canonical_artist)
            except Exception:
                pass

        if update_alias_titles and _admin_table_exists(conn, "song_alias"):
            alias_rows = cur.execute(
                "SELECT alias_id, alias_artist FROM song_alias WHERE canonical_song_id = ?",
                (canonical_song_id,),
            ).fetchall()
            seen_alias_keys: set[str] = set()
            for alias in alias_rows:
                alias_artist = alias["alias_artist"] or canonical_artist
                alias_title_key = new_title_key
                alias_artist_key = normalize_search_text(alias_artist)
                alias_group_key = f"{alias_title_key}||{alias_artist_key}"
                alias_display_key = normalize_search_text(f"{new_title}||{alias_artist}")
                if alias_display_key in seen_alias_keys:
                    cur.execute("DELETE FROM song_alias WHERE alias_id = ?", (int(alias["alias_id"]),))
                    continue
                seen_alias_keys.add(alias_display_key)
                try:
                    cur.execute(
                        """
                        UPDATE song_alias
                        SET alias_song_title = ?,
                            alias_title_key = ?,
                            alias_group_key = ?,
                            alias_display_key = ?
                        WHERE alias_id = ?
                        """,
                        (new_title, alias_title_key, alias_group_key, alias_display_key, int(alias["alias_id"])),
                    )
                except sqlite3.IntegrityError:
                    cur.execute("DELETE FROM song_alias WHERE alias_id = ?", (int(alias["alias_id"]),))

        _refresh_entry_fts_rows_for_title_artist(cur, entry_rows, new_title, canonical_artist)
        _refresh_canonical_song_rollup(cur, canonical_song_id)
        conn.commit()
        _reset_app_caches()
        if title_key_changed:
            return True, f'Updated title from "{old_title}" to "{new_title}" across {len(entry_rows)} chart entr{"y" if len(entry_rows) == 1 else "ies"}.'
        alias_note = " and alias title strings" if update_alias_titles else ""
        return True, f'Unified the title as "{new_title}" across {len(entry_rows)} chart entr{"y" if len(entry_rows) == 1 else "ies"}{alias_note}.'
    except Exception as exc:
        conn.rollback()
        return False, f"Song title update failed: {exc}"
    finally:
        conn.close()


def _clean_artist_credit_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = _fold_quotes(text).strip()
    text = re.sub(r"\s*;\s*", "; ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ;")


def _build_full_artist_credit(lead_artist: str, featured_artist: str, full_artist: str = "") -> str:
    lead_artist = _clean_artist_credit_text(lead_artist)
    featured_artist = _clean_artist_credit_text(featured_artist)
    full_artist = _clean_artist_credit_text(full_artist)
    if full_artist:
        return full_artist
    parts = [p for p in [lead_artist, featured_artist] if p]
    return "; ".join(parts)


def _refresh_entry_fts_rows(cur: sqlite3.Cursor, rows: list[sqlite3.Row], full_artist: str) -> None:
    # entry_fts is a contentless FTS5 table in this app. A normal DELETE is not allowed,
    # so remove old tokens via the special 'delete' command and then insert fresh tokens.
    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO entry_fts(entry_fts, rowid, song_title_display, full_artist_display, normalized_display, raw_slug, source_file)
                VALUES('delete', ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["entry_id"]),
                    row["song_title_display"] or "",
                    row["old_full_artist_display"] or "",
                    row["old_normalized_display"] or "",
                    row["raw_slug"] or "",
                    row["source_file"] or "",
                ),
            )
            new_normalized_display = normalize_search_text(
                f"{row['song_title_display'] or ''} {full_artist} {row['raw_slug'] or ''}"
            )
            cur.execute(
                """
                INSERT INTO entry_fts(rowid, song_title_display, full_artist_display, normalized_display, raw_slug, source_file)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["entry_id"]),
                    row["song_title_display"] or "",
                    full_artist,
                    new_normalized_display,
                    row["raw_slug"] or "",
                    row["source_file"] or "",
                ),
            )
    except Exception:
        # Search indexing should never make the primary DB cleanup fail.
        pass


def admin_update_song_artist_credits(
    canonical_song_id: int,
    lead_artist: str,
    featured_artist: str,
    full_artist: str,
    update_alias_artists: bool = True,
) -> tuple[bool, str]:
    lead_artist = _clean_artist_credit_text(lead_artist)
    featured_artist = _clean_artist_credit_text(featured_artist)
    full_artist = _build_full_artist_credit(lead_artist, featured_artist, full_artist)
    if not lead_artist:
        return False, "Lead artist cannot be blank."
    if not full_artist:
        return False, "Full artist credit cannot be blank."
    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"

    normalized_lead = normalize_search_text(lead_artist)
    normalized_featured = normalize_search_text(featured_artist)
    normalized_full = normalize_search_text(full_artist)

    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        src = cur.execute(
            """
            SELECT canonical_song_id, canonical_title, canonical_title_key
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (canonical_song_id,),
        ).fetchone()
        if src is None:
            return False, "Selected canonical song was not found."

        entry_rows = cur.execute(
            """
            SELECT
                e.entry_id,
                e.song_title_display,
                e.full_artist_display AS old_full_artist_display,
                e.normalized_display AS old_normalized_display,
                e.raw_slug,
                cw.source_file
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id = ?
            ORDER BY cw.chart_date, e.position, e.entry_id
            """,
            (canonical_song_id,),
        ).fetchall()
        if not entry_rows:
            return False, "No entry rows are attached to this canonical song."

        title_key = src["canonical_title_key"] or normalize_search_text(src["canonical_title"] or "")
        new_group_key = f"{title_key}||{normalized_full}"
        collision = cur.execute(
            """
            SELECT canonical_song_id
            FROM canonical_song
            WHERE canonical_group_key = ?
              AND canonical_song_id <> ?
            LIMIT 1
            """,
            (new_group_key, canonical_song_id),
        ).fetchone()
        if collision is not None:
            return False, "Another canonical song already uses this title + full artist key. Merge those songs first, or choose a different full artist credit."

        cur.execute("BEGIN")
        cur.execute(
            """
            UPDATE canonical_song
            SET canonical_artist = ?,
                canonical_full_artist = ?,
                canonical_lead_artist = ?,
                canonical_featured_artist = ?,
                canonical_artist_key = ?,
                canonical_group_key = ?
            WHERE canonical_song_id = ?
            """,
            (
                full_artist,
                full_artist,
                lead_artist,
                featured_artist,
                normalized_full,
                new_group_key,
                canonical_song_id,
            ),
        )

        cur.execute(
            """
            UPDATE entry
            SET artist_display = ?,
                featured_display = ?,
                full_artist_display = ?,
                lead_artist_display = ?,
                featured_artist_display = ?,
                normalized_artist = ?,
                normalized_featured = ?,
                normalized_full_artist = ?,
                normalized_lead_artist = ?,
                normalized_featured_artist = ?,
                canonical_artist_key = ?,
                canonical_group_key = ?,
                normalized_display = LOWER(TRIM(COALESCE(song_title_display, '') || ' ' || ? || ' ' || COALESCE(raw_slug, '')))
            WHERE canonical_song_id = ?
            """,
            (
                lead_artist,
                featured_artist,
                full_artist,
                lead_artist,
                featured_artist,
                normalized_lead,
                normalized_featured,
                normalized_full,
                normalized_lead,
                normalized_featured,
                normalized_full,
                new_group_key,
                full_artist,
                canonical_song_id,
            ),
        )

        if update_alias_artists and _admin_table_exists(conn, "song_alias"):
            alias_rows = cur.execute(
                "SELECT alias_id, alias_song_title FROM song_alias WHERE canonical_song_id = ?",
                (canonical_song_id,),
            ).fetchall()
            seen_alias_keys: set[str] = set()
            for alias in alias_rows:
                alias_title = alias["alias_song_title"] or src["canonical_title"] or ""
                alias_title_key = normalize_search_text(alias_title)
                alias_artist_key = normalized_full
                alias_group_key = f"{alias_title_key}||{alias_artist_key}"
                alias_display_key = normalize_search_text(f"{alias_title}||{full_artist}")
                if alias_display_key in seen_alias_keys:
                    cur.execute("DELETE FROM song_alias WHERE alias_id = ?", (int(alias["alias_id"]),))
                    continue
                seen_alias_keys.add(alias_display_key)
                try:
                    cur.execute(
                        """
                        UPDATE song_alias
                        SET alias_artist = ?,
                            alias_artist_key = ?,
                            alias_group_key = ?,
                            alias_display_key = ?
                        WHERE alias_id = ?
                        """,
                        (full_artist, alias_artist_key, alias_group_key, alias_display_key, int(alias["alias_id"])),
                    )
                except sqlite3.IntegrityError:
                    cur.execute("DELETE FROM song_alias WHERE alias_id = ?", (int(alias["alias_id"]),))

        try:
            _insert_song_alias_row(cur, canonical_song_id, src["canonical_title"] or "", full_artist)
        except Exception:
            pass

        _refresh_entry_fts_rows(cur, entry_rows, full_artist)
        _refresh_canonical_song_rollup(cur, canonical_song_id)
        conn.commit()
        _reset_app_caches()
        return True, f'Updated artist credits for "{src["canonical_title"]}" across {len(entry_rows)} chart entr{"y" if len(entry_rows) == 1 else "ies"}.'
    except Exception as exc:
        conn.rollback()
        return False, f"Artist credit update failed: {exc}"
    finally:
        conn.close()

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
    alias_title_key = normalize_search_text(old_title)
    alias_artist_key = normalize_search_text(old_artist)
    alias_group_key = f"{alias_title_key}||{alias_artist_key}"
    if "alias_title_key" in cols:
        payload["alias_title_key"] = alias_title_key
    if "alias_artist_key" in cols:
        payload["alias_artist_key"] = alias_artist_key
    if "alias_group_key" in cols:
        payload["alias_group_key"] = alias_group_key
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


def _recalculate_derived_markers_with_cursor(cur: sqlite3.Cursor) -> tuple[int, int, int]:
    """Rebuild DEBUT / TOP DEBUT / RE-ENTRY flags from current canonical_song_id assignments."""
    rows = cur.execute(
        """
        SELECT
            e.entry_id,
            e.chart_week_id,
            e.position,
            e.canonical_song_id,
            e.normalized_song_title,
            e.normalized_full_artist,
            cw.chart_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        ORDER BY cw.chart_date, e.position, e.entry_id
        """
    ).fetchall()

    by_week: dict[int, list[sqlite3.Row]] = {}
    week_order: list[int] = []
    for row in rows:
        week_id = int(row["chart_week_id"])
        if week_id not in by_week:
            by_week[week_id] = []
            week_order.append(week_id)
        by_week[week_id].append(row)

    def _entry_song_key(row: sqlite3.Row) -> str:
        canonical_song_id = row["canonical_song_id"]
        if canonical_song_id is not None:
            return f"cs:{int(canonical_song_id)}"
        title_key = normalize_search_text(row["normalized_song_title"])
        artist_key = normalize_search_text(row["normalized_full_artist"])
        if title_key and artist_key:
            return f"fallback:{title_key}||{artist_key}"
        return f"entry:{int(row['entry_id'])}"

    seen_song_keys: set[str] = set()
    previous_week_song_keys: set[str] = set()
    updates: list[tuple[int, int, int, object, int]] = []
    debut_count = 0
    top_debut_count = 0
    reentry_count = 0

    for week_id in week_order:
        week_rows = by_week[week_id]
        week_updates: list[dict[str, object]] = []
        current_week_song_keys: set[str] = set()

        for row in week_rows:
            song_key = _entry_song_key(row)
            current_week_song_keys.add(song_key)
            is_debut = song_key not in seen_song_keys
            is_reentry = (not is_debut) and song_key not in previous_week_song_keys
            week_updates.append(
                {
                    "entry_id": int(row["entry_id"]),
                    "position": int(row["position"] or 9999),
                    "is_debut": bool(is_debut),
                    "is_top_debut": False,
                    "is_reentry": bool(is_reentry),
                    "marker": None,
                }
            )

        debut_rows = [u for u in week_updates if u["is_debut"]]
        if debut_rows:
            top_debut = min(debut_rows, key=lambda u: (int(u["position"]), int(u["entry_id"])))
            top_debut["is_top_debut"] = True

        for u in week_updates:
            if u["is_debut"]:
                debut_count += 1
                if u["is_top_debut"]:
                    top_debut_count += 1
                    u["marker"] = "TOP DEBUT"
                else:
                    u["marker"] = "DEBUT"
            elif u["is_reentry"]:
                reentry_count += 1
                u["marker"] = "RE-ENTRY"

            updates.append(
                (
                    1 if u["is_debut"] else 0,
                    1 if u["is_top_debut"] else 0,
                    1 if u["is_reentry"] else 0,
                    u["marker"],
                    int(u["entry_id"]),
                )
            )

        seen_song_keys.update(current_week_song_keys)
        previous_week_song_keys = current_week_song_keys

    cur.executemany(
        """
        UPDATE entry
        SET derived_is_debut = ?,
            derived_is_top_debut = ?,
            derived_is_reentry = ?,
            derived_marker = ?
        WHERE entry_id = ?
        """,
        updates,
    )
    return len(updates), debut_count, reentry_count


def admin_recalculate_derived_markers() -> tuple[bool, str]:
    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        total, debut_count, reentry_count = _recalculate_derived_markers_with_cursor(cur)
        conn.commit()
        _reset_app_caches()
        return True, f"Recalculated derived markers for {total:,} chart entries ({debut_count:,} debuts, {reentry_count:,} re-entries)."
    except Exception as exc:
        conn.rollback()
        return False, f"Derived marker recalculation failed: {exc}"
    finally:
        conn.close()


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
        _recalculate_derived_markers_with_cursor(cur)

        conn.commit()
        _reset_app_caches()
        return True, f'Merged "{src["canonical_title"]}" into "{tgt["canonical_title"]}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Song merge failed: {exc}"
    finally:
        conn.close()




@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def admin_entries_for_canonical_song(canonical_song_id: int) -> pd.DataFrame:
    conn = get_connection()
    if not _admin_table_exists(conn, "entry"):
        return pd.DataFrame(columns=["entry_id", "chart_date", "position", "song", "artist", "lead_artist", "featured_artist", "derived_marker"])
    return pd.read_sql_query(
        """
        SELECT
            e.entry_id,
            cw.chart_date,
            e.position,
            e.song_title_display AS song,
            e.full_artist_display AS artist,
            e.lead_artist_display AS lead_artist,
            e.featured_artist_display AS featured_artist,
            e.derived_marker
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE e.canonical_song_id = ?
        ORDER BY cw.chart_date, e.position, e.entry_id
        """,
        conn,
        params=(canonical_song_id,),
    )


def _song_key_parts(title: object, artist: object) -> tuple[str, str, str]:
    title_key = normalize_search_text(title)
    artist_key = normalize_search_text(artist)
    group_key = f"{title_key}||{artist_key}"
    return title_key, artist_key, group_key


def _create_canonical_song_row(cur: sqlite3.Cursor, title: str, artist: str) -> int:
    title = (title or "").strip()
    artist = (artist or "").strip()
    title_key, artist_key, group_key = _song_key_parts(title, artist)
    cur.execute(
        """
        INSERT INTO canonical_song (
            canonical_title,
            canonical_artist,
            canonical_title_key,
            canonical_artist_key,
            canonical_group_key,
            entry_count,
            alias_count,
            first_chart_date,
            last_chart_date,
            canonical_full_artist,
            canonical_lead_artist,
            canonical_featured_artist
        ) VALUES (?, ?, ?, ?, ?, 0, 0, NULL, NULL, ?, ?, '')
        """,
        (title, artist, title_key, artist_key, group_key, artist, artist),
    )
    return int(cur.lastrowid)


def admin_split_canonical_song(source_canonical_song_id: int, entry_ids: list[int], new_title: str, new_artist: str) -> tuple[bool, str]:
    entry_ids = [int(x) for x in (entry_ids or [])]
    new_title = (new_title or "").strip()
    new_artist = (new_artist or "").strip()

    if not entry_ids:
        return False, "Choose at least one chart entry to split out."
    if not new_title:
        return False, "New canonical song title cannot be blank."
    if not new_artist:
        return False, "New canonical artist cannot be blank."
    if not Path(DB_PATH).exists():
        return False, f"Database not found: {DB_PATH}"

    placeholders = ",".join("?" for _ in entry_ids)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        src = cur.execute(
            """
            SELECT canonical_song_id, canonical_title, COALESCE(canonical_full_artist, canonical_artist) AS canonical_artist
            FROM canonical_song
            WHERE canonical_song_id = ?
            """,
            (source_canonical_song_id,),
        ).fetchone()
        if src is None:
            return False, "Source canonical song was not found."

        selected = cur.execute(
            f"""
            SELECT
                e.entry_id,
                e.song_title_display,
                e.full_artist_display,
                cw.chart_date,
                e.position
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id = ?
              AND e.entry_id IN ({placeholders})
            ORDER BY cw.chart_date, e.position, e.entry_id
            """,
            (source_canonical_song_id, *entry_ids),
        ).fetchall()
        if len(selected) != len(set(entry_ids)):
            return False, "One or more selected entries no longer belongs to the source canonical song."

        title_key, artist_key, group_key = _song_key_parts(new_title, new_artist)

        cur.execute("BEGIN")
        new_canonical_song_id = _create_canonical_song_row(cur, new_title, new_artist)

        cur.execute(
            f"""
            UPDATE entry
            SET canonical_song_id = ?,
                canonical_title_key = ?,
                canonical_artist_key = ?,
                canonical_group_key = ?
            WHERE canonical_song_id = ?
              AND entry_id IN ({placeholders})
            """,
            (new_canonical_song_id, title_key, artist_key, group_key, source_canonical_song_id, *entry_ids),
        )

        # Preserve the selected visible variants as aliases for the new canonical song.
        for row in selected:
            try:
                _insert_song_alias_row(
                    cur,
                    new_canonical_song_id,
                    (row["song_title_display"] or "").strip() or new_title,
                    (row["full_artist_display"] or "").strip() or new_artist,
                )
            except Exception:
                pass

        # Also keep the new canonical label as an alias, which helps future imports land here.
        try:
            _insert_song_alias_row(cur, new_canonical_song_id, new_title, new_artist)
        except Exception:
            pass

        _refresh_canonical_song_rollup(cur, source_canonical_song_id)
        _refresh_canonical_song_rollup(cur, new_canonical_song_id)
        _recalculate_derived_markers_with_cursor(cur)

        conn.commit()
        _reset_app_caches()
        return True, f'Split {len(selected)} chart entr{"y" if len(selected) == 1 else "ies"} from "{src["canonical_title"]}" into new canonical song "{new_title}".'
    except Exception as exc:
        conn.rollback()
        return False, f"Song split failed: {exc}"
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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




@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
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
        ["Songs", "Artists", "Data Quality", "Data Health / Import QA", "Maintenance"],
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

            st.markdown("#### Edit song title for this canonical song")
            st.caption("Use this when the title itself is wrong or inconsistent. This updates every chart entry attached to the selected canonical song, plus canonical title keys and search tokens, so Canonical Song History, Artist History, Analytics, search, and Admin labels all see the same title.")
            title_summary = admin_song_title_summary(selected_song_id)
            if not title_summary.empty:
                with st.expander("Current title variants attached to this song", expanded=False):
                    _display_df(title_summary)
            title_edit_value = st.text_input(
                "Correct song title display",
                value=str(selected_row["canonical_title"] or ""),
                key="admin_song_title_everywhere",
            )
            update_alias_titles = st.checkbox(
                "Also rewrite alias song-title strings for this canonical song",
                value=False,
                key="admin_song_title_update_aliases",
                help="Leave this off if the alias variants are legitimate historical/source-title variants. Turn it on when the aliases themselves are wrong and should all match the corrected title.",
            )
            if st.button("Update song title everywhere", key="admin_song_title_update_btn"):
                ok, msg = admin_update_song_title_everywhere(
                    selected_song_id,
                    title_edit_value,
                    update_alias_titles,
                )
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


            st.markdown("#### Edit artist credits for this canonical song")
            st.caption("Use this when a song's artist credit is wrong or inconsistent. This updates every chart entry attached to the selected canonical song, plus the canonical-song artist fields, so Artist History, Analytics, search, and Admin labels all see the same credit.")
            credit_defaults = admin_song_artist_credit_defaults(selected_song_id)
            credit_summary = admin_song_artist_credit_summary(selected_song_id)
            if not credit_summary.empty:
                with st.expander("Current artist-credit variants attached to this song", expanded=False):
                    _display_df(credit_summary)
            credit_cols = st.columns([1.2, 1.2, 1.2])
            edit_lead_artist = credit_cols[0].text_input(
                "Lead artist display",
                value=credit_defaults.get("canonical_lead_artist") or credit_defaults.get("entry_lead_artist") or "",
                key="admin_song_credit_lead",
            )
            edit_featured_artist = credit_cols[1].text_input(
                "Featured artist display",
                value=credit_defaults.get("canonical_featured_artist") or credit_defaults.get("entry_featured_artist") or "",
                key="admin_song_credit_featured",
            )
            edit_full_artist = credit_cols[2].text_input(
                "Full artist display",
                value=credit_defaults.get("canonical_full_artist") or credit_defaults.get("entry_full_artist") or "",
                key="admin_song_credit_full",
                help="Leave this matching the exact full credit you want shown. If blank, the app combines Lead + Featured with a semicolon.",
            )
            update_alias_artists = st.checkbox(
                "Also update alias artist strings for this canonical song",
                value=True,
                key="admin_song_credit_update_aliases",
            )
            if st.button("Update artist credits for this song", key="admin_song_credit_update_btn"):
                ok, msg = admin_update_song_artist_credits(
                    selected_song_id,
                    edit_lead_artist,
                    edit_featured_artist,
                    edit_full_artist,
                    update_alias_artists,
                )
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

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

            st.markdown("#### Split selected chart entries into a new canonical song")
            st.caption("Use this when a canonical song accidentally contains entries from two different songs. The selected chart rows are moved to a newly created canonical_song row; the original canonical song is kept for the unselected rows.")
            split_entries = admin_entries_for_canonical_song(selected_song_id)
            if split_entries.empty:
                st.caption("No chart entries are attached to this canonical song.")
            else:
                _display_df(split_entries, ["entry_id", "chart_date", "position", "song", "artist", "lead_artist", "featured_artist", "derived_marker"])
                split_label_map = {
                    f"{row.chart_date} | #{int(row.position)} | {row.song} — {row.artist} | entry {int(row.entry_id)}": int(row.entry_id)
                    for row in split_entries.itertuples(index=False)
                }
                split_labels = st.multiselect(
                    "Entries to split out",
                    list(split_label_map.keys()),
                    key="admin_song_split_entries",
                )
                default_split_title = str(selected_row["canonical_title"] or "")
                default_split_artist = str(selected_row["canonical_artist"] or "")
                split_cols = st.columns([1.2, 1.2])
                split_title = split_cols[0].text_input(
                    "New canonical title for selected entries",
                    value=default_split_title,
                    key="admin_song_split_title",
                )
                split_artist = split_cols[1].text_input(
                    "New canonical artist for selected entries",
                    value=default_split_artist,
                    key="admin_song_split_artist",
                )
                if st.button("Split selected entries", key="admin_song_split_btn"):
                    selected_entry_ids = [split_label_map[label] for label in split_labels]
                    ok, msg = admin_split_canonical_song(selected_song_id, selected_entry_ids, split_title, split_artist)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            st.markdown("#### Repair derived debut/re-entry markers")
            st.caption("Use this after manual song cleanup if DEBUT, TOP DEBUT, or RE-ENTRY labels look stale. Merge and split actions also run this automatically.")
            if st.button("Recalculate debut/re-entry markers now", key="admin_song_recalc_markers_btn"):
                ok, msg = admin_recalculate_derived_markers()
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

    elif admin_section == "Data Health / Import QA":
        render_data_health_tab()

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
    min_date, max_date = load_analytics_date_bounds()
    if min_date is None or max_date is None:
        st.info("No analytics data is available in the database yet.")
        return

    controls = st.columns([1.4, 1.4, 1.0, 1.0])
    start_date = controls[0].date_input("Start date", value=min_date, min_value=min_date, max_value=max_date, key="analytics_start")
    end_date = controls[1].date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="analytics_end")
    include_reentries = controls[2].checkbox("Include re-entries", value=True, key="analytics_include_reentries")
    min_weeks = int(controls[3].number_input("Min weeks on chart", min_value=1, max_value=500, value=1, step=1, key="analytics_min_weeks"))

    section_cols = st.columns([1.8, 1.0, 1.0, 1.1])
    section = section_cols[0].selectbox("Analytics section", ANALYTICS_SECTIONS, key="analytics_section")
    top_n = int(section_cols[1].slider("Top N rows", 5, 100, 25, 5, key="analytics_top_n"))
    chart_key = "analytics_show_charts_" + re.sub(r"[^a-z0-9]+", "_", section.lower()).strip("_")
    show_charts = section_cols[2].checkbox("Load charts", value=False, key=chart_key)
    load_section_key = "analytics_load_section_" + re.sub(r"[^a-z0-9]+", "_", section.lower()).strip("_")
    load_section = section_cols[3].checkbox("Load section", value=False, key=load_section_key)

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    if not load_section:
        st.caption("Analytics section data is paused. Turn on 'Load section' when you want to render the selected Analytics view.")
        return

    pkg = _analytics_pkg_for_section(section, start_date, end_date, include_reentries, min_weeks)

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



def _weekly_top_artist_scores_from_chart(df_chart: pd.DataFrame, credit_mode: str) -> pd.DataFrame:
    """Build a normalized weekly artist leaderboard from chart rows already in memory."""
    if df_chart.empty:
        return pd.DataFrame()

    credits = build_artist_credit_rows(df_chart)
    if credits.empty:
        return pd.DataFrame()

    credits = credits.copy()
    credits["position"] = pd.to_numeric(credits["position"], errors="coerce")
    credits = credits.loc[credits["position"].between(1, 40, inclusive="both")].copy()
    if credits.empty:
        return pd.DataFrame()

    if credit_mode == "Lead artists only":
        credits = credits.loc[credits["artist_role_mode"] == "Lead"].copy()
        credits["credit_multiplier"] = 1.0
    else:
        credits["credit_multiplier"] = credits["artist_role_mode"].map({"Lead": 1.0, "Featured": 0.5}).fillna(0.0)
        credits = credits.loc[credits["credit_multiplier"] > 0].copy()

    if credits.empty:
        return pd.DataFrame()

    credits["base_points"] = (42 - credits["position"]).map(lambda v: math.log(float(v)) if pd.notna(v) and float(v) > 0 else 0.0)
    credits["weighted_points"] = credits["base_points"] * credits["credit_multiplier"]
    credits["lead_song_key"] = credits["song_key"].where(credits["artist_role_mode"].eq("Lead"))
    credits["featured_song_key"] = credits["song_key"].where(credits["artist_role_mode"].eq("Featured"))
    credits["top10_song_key"] = credits["song_key"].where(credits["position"].le(10))
    credits["num1_song_key"] = credits["song_key"].where(credits["position"].eq(1))

    top_rows = (
        credits.sort_values(["artist_key", "position", "weighted_points", "title"], ascending=[True, True, False, True])
        .drop_duplicates("artist_key")
        [["artist_key", "title", "artist", "position"]]
        .rename(columns={"title": "top_song", "position": "top_position", "artist": "display_artist"})
    )

    grouped = credits.groupby("artist_key", dropna=True).agg(
        raw_score=("weighted_points", "sum"),
        songs=("song_key", "nunique"),
        lead_songs=("lead_song_key", "nunique"),
        featured_songs=("featured_song_key", "nunique"),
        top_10_songs=("top10_song_key", "nunique"),
        num1_songs=("num1_song_key", "nunique"),
        best_position=("position", "min"),
    ).reset_index()

    out = grouped.merge(top_rows, on="artist_key", how="left")
    out["artist"] = out.apply(lambda r: preferred_artist_display(r["artist_key"], r.get("display_artist", "")), axis=1)
    max_raw = float(out["raw_score"].max()) if not out.empty else 0.0
    out["score"] = (out["raw_score"] / max_raw * 100.0) if max_raw > 0 else 0.0
    out = out.sort_values(
        ["score", "best_position", "lead_songs", "songs", "artist"],
        ascending=[False, True, False, False, True],
    ).reset_index(drop=True)
    out["rank"] = out.index + 1
    out["score"] = out["score"].round(1)
    out["raw_score"] = out["raw_score"].round(3)
    return out[[
        "rank", "artist", "score", "raw_score", "songs", "lead_songs", "featured_songs",
        "top_song", "top_position", "top_10_songs", "num1_songs", "artist_key", "best_position",
    ]]


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_weekly_top_artist_scores(chart_date: str, credit_mode: str) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame()
    selected = pd.to_datetime(chart_date)
    week = chart.loc[chart["chart_date"].eq(selected)].copy()
    return _weekly_top_artist_scores_from_chart(week, credit_mode)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_weekly_top_artist_history(credit_mode: str) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame()
    rows: list[pd.DataFrame] = []
    for chart_date, week in chart.groupby("chart_date", sort=True):
        scores = _weekly_top_artist_scores_from_chart(week, credit_mode)
        if scores.empty:
            continue
        scores = scores.copy()
        scores.insert(0, "chart_date", chart_date)
        rows.append(scores)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _weekly_artist_num1_streaks(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    winners = history.loc[history["rank"].eq(1)].copy().sort_values("chart_date")
    if winners.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    cur_key = None
    cur_artist = ""
    start_date = None
    end_date = None
    weeks = 0
    songs: set[str] = set()

    for rec in winners.to_dict("records"):
        artist_key = rec.get("artist_key")
        if artist_key == cur_key:
            weeks += 1
            end_date = rec.get("chart_date")
        else:
            if cur_key is not None:
                rows.append({
                    "artist": cur_artist,
                    "weeks": weeks,
                    "start_date": start_date,
                    "end_date": end_date,
                    "songs_during_streak": "; ".join(sorted(s for s in songs if s)),
                })
            cur_key = artist_key
            cur_artist = rec.get("artist", "")
            start_date = rec.get("chart_date")
            end_date = rec.get("chart_date")
            weeks = 1
            songs = set()
        top_song = str(rec.get("top_song", "") or "")
        if top_song:
            songs.add(top_song)

    if cur_key is not None:
        rows.append({
            "artist": cur_artist,
            "weeks": weeks,
            "start_date": start_date,
            "end_date": end_date,
            "songs_during_streak": "; ".join(sorted(s for s in songs if s)),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["weeks", "end_date", "artist"], ascending=[False, False, True]).reset_index(drop=True)


def render_weekly_top_artists_tab() -> None:
    st.subheader("Weekly Top Artists")
    st.caption(
        "Artist scores use ln(42 − chart position), then normalize each chart week so that the #1 artist receives 100 points. "
        "In weighted mode, lead artists receive full credit and featured artists receive half credit."
    )

    chart_dates = load_chart_dates()
    if not chart_dates:
        st.info("No chart weeks are available yet.")
        return

    sorted_dates = sorted(chart_dates)
    controls = st.columns([1.4, 1.4, 1.0])
    selected_date = controls[0].selectbox(
        "Chart week",
        sorted_dates,
        index=len(sorted_dates) - 1,
        key="weekly_top_artists_chart_date",
    )
    credit_mode = controls[1].radio(
        "Credit mode",
        ["Lead + featured artists, weighted", "Lead artists only"],
        horizontal=False,
        key="weekly_top_artists_credit_mode",
    )
    top_n = int(controls[2].slider("Top N rows", 5, 100, 25, 5, key="weekly_top_artists_top_n"))

    view = st.selectbox(
        "Weekly Top Artists view",
        [
            "Weekly chart",
            "Artist #1 weeks",
            "Most artist #1s",
            "Longest #1 streaks",
            "Biggest artist weeks",
        ],
        key="weekly_top_artists_view",
    )

    weekly_scores = build_weekly_top_artist_scores(selected_date, credit_mode)
    if weekly_scores.empty:
        st.info("No artist-credit rows were available for that chart week.")
        return

    top_artist = weekly_scores.iloc[0]
    runner_up = weekly_scores.iloc[1] if len(weekly_scores) > 1 else None
    render_kpis([
        ("Chart week", selected_date),
        ("#1 artist", top_artist["artist"]),
        ("#1 raw score", f'{float(top_artist["raw_score"]):.3f}'),
        ("Runner-up", runner_up["artist"] if runner_up is not None else "—"),
        ("Artists ranked", len(weekly_scores)),
    ])

    display_cols = ["rank", "artist", "score", "raw_score", "songs", "lead_songs", "featured_songs", "top_song", "top_position", "top_10_songs", "num1_songs"]

    if view == "Weekly chart":
        st.markdown("**Weekly artist chart**")
        _display_df(weekly_scores.head(top_n), display_cols)
        return

    # The all-time artist-history table is the heaviest part of this section, so
    # build it only for views that actually need it. This avoids the old st.tabs
    # behavior where every tab's content could execute during a rerun.
    history = build_weekly_top_artist_history(credit_mode)
    if history.empty:
        st.info("No weekly artist history rows are available yet.")
        return

    winners = history.loc[history["rank"].eq(1)].copy().sort_values("chart_date", ascending=False)
    if not winners.empty:
        winners["year"] = pd.to_datetime(winners["chart_date"]).dt.year

    if view == "Artist #1 weeks":
        st.markdown("**Weekly #1 artists**")
        if winners.empty:
            st.info("No weekly #1 artist rows are available yet.")
        else:
            year_options = ["All"] + [str(y) for y in sorted(winners["year"].dropna().astype(int).unique(), reverse=True)]
            selected_year = st.selectbox(
                "Year",
                year_options,
                key="weekly_top_artists_num1_year",
            )
            display_winners = winners.copy()
            if selected_year != "All":
                display_winners = display_winners.loc[display_winners["year"].eq(int(selected_year))].copy()
                display_winners = display_winners.sort_values(["chart_date", "artist"], ascending=[True, True])
            st.caption(f"Showing {len(display_winners):,} weekly #1 artist row(s). This view ignores the Top N rows slider.")
            _display_df(display_winners, ["chart_date", "artist", "raw_score", "songs", "lead_songs", "featured_songs", "top_song", "top_position", "top_10_songs", "num1_songs"])
        return

    if view == "Most artist #1s":
        st.markdown("**Most weeks at #1 on the weekly artist chart**")
        if winners.empty:
            st.info("No weekly #1 artist rows are available yet.")
            return
        leaders = (
            winners.groupby(["artist_key", "artist"], dropna=True)
            .agg(
                artist_num1_weeks=("chart_date", "count"),
                first_num1_week=("chart_date", "min"),
                last_num1_week=("chart_date", "max"),
                best_raw_score=("raw_score", "max"),
                max_songs=("songs", "max"),
            )
            .reset_index()
            .sort_values(["artist_num1_weeks", "last_num1_week", "artist"], ascending=[False, False, True])
        )
        _display_df(leaders.head(top_n), ["artist", "artist_num1_weeks", "first_num1_week", "last_num1_week", "best_raw_score", "max_songs"])
        return

    if view == "Longest #1 streaks":
        st.markdown("**Longest consecutive runs as the weekly #1 artist**")
        streaks = _weekly_artist_num1_streaks(history)
        _display_df(streaks.head(top_n), ["artist", "weeks", "start_date", "end_date", "songs_during_streak"])
        return

    st.markdown("**Biggest raw artist weeks**")
    biggest = history.sort_values(["raw_score", "score", "songs", "chart_date"], ascending=[False, False, False, False]).copy()
    _display_df(biggest.head(top_n), ["chart_date", "rank", "artist", "score", "raw_score", "songs", "lead_songs", "featured_songs", "top_song", "top_position", "top_10_songs", "num1_songs"])

def render_forecast_lab_tab() -> None:
    st.subheader("Forecast Lab")
    st.caption("Choose between the historical neighbor forecast and the imported Last.fm play-data view.")

    forecast_mode = st.radio(
        "Forecast mode",
        ["Historical chart model", "Last.fm play data model"],
        horizontal=True,
        key="forecast_lab_mode",
    )
    top_n = int(st.slider("Top N rows", 5, 100, 25, 5, key="forecast_lab_top_n"))

    if forecast_mode == "Last.fm play data model":
        _render_lastfm_forecast_lab(top_n)
        return

    base = load_analytics_base()
    if base.empty:
        st.info("No chart data is available in the database yet.")
        return
    _render_forecast_lab({"chart": base}, top_n)


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

def _week_row_label(row: pd.Series | None) -> str:
    if row is None:
        return "—"
    song = str(row.get("song", "") or "").strip()
    artist = str(row.get("artist", "") or "").strip()
    if song and artist:
        return f"{song} — {artist}"
    return song or artist or "—"


def _marker_contains(df: pd.DataFrame, text: str) -> pd.Series:
    if df.empty or "derived_marker" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["derived_marker"].fillna("").astype(str).str.upper().str.contains(text.upper(), regex=False)


def _week_identity_set(df: pd.DataFrame) -> set[str]:
    if df.empty or "song_identity" not in df.columns:
        return set()
    return set(df["song_identity"].dropna().astype(str).tolist())


def _format_movement(value: object) -> str:
    iv = _safe_int(value)
    if iv is None:
        return "—"
    if iv > 0:
        return f"+{iv}"
    return str(iv)


def _week_browser_summary(df: pd.DataFrame, previous_df: pd.DataFrame | None) -> dict[str, object]:
    if df.empty:
        return {
            "number_one": "—",
            "top_debut": "—",
            "biggest_climber": "—",
            "biggest_faller": "—",
            "debuts": 0,
            "reentries": 0,
            "dropouts": 0,
        }

    number_one_row = df.sort_values("position").head(1).iloc[0]

    debuts = df.loc[_marker_contains(df, "DEBUT")].sort_values("position")
    top_debuts = df.loc[_marker_contains(df, "TOP DEBUT")].sort_values("position")
    top_debut_row = None
    if not top_debuts.empty:
        top_debut_row = top_debuts.iloc[0]
    elif not debuts.empty:
        top_debut_row = debuts.iloc[0]

    movers = df.loc[df["movement"].notna()].copy() if "movement" in df.columns else pd.DataFrame()
    climbers = movers.loc[movers["movement"] > 0].sort_values(["movement", "position"], ascending=[False, True]) if not movers.empty else pd.DataFrame()
    fallers = movers.loc[movers["movement"] < 0].sort_values(["movement", "position"], ascending=[True, True]) if not movers.empty else pd.DataFrame()

    previous_count = 0
    if previous_df is not None and not previous_df.empty:
        current_ids = _week_identity_set(df)
        previous_count = int(~previous_df["song_identity"].astype(str).isin(current_ids).sum()) if False else 0
        previous_count = int(previous_df.loc[~previous_df["song_identity"].astype(str).isin(current_ids)].shape[0])

    return {
        "number_one": _week_row_label(number_one_row),
        "top_debut": _week_row_label(top_debut_row),
        "biggest_climber": (
            f"{_week_row_label(climbers.iloc[0])} ({_format_movement(climbers.iloc[0]['movement'])})"
            if not climbers.empty else "—"
        ),
        "biggest_faller": (
            f"{_week_row_label(fallers.iloc[0])} ({_format_movement(fallers.iloc[0]['movement'])})"
            if not fallers.empty else "—"
        ),
        "debuts": int(_marker_contains(df, "DEBUT").sum()),
        "reentries": int(_marker_contains(df, "RE-ENTRY").sum()),
        "dropouts": previous_count,
    }


def _week_browser_display_table(df: pd.DataFrame) -> pd.DataFrame:
    visible_cols = [
        "position",
        "last_week_position",
        "weeks_on_chart",
        "movement",
        "song",
        "artist",
        "derived_marker",
        "peak_position",
        "week_hit_peak",
        "canonical_song_id",
    ]
    out = df[[c for c in visible_cols if c in df.columns]].copy()
    if "movement" in out.columns:
        out["movement"] = out["movement"].apply(_format_movement)
    rename_map = {
        "position": "Position",
        "last_week_position": "Last Week",
        "movement": "Movement",
        "weeks_on_chart": "Weeks",
        "song": "Song",
        "artist": "Artist",
        "derived_marker": "Marker",
        "peak_position": "Peak Position",
        "week_hit_peak": "Week Hit Peak",
        "canonical_song_id": "Canonical Song ID",
    }
    return out.rename(columns=rename_map)


def _week_artist_appearances(df: pd.DataFrame) -> pd.DataFrame:
    """Count artist appearances on the selected chart week using lead/featured credits when available."""
    if df.empty:
        return pd.DataFrame(columns=["Artist", "Appearances"])

    rows: list[dict[str, object]] = []
    for row in df.to_dict("records"):
        entry_key = row.get("song_identity") or row.get("canonical_song_id") or row.get("song")
        pairs = []
        pairs.extend(_split_credit_people(None, row.get("lead_artist")))
        pairs.extend(_split_credit_people(None, row.get("featured_artist")))
        if not pairs:
            pairs.extend(_split_credit_people(None, row.get("artist")))

        seen_in_entry: set[str] = set()
        for artist_key, artist in pairs:
            resolved = resolve_artist_key_alias(artist_key)
            if resolved is None or (isinstance(resolved, float) and pd.isna(resolved)):
                continue
            resolved_key = normalize_search_text(resolved)
            if not resolved_key or resolved_key in seen_in_entry:
                continue
            seen_in_entry.add(resolved_key)
            rows.append({
                "artist_key": resolved_key,
                "artist": preferred_artist_display(resolved_key, artist),
                "entry_key": entry_key,
            })

    if not rows:
        return pd.DataFrame(columns=["Artist", "Appearances"])

    artists = pd.DataFrame(rows)
    out = (
        artists.groupby("artist_key", dropna=True)
        .agg(
            Artist=("artist", lambda s: s.dropna().astype(str).mode().iloc[0] if not s.dropna().empty else ""),
            Appearances=("entry_key", "nunique"),
        )
        .reset_index(drop=True)
        .sort_values(["Appearances", "Artist"], ascending=[False, True])
        .head(5)
    )
    return out


def _render_week_summary_expander(summary: dict[str, object]) -> None:
    summary_rows = pd.DataFrame([
        {"Metric": "#1 song", "Value": summary["number_one"]},
        {"Metric": "Top debut", "Value": summary["top_debut"]},
        {"Metric": "Dropouts", "Value": summary["dropouts"]},
        {"Metric": "Debuts", "Value": summary["debuts"]},
        {"Metric": "Re-entries", "Value": summary["reentries"]},
        {"Metric": "Biggest climber", "Value": summary["biggest_climber"]},
        {"Metric": "Biggest faller", "Value": summary["biggest_faller"]},
    ])
    # Keep the compact summary Arrow-friendly: this column mixes text labels
    # and numeric counts, so render all values as strings.
    summary_rows["Value"] = summary_rows["Value"].map(lambda v: "—" if pd.isna(v) else str(v))
    with st.expander("Quick week summary"):
        st.markdown(
            """
            <style>
            div[data-testid="stExpander"] div[data-testid="stDataFrame"] {
                font-size: 0.85rem;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        _display_df(summary_rows)


def _render_week_detail_tables(df: pd.DataFrame, previous_df: pd.DataFrame | None, selected_date: str) -> None:
    if df.empty:
        st.info("No chart rows are available for this week.")
        return

    current_ids = _week_identity_set(df)

    if previous_df is not None and not previous_df.empty and current_ids:
        dropouts = previous_df.loc[~previous_df["song_identity"].astype(str).isin(current_ids)].copy()
    else:
        dropouts = pd.DataFrame()

    debuts_reentries = df.loc[_marker_contains(df, "DEBUT") | _marker_contains(df, "RE-ENTRY")].copy()
    movers = df.loc[df["movement"].notna()].copy() if "movement" in df.columns else pd.DataFrame()
    gains = (
        movers.loc[movers["movement"] > 0]
        .sort_values(["movement", "position"], ascending=[False, True])
        .head(5)
        if not movers.empty else pd.DataFrame()
    )
    falls = (
        movers.loc[movers["movement"] < 0]
        .sort_values(["movement", "position"], ascending=[True, True])
        .head(5)
        if not movers.empty else pd.DataFrame()
    )

    new_peaks = df.loc[
        (pd.to_numeric(df.get("position"), errors="coerce") == pd.to_numeric(df.get("peak_position"), errors="coerce"))
        & (df.get("week_hit_peak", pd.Series(index=df.index, dtype=object)).astype(str) == selected_date)
    ].copy()
    artist_appearances = _week_artist_appearances(df)

    st.markdown("**Dropouts from previous chart**")
    if dropouts.empty:
        st.caption("No dropouts from the previous chart week.")
    else:
        dropouts_display = dropouts[["position", "song", "artist", "weeks_on_chart", "peak_position", "week_hit_peak"]].rename(columns={
            "position": "Previous Position",
            "song": "Song",
            "artist": "Artist",
            "weeks_on_chart": "Weeks",
            "peak_position": "Peak Position",
            "week_hit_peak": "Week Hit Peak",
        })
        _display_df(dropouts_display)

    st.markdown("**Debuts and re-entries**")
    if debuts_reentries.empty:
        st.caption("No debuts or re-entries this week.")
    else:
        _display_df(_week_browser_display_table(debuts_reentries))

    st.markdown("**Top 5 gains**")
    if gains.empty:
        st.caption("No upward movement this week.")
    else:
        _display_df(_week_browser_display_table(gains))

    st.markdown("**Top 5 falls**")
    if falls.empty:
        st.caption("No downward movement this week.")
    else:
        _display_df(_week_browser_display_table(falls))

    st.markdown("**Songs reaching new peaks**")
    if new_peaks.empty:
        st.caption("No songs reached their all-time peak for the first time this week.")
    else:
        _display_df(_week_browser_display_table(new_peaks))

    st.markdown("**Top 5 most artist appearances**")
    if artist_appearances.empty:
        st.caption("No artist appearance rows available.")
    else:
        _display_df(artist_appearances)


def render_week_browser_tab() -> None:
    st.subheader("Browse a chart week")
    dates = load_chart_dates()
    if dates:
        valid_dates = sorted(dates)
        min_date = dt.date.fromisoformat(valid_dates[0])
        max_date = dt.date.fromisoformat(valid_dates[-1])
        date_key = "week_browser_chart_date"
        nav_target_key = "week_browser_nav_target"
        if nav_target_key in st.session_state:
            nav_target = st.session_state.pop(nav_target_key)
            if nav_target in valid_dates:
                st.session_state[date_key] = dt.date.fromisoformat(nav_target)
        elif date_key not in st.session_state:
            st.session_state[date_key] = max_date

        selected_date_obj = st.date_input(
            "Chart date",
            min_value=min_date,
            max_value=max_date,
            format="YYYY-MM-DD",
            key=date_key,
        )
        selected_date, snapped = nearest_chart_date(selected_date_obj.isoformat(), valid_dates)

        selected_idx = valid_dates.index(selected_date) if selected_date in valid_dates else len(valid_dates) - 1
        nav_cols = st.columns([1, 1, 3])
        with nav_cols[0]:
            if st.button("◀ Previous chart week", disabled=selected_idx <= 0, key="week_browser_prev"):
                st.session_state[nav_target_key] = valid_dates[selected_idx - 1]
                st.rerun()
        with nav_cols[1]:
            if st.button("Next chart week ▶", disabled=selected_idx >= len(valid_dates) - 1, key="week_browser_next"):
                st.session_state[nav_target_key] = valid_dates[selected_idx + 1]
                st.rerun()

        if selected_date:
            if snapped:
                st.info(f"No chart exists for {selected_date_obj.isoformat()}. Showing nearest prior chart week: {selected_date}.")
            previous_date = valid_dates[selected_idx - 1] if selected_idx > 0 else None
            previous_df = load_chart(previous_date)[0] if previous_date else pd.DataFrame()

            df, meta = load_chart(selected_date)
            if meta:
                k1, k2, k3 = st.columns(3)
                k1.metric("Rows stored", meta["row_count"])
                k2.metric("Chart ID", meta["chart_id"] or "—")
                k3.metric("Source ZIP", meta["source_zip"] or "—")
                st.caption(f"Source file: {meta['source_file']}")
                if meta.get("notes"):
                    st.warning(meta["notes"])

            summary = _week_browser_summary(df, previous_df)
            _render_week_summary_expander(summary)

            _display_df(_week_browser_display_table(df))

            with st.expander("Week details"):
                _render_week_detail_tables(df, previous_df, selected_date)
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
                full_credit = _escape_streamlit_caption_text(stats['artist'])
                lead_credit = _escape_streamlit_caption_text(stats['lead_artist'])
                featured_credit = _escape_streamlit_caption_text(stats['featured_artist'] or '—')
                st.caption(
                    f"Canonical full credit: {full_credit} | "
                    f"Lead: {lead_credit} | "
                    f"Featured: {featured_credit} | "
                    f"Alias variants: {int(stats['alias_count'])}"
                )

                view = st.radio(
                    "Canonical song view",
                    ["History", "Lifecycle"],
                    horizontal=True,
                    key="canonical_song_history_view",
                )

                if view == "History":
                    chart_df = history.set_index("chart_date")["position"].sort_index()
                    st.line_chart((-chart_df).rename("inverted_position"))
                    st.caption("Line chart uses inverted positions so higher placements plot higher.")
                    st.markdown("**Week-by-week history**")
                    _display_df(history)
                    st.markdown("**Alias variants in this canonical song**")
                    _display_df(aliases)
                else:
                    st.caption("Milestone arc for this canonical song: debut, Top 20/10/5, #1, peak, final week, and trajectory type.")
                    render_kpis([("Lifecycle type", _classify_song_lifecycle(history))])
                    st.markdown("**Milestone timeline**")
                    _display_df(_song_lifecycle_table(history))
                    st.markdown("**Position history**")
                    chart_line = history.copy()
                    chart_line["chart_date"] = pd.to_datetime(chart_line["chart_date"])
                    chart_line["position"] = pd.to_numeric(chart_line["position"], errors="coerce")
                    st.line_chart(chart_line.set_index("chart_date")[["position"]], width="stretch")
                    st.markdown("**Full history**")
                    _display_df(history)
                    if not aliases.empty:
                        with st.expander("Aliases / source variants"):
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

                view = st.radio(
                    "Artist view",
                    ["History", "Career timeline"],
                    horizontal=True,
                    key="artist_history_view",
                )

                if view == "History":
                    st.markdown("**Song summary**")
                    _display_df(songs)
                    st.markdown("**Full week-by-week history**")
                    _display_df(history)
                else:
                    st.caption("Milestone view for this artist: first charting week, first Top 20/Top 10/Top 5/#1, signature songs, and yearly activity.")
                    h = history.copy()
                    h["chart_date"] = pd.to_datetime(h["chart_date"])
                    h["position"] = pd.to_numeric(h["position"], errors="coerce")
                    milestones = []
                    for name, subset in [
                        ("First chart appearance", h.head(1)),
                        ("First Top 20", h.loc[h["position"] <= 20].head(1)),
                        ("First Top 10", h.loc[h["position"] <= 10].head(1)),
                        ("First Top 5", h.loc[h["position"] <= 5].head(1)),
                        ("First #1", h.loc[h["position"] == 1].head(1)),
                    ]:
                        if not subset.empty:
                            r = subset.iloc[0]
                            milestones.append({"Milestone": name, "Chart date": _date_string(r["chart_date"]), "Song": r.get("song", ""), "Position": _fmt_rank(r.get("position"))})
                    st.markdown("**Career milestones**")
                    _display_df(pd.DataFrame(milestones))
                    st.markdown("**Signature songs**")
                    _display_df(songs.head(25), ["song", "chart_weeks", "first_date", "last_date", "peak"])
                    yearly = h.groupby(h["chart_date"].dt.year).agg(
                        chart_weeks=("song", "count"),
                        distinct_songs=("song", "nunique"),
                        best_peak=("position", "min"),
                    ).reset_index().rename(columns={"chart_date": "year"})
                    yearly.columns = ["year", "chart_weeks", "distinct_songs", "best_peak"]
                    st.markdown("**Yearly activity**")
                    _display_df(yearly.sort_values("year"))
                    st.markdown("**Full artist history**")
                    _display_df(history)
    else:
        st.info("Type part of an artist name to load an artist history.")

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_num1_gains(limit: int = 100) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["chart_date", "title", "artist", "last_week_position", "gain_to_num1", "weeks_on_chart"])
    out = chart.loc[
        chart["position"].eq(1)
        & chart["last_week_position"].notna()
        & (chart["last_week_position"] > 1)
        & ~chart["is_debut"]
        & ~chart["is_reentry"]
    , ["chart_date", "title", "artist", "last_week_position", "weeks_on_chart"]].copy()
    if out.empty:
        return out
    out["gain_to_num1"] = pd.to_numeric(out["last_week_position"], errors="coerce") - 1
    out = out.sort_values(["gain_to_num1", "chart_date", "title"], ascending=[False, False, True]).head(limit)
    return out.rename(columns={
        "chart_date": "Chart date",
        "title": "Song",
        "artist": "Artist",
        "last_week_position": "Last week",
        "gain_to_num1": "Gain to #1",
        "weeks_on_chart": "Weeks",
    })


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_num1_falls(limit: int = 100) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["chart_date", "title", "artist", "position", "fall_from_num1", "weeks_on_chart"])
    out = chart.loc[
        chart["last_week_position"].eq(1)
        & chart["position"].notna()
        & (chart["position"] > 1)
        & ~chart["is_debut"]
        & ~chart["is_reentry"]
    , ["chart_date", "title", "artist", "position", "weeks_on_chart"]].copy()
    if out.empty:
        return out
    out["fall_from_num1"] = pd.to_numeric(out["position"], errors="coerce") - 1
    out = out.sort_values(["fall_from_num1", "chart_date", "title"], ascending=[False, False, True]).head(limit)
    return out.rename(columns={
        "chart_date": "Chart date",
        "title": "Song",
        "artist": "Artist",
        "position": "This week",
        "fall_from_num1": "Fall from #1",
        "weeks_on_chart": "Weeks",
    })


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_from_position_to_num1(start_position: int, limit: int = 100) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["chart_date", "title", "artist", "weeks_on_chart"])
    out = chart.loc[
        chart["position"].eq(1)
        & chart["last_week_position"].eq(start_position)
        & ~chart["is_debut"]
        & ~chart["is_reentry"]
    , ["chart_date", "title", "artist", "weeks_on_chart"]].copy()
    out = out.sort_values(["chart_date", "title"], ascending=[False, True]).head(limit)
    return out.rename(columns={
        "chart_date": "Chart date",
        "title": "Song",
        "artist": "Artist",
        "weeks_on_chart": "Weeks",
    })


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_debut_position_to_num1(start_position: int, limit: int = 100) -> pd.DataFrame:
    songs = build_song_summary(load_analytics_base())
    if songs.empty:
        return pd.DataFrame(columns=["Debut date", "Song", "Artist", "Peak date", "#1 weeks", "Chart weeks"])
    out = songs.loc[
        pd.to_numeric(songs.get("debut_position"), errors="coerce").eq(start_position)
        & pd.to_numeric(songs.get("peak_position"), errors="coerce").eq(1)
    , ["first_chart_date", "title", "artist", "peak_date", "num1_weeks", "total_chart_weeks"]].copy()
    if out.empty:
        return pd.DataFrame(columns=["Debut date", "Song", "Artist", "Peak date", "#1 weeks", "Chart weeks"])
    out = out.sort_values(["first_chart_date", "peak_date", "title"], ascending=[False, False, True]).head(limit)
    return out.rename(columns={
        "first_chart_date": "Debut date",
        "title": "Song",
        "artist": "Artist",
        "peak_date": "Peak date",
        "num1_weeks": "#1 weeks",
        "total_chart_weeks": "Chart weeks",
    })

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_top10_hits(selected_years: tuple[int, ...] | tuple() = (), limit: int = 1000000) -> pd.DataFrame:
    songs = build_song_summary(load_analytics_base())
    if songs.empty:
        return pd.DataFrame(columns=["First Top 10 Week", "Song", "Artist", "Weeks in Top 10", "Peak"])

    songs = songs.loc[pd.to_numeric(songs.get("top10_weeks"), errors="coerce").fillna(0) > 0].copy()
    if songs.empty:
        return pd.DataFrame(columns=["First Top 10 Week", "Song", "Artist", "Weeks in Top 10", "Peak"])

    chart = load_analytics_base()
    top10_rows = chart.loc[chart["position"].le(10), ["song_key", "chart_date"]].copy()
    if top10_rows.empty:
        return pd.DataFrame(columns=["First Top 10 Week", "Song", "Artist", "Weeks in Top 10", "Peak"])

    first_top10 = (
        top10_rows.groupby("song_key", dropna=False)["chart_date"]
        .min()
        .reset_index()
        .rename(columns={"chart_date": "first_top10_week"})
    )
    top10_rows = top10_rows.assign(top10_year=pd.to_datetime(top10_rows["chart_date"]).dt.year)
    top10_by_year = (
        top10_rows
        .groupby(["song_key", "top10_year"], dropna=False)
        .size()
        .reset_index(name="top10_weeks_in_selected_years")
    )

    out = songs.merge(first_top10, on="song_key", how="left")
    if selected_years:
        yrs = sorted({int(y) for y in selected_years})
        selected_top10_rows = top10_rows.loc[top10_rows["top10_year"].isin(yrs)].copy()
        eligible_keys = set(selected_top10_rows["song_key"].tolist())
        out = out.loc[out["song_key"].isin(eligible_keys)].copy()
        if out.empty or selected_top10_rows.empty:
            return pd.DataFrame(columns=["First Top 10 Week", "Song", "Artist", "Weeks in Top 10 in Selected Year(s)", "Weeks in Top 10", "Peak"])
        first_top10_selected = (
            selected_top10_rows.groupby("song_key", dropna=False)["chart_date"]
            .min()
            .reset_index()
            .rename(columns={"chart_date": "first_top10_week_selected"})
        )
        yr_counts = (
            selected_top10_rows.groupby("song_key", dropna=False)
            .size()
            .reset_index(name="top10_weeks_in_selected_years")
        )
        out = out.merge(first_top10_selected, on="song_key", how="left")
        out = out.merge(yr_counts, on="song_key", how="left")
        out["first_top10_week"] = out["first_top10_week_selected"]
        out = out.sort_values(["first_top10_week", "title", "artist"], ascending=[True, True, True]).head(limit)
        out = out.rename(columns={
            "first_top10_week": "First Top 10 Week",
            "title": "Song",
            "artist": "Artist",
            "top10_weeks_in_selected_years": "Weeks in Top 10 in Selected Year(s)",
            "top10_weeks": "Weeks in Top 10",
            "peak_position": "Peak",
        })
        return out[["First Top 10 Week", "Song", "Artist", "Weeks in Top 10 in Selected Year(s)", "Weeks in Top 10", "Peak"]]

    out = out.sort_values(["first_top10_week", "title", "artist"], ascending=[False, True, True]).head(limit)
    out = out.rename(columns={
        "first_top10_week": "First Top 10 Week",
        "title": "Song",
        "artist": "Artist",
        "top10_weeks": "Weeks in Top 10",
        "peak_position": "Peak",
    })
    return out[["First Top 10 Week", "Song", "Artist", "Weeks in Top 10", "Peak"]]


def _consecutive_run_len(dates: list[pd.Timestamp], ordered_dates: list[pd.Timestamp]) -> int:
    if not dates:
        return 0
    idx_map = {d: i for i, d in enumerate(ordered_dates)}
    idxs = sorted(idx_map[d] for d in dates if d in idx_map)
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_num1_runs_by_year() -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["Year", "Song", "Artist", "Consecutive weeks at #1", "Reign dates"])
    num1 = chart.loc[chart["position"].eq(1)].copy()
    if num1.empty:
        return pd.DataFrame(columns=["Year", "Song", "Artist", "Consecutive weeks at #1", "Reign dates"])

    rows: list[dict[str, object]] = []
    for year, year_rows in num1.groupby("year", sort=True):
        year_dates = sorted(chart.loc[chart["year"] == year, "chart_date"].dropna().unique())
        idx_map = {d: i for i, d in enumerate(year_dates)}
        best_len = 0
        best_song = None
        best_artist = None
        best_start = None
        best_end = None

        for _, sg in year_rows.groupby("song_key", sort=False):
            sg = sg.sort_values(["chart_date", "entry_id"]).copy()
            dates = [d for d in sg["chart_date"].tolist() if d in idx_map]
            if not dates:
                continue
            run_start = dates[0]
            run_prev = dates[0]
            cur_len = 1
            cur_best_len = 1
            cur_best_start = dates[0]
            cur_best_end = dates[0]
            for d in dates[1:]:
                if idx_map[d] == idx_map[run_prev] + 1:
                    cur_len += 1
                else:
                    if cur_len > cur_best_len:
                        cur_best_len = cur_len
                        cur_best_start = run_start
                        cur_best_end = run_prev
                    run_start = d
                    cur_len = 1
                run_prev = d
            if cur_len > cur_best_len:
                cur_best_len = cur_len
                cur_best_start = run_start
                cur_best_end = run_prev

            challenger_start = cur_best_start
            if (cur_best_len > best_len) or (cur_best_len == best_len and cur_best_len > 0 and (best_start is None or challenger_start < best_start)):
                best_len = cur_best_len
                best_song = sg["title"].iloc[0]
                best_artist = sg["artist"].iloc[0]
                best_start = cur_best_start
                best_end = cur_best_end

        if best_len > 0:
            reign_dates = str(pd.to_datetime(best_start).date())
            if best_end is not None and best_end != best_start:
                reign_dates = f"{pd.to_datetime(best_start).date()} – {pd.to_datetime(best_end).date()}"
            rows.append({
                "Year": int(year),
                "Song": best_song,
                "Artist": best_artist,
                "Consecutive weeks at #1": int(best_len),
                "Reign dates": reign_dates,
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["Year", "Consecutive weeks at #1", "Song"], ascending=[False, False, True])



def _quick_number_one_artist_credits() -> pd.DataFrame:
    """Artist-credit rows for #1 entries, including lead and featured artists."""
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame()

    date_order = (
        chart[["chart_date", "year"]]
        .drop_duplicates()
        .sort_values(["year", "chart_date"])
        .copy()
    )
    date_order["chart_week_number"] = date_order.groupby("year").cumcount() + 1

    num1 = chart.loc[chart["position"].eq(1)].copy()
    if num1.empty:
        return pd.DataFrame()

    num1 = num1.merge(date_order[["chart_date", "chart_week_number"]], on="chart_date", how="left")
    credits = build_artist_credit_rows(num1)
    if credits.empty:
        return pd.DataFrame()

    credits["year"] = pd.to_numeric(credits["year"], errors="coerce").astype("Int64")
    credits["chart_week_number"] = pd.to_numeric(credits["chart_week_number"], errors="coerce").astype("Int64")
    credits = credits.loc[credits["artist_key"].notna() & credits["year"].notna()].copy()
    return credits


def _quick_join_unique(values: pd.Series) -> str:
    vals = [str(v).strip() for v in values.dropna().tolist() if str(v).strip()]
    return " | ".join(dict.fromkeys(vals))


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_artist_num1_year_streaks(song_mode: str = "All #1 songs") -> pd.DataFrame:
    """
    Artists credited on #1 songs in two or more consecutive calendar years.

    song_mode options:
    - "Same song only": keep streaks where at least one #1 song appears in multiple years of the streak.
    - "Different songs only": keep only streaks where every #1 song appears in exactly one year of the streak.
    - anything else: keep all consecutive-year artist #1 streaks.
    """
    credits = _quick_number_one_artist_credits()
    empty_cols = ["Rank", "Artist", "Start Year", "End Year", "Years", "Year Streak", "#1 Weeks During Streak", "#1 Songs"]
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)

    yearly = (
        credits.groupby(["artist_key", "artist", "year"], dropna=True)
        .agg(
            number_one_weeks=("chart_date", "nunique"),
            number_one_songs=("title", _quick_join_unique),
            number_one_song_keys=("song_key", _quick_join_unique),
            first_number_one_date=("chart_date", "min"),
        )
        .reset_index()
        .sort_values(["artist_key", "year", "first_number_one_date"])
    )

    rows: list[dict[str, object]] = []
    for (artist_key, artist), g in yearly.groupby(["artist_key", "artist"], dropna=True, sort=False):
        g = g.sort_values("year").reset_index(drop=True)
        run: list[pd.Series] = [g.iloc[0]]

        for i in range(1, len(g)):
            prev_year = int(run[-1]["year"])
            cur_year = int(g.iloc[i]["year"])
            if cur_year == prev_year + 1:
                run.append(g.iloc[i])
            else:
                if len(run) >= 2 and _quick_artist_num1_year_run_matches_mode(run, song_mode):
                    rows.append(_quick_artist_num1_year_streak_row(str(artist), run))
                run = [g.iloc[i]]

        if len(run) >= 2 and _quick_artist_num1_year_run_matches_mode(run, song_mode):
            rows.append(_quick_artist_num1_year_streak_row(str(artist), run))

    if not rows:
        return pd.DataFrame(columns=empty_cols)

    out = pd.DataFrame(rows)
    out = out.sort_values(["Years", "Start Year", "Artist"], ascending=[False, True, True]).reset_index(drop=True)
    out.insert(0, "Rank", range(1, len(out) + 1))
    return out


def _quick_song_keys_for_year_row(row: pd.Series) -> list[str]:
    raw = str(row.get("number_one_song_keys", "") or "")
    return [part.strip() for part in raw.split(" | ") if part.strip()]


def _quick_artist_num1_year_run_song_counts(run_rows: list[pd.Series]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in run_rows:
        # Count whether a song appears in a year, not how many #1 weeks it had within that year.
        for song_key in set(_quick_song_keys_for_year_row(row)):
            counts[song_key] = counts.get(song_key, 0) + 1
    return counts


def _quick_artist_num1_year_run_matches_mode(run_rows: list[pd.Series], song_mode: str) -> bool:
    song_counts = _quick_artist_num1_year_run_song_counts(run_rows)
    if not song_counts:
        return False
    if song_mode == "Same song only":
        return any(count > 1 for count in song_counts.values())
    if song_mode == "Different songs only":
        return all(count == 1 for count in song_counts.values())
    return True


def _quick_artist_num1_year_streak_row(artist: str, run_rows: list[pd.Series]) -> dict[str, object]:
    years = [int(r["year"]) for r in run_rows]
    song_bits = [f"{int(r['year'])}: {r['number_one_songs']}" for r in run_rows]
    return {
        "Artist": artist,
        "Start Year": min(years),
        "End Year": max(years),
        "Years": len(years),
        "Year Streak": f"{min(years)}–{max(years)}",
        "#1 Weeks During Streak": int(sum(int(r["number_one_weeks"]) for r in run_rows)),
        "#1 Songs": "; ".join(song_bits),
    }


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_artist_num1_same_chart_week_streaks() -> pd.DataFrame:
    """Artists credited on #1 songs in the same sequential chart week across consecutive years."""
    credits = _quick_number_one_artist_credits()
    empty_cols = ["Rank", "Artist", "Chart Week #", "Start Year", "End Year", "Years", "Year Streak", "#1 Songs"]
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)

    weekly = (
        credits.groupby(["artist_key", "artist", "year", "chart_week_number"], dropna=True)
        .agg(
            number_one_songs=("title", _quick_join_unique),
            first_number_one_date=("chart_date", "min"),
        )
        .reset_index()
        .sort_values(["artist_key", "chart_week_number", "year", "first_number_one_date"])
    )

    rows: list[dict[str, object]] = []
    for (artist_key, artist, chart_week_number), g in weekly.groupby(["artist_key", "artist", "chart_week_number"], dropna=True, sort=False):
        g = g.sort_values("year").reset_index(drop=True)
        run: list[pd.Series] = [g.iloc[0]]

        for i in range(1, len(g)):
            prev_year = int(run[-1]["year"])
            cur_year = int(g.iloc[i]["year"])
            if cur_year == prev_year + 1:
                run.append(g.iloc[i])
            else:
                if len(run) >= 2:
                    rows.append(_quick_artist_num1_same_week_row(str(artist), int(chart_week_number), run))
                run = [g.iloc[i]]

        if len(run) >= 2:
            rows.append(_quick_artist_num1_same_week_row(str(artist), int(chart_week_number), run))

    if not rows:
        return pd.DataFrame(columns=empty_cols)

    out = pd.DataFrame(rows)
    out = out.sort_values(["Years", "Start Year", "Chart Week #", "Artist"], ascending=[False, True, True, True]).reset_index(drop=True)
    out.insert(0, "Rank", range(1, len(out) + 1))
    return out


def _quick_artist_num1_same_week_row(artist: str, chart_week_number: int, run_rows: list[pd.Series]) -> dict[str, object]:
    years = [int(r["year"]) for r in run_rows]
    song_bits = [f"{int(r['year'])}: {r['number_one_songs']}" for r in run_rows]
    return {
        "Artist": artist,
        "Chart Week #": int(chart_week_number),
        "Start Year": min(years),
        "End Year": max(years),
        "Years": len(years),
        "Year Streak": f"{min(years)}–{max(years)}",
        "#1 Songs": "; ".join(song_bits),
    }


def _quick_mode_value(values: pd.Series) -> str:
    vals = [str(v).strip() for v in values.dropna().tolist() if str(v).strip()]
    if not vals:
        return ""
    return pd.Series(vals).mode().iloc[0]


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_artist_num1_droughts(
    credit_mode: str = "Lead + featured artists",
    song_mode: str = "Different songs only",
    min_drought_weeks: int = 52,
    limit: int = 100,
) -> pd.DataFrame:
    """Longest gaps between #1 appearances by artist-credit row."""
    empty_cols = [
        "Rank",
        "Artist",
        "Drought weeks",
        "Approx. years",
        "From #1 song",
        "From week",
        "To #1 song",
        "To week",
        "Credit type",
    ]
    credits = _quick_number_one_artist_credits()
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)

    if credit_mode == "Lead artists only":
        credits = credits.loc[credits["artist_role_mode"].eq("Lead")].copy()
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)

    credits = credits.loc[credits["artist_key"].notna() & credits["chart_date"].notna() & credits["song_key"].notna()].copy()
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)

    credits["chart_date"] = pd.to_datetime(credits["chart_date"])
    all_chart_dates = sorted(load_analytics_base()["chart_date"].dropna().unique())
    date_index = {d: i for i, d in enumerate(all_chart_dates)}
    credits["chart_index"] = credits["chart_date"].map(date_index)
    credits = credits.loc[credits["chart_index"].notna()].copy()
    if credits.empty:
        return pd.DataFrame(columns=empty_cols)
    credits["chart_index"] = credits["chart_index"].astype(int)

    appearances = (
        credits.groupby(["artist_key", "chart_date", "chart_index", "song_key"], dropna=True)
        .agg(
            Artist=("artist", _quick_mode_value),
            Song=("title", _quick_mode_value),
            Roles=("artist_role_mode", _quick_join_unique),
        )
        .reset_index()
        .sort_values(["artist_key", "chart_index", "Song"])
    )

    rows: list[dict[str, object]] = []
    for artist_key, g in appearances.groupby("artist_key", dropna=True, sort=False):
        g = g.sort_values(["chart_index", "Song"]).reset_index(drop=True)
        if g.empty:
            continue

        first = g.iloc[0].to_dict()
        cur_run = {
            "artist": preferred_artist_display(artist_key, first.get("Artist", "")),
            "song_key": first.get("song_key"),
            "song": first.get("Song", ""),
            "start_date": first.get("chart_date"),
            "end_date": first.get("chart_date"),
            "start_index": int(first.get("chart_index")),
            "end_index": int(first.get("chart_index")),
            "start_roles": first.get("Roles", ""),
            "end_roles": first.get("Roles", ""),
        }
        runs: list[dict[str, object]] = []

        for i in range(1, len(g)):
            rec = g.iloc[i].to_dict()
            rec_idx = int(rec.get("chart_index"))
            rec_song_key = rec.get("song_key")
            # Consecutive weeks at #1 by the same song/artist are one reign.
            if rec_song_key == cur_run["song_key"] and rec_idx == cur_run["end_index"] + 1:
                cur_run["end_date"] = rec.get("chart_date")
                cur_run["end_index"] = rec_idx
                cur_run["end_roles"] = rec.get("Roles", cur_run["end_roles"])
            else:
                runs.append(cur_run)
                cur_run = {
                    "artist": preferred_artist_display(artist_key, rec.get("Artist", "")),
                    "song_key": rec_song_key,
                    "song": rec.get("Song", ""),
                    "start_date": rec.get("chart_date"),
                    "end_date": rec.get("chart_date"),
                    "start_index": rec_idx,
                    "end_index": rec_idx,
                    "start_roles": rec.get("Roles", ""),
                    "end_roles": rec.get("Roles", ""),
                }
        runs.append(cur_run)

        for prev_run, next_run in zip(runs, runs[1:]):
            if song_mode == "Different songs only" and prev_run["song_key"] == next_run["song_key"]:
                continue
            drought_weeks = int(next_run["start_index"] - prev_run["end_index"])
            if drought_weeks < int(min_drought_weeks):
                continue
            rows.append({
                "Artist": prev_run["artist"],
                "Drought weeks": drought_weeks,
                "Approx. years": round(drought_weeks / 52.0, 1),
                "From #1 song": prev_run["song"],
                "From week": pd.to_datetime(prev_run["end_date"]).strftime("%Y-%m-%d"),
                "To #1 song": next_run["song"],
                "To week": pd.to_datetime(next_run["start_date"]).strftime("%Y-%m-%d"),
                "Credit type": f"{prev_run['end_roles']} → {next_run['start_roles']}",
            })

    if not rows:
        return pd.DataFrame(columns=empty_cols)

    out = pd.DataFrame(rows)
    out = out.sort_values(["Drought weeks", "To week", "Artist"], ascending=[False, False, True]).head(int(limit)).reset_index(drop=True)
    out.insert(0, "Rank", range(1, len(out) + 1))
    return out


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS, max_entries=CACHE_MAX_ENTRIES)
def build_quick_artist_exclusive_top25(limit: int = 100) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame(columns=["Chart date", "Artist", "Exclusive tier", "Songs"])
    top5 = chart.loc[chart["position"].between(1, 5)].copy()
    if top5.empty:
        return pd.DataFrame(columns=["Chart date", "Artist", "Exclusive tier", "Songs"])

    rows: list[dict[str, object]] = []
    for chart_date, g in top5.groupby("chart_date", sort=True):
        g = g.copy()
        pos_to_keys: dict[int, set[str]] = {}
        pos_to_title: dict[int, str] = {}
        for _, row in g.iterrows():
            pos = _safe_int(row.get("position"))
            if pos is None or pos < 1 or pos > 5:
                continue
            pairs = _split_credit_people(row.get("normalized_lead_artist"), row.get("lead_artist"))
            pairs.extend(_split_credit_people(row.get("normalized_featured_artist"), row.get("featured_artist")))
            entry_keys = {normalize_search_text(k) for k, _ in pairs if k}
            pos_to_keys[pos] = entry_keys
            pos_to_title[pos] = str(row.get("title", "") or "")

        exclusive_tier = None
        exclusive_artists: set[str] = set()
        for tier in (5, 4, 3, 2):
            required_positions = list(range(1, tier + 1))
            if sorted(pos for pos in required_positions if pos in pos_to_keys) != required_positions:
                continue
            common_keys: set[str] | None = None
            valid = True
            for pos in required_positions:
                entry_keys = pos_to_keys.get(pos, set())
                if not entry_keys:
                    valid = False
                    break
                common_keys = entry_keys if common_keys is None else (common_keys & entry_keys)
                if not common_keys:
                    valid = False
                    break
            if valid and common_keys:
                exclusive_tier = tier
                exclusive_artists = common_keys
                break

        if exclusive_tier is None or not exclusive_artists:
            continue

        songs = [f"#{pos} {pos_to_title.get(pos, '')}" for pos in range(1, exclusive_tier + 1)]
        for artist_key in sorted(exclusive_artists):
            rows.append({
                "Chart date": str(pd.to_datetime(chart_date).date()),
                "Artist": preferred_artist_display(artist_key, artist_key),
                "Exclusive tier": f"Top {exclusive_tier}",
                "Songs": " | ".join(songs),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    tier_order = {"Top 5": 5, "Top 4": 4, "Top 3": 3, "Top 2": 2}
    out["__tier_order"] = out["Exclusive tier"].map(tier_order).fillna(0)
    out = out.sort_values(["__tier_order", "Chart date", "Artist"], ascending=[False, False, True]).drop(columns=["__tier_order"])
    return out.head(limit)



# -----------------------------------------------------------------------------
# New historian/workflow sections: recap, QA, lifecycle, career timeline,
# forecast scorecard, and head-to-head rivalries.
# -----------------------------------------------------------------------------


def _date_string(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def _song_artist_label_from_row(row: pd.Series | dict[str, object], song_col: str = "song", artist_col: str = "artist") -> str:
    getter = row.get if hasattr(row, "get") else lambda k, d=None: d
    song = str(getter(song_col, "") or getter("title", "") or "").strip()
    artist = str(getter(artist_col, "") or "").strip()
    if song and artist:
        return f"{song} — {artist}"
    return song or artist or "—"


def _chart_date_selectbox(label: str, key: str, default_latest: bool = True) -> str | None:
    dates = load_chart_dates()
    if not dates:
        st.info("No chart dates are available.")
        return None
    return st.selectbox(label, dates, index=0 if default_latest else len(dates) - 1, key=key)


def _previous_chart_for_date(chart_date: str) -> tuple[str | None, pd.DataFrame]:
    dates = sorted(load_chart_dates())
    prior = [d for d in dates if d < chart_date]
    if not prior:
        return None, pd.DataFrame()
    prev_date = prior[-1]
    prev_df, _ = load_chart(prev_date)
    return prev_date, prev_df


def _build_chart_recap_text(chart_date: str, df: pd.DataFrame, previous_df: pd.DataFrame) -> str:
    if df.empty:
        return "No chart rows are available for this date."

    top = df.sort_values("position").iloc[0]
    prev_no1 = ""
    if previous_df is not None and not previous_df.empty:
        prev_top = previous_df.sort_values("position").iloc[0]
        prev_no1 = _song_artist_label_from_row(prev_top)
    no1_label = _song_artist_label_from_row(top)
    last_week = _safe_int(top.get("last_week_position"))
    no1_move = f" moves #{last_week} → #1" if last_week and last_week != 1 else " holds at #1" if last_week == 1 else " is #1"

    movers = df.loc[df.get("movement", pd.Series(index=df.index, dtype="float64")).notna()].copy()
    climbers = movers.loc[movers["movement"] > 0].sort_values(["movement", "position"], ascending=[False, True]) if not movers.empty else pd.DataFrame()
    fallers = movers.loc[movers["movement"] < 0].sort_values(["movement", "position"], ascending=[True, True]) if not movers.empty else pd.DataFrame()
    debuts = df.loc[_marker_contains(df, "DEBUT")].sort_values("position")
    reentries = df.loc[_marker_contains(df, "RE-ENTRY")].sort_values("position")
    top10_arrivals = df.loc[(pd.to_numeric(df.get("last_week_position"), errors="coerce") > 10) & (pd.to_numeric(df.get("position"), errors="coerce") <= 10)].sort_values("position")
    veterans = df.sort_values("weeks_on_chart", ascending=False).head(3) if "weeks_on_chart" in df.columns else pd.DataFrame()

    def labels(table: pd.DataFrame, n: int = 3) -> str:
        if table.empty:
            return "none"
        return "; ".join(_song_artist_label_from_row(r) for _, r in table.head(n).iterrows())

    if prev_no1 and prev_no1 != no1_label:
        p1 = f"This week's Torrey's Corner Top 40 ({chart_date}) gets a new #1 as {no1_label}{no1_move}. Last week's leader was {prev_no1}, so the top slot changes hands this week."
    else:
        p1 = f"This week's Torrey's Corner Top 40 ({chart_date}) is led by {no1_label}, which{no1_move}."

    if not climbers.empty:
        climber = climbers.iloc[0]
        climber_text = f"The biggest upward move belongs to {_song_artist_label_from_row(climber)}, climbing {_format_movement(climber.get('movement'))} spots."
    else:
        climber_text = "There were no ranked upward moves among returning songs."
    if not fallers.empty:
        faller = fallers.iloc[0]
        faller_text = f"The sharpest decline is {_song_artist_label_from_row(faller)}, falling {_format_movement(faller.get('movement'))} spots."
    else:
        faller_text = "There were no ranked declines among returning songs."
    p2 = f"Movement is headlined by the returning songs: {climber_text} {faller_text} Top 10 movement includes {labels(top10_arrivals)}."

    p3 = f"Fresh activity includes {len(debuts)} debut(s) and {len(reentries)} re-entry/re-entries. The top debut group is {labels(debuts)}, while the re-entry list is {labels(reentries)}. The chart veterans worth watching are {labels(veterans)}, giving the week a mix of new arrivals, return traffic, and long-running holdovers."
    return "\n\n".join([p1, p2, p3])


def render_chart_recap_tab() -> None:
    st.subheader("Chart Recap Generator")
    st.caption("Creates a copy-ready weekly summary draft from the selected chart week.")
    selected_date = _chart_date_selectbox("Chart week", "chart_recap_date")
    if not selected_date:
        return
    df, meta = load_chart(selected_date)
    prev_date, prev_df = _previous_chart_for_date(selected_date)
    if df.empty:
        st.info("No chart rows are available for this week.")
        return
    summary = _week_browser_summary(df, prev_df)
    render_kpis([
        ("#1", summary["number_one"]),
        ("Top debut", summary["top_debut"]),
        ("Biggest climber", summary["biggest_climber"]),
        ("Debuts", summary["debuts"]),
        ("Re-entries", summary["reentries"]),
        ("Dropouts", summary["dropouts"]),
    ])
    if prev_date:
        st.caption(f"Previous chart used for dropout comparison: {prev_date}")
    recap = _build_chart_recap_text(selected_date, df, prev_df)
    st.text_area("Generated recap draft", recap, height=260, key="chart_recap_text")
    with st.expander("Show source chart rows"):
        _display_df(_week_browser_display_table(df))


def _db_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _db_has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _query_df(conn: sqlite3.Connection, sql: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception as exc:
        return pd.DataFrame({"error": [str(exc)]})


def render_data_health_tab() -> None:
    st.subheader("Data Health / Import QA")
    st.caption("Sanity checks for imports, canonical IDs, markers, chart-week row counts, and duplicate-looking records.")
    conn = get_connection()
    top_n = st.slider("Rows per check", 10, 500, 100, 10, key="data_health_rows")

    checks: list[tuple[str, str, pd.DataFrame]] = []
    checks.append((
        "Chart weeks with row count not equal to 40",
        "Useful after imports; every weekly chart should normally have exactly 40 rows.",
        _query_df(conn, """
            SELECT cw.chart_date, COUNT(e.entry_id) AS entry_rows, cw.row_count, cw.source_file
            FROM chart_week cw
            LEFT JOIN entry e ON e.chart_week_id = cw.chart_week_id
            GROUP BY cw.chart_week_id, cw.chart_date, cw.row_count, cw.source_file
            HAVING COUNT(e.entry_id) <> 40 OR COALESCE(cw.row_count, 40) <> 40
            ORDER BY cw.chart_date DESC
            LIMIT ?
        """, (top_n,)),
    ))
    checks.append((
        "Entries missing canonical_song_id",
        "These usually appear right after import before the artist-role/canonical assignment script is run.",
        _query_df(conn, """
            SELECT cw.chart_date, e.position, e.song_title_display AS song, e.full_artist_display AS artist, e.derived_marker
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id IS NULL
            ORDER BY cw.chart_date DESC, e.position
            LIMIT ?
        """, (top_n,)),
    ))
    checks.append((
        "Duplicate chart dates",
        "Flags cases where more than one chart_week row has the same chart_date.",
        _query_df(conn, """
            SELECT chart_date, COUNT(*) AS chart_week_rows, GROUP_CONCAT(chart_week_id) AS chart_week_ids
            FROM chart_week
            GROUP BY chart_date
            HAVING COUNT(*) > 1
            ORDER BY chart_date DESC
            LIMIT ?
        """, (top_n,)),
    ))
    checks.append((
        "Duplicate positions within a chart week",
        "A chart week should not have two entries with the same position.",
        _query_df(conn, """
            SELECT cw.chart_date, e.position, COUNT(*) AS entries_at_position,
                   GROUP_CONCAT(e.song_title_display || ' — ' || e.full_artist_display, ' | ') AS entries
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            GROUP BY cw.chart_week_id, cw.chart_date, e.position
            HAVING COUNT(*) > 1
            ORDER BY cw.chart_date DESC, e.position
            LIMIT ?
        """, (top_n,)),
    ))
    checks.append((
        "Invalid chart positions",
        "Flags positions outside the expected 1–40 range.",
        _query_df(conn, """
            SELECT cw.chart_date, e.position, e.song_title_display AS song, e.full_artist_display AS artist
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.position < 1 OR e.position > 40 OR e.position IS NULL
            ORDER BY cw.chart_date DESC, e.position
            LIMIT ?
        """, (top_n,)),
    ))
    checks.append((
        "Rows marked as both debut and re-entry",
        "Derived debut/re-entry flags should be mutually exclusive.",
        _query_df(conn, """
            SELECT cw.chart_date, e.position, e.song_title_display AS song, e.full_artist_display AS artist,
                   e.derived_marker, e.derived_is_debut, e.derived_is_reentry
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE COALESCE(e.derived_is_debut, 0) = 1 AND COALESCE(e.derived_is_reentry, 0) = 1
            ORDER BY cw.chart_date DESC, e.position
            LIMIT ?
        """, (top_n,)),
    ))
    if _db_has_table(conn, "canonical_song"):
        checks.append((
            "Canonical songs with zero attached entries",
            "Usually harmless after editing, but worth reviewing if the count grows.",
            _query_df(conn, """
                SELECT cs.canonical_song_id, cs.canonical_title AS song,
                       COALESCE(cs.canonical_full_artist, cs.canonical_artist) AS artist
                FROM canonical_song cs
                LEFT JOIN entry e ON e.canonical_song_id = cs.canonical_song_id
                GROUP BY cs.canonical_song_id, cs.canonical_title, cs.canonical_artist, cs.canonical_full_artist
                HAVING COUNT(e.entry_id) = 0
                ORDER BY cs.canonical_song_id DESC
                LIMIT ?
            """, (top_n,)),
        ))
        checks.append((
            "Possible duplicate canonical songs by normalized title + artist",
            "These are candidates for manual merge review, not automatic errors.",
            _query_df(conn, """
                SELECT LOWER(TRIM(canonical_title)) AS title_key,
                       LOWER(TRIM(COALESCE(canonical_full_artist, canonical_artist))) AS artist_key,
                       COUNT(*) AS canonical_rows,
                       GROUP_CONCAT(canonical_song_id) AS canonical_song_ids,
                       GROUP_CONCAT(canonical_title || ' — ' || COALESCE(canonical_full_artist, canonical_artist), ' | ') AS labels
                FROM canonical_song
                GROUP BY title_key, artist_key
                HAVING COUNT(*) > 1
                ORDER BY canonical_rows DESC, title_key
                LIMIT ?
            """, (top_n,)),
        ))

    overview_rows = []
    for title, _, df in checks:
        issue_count = 0 if df.empty else len(df)
        if "error" in df.columns:
            status = "Check error"
        elif issue_count == 0:
            status = "OK"
        else:
            status = f"Review {issue_count:,} row(s)"
        overview_rows.append({"Check": title, "Status": status})
    st.markdown("**QA summary**")
    st.dataframe(pd.DataFrame(overview_rows), width="stretch", hide_index=True)

    for title, help_text, df in checks:
        with st.expander(title, expanded=not df.empty and "error" not in df.columns):
            st.caption(help_text)
            if df.empty:
                st.success("No issues found.")
            else:
                _display_df(df)


def _song_lifecycle_table(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    h = history.copy()
    h["chart_date"] = pd.to_datetime(h["chart_date"])
    h["position"] = pd.to_numeric(h["position"], errors="coerce")
    milestones: list[dict[str, object]] = []
    checks = [
        ("Debut", h.head(1)),
        ("First Top 20", h.loc[h["position"] <= 20].head(1)),
        ("First Top 10", h.loc[h["position"] <= 10].head(1)),
        ("First Top 5", h.loc[h["position"] <= 5].head(1)),
        ("First #1", h.loc[h["position"] == 1].head(1)),
    ]
    peak_pos = h["position"].min()
    checks.append(("Peak", h.loc[h["position"] == peak_pos].head(1)))
    checks.append(("Final chart week", h.tail(1)))
    for name, rows in checks:
        if rows.empty:
            continue
        r = rows.iloc[0]
        milestones.append({
            "Milestone": name,
            "Chart date": _date_string(r.get("chart_date")),
            "Position": _fmt_rank(r.get("position")),
            "Last Week": _fmt_rank(r.get("last_week_position")),
            "Weeks on chart": _safe_int(r.get("weeks_on_chart")) or "—",
            "Marker": r.get("derived_marker", ""),
        })
    return pd.DataFrame(milestones)


def _classify_song_lifecycle(history: pd.DataFrame) -> str:
    if history.empty:
        return "Unknown"
    h = history.copy()
    h["position"] = pd.to_numeric(h["position"], errors="coerce")
    weeks = len(h)
    peak = int(h["position"].min())
    weeks_to_peak = int(h["position"].idxmin()) if h.index.is_monotonic_increasing else int(h.reset_index(drop=True)["position"].idxmin())
    reentries = int(_marker_contains(h, "RE-ENTRY").sum())
    if peak == 1 and weeks >= 20:
        return "Blockbuster / long-running #1"
    if weeks_to_peak >= 8 and peak <= 10:
        return "Slow-burn climber"
    if weeks <= 4 and peak <= 10:
        return "Flash hit"
    if reentries > 0:
        return "Comeback / re-entry runner"
    if weeks >= 20:
        return "Long-runner"
    return "Standard chart run"


def render_song_lifecycle_tab() -> None:
    st.subheader("Song Lifecycle")
    st.caption("Turns a canonical song history into a milestone arc: debut, Top 20/10/5, #1, peak, final week, and trajectory type.")
    term = st.text_input("Find a song", key="song_lifecycle_search", placeholder="Search title or artist")
    if not term.strip():
        st.info("Search for a canonical song to view its lifecycle.")
        return
    matches = canonical_song_matches(term, 200)
    if matches.empty:
        st.warning("No canonical songs matched that search.")
        return
    matches["label"] = matches.apply(lambda r: f"{r['canonical_title']} — {r['canonical_artist']} ({int(r['chart_weeks'] or 0)} weeks, peak #{int(r['peak']) if pd.notna(r['peak']) else '—'})", axis=1)
    choice = st.selectbox("Canonical song", matches["label"].tolist(), key="song_lifecycle_choice")
    song_id = int(matches.loc[matches["label"] == choice, "canonical_song_id"].iloc[0])
    history, stats, aliases = canonical_song_history(song_id)
    if history.empty or stats is None:
        st.info("No history rows are attached to this canonical song.")
        return
    render_kpis([
        ("Peak", _fmt_rank(stats.get("peak"))),
        ("Chart weeks", stats.get("chart_weeks", "—")),
        ("First week", stats.get("first_date", "—")),
        ("Last week", stats.get("last_date", "—")),
        ("Lifecycle type", _classify_song_lifecycle(history)),
    ])
    st.markdown("**Milestone timeline**")
    _display_df(_song_lifecycle_table(history))
    st.markdown("**Position history**")
    chart_line = history.copy()
    chart_line["chart_date"] = pd.to_datetime(chart_line["chart_date"])
    chart_line["position"] = pd.to_numeric(chart_line["position"], errors="coerce")
    st.line_chart(chart_line.set_index("chart_date")[["position"]], width="stretch")
    st.markdown("**Full history**")
    _display_df(history)
    if not aliases.empty:
        with st.expander("Aliases / source variants"):
            _display_df(aliases)


def render_artist_career_timeline_tab() -> None:
    st.subheader("Artist Career Timeline")
    st.caption("Milestone view for an artist: first charting week, first Top 10/Top 5/#1, signature songs, yearly activity, and gaps.")
    role_mode = st.radio("Credit mode", ["Full credit", "Lead artist", "Featured artist"], horizontal=True, key="artist_career_role")
    term = st.text_input("Find an artist", key="artist_career_search", placeholder="Search artist name")
    if not term.strip():
        st.info("Search for an artist to view their career timeline.")
        return
    matches = artist_matches(term, role_mode, 200)
    if matches.empty:
        st.warning("No artists matched that search.")
        return
    matches["label"] = matches.apply(lambda r: f"{r['display_artist']} ({int(r['chart_weeks'] or 0)} weeks, peak #{int(r['peak']) if pd.notna(r['peak']) else '—'})", axis=1)
    choice = st.selectbox("Artist", matches["label"].tolist(), key="artist_career_choice")
    artist_key = str(matches.loc[matches["label"] == choice, "normalized_artist"].iloc[0])
    history, stats, songs = artist_history(artist_key, role_mode)
    if history.empty or stats is None:
        st.info("No chart history is available for this artist under the selected credit mode.")
        return
    h = history.copy()
    h["chart_date"] = pd.to_datetime(h["chart_date"])
    h["position"] = pd.to_numeric(h["position"], errors="coerce")
    milestones = []
    for name, subset in [
        ("First chart appearance", h.head(1)),
        ("First Top 20", h.loc[h["position"] <= 20].head(1)),
        ("First Top 10", h.loc[h["position"] <= 10].head(1)),
        ("First Top 5", h.loc[h["position"] <= 5].head(1)),
        ("First #1", h.loc[h["position"] == 1].head(1)),
    ]:
        if not subset.empty:
            r = subset.iloc[0]
            milestones.append({"Milestone": name, "Chart date": _date_string(r["chart_date"]), "Song": r.get("song", ""), "Position": _fmt_rank(r.get("position"))})
    render_kpis([
        ("Chart weeks", stats.get("chart_weeks", "—")),
        ("Distinct songs", stats.get("distinct_songs", "—")),
        ("Peak", _fmt_rank(stats.get("peak"))),
        ("First week", stats.get("first_date", "—")),
        ("Last week", stats.get("last_date", "—")),
    ])
    st.markdown("**Career milestones**")
    _display_df(pd.DataFrame(milestones))
    st.markdown("**Signature songs**")
    _display_df(songs.head(25), ["song", "chart_weeks", "first_date", "last_date", "peak"])
    yearly = h.groupby(h["chart_date"].dt.year).agg(
        chart_weeks=("song", "count"),
        distinct_songs=("song", "nunique"),
        best_peak=("position", "min"),
    ).reset_index().rename(columns={"chart_date": "year"})
    yearly.columns = ["year", "chart_weeks", "distinct_songs", "best_peak"]
    st.markdown("**Yearly activity**")
    _display_df(yearly.sort_values("year"))
    st.markdown("**Full artist history**")
    _display_df(history)


def _forecast_backtest_rows(limit_weeks: int, max_neighbors: int) -> pd.DataFrame:
    chart = load_analytics_base()
    if chart.empty:
        return pd.DataFrame()
    chart = _add_num1_reign_features(chart)
    dates = sorted(pd.to_datetime(chart.loc[chart["next_chart_date"].notna(), "chart_date"].dropna().unique()))
    if not dates:
        return pd.DataFrame()
    dates = dates[-limit_weeks:]
    rows: list[pd.DataFrame] = []
    for d in dates:
        forecast, _ = _forecast_for_chart_date(chart, d, max_neighbors=max_neighbors)
        if forecast.empty:
            continue
        actual = chart.loc[chart["chart_date"] == d, ["title", "artist", "position", "next_position", "dropped_out_next_week"]].copy()
        comp = forecast.merge(actual, on=["title", "artist", "position"], how="left")
        comp["backtest_chart_date"] = d
        rows.append(comp)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["rank_error"] = (pd.to_numeric(out["expected_next_position"], errors="coerce") - pd.to_numeric(out["next_position"], errors="coerce")).abs()
    out["predicted_top10"] = pd.to_numeric(out["top10_probability"], errors="coerce") >= 0.5
    out["actual_top10"] = pd.to_numeric(out["next_position"], errors="coerce") <= 10
    out["predicted_dropout"] = pd.to_numeric(out["dropout_risk"], errors="coerce") >= 0.5
    out["actual_dropout"] = out["dropped_out_next_week"].fillna(False).astype(bool)
    return out


def render_forecast_scorecard_tab() -> None:
    st.subheader("Forecast Lab Scorecard")
    st.caption("Backtests the historical-neighbor Forecast Lab across recent historical weeks with known next-week outcomes.")
    cols = st.columns(2)
    weeks = cols[0].slider("Historical weeks to test", 5, 60, 20, 5, key="forecast_scorecard_weeks")
    max_neighbors = cols[1].slider("Similar cases per song", 25, 250, 100, 25, key="forecast_scorecard_neighbors")
    if not st.checkbox("Run scorecard", value=False, key="forecast_scorecard_run"):
        st.info("Turn on the checkbox to run the backtest. This is intentionally gated because it loops through multiple forecast weeks.")
        return
    scored = _forecast_backtest_rows(weeks, max_neighbors)
    if scored.empty:
        st.info("No backtest rows were available.")
        return
    present = scored.loc[scored["next_position"].notna()].copy()
    avg_rank_error = present["rank_error"].mean() if not present.empty else float("nan")
    top10_accuracy = (scored["predicted_top10"] == scored["actual_top10"]).mean()
    dropout_accuracy = (scored["predicted_dropout"] == scored["actual_dropout"]).mean()
    num1_hits = []
    for d, g in scored.groupby("backtest_chart_date"):
        pred = g.sort_values("projected_rank").head(1)
        actual = g.loc[g["next_position"] == 1].head(1)
        if not pred.empty and not actual.empty:
            num1_hits.append(str(pred.iloc[0]["title"]) == str(actual.iloc[0]["title"]))
    render_kpis([
        ("Rows tested", f"{len(scored):,}"),
        ("Avg rank error", f"{avg_rank_error:.2f}" if pd.notna(avg_rank_error) else "—"),
        ("Top 10 accuracy", f"{top10_accuracy * 100:.1f}%"),
        ("Dropout accuracy", f"{dropout_accuracy * 100:.1f}%"),
        ("#1 exact hit rate", f"{(sum(num1_hits) / len(num1_hits) * 100):.1f}%" if num1_hits else "—"),
    ])
    by_week = scored.groupby("backtest_chart_date").agg(
        rows=("title", "count"),
        avg_rank_error=("rank_error", "mean"),
        top10_accuracy=("predicted_top10", lambda s: float((s == scored.loc[s.index, "actual_top10"]).mean())),
        dropout_accuracy=("predicted_dropout", lambda s: float((s == scored.loc[s.index, "actual_dropout"]).mean())),
    ).reset_index()
    st.markdown("**Week-by-week scorecard**")
    _display_df(by_week.sort_values("backtest_chart_date", ascending=False))
    st.markdown("**Largest rank misses**")
    _display_df(
        _format_probability_columns(scored.sort_values("rank_error", ascending=False).head(50)),
        ["backtest_chart_date", "title", "artist", "position", "projected_rank", "expected_next_position", "next_position", "rank_error", "dropout_risk", "top10_probability"],
    )


def _battle_summary(a: pd.DataFrame, b: pd.DataFrame, a_name: str, b_name: str) -> tuple[pd.DataFrame, list[tuple[str, object]]]:
    if a.empty or b.empty:
        return pd.DataFrame(), []
    aa = a.copy()
    bb = b.copy()
    aa["chart_date"] = pd.to_datetime(aa["chart_date"])
    bb["chart_date"] = pd.to_datetime(bb["chart_date"])
    aa["position"] = pd.to_numeric(aa["position"], errors="coerce")
    bb["position"] = pd.to_numeric(bb["position"], errors="coerce")
    aa = aa.sort_values(["chart_date", "position"]).drop_duplicates("chart_date")[["chart_date", "position", "song"]].rename(columns={"position": f"{a_name} position", "song": f"{a_name} song"})
    bb = bb.sort_values(["chart_date", "position"]).drop_duplicates("chart_date")[["chart_date", "position", "song"]].rename(columns={"position": f"{b_name} position", "song": f"{b_name} song"})
    both = aa.merge(bb, on="chart_date", how="inner")
    if both.empty:
        metrics = [
            (f"{a_name} chart weeks", len(a)),
            (f"{b_name} chart weeks", len(b)),
            ("Same-week battles", 0),
        ]
        return both, metrics
    both["winner"] = both.apply(lambda r: a_name if r[f"{a_name} position"] < r[f"{b_name} position"] else b_name if r[f"{b_name} position"] < r[f"{a_name} position"] else "Tie", axis=1)
    both["rank_gap"] = (both[f"{a_name} position"] - both[f"{b_name} position"]).abs()
    metrics = [
        (f"{a_name} chart weeks", len(a)),
        (f"{b_name} chart weeks", len(b)),
        ("Same-week battles", len(both)),
        (f"{a_name} wins", int((both["winner"] == a_name).sum())),
        (f"{b_name} wins", int((both["winner"] == b_name).sum())),
        ("Ties", int((both["winner"] == "Tie").sum())),
    ]
    return both.sort_values("chart_date", ascending=False), metrics


def render_rivalries_tab() -> None:
    st.subheader("Rivalries / Head-to-Head")
    st.caption("Compare two songs or two artists on chart weeks, peaks, #1 weeks, and same-week battles.")
    mode = st.radio("Comparison type", ["Songs", "Artists"], horizontal=True, key="rivalry_mode")
    if mode == "Songs":
        c1, c2 = st.columns(2)
        term_a = c1.text_input("Song/artist A search", key="rival_song_a")
        term_b = c2.text_input("Song/artist B search", key="rival_song_b")
        if not term_a.strip() or not term_b.strip():
            st.info("Search and select two canonical songs.")
            return
        ma = canonical_song_matches(term_a, 100)
        mb = canonical_song_matches(term_b, 100)
        if ma.empty or mb.empty:
            st.warning("One of the searches returned no canonical song matches.")
            return
        ma["label"] = ma.apply(lambda r: f"{r['canonical_title']} — {r['canonical_artist']}", axis=1)
        mb["label"] = mb.apply(lambda r: f"{r['canonical_title']} — {r['canonical_artist']}", axis=1)
        la = c1.selectbox("Song A", ma["label"].tolist(), key="rival_song_a_choice")
        lb = c2.selectbox("Song B", mb["label"].tolist(), key="rival_song_b_choice")
        ha, sa, _ = canonical_song_history(int(ma.loc[ma["label"] == la, "canonical_song_id"].iloc[0]))
        hb, sb, _ = canonical_song_history(int(mb.loc[mb["label"] == lb, "canonical_song_id"].iloc[0]))
        a_name = str(sa.get("song", la)) if sa else la
        b_name = str(sb.get("song", lb)) if sb else lb
    else:
        c1, c2 = st.columns(2)
        role_mode = st.radio("Credit mode", ["Full credit", "Lead artist", "Featured artist"], horizontal=True, key="rival_artist_role")
        term_a = c1.text_input("Artist A search", key="rival_artist_a")
        term_b = c2.text_input("Artist B search", key="rival_artist_b")
        if not term_a.strip() or not term_b.strip():
            st.info("Search and select two artists.")
            return
        ma = artist_matches(term_a, role_mode, 100)
        mb = artist_matches(term_b, role_mode, 100)
        if ma.empty or mb.empty:
            st.warning("One of the searches returned no artist matches.")
            return
        ma["label"] = ma["display_artist"]
        mb["label"] = mb["display_artist"]
        la = c1.selectbox("Artist A", ma["label"].tolist(), key="rival_artist_a_choice")
        lb = c2.selectbox("Artist B", mb["label"].tolist(), key="rival_artist_b_choice")
        ha, sa, _ = artist_history(str(ma.loc[ma["label"] == la, "normalized_artist"].iloc[0]), role_mode)
        hb, sb, _ = artist_history(str(mb.loc[mb["label"] == lb, "normalized_artist"].iloc[0]), role_mode)
        a_name = str(sa.get("artist", la)) if sa else la
        b_name = str(sb.get("artist", lb)) if sb else lb

    battle, metrics = _battle_summary(ha, hb, a_name, b_name)
    if metrics:
        render_kpis(metrics)
    st.markdown("**Same-week battles**")
    if battle.empty:
        st.info("These two did not overlap on the chart under the selected comparison mode.")
    else:
        _display_df(battle)
    st.markdown("**A history**")
    _display_df(ha.head(200))
    st.markdown("**B history**")
    _display_df(hb.head(200))

def render_special_tables_tab() -> None:
    st.subheader("Quick tables")
    subsection = st.selectbox(
        "Section",
        [
            "Hits & milestones",
            "Movement",
            "Artists",
            "Debuts / Re-entries",
            "Chart feats",
        ],
        key="quick_tables_section",
    )

    if subsection == "Hits & milestones":
        table_kind = st.selectbox(
            "View",
            ["#1 hits", "Top 10 hits"],
            key="quick_hits_view",
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
                table = table.sort_values(["chart_date", "position", "song"], ascending=[True, True, True])
            st.markdown("**#1 Hits**")
            _display_df(table)
        else:
            conn = get_connection()
            year_rows = conn.execute(
                "SELECT DISTINCT SUBSTR(chart_date, 1, 4) AS year FROM chart_week ORDER BY year DESC"
            ).fetchall()
            year_options = ["All years"] + [row[0] for row in year_rows if row[0]]
            selected_year = st.selectbox("Year", year_options, key="quick_top10_year")
            selected_years: tuple[int, ...] = ()
            if selected_year != "All years":
                selected_years = (int(selected_year),)
            limit = st.slider("Rows", 10, 5000, 500, 10, key="quick_hits_limit")
            st.markdown("**Top 10 Hits**")
            table = build_quick_top10_hits(selected_years, limit)
            _display_df(table)
            if selected_year == "All years":
                st.caption("All-time list shows each song once, using its first-ever Top 10 week.")
            else:
                st.caption("Selected year list includes each song's first Top 10 appearance within that selected year, even if the song first reached the Top 10 in an earlier year.")

    elif subsection == "Movement":
        table_kind = st.selectbox(
            "View",
            ["Biggest climbers"],
            key="quick_movement_view",
        )
        limit = st.slider("Rows", 10, 500, 100, 10, key="quick_movement_limit")
        st.markdown("**Biggest Climbers**")
        _display_df(load_special_entries(table_kind, limit))

    elif subsection == "Artists":
        table_kind = st.selectbox(
            "View",
            [
                "Artists with most Top 10 weeks",
                "Artists reaching #1 in the same chart week in consecutive years",
                "Artists with #1 hits in consecutive years",
                "Longest #1 droughts",
            ],
            key="quick_artists_view",
        )

        if table_kind == "Artists with most Top 10 weeks":
            limit = st.slider("Rows", 10, 500, 100, 10, key="quick_artists_limit")
            st.markdown("**Artists with most Top 10 weeks**")
            _display_df(load_special_entries(table_kind, limit))
        elif table_kind == "Artists reaching #1 in the same chart week in consecutive years":
            st.markdown("**Artists reaching #1 in the same chart week in consecutive years**")
            st.caption("Uses the sequential chart-week number within each calendar year, so Chart Week #1 is the first chart published that year, Chart Week #2 is the second, and so on. Lead and featured artists both count.")
            _display_df(build_quick_artist_num1_same_chart_week_streaks())
        elif table_kind == "Artists with #1 hits in consecutive years":
            st.markdown("**Artists with #1 hits in consecutive years**")
            song_mode = st.radio(
                "Song filter",
                ["Same song only", "Different songs only"],
                horizontal=True,
                key="quick_artist_num1_year_song_filter",
            )
            if song_mode == "Same song only":
                st.caption("Shows artist streaks where at least one credited #1 song reached #1 in multiple years of the streak. Lead and featured artists both count.")
            else:
                st.caption("Shows only artist streaks where every credited #1 song appears in exactly one year of the streak. If a song repeats across years, that streak is omitted. Lead and featured artists both count.")
            _display_df(build_quick_artist_num1_year_streaks(song_mode))
        else:
            st.markdown("**Longest #1 droughts**")
            st.caption("Droughts are measured in available chart weeks between #1 appearances. Consecutive weeks at #1 by the same song/artist are treated as one reign, so the drought starts after the final week of that reign.")
            drought_cols = st.columns(4)
            credit_mode = drought_cols[0].radio(
                "Credit mode",
                ["Lead + featured artists", "Lead artists only"],
                horizontal=False,
                key="quick_artist_num1_drought_credit_mode",
            )
            song_mode = drought_cols[1].radio(
                "Song filter",
                ["Different songs only", "Any #1 return"],
                horizontal=False,
                key="quick_artist_num1_drought_song_mode",
            )
            drought_week_options = [52] + list(range(55, 501, 5))
            min_drought_weeks = drought_cols[2].select_slider(
                "Minimum drought length",
                options=drought_week_options,
                value=52,
                key="quick_artist_num1_drought_min_weeks",
            )
            limit = drought_cols[3].slider("Rows", 10, 500, 100, 10, key="quick_artist_num1_drought_limit")
            _display_df(build_quick_artist_num1_droughts(credit_mode, song_mode, min_drought_weeks, limit))

    elif subsection == "Debuts / Re-entries":
        table_kind = st.selectbox(
            "View",
            ["Top debuts", "Top 5 debuts", "Debut weeks", "Re-entries"],
            key="quick_debuts_view",
        )
        limit = st.slider("Rows", 10, 500, 100, 10, key="quick_debuts_limit")
        st.markdown(f"**{table_kind}**")
        table = load_special_entries(table_kind, limit).drop(columns=["last_week_position"], errors="ignore")
        _display_df(table)

    else:
        feat_view = st.selectbox(
            "View",
            [
                "Biggest gains to #1",
                "Biggest falls from #1",
                "Songs gaining from a selected position to #1",
                "Most consecutive weeks at #1 by year",
                "Songs debuting at X position that eventually reached #1",
                "Artist-exclusive Top 2–5",
            ],
            key="quick_feats_view",
        )
        if feat_view != "Most consecutive weeks at #1 by year":
            limit = st.slider("Rows", 10, 500, 100, 10, key="quick_feats_limit")
        else:
            limit = 1000000
        if feat_view == "Biggest gains to #1":
            st.markdown("**Biggest gains to #1**")
            _display_df(build_quick_num1_gains(limit))
        elif feat_view == "Biggest falls from #1":
            st.markdown("**Biggest falls from #1**")
            _display_df(build_quick_num1_falls(limit))
        elif feat_view == "Songs gaining from a selected position to #1":
            start_position = st.slider("Starting position", 2, 40, 2, 1, key="quick_to_num1_start")
            st.markdown(f"**Songs gaining from #{start_position} to #1**")
            _display_df(build_quick_from_position_to_num1(start_position, limit))
        elif feat_view == "Most consecutive weeks at #1 by year":
            st.markdown("**Most consecutive weeks at #1 by year**")
            _display_df(build_quick_num1_runs_by_year())
        elif feat_view == "Songs debuting at X position that eventually reached #1":
            debut_position = st.slider("Debut position", 1, 40, 1, 1, key="quick_debut_to_num1_start")
            st.markdown(f"**Songs debuting at #{debut_position} that eventually reached #1**")
            _display_df(build_quick_debut_position_to_num1(debut_position, limit))
        else:
            st.markdown("**Artist-exclusive Top 2–5**")
            st.caption("Top 2 means an artist appears on both #1 and #2; Top 3 means #1–#3 only; Top 4 means #1–#4 only; Top 5 means #1–#5. Lead and featured/GUEST appearances count.")
            _display_df(build_quick_artist_exclusive_top25(limit))

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
            "Chart Recap Generator",
            "Rivalries / Head-to-Head",
            "Quick tables",
            "Analytics",
            "Weekly Top Artists",
            "Forecast Lab",
            "Forecast Lab Scorecard",
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
    elif main_section == "Chart Recap Generator":
        render_chart_recap_tab()
    elif main_section == "Rivalries / Head-to-Head":
        render_rivalries_tab()
    elif main_section == "Quick tables":
        render_special_tables_tab()
    elif main_section == "Analytics":
        render_analytics_tab()
    elif main_section == "Weekly Top Artists":
        render_weekly_top_artists_tab()
    elif main_section == "Forecast Lab":
        render_forecast_lab_tab()
    elif main_section == "Forecast Lab Scorecard":
        render_forecast_scorecard_tab()
    else:
        render_admin_tab()


if __name__ == "__main__":
    main()
