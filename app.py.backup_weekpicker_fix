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

    tab_search, tab_week, tab_song, tab_artist, tab_special = st.tabs(
        ["Full-text search", "Week browser", "Canonical song history", "Artist history", "Quick tables"]
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
                st.dataframe(results, width="stretch", hide_index=True)
            except Exception as exc:
                st.error(f"Search query could not be run: {exc}")
        else:
            st.info("Enter a search query to browse the database.")

    with tab_week:
        st.subheader("Browse a chart week")
        dates = load_chart_dates()
        valid_dates = {dt.date.fromisoformat(d): d for d in dates}
        max_date = dt.date.fromisoformat(overview["max_date"])
        min_date = dt.date.fromisoformat(overview["min_date"])
        selected_date_obj = st.date_input(
            "Chart date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            format="YYYY-MM-DD",
        )
        selected_date = selected_date_obj.isoformat()
        if selected_date in valid_dates:
            df, meta = load_chart(selected_date)
            if meta:
                k1, k2, k3 = st.columns(3)
                k1.metric("Rows stored", meta["row_count"])
                k2.metric("Chart ID", meta["chart_id"] or "—")
                k3.metric("Source ZIP", meta["source_zip"] or "—")
                st.caption(f"Source file: {meta['source_file']}")
                if meta.get("notes"):
                    st.warning(meta["notes"])
            st.dataframe(df, width="stretch", hide_index=True)
        else:
            st.info("No chart exists for that date. Pick one of the actual chart weeks.")

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
                    st.dataframe(history, width="stretch", hide_index=True)
                    st.markdown("**Alias variants in this canonical song**")
                    st.dataframe(aliases, width="stretch", hide_index=True)
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
                    st.dataframe(songs, width="stretch", hide_index=True)
                    st.markdown("**Full week-by-week history**")
                    st.dataframe(history, width="stretch", hide_index=True)
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
        st.dataframe(table, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
