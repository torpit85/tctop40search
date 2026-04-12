#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import sqlite3
from pathlib import Path


DEFAULT_DB = Path("db/tctop40.sqlite")

FEAT_SPLIT_RE = re.compile(r"\s+(?:featuring|feat\.?|ft\.?|f/)\s+", re.IGNORECASE)


def normalize_spaces(value: str | None) -> str:
    value = (value or "").replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    return re.sub(r"\s+", " ", value.strip())


def normalize_title_key(value: str | None) -> str:
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = value.replace("'", "")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_artist_key(value: str | None) -> str:
    value = normalize_spaces(value).lower()
    value = value.replace("&", " and ")
    value = value.replace("'", "")
    value = re.sub(r"\b(featuring|feat\.?|ft\.?|f/)\b", " feat ", value)
    value = re.sub(r"[^a-z0-9;,/+()& -]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def resolve_song_bucket_key(value: str | None) -> str:
    text = normalize_spaces(value).lower()
    if not text:
        return ""

    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')

    prev = None
    while text != prev:
        prev = text

        # Strip trailing "from ..." labels
        text = re.sub(r'\s*-\s*from\s+"[^"]+"\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*-\s*from\s+'[^']+'\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*-\s*from\s+[^-()]+\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s+from\s+"[^"]+"\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+from\s+'[^']+'\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s+from\s+[^-()]+\s*$', "", text, flags=re.IGNORECASE)

        # Explicit soundtrack/source-style parentheticals
        text = re.sub(r'\s*\((original gangstas)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\((from [^)]*)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\((music from [^)]*)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(([^)]*soundtrack[^)]*)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(([^)]*motion picture[^)]*)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(([^)]*film[^)]*)\)\s*$', "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\(([^)]*movie[^)]*)\)\s*$', "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def artist_family_tokens(lead_key: str, featured_key: str) -> set[str]:
    text = " ".join(part for part in [lead_key or "", featured_key or ""] if part).strip()
    if not text:
        return set()
    text = text.replace(" feat ", " ")
    text = text.replace(" and ", " ")
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    stop = {"the", "and", "feat", "ft", "featuring"}
    return {tok for tok in tokens if tok not in stop}


def artist_families_related(a_lead: str, a_feat: str, b_lead: str, b_feat: str) -> bool:
    a_tokens = artist_family_tokens(a_lead, a_feat)
    b_tokens = artist_family_tokens(b_lead, b_feat)
    if not a_tokens or not b_tokens:
        return False
    if a_tokens == b_tokens:
        return True
    if a_tokens & b_tokens:
        return True
    a_text = " ".join(sorted(a_tokens))
    b_text = " ".join(sorted(b_tokens))
    if a_text in b_text or b_text in a_text:
        return True
    return False


def split_artist_roles(full_artist_display: str | None) -> tuple[str, str]:
    full_artist_display = normalize_spaces(full_artist_display)
    if not full_artist_display:
        return "", ""

    parts = FEAT_SPLIT_RE.split(full_artist_display, maxsplit=1)
    if len(parts) == 1:
        return full_artist_display, ""

    lead = normalize_spaces(parts[0])
    featured = normalize_spaces(parts[1])
    return lead, featured


def split_credit_people_list(value: str | None) -> str:
    text = normalize_spaces(value)
    if not text:
        return ""

    text = re.sub(r"\s*;\s*", ";", text)

    # Only treat commas as artist separators when there are multiple commas.
    if text.count(",") >= 2:
        text = re.sub(r"\s*,\s*", ";", text)

    text = re.sub(r"\s*&\s*", ";", text)
    text = re.sub(r"\s+(?:and|x|with)\s+", ";", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\+\s*", ";", text)

    parts = [normalize_spaces(part) for part in text.split(";") if normalize_spaces(part)]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        key = part.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(part)
    return "; ".join(deduped)


def make_backup(db_path: Path) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_artist_roles_{timestamp}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def ensure_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    for col, decl in [
        ("lead_artist_display", "TEXT"),
        ("featured_artist_display", "TEXT"),
        ("normalized_lead_artist", "TEXT"),
        ("normalized_featured_artist", "TEXT"),
    ]:
        ensure_column(conn, "entry", col, decl)

    for col, decl in [
        ("canonical_full_artist", "TEXT"),
        ("canonical_lead_artist", "TEXT"),
        ("canonical_featured_artist", "TEXT"),
    ]:
        ensure_column(conn, "canonical_song", col, decl)


def populate_artist_role_columns(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT entry_id, song_title_display, full_artist_display
        FROM entry
        ORDER BY entry_id
        """
    ).fetchall()

    updated = 0
    for entry_id, song_title_display, full_artist_display in rows:
        lead, featured = split_artist_roles(full_artist_display)
        lead = split_credit_people_list(lead)
        featured = split_credit_people_list(featured)
        norm_title = normalize_title_key(song_title_display)
        norm_full = normalize_artist_key(full_artist_display)
        norm_lead = normalize_artist_key(lead)
        norm_featured = normalize_artist_key(featured)
        conn.execute(
            """
            UPDATE entry
            SET normalized_song_title = ?,
                normalized_full_artist = ?,
                lead_artist_display = ?,
                featured_artist_display = ?,
                normalized_lead_artist = ?,
                normalized_featured_artist = ?
            WHERE entry_id = ?
            """,
            (norm_title, norm_full, lead, featured, norm_lead, norm_featured, entry_id),
        )
        updated += 1
    return updated


def rebuild_canonical_from_song_title_latest_artist(conn: sqlite3.Connection) -> None:
    """Resolve by cleaned song bucket, but keep separate unrelated artist families within a title bucket."""
    raw_rows = conn.execute(
        """
        SELECT
            e.entry_id,
            e.normalized_song_title,
            e.normalized_lead_artist,
            e.normalized_featured_artist,
            cw.chart_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE COALESCE(e.normalized_song_title, '') <> ''
        ORDER BY cw.chart_date DESC, e.entry_id DESC
        """
    ).fetchall()

    bucket_clusters: dict[str, list[dict[str, str]]] = {}
    entry_assignments: list[tuple[int, str, int]] = []

    for entry_id, normalized_song_title, normalized_lead_artist, normalized_featured_artist, _chart_date in raw_rows:
        bucket_key = resolve_song_bucket_key(normalized_song_title)
        if not bucket_key:
            continue

        lead_key = normalize_spaces(normalized_lead_artist or "")
        featured_key = normalize_spaces(normalized_featured_artist or "")

        clusters = bucket_clusters.setdefault(bucket_key, [])
        cluster_index = None

        for idx, cluster in enumerate(clusters):
            if artist_families_related(
                lead_key,
                featured_key,
                cluster["lead_key"],
                cluster["featured_key"],
            ):
                cluster_index = idx
                break

        if cluster_index is None:
            clusters.append({
                "lead_key": lead_key,
                "featured_key": featured_key,
            })
            cluster_index = len(clusters) - 1

        entry_assignments.append((entry_id, bucket_key, cluster_index))

    group_to_id: dict[str, int] = {}
    next_id = 1
    updates: list[tuple[int, str, str, str, int]] = []

    for entry_id, bucket_key, cluster_index in sorted(entry_assignments, key=lambda x: x[0]):
        cluster = bucket_clusters[bucket_key][cluster_index]
        lead_key = cluster["lead_key"]
        featured_key = cluster["featured_key"]
        artist_key = lead_key if not featured_key else f"{lead_key} feat {featured_key}"
        group_key = f"{bucket_key}||{lead_key}||{featured_key}"
        if group_key not in group_to_id:
            group_to_id[group_key] = next_id
            next_id += 1
        canonical_song_id = group_to_id[group_key]
        updates.append((canonical_song_id, bucket_key, artist_key, group_key, entry_id))

    conn.execute(
        """
        UPDATE entry
        SET canonical_song_id = NULL,
            canonical_title_key = NULL,
            canonical_artist_key = NULL,
            canonical_group_key = NULL
        """
    )

    conn.executemany(
        """
        UPDATE entry
        SET canonical_song_id = ?,
            canonical_title_key = ?,
            canonical_artist_key = ?,
            canonical_group_key = ?
        WHERE entry_id = ?
        """,
        updates,
    )


def rebuild_canonical_song_table(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM canonical_song")
    conn.execute("DELETE FROM song_alias")

    conn.execute(
        """
        INSERT INTO canonical_song (
            canonical_song_id,
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
        )
        WITH latest_per_group AS (
            SELECT
                e.canonical_group_key,
                e.entry_id,
                e.song_title_display,
                e.full_artist_display,
                e.lead_artist_display,
                e.featured_artist_display,
                ROW_NUMBER() OVER (
                    PARTITION BY e.canonical_group_key
                    ORDER BY cw.chart_date DESC, e.entry_id DESC
                ) AS rn
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_group_key IS NOT NULL
              AND e.canonical_group_key <> ''
        )
        SELECT
            MIN(e.canonical_song_id) AS canonical_song_id,
            MAX(CASE WHEN lpg.rn = 1 THEN lpg.song_title_display END) AS canonical_title,
            MAX(CASE WHEN lpg.rn = 1 THEN lpg.full_artist_display END) AS canonical_artist,
            MAX(e.canonical_title_key) AS canonical_title_key,
            MAX(e.canonical_artist_key) AS canonical_artist_key,
            e.canonical_group_key,
            COUNT(*) AS entry_count,
            COUNT(DISTINCT e.normalized_song_title || '||' || COALESCE(e.normalized_full_artist, '')) AS alias_count,
            MIN(cw.chart_date) AS first_chart_date,
            MAX(cw.chart_date) AS last_chart_date,
            MAX(CASE WHEN lpg.rn = 1 THEN lpg.full_artist_display END) AS canonical_full_artist,
            MAX(CASE WHEN lpg.rn = 1 THEN lpg.lead_artist_display END) AS canonical_lead_artist,
            MAX(CASE WHEN lpg.rn = 1 THEN lpg.featured_artist_display END) AS canonical_featured_artist
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        JOIN latest_per_group lpg
          ON lpg.canonical_group_key = e.canonical_group_key
        WHERE e.canonical_group_key IS NOT NULL
          AND e.canonical_group_key <> ''
        GROUP BY e.canonical_group_key
        ORDER BY MIN(e.canonical_song_id)
        """
    )

    conn.execute(
        """
        INSERT INTO song_alias (
            alias_song_title,
            alias_artist,
            alias_title_key,
            alias_artist_key,
            alias_group_key,
            alias_display_key,
            canonical_song_id,
            entry_count,
            week_count,
            first_chart_date,
            last_chart_date
        )
        WITH alias_rows AS (
            SELECT
                e.song_title_display,
                e.full_artist_display,
                e.normalized_song_title,
                e.normalized_full_artist,
                e.canonical_group_key,
                e.normalized_song_title || '||' || e.normalized_full_artist AS alias_display_key,
                e.canonical_song_id,
                e.chart_week_id,
                cw.chart_date,
                e.entry_id
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.normalized_song_title IS NOT NULL
              AND e.normalized_song_title <> ''
              AND e.normalized_full_artist IS NOT NULL
              AND e.normalized_full_artist <> ''
        ),
        latest_alias AS (
            SELECT
                alias_display_key,
                song_title_display,
                full_artist_display,
                canonical_group_key,
                ROW_NUMBER() OVER (
                    PARTITION BY alias_display_key
                    ORDER BY chart_date DESC, entry_id DESC
                ) AS rn
            FROM alias_rows
        )
        SELECT
            MAX(CASE WHEN la.rn = 1 THEN la.song_title_display END) AS alias_song_title,
            MAX(CASE WHEN la.rn = 1 THEN la.full_artist_display END) AS alias_artist,
            ar.normalized_song_title AS alias_title_key,
            ar.normalized_full_artist AS alias_artist_key,
            MAX(CASE WHEN la.rn = 1 THEN la.canonical_group_key END) AS alias_group_key,
            ar.alias_display_key,
            MIN(ar.canonical_song_id) AS canonical_song_id,
            COUNT(*) AS entry_count,
            COUNT(DISTINCT ar.chart_week_id) AS week_count,
            MIN(ar.chart_date) AS first_chart_date,
            MAX(ar.chart_date) AS last_chart_date
        FROM alias_rows ar
        JOIN latest_alias la
          ON la.alias_display_key = ar.alias_display_key
        GROUP BY
            ar.alias_display_key,
            ar.normalized_song_title,
            ar.normalized_full_artist
        ORDER BY
            MAX(CASE WHEN la.rn = 1 THEN la.full_artist_display END),
            MAX(CASE WHEN la.rn = 1 THEN la.song_title_display END)
        """
    )


def rederive_markers(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE entry
        SET derived_is_debut = 0,
            derived_is_top_debut = 0,
            derived_is_reentry = 0,
            derived_marker = NULL
        """
    )

    chart_weeks = conn.execute(
        "SELECT chart_week_id, chart_date FROM chart_week ORDER BY chart_date, chart_week_id"
    ).fetchall()

    seen: set[int] = set()
    previous_week_songs: set[int] = set()

    for chart_week_id, _chart_date in chart_weeks:
        rows = conn.execute(
            """
            SELECT entry_id, position, canonical_song_id
            FROM entry
            WHERE chart_week_id = ?
            ORDER BY position, entry_id
            """,
            (chart_week_id,),
        ).fetchall()

        week_song_ids = {row[2] for row in rows if row[2] is not None}
        debut_rows: list[tuple[int, int]] = []

        for entry_id, position, canonical_song_id in rows:
            if canonical_song_id is None:
                continue
            if canonical_song_id not in seen:
                conn.execute(
                    "UPDATE entry SET derived_is_debut = 1, derived_marker = 'DEBUT' WHERE entry_id = ?",
                    (entry_id,),
                )
                debut_rows.append((position, entry_id))
                seen.add(canonical_song_id)
            elif canonical_song_id not in previous_week_songs:
                conn.execute(
                    "UPDATE entry SET derived_is_reentry = 1, derived_marker = 'RE-ENTRY' WHERE entry_id = ?",
                    (entry_id,),
                )

        if debut_rows:
            _best_position, best_entry_id = min(debut_rows)
            conn.execute(
                """
                UPDATE entry
                SET derived_is_top_debut = 1,
                    derived_marker = 'TOP DEBUT'
                WHERE entry_id = ?
                """,
                (best_entry_id,),
            )

        previous_week_songs = week_song_ids


def print_preview(conn: sqlite3.Connection, limit: int) -> None:
    rows = conn.execute(
        """
        SELECT entry_id, full_artist_display, lead_artist_display, featured_artist_display
        FROM entry
        WHERE full_artist_display IS NOT NULL
          AND TRIM(full_artist_display) <> ''
        ORDER BY entry_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    print("Preview of split artist roles:")
    for entry_id, full_credit, lead, featured in rows:
        print(f"{entry_id}: full={full_credit!r} | lead={lead!r} | featured={featured!r}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Split full artist credits into lead/featured roles, prefer the most recent "
            "lead/featured display strings when rebuilding canonical songs, and optionally "
            "rebuild canonical identity using song title + the most recent lead/featured pairing."
        )
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Preview splits without writing changes")
    parser.add_argument("--preview-limit", type=int, default=25, help="How many rows to show in dry-run preview")
    parser.add_argument(
        "--no-rebuild-canonical",
        action="store_true",
        help="Only populate lead/featured artist columns; do not rebuild canonical song identity",
    )
    args = parser.parse_args()

    db_path = args.db.resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        ensure_schema(conn)

        if args.dry_run:
            conn.execute("BEGIN")
            updated = populate_artist_role_columns(conn)
            print_preview(conn, args.preview_limit)
            conn.rollback()
            print(f"\nDry run only. No changes were written. Rows examined: {updated}")
            return

        backup = make_backup(db_path)
        print(f"Backup created: {backup}")

        with conn:
            updated = populate_artist_role_columns(conn)
            if not args.no_rebuild_canonical:
                rebuild_canonical_from_song_title_latest_artist(conn)
                rebuild_canonical_song_table(conn)
                rederive_markers(conn)

        print(f"Updated entry rows: {updated}")
        if args.no_rebuild_canonical:
            print("Canonical song tables were left unchanged.")
        else:
            canonical_songs = conn.execute("SELECT COUNT(*) FROM canonical_song").fetchone()[0]
            aliases = conn.execute("SELECT COUNT(*) FROM song_alias").fetchone()[0]
            print(f"Canonical songs: {canonical_songs}")
            print(f"Song aliases: {aliases}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
