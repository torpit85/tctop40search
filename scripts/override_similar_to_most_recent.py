#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path


DEFAULT_DB = Path("db/tctop40.sqlite")


@dataclass
class AliasRow:
    alias_id: int
    alias_song_title: str
    alias_artist: str
    alias_title_key: str
    alias_artist_key: str
    alias_group_key: str
    canonical_song_id: int
    entry_count: int
    week_count: int
    first_chart_date: str | None
    last_chart_date: str | None


@dataclass
class Cluster:
    member_alias_ids: list[int]
    chosen_alias_id: int
    chosen_title: str
    chosen_artist: str
    chosen_title_key: str
    chosen_artist_key: str
    chosen_group_key: str


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def tokenize(value: str) -> list[str]:
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\b(featuring|feat\.?|ft\.?|f/)\b", " feat ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return [part for part in value.split() if part]


def soften_token(token: str) -> str:
    token = re.sub(r"(.)\1+$", r"\1", token)
    token = re.sub(r"(.)\1+", r"\1", token)
    return token


def soft_title_key(title: str) -> str:
    return " ".join(soften_token(tok) for tok in tokenize(title))


def token_sort_key(title: str) -> str:
    return " ".join(sorted(tokenize(title)))


def sequence_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def parse_date(value: str | None) -> tuple[int, int, int]:
    if not value:
        return (0, 0, 0)
    try:
        d = dt.date.fromisoformat(value)
        return (d.year, d.month, d.day)
    except ValueError:
        return (0, 0, 0)


def choose_recent_alias(rows: list[AliasRow]) -> AliasRow:
    def keyfunc(row: AliasRow):
        return (
            parse_date(row.last_chart_date),
            row.week_count,
            row.entry_count,
            row.alias_id,
        )

    return max(rows, key=keyfunc)


def number_tokens(tokens: list[str]) -> set[str]:
    return {tok for tok in tokens if tok.isdigit()}


def should_merge(a: AliasRow, b: AliasRow, threshold: float) -> bool:
    if a.alias_artist_key != b.alias_artist_key:
        return False

    a_title = normalize_spaces(a.alias_song_title)
    b_title = normalize_spaces(b.alias_song_title)

    if not a_title or not b_title:
        return False

    if a.alias_title_key == b.alias_title_key:
        return False

    a_tokens = tokenize(a_title)
    b_tokens = tokenize(b_title)

    if not a_tokens or not b_tokens:
        return False

    # Do not merge titles that disagree on explicit number tokens.
    # This blocks chapter / part / numbered-track collisions.
    if number_tokens(a_tokens) != number_tokens(b_tokens):
        return False

    shared = set(a_tokens) & set(b_tokens)
    union = set(a_tokens) | set(b_tokens)
    jaccard = len(shared) / len(union) if union else 0.0

    # Require decent token overlap before allowing fuzzy title merges.
    if jaccard < 0.6:
        return False

    soft_a = soft_title_key(a_title)
    soft_b = soft_title_key(b_title)
    if soft_a and soft_a == soft_b:
        return True

    sort_a = token_sort_key(a_title)
    sort_b = token_sort_key(b_title)
    if sort_a and sort_a == sort_b and len(sort_a.split()) >= 3:
        return True

    ratio = sequence_ratio(a.alias_title_key or a_title.lower(), b.alias_title_key or b_title.lower())
    return ratio >= threshold


def fetch_alias_rows(conn: sqlite3.Connection) -> list[AliasRow]:
    rows = conn.execute(
        """
        SELECT
            alias_id,
            alias_song_title,
            alias_artist,
            alias_title_key,
            alias_artist_key,
            alias_group_key,
            canonical_song_id,
            entry_count,
            week_count,
            first_chart_date,
            last_chart_date
        FROM song_alias
        ORDER BY alias_artist_key, alias_song_title, alias_id
        """
    ).fetchall()
    return [AliasRow(*row) for row in rows]


def connected_components(rows: list[AliasRow], threshold: float) -> list[list[AliasRow]]:
    by_artist: dict[str, list[AliasRow]] = {}
    for row in rows:
        by_artist.setdefault(row.alias_artist_key, []).append(row)

    components: list[list[AliasRow]] = []
    for _artist_key, group in by_artist.items():
        if len(group) == 1:
            continue

        adjacency: dict[int, set[int]] = {row.alias_id: set() for row in group}
        for i, left in enumerate(group):
            for right in group[i + 1 :]:
                if should_merge(left, right, threshold=threshold):
                    adjacency[left.alias_id].add(right.alias_id)
                    adjacency[right.alias_id].add(left.alias_id)

        seen: set[int] = set()
        lookup = {row.alias_id: row for row in group}
        for row in group:
            if row.alias_id in seen or not adjacency[row.alias_id]:
                continue
            stack = [row.alias_id]
            comp_ids: list[int] = []
            seen.add(row.alias_id)
            while stack:
                node = stack.pop()
                comp_ids.append(node)
                for nxt in adjacency[node]:
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            components.append([lookup[alias_id] for alias_id in sorted(comp_ids)])
    return components


def build_clusters(rows: list[AliasRow], threshold: float) -> list[Cluster]:
    clusters: list[Cluster] = []
    for component in connected_components(rows, threshold=threshold):
        chosen = choose_recent_alias(component)
        clusters.append(
            Cluster(
                member_alias_ids=[row.alias_id for row in component],
                chosen_alias_id=chosen.alias_id,
                chosen_title=chosen.alias_song_title,
                chosen_artist=chosen.alias_artist,
                chosen_title_key=chosen.alias_title_key,
                chosen_artist_key=chosen.alias_artist_key,
                chosen_group_key=chosen.alias_group_key,
            )
        )
    return clusters


def ensure_support_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS canonical_override_log (
            override_run_id INTEGER PRIMARY KEY,
            run_ts TEXT NOT NULL,
            alias_ids TEXT NOT NULL,
            chosen_alias_id INTEGER NOT NULL,
            chosen_title TEXT NOT NULL,
            chosen_artist TEXT NOT NULL,
            chosen_group_key TEXT NOT NULL,
            threshold REAL NOT NULL
        )
        """
    )


def normalize_canonical_ids(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT canonical_group_key, MIN(canonical_song_id) AS keep_id
        FROM entry
        WHERE canonical_group_key IS NOT NULL
          AND canonical_group_key <> ''
        GROUP BY canonical_group_key
        """
    ).fetchall()

    for canonical_group_key, keep_id in rows:
        conn.execute(
            """
            UPDATE entry
            SET canonical_song_id = ?
            WHERE canonical_group_key = ?
            """,
            (keep_id, canonical_group_key),
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
            last_chart_date
        )
        WITH latest_per_group AS (
            SELECT
                e.canonical_group_key,
                e.entry_id,
                e.song_title_display,
                e.full_artist_display,
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
            COUNT(DISTINCT e.normalized_song_title || '||' || e.normalized_full_artist) AS alias_count,
            MIN(cw.chart_date) AS first_chart_date,
            MAX(cw.chart_date) AS last_chart_date
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
            WHERE e.canonical_group_key IS NOT NULL
              AND e.canonical_group_key <> ''
        ),
        latest_alias AS (
            SELECT
                alias_display_key,
                canonical_group_key,
                song_title_display,
                full_artist_display,
                ROW_NUMBER() OVER (
                    PARTITION BY alias_display_key, canonical_group_key
                    ORDER BY chart_date DESC, entry_id DESC
                ) AS rn
            FROM alias_rows
        )
        SELECT
            MAX(CASE WHEN la.rn = 1 THEN la.song_title_display END) AS alias_song_title,
            MAX(CASE WHEN la.rn = 1 THEN la.full_artist_display END) AS alias_artist,
            ar.normalized_song_title AS alias_title_key,
            ar.normalized_full_artist AS alias_artist_key,
            ar.canonical_group_key,
            ar.alias_display_key,
            MIN(ar.canonical_song_id) AS canonical_song_id,
            COUNT(*) AS entry_count,
            COUNT(DISTINCT ar.chart_week_id) AS week_count,
            MIN(ar.chart_date) AS first_chart_date,
            MAX(ar.chart_date) AS last_chart_date
        FROM alias_rows ar
        JOIN latest_alias la
          ON la.alias_display_key = ar.alias_display_key
         AND la.canonical_group_key = ar.canonical_group_key
        GROUP BY
            ar.alias_display_key,
            ar.normalized_song_title,
            ar.normalized_full_artist,
            ar.canonical_group_key
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


def apply_clusters(conn: sqlite3.Connection, clusters: list[Cluster], threshold: float, dry_run: bool) -> None:
    ensure_support_tables(conn)

    alias_lookup = {
        row[0]: row
        for row in conn.execute(
            """
            SELECT alias_id, alias_song_title, alias_artist,
                   alias_title_key, alias_artist_key, alias_group_key, canonical_song_id
            FROM song_alias
            """
        ).fetchall()
    }

    for cluster in clusters:
        member_rows = [alias_lookup[alias_id] for alias_id in cluster.member_alias_ids if alias_id in alias_lookup]
        if not member_rows:
            continue

        canonical_song_ids = sorted({row[6] for row in member_rows})
        keep_canonical_song_id = min(canonical_song_ids)

        alias_pairs = [(row[1], row[2], row[3], row[4]) for row in member_rows]
        if not dry_run:
            for song_title_display, full_artist_display, normalized_song_title, normalized_full_artist in alias_pairs:
                conn.execute(
                    """
                    UPDATE entry
                    SET canonical_song_id = ?,
                        canonical_title_key = ?,
                        canonical_artist_key = ?,
                        canonical_group_key = ?
                    WHERE song_title_display = ?
                      AND full_artist_display = ?
                      AND normalized_song_title = ?
                      AND normalized_full_artist = ?
                    """,
                    (
                        keep_canonical_song_id,
                        cluster.chosen_title_key,
                        cluster.chosen_artist_key,
                        cluster.chosen_group_key,
                        song_title_display,
                        full_artist_display,
                        normalized_song_title,
                        normalized_full_artist,
                    ),
                )

            conn.execute(
                """
                INSERT INTO canonical_override_log (
                    run_ts, alias_ids, chosen_alias_id, chosen_title,
                    chosen_artist, chosen_group_key, threshold
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dt.datetime.now().isoformat(timespec="seconds"),
                    ",".join(str(alias_id) for alias_id in cluster.member_alias_ids),
                    cluster.chosen_alias_id,
                    cluster.chosen_title,
                    cluster.chosen_artist,
                    cluster.chosen_group_key,
                    threshold,
                ),
            )


def make_backup(db_path: Path) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}.backup_{timestamp}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    return backup


def print_preview(clusters: list[Cluster], conn: sqlite3.Connection, limit: int) -> None:
    alias_lookup = {
        row[0]: row[1:]
        for row in conn.execute(
            "SELECT alias_id, alias_song_title, alias_artist, last_chart_date FROM song_alias"
        ).fetchall()
    }
    print(f"Proposed clusters: {len(clusters)}")
    for cluster in clusters[:limit]:
        print("-" * 80)
        print(f"Chosen: {cluster.chosen_title} — {cluster.chosen_artist} (alias_id={cluster.chosen_alias_id})")
        for alias_id in cluster.member_alias_ids:
            title, artist, last_chart_date = alias_lookup[alias_id]
            print(f"  {alias_id}: {title} — {artist} [last={last_chart_date}]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Merge similar song/artist alias strings inside tctop40.sqlite and "
            "prefer the most recent string as the canonical display."
        )
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.97,
        help="Sequence similarity threshold for title matching (default: 0.97)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview proposed merges without writing changes")
    parser.add_argument("--preview-limit", type=int, default=50, help="How many clusters to print in preview")
    args = parser.parse_args()

    db_path = args.db.resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        aliases = fetch_alias_rows(conn)
        clusters = build_clusters(aliases, threshold=args.threshold)
        print_preview(clusters, conn, limit=args.preview_limit)

        if args.dry_run:
            print("\nDry run only. No changes were written.")
            return

        backup = make_backup(db_path)
        print(f"\nBackup created: {backup}")

        with conn:
            apply_clusters(conn, clusters, threshold=args.threshold, dry_run=False)
            normalize_canonical_ids(conn)
            rebuild_canonical_song_table(conn)
            rederive_markers(conn)

        print("\nDone.")
        print(f"Merged clusters applied: {len(clusters)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
