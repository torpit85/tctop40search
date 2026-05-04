#!/usr/bin/env python3
"""
Import Last.fm weekly track play data for a Torrey's Corner Top 40 chart week.

Forecast Lab window:
    chart_date - 10 days at 00:00:00
    through
    chart_date - 4 days at 23:59:59
    America/Chicago time

Example:
    Chart date 2026-05-05
    Last.fm window 2026-04-25 00:00:00 through 2026-05-01 23:59:59

Usage:
    export LASTFM_API_KEY="your_key_here"
    export LASTFM_USER="IsidoreFerris"

    python3 scripts/import_lastfm_weekly.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-05-05
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

import requests


API_ROOT = "https://ws.audioscrobbler.com/2.0/"
LOCAL_TZ = ZoneInfo("America/Chicago")


def normalize_text(value: str) -> str:
    """
    Roughly match the normalization style used in the TC Top 40 app.
    This does not need to be perfect because we also preserve raw Last.fm names.
    """
    value = value or ""
    value = value.lower()
    value = value.replace("&", " and ")

    # Normalize common featured-artist words.
    value = re.sub(r"\b(feat|ft|featuring)\.?\b", " featuring ", value)

    # Remove punctuation/noise.
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def lastfm_forecast_window_for_chart(chart_date_str: str) -> tuple[int, int, str, str]:
    """
    For a Tuesday chart date, return:
      previous Saturday 00:00:00
      through current Friday 23:59:59

    Example:
      2026-05-05 -> 2026-04-25 00:00:00 through 2026-05-01 23:59:59
    """
    chart_date = datetime.strptime(chart_date_str, "%Y-%m-%d").date()

    start_date = chart_date - timedelta(days=10)
    end_date = chart_date - timedelta(days=4)

    start_dt = datetime.combine(start_date, time(0, 0, 0), tzinfo=LOCAL_TZ)
    end_dt = datetime.combine(end_date, time(23, 59, 59), tzinfo=LOCAL_TZ)

    return (
        int(start_dt.timestamp()),
        int(end_dt.timestamp()),
        start_dt.isoformat(),
        end_dt.isoformat(),
    )


def fetch_lastfm_weekly_tracks(
    api_key: str,
    username: str,
    period_from: int,
    period_to: int,
) -> list[dict]:
    params = {
        "method": "user.getweeklytrackchart",
        "user": username,
        "api_key": api_key,
        "from": period_from,
        "to": period_to,
        "format": "json",
    }

    response = requests.get(API_ROOT, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "error" in data:
        raise RuntimeError(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    tracks = data.get("weeklytrackchart", {}).get("track", [])

    # Last.fm sometimes returns a single object instead of a list.
    if isinstance(tracks, dict):
        tracks = [tracks]

    return tracks


def ensure_lastfm_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS lastfm_weekly_track_play (
            lastfm_play_id INTEGER PRIMARY KEY AUTOINCREMENT,

            chart_week_id INTEGER,

            period_from INTEGER NOT NULL,
            period_to INTEGER NOT NULL,
            period_from_local TEXT,
            period_to_local TEXT,

            lastfm_track_name TEXT NOT NULL,
            lastfm_artist_name TEXT NOT NULL,
            lastfm_mbid TEXT,

            song_title_key TEXT NOT NULL,
            artist_key TEXT NOT NULL,

            canonical_song_id INTEGER,

            playcount INTEGER NOT NULL,
            lastfm_rank INTEGER,

            imported_at TEXT DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(period_from, period_to, song_title_key, artist_key)
        );

        CREATE INDEX IF NOT EXISTS idx_lastfm_weekly_track_play_chart_week
        ON lastfm_weekly_track_play(chart_week_id);

        CREATE INDEX IF NOT EXISTS idx_lastfm_weekly_track_play_canonical_song
        ON lastfm_weekly_track_play(canonical_song_id);

        CREATE INDEX IF NOT EXISTS idx_lastfm_weekly_track_play_keys
        ON lastfm_weekly_track_play(song_title_key, artist_key);

        CREATE INDEX IF NOT EXISTS idx_lastfm_weekly_track_play_period
        ON lastfm_weekly_track_play(period_from, period_to);
        """
    )


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def find_chart_week_id(conn: sqlite3.Connection, chart_date: str) -> int | None:
    row = conn.execute(
        """
        SELECT chart_week_id
        FROM chart_week
        WHERE chart_date = ?
        LIMIT 1
        """,
        (chart_date,),
    ).fetchone()

    return int(row[0]) if row else None


def find_canonical_song_id(
    conn: sqlite3.Connection,
    raw_track_name: str,
    raw_artist_name: str,
    song_key: str,
    artist_key: str,
) -> int | None:
    """
    Conservative matcher.

    First tries entry rows because entry definitely connects displayed song/artist
    names to canonical_song_id in your app. Then tries canonical_song if compatible
    normalized columns exist.

    This is intentionally simple. You can improve it later with alias matching.
    """
    entry_cols = get_table_columns(conn, "entry")

    # Best case: entry has normalized fields.
    if {
        "normalized_song_title",
        "normalized_full_artist",
        "canonical_song_id",
    }.issubset(entry_cols):
        row = conn.execute(
            """
            SELECT canonical_song_id
            FROM entry
            WHERE normalized_song_title = ?
              AND normalized_full_artist = ?
              AND canonical_song_id IS NOT NULL
            GROUP BY canonical_song_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """,
            (song_key, artist_key),
        ).fetchone()

        if row:
            return int(row[0])

    # Fallback: compare normalized display strings from entry.
    if {
        "song_title_display",
        "full_artist_display",
        "canonical_song_id",
    }.issubset(entry_cols):
        rows = conn.execute(
            """
            SELECT canonical_song_id, song_title_display, full_artist_display
            FROM entry
            WHERE canonical_song_id IS NOT NULL
            """
        ).fetchall()

        counts: dict[int, int] = {}

        for row in rows:
            cid = row[0]
            entry_song_key = normalize_text(row[1])
            entry_artist_key = normalize_text(row[2])

            if entry_song_key == song_key and entry_artist_key == artist_key:
                counts[int(cid)] = counts.get(int(cid), 0) + 1

        if counts:
            return sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0]

    # Optional canonical_song matching, depending on your schema.
    canonical_cols = get_table_columns(conn, "canonical_song")

    possible_title_cols = [
        "normalized_title",
        "normalized_song_title",
        "title_key",
        "song_title_key",
        "canonical_title_key",
    ]

    possible_artist_cols = [
        "normalized_full_artist",
        "artist_key",
        "full_artist_key",
        "canonical_artist_key",
    ]

    title_col = next((c for c in possible_title_cols if c in canonical_cols), None)
    artist_col = next((c for c in possible_artist_cols if c in canonical_cols), None)

    if title_col and artist_col and "canonical_song_id" in canonical_cols:
        sql = f"""
            SELECT canonical_song_id
            FROM canonical_song
            WHERE {title_col} = ?
              AND {artist_col} = ?
            LIMIT 1
        """
        row = conn.execute(sql, (song_key, artist_key)).fetchone()
        if row:
            return int(row[0])

    return None


def import_lastfm_week(
    db_path: str,
    chart_date: str,
    api_key: str,
    username: str,
    dry_run: bool = False,
) -> None:
    period_from, period_to, period_from_local, period_to_local = lastfm_forecast_window_for_chart(
        chart_date
    )

    print("Forecast Lab Last.fm window:")
    print(f"  Chart date: {chart_date}")
    print(f"  From:       {period_from_local}")
    print(f"  To:         {period_to_local}")

    tracks = fetch_lastfm_weekly_tracks(
        api_key=api_key,
        username=username,
        period_from=period_from,
        period_to=period_to,
    )

    print(f"\nLast.fm returned {len(tracks)} tracks.")

    conn = sqlite3.connect(db_path)

    try:
        ensure_lastfm_table(conn)

        chart_week_id = find_chart_week_id(conn, chart_date)

        if chart_week_id is None:
            print(f"\nWARNING: No chart_week row found for chart_date={chart_date}.")
            print("Rows will still import, but chart_week_id will be NULL.")
        else:
            print(f"Matched chart_week_id: {chart_week_id}")

        written = 0
        matched = 0
        unmatched_examples: list[str] = []

        for index, track in enumerate(tracks, start=1):
            track_name = (track.get("name") or "").strip()

            artist_obj = track.get("artist", {})
            if isinstance(artist_obj, dict):
                artist_name = (artist_obj.get("#text") or "").strip()
            else:
                artist_name = str(artist_obj).strip()

            if not track_name or not artist_name:
                continue

            song_key = normalize_text(track_name)
            artist_key = normalize_text(artist_name)

            try:
                playcount = int(track.get("playcount", 0) or 0)
            except ValueError:
                playcount = 0

            try:
                lastfm_rank = int(track.get("@attr", {}).get("rank", index) or index)
            except ValueError:
                lastfm_rank = index

            lastfm_mbid = track.get("mbid") or None

            canonical_song_id = find_canonical_song_id(
                conn=conn,
                raw_track_name=track_name,
                raw_artist_name=artist_name,
                song_key=song_key,
                artist_key=artist_key,
            )

            if canonical_song_id is not None:
                matched += 1
            elif len(unmatched_examples) < 10:
                unmatched_examples.append(f"{track_name} - {artist_name}")

            if dry_run:
                print(
                    f"{lastfm_rank:>3}. {track_name} - {artist_name} "
                    f"plays={playcount} canonical_song_id={canonical_song_id}"
                )
                continue

            conn.execute(
                """
                INSERT INTO lastfm_weekly_track_play (
                    chart_week_id,
                    period_from,
                    period_to,
                    period_from_local,
                    period_to_local,
                    lastfm_track_name,
                    lastfm_artist_name,
                    lastfm_mbid,
                    song_title_key,
                    artist_key,
                    canonical_song_id,
                    playcount,
                    lastfm_rank
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(period_from, period_to, song_title_key, artist_key)
                DO UPDATE SET
                    chart_week_id = excluded.chart_week_id,
                    period_from_local = excluded.period_from_local,
                    period_to_local = excluded.period_to_local,
                    lastfm_track_name = excluded.lastfm_track_name,
                    lastfm_artist_name = excluded.lastfm_artist_name,
                    lastfm_mbid = excluded.lastfm_mbid,
                    canonical_song_id = excluded.canonical_song_id,
                    playcount = excluded.playcount,
                    lastfm_rank = excluded.lastfm_rank,
                    imported_at = CURRENT_TIMESTAMP
                """,
                (
                    chart_week_id,
                    period_from,
                    period_to,
                    period_from_local,
                    period_to_local,
                    track_name,
                    artist_name,
                    lastfm_mbid,
                    song_key,
                    artist_key,
                    canonical_song_id,
                    playcount,
                    lastfm_rank,
                ),
            )

            written += 1

        if not dry_run:
            conn.commit()

        print("\nImport summary:")
        print(f"  Tracks returned:     {len(tracks)}")
        print(f"  Rows written:        {written if not dry_run else 0}")
        print(f"  Canonical matches:   {matched}")
        print(f"  Unmatched tracks:    {len(tracks) - matched}")

        if unmatched_examples:
            print("\nSample unmatched tracks:")
            for item in unmatched_examples:
                print(f"  - {item}")

        if dry_run:
            print("\nDry run only. No rows were written.")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Last.fm weekly track plays for a TC Top 40 Forecast Lab chart date."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database.")
    parser.add_argument("--chart-date", required=True, help="Chart date in YYYY-MM-DD format.")
    parser.add_argument(
        "--user",
        default=os.environ.get("LASTFM_USER"),
        help="Last.fm username. Defaults to LASTFM_USER env var.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("LASTFM_API_KEY"),
        help="Last.fm API key. Defaults to LASTFM_API_KEY env var.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and match tracks, but do not write rows.",
    )

    args = parser.parse_args()

    if not args.user:
        raise SystemExit("Missing Last.fm username. Set LASTFM_USER or pass --user.")

    if not args.api_key:
        raise SystemExit("Missing Last.fm API key. Set LASTFM_API_KEY or pass --api-key.")

    import_lastfm_week(
        db_path=args.db,
        chart_date=args.chart_date,
        api_key=args.api_key,
        username=args.user,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
