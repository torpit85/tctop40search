#!/usr/bin/env python3
"""
Fuzzy-match imported Last.fm tracks to nearest canonical songs in the TC Top 40 DB.

Default behavior:
    Dry run only. Shows proposed matches but writes nothing.

Usage examples:

    python3 scripts/match_lastfm_tracks.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-04-28

    python3 scripts/match_lastfm_tracks.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-04-28 \
      --min-score 82

    python3 scripts/match_lastfm_tracks.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-04-28 \
      --write

    python3 scripts/match_lastfm_tracks.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-04-28 \
      --interactive

    # Search the entire DB when Last.fm played tracks are not on the selected chart:
    python3 scripts/match_lastfm_tracks.py \
      --db db/tctop40.sqlite \
      --chart-date 2026-04-28 \
      --all-db
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path


FEATURE_PATTERNS = [
    r"\bfeat\.?\b",
    r"\bft\.?\b",
    r"\bfeaturing\b",
    r"\bwith\b",
]


def normalize_text(value: str) -> str:
    value = value or ""
    value = value.lower()
    value = (
        value.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("`", "'")
    )
    value = value.replace("&", " and ")

    # Last.fm and the chart DB may disagree on apostrophes:
    #   "I'm Da Man" vs "Im Da Man"
    # Treat apostrophes as removable inside words instead of word separators.
    value = re.sub(r"(?<=[a-z0-9])['’](?=[a-z0-9])", "", value)

    # Normalize feature wording.
    value = re.sub(r"\b(feat|ft|featuring)\.?\b", " featuring ", value)

    # Remove punctuation/noise.
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def compact_text(value: str) -> str:
    """Normalized text with all separators removed. Useful for punctuation variants."""
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value))


def compact_title(value: str) -> str:
    return compact_text(normalize_title(value))


def normalize_title(value: str) -> str:
    value = value or ""

    # Remove parenthetical feature/version notes:
    # "Big Dog (with That Mexican OT & 2 Chainz)" -> "Big Dog"
    value = re.sub(r"\([^)]*\)", " ", value)

    # Remove bracketed version notes.
    value = re.sub(r"\[[^]]*\]", " ", value)

    # Remove common remix/version words, conservatively.
    value = re.sub(
        r"\b(remix|radio edit|clean|dirty|explicit|instrumental|bonus track|main track)\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    return normalize_text(value)


def token_sort(value: str) -> str:
    tokens = normalize_text(value).split()
    return " ".join(sorted(tokens))


def ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio() * 100.0


def best_text_score(a: str, b: str) -> float:
    """
    Compare both normal order and token-sorted forms.
    """
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    return max(
        ratio(a_norm, b_norm),
        ratio(token_sort(a_norm), token_sort(b_norm)),
    )


def title_score(lastfm_title: str, db_title: str) -> float:
    """
    Title score is deliberately strong because Last.fm track title formatting is
    usually more reliable than artist-credit formatting.

    It catches:
      "I'm Da Man" vs "Im Da Man"
      "Big Dog (with That Mexican OT & 2 Chainz)" vs "Big Dog"
      "It Depends (The Remix) (feat. ...)" vs "It Depends (The Remix)"
    """
    lf_clean = normalize_title(lastfm_title)
    db_clean = normalize_title(db_title)
    lf_compact = compact_text(lf_clean)
    db_compact = compact_text(db_clean)

    scores = [
        best_text_score(lastfm_title, db_title),
        best_text_score(lf_clean, db_clean),
        ratio(lf_compact, db_compact),
        ratio(token_sort(lf_clean), token_sort(db_clean)),
    ]

    # Exact after punctuation cleanup should be treated as a perfect title match.
    if lf_clean and db_clean and lf_clean == db_clean:
        scores.append(100.0)
    if lf_compact and db_compact and lf_compact == db_compact:
        scores.append(100.0)

    # If one title contains the other after cleanup, it is usually a feature/version
    # issue rather than a different song. Require at least 4 chars to avoid junk hits.
    if lf_compact and db_compact and min(len(lf_compact), len(db_compact)) >= 4:
        if lf_compact in db_compact or db_compact in lf_compact:
            scores.append(96.0)

    return max(scores)


def artist_score(lastfm_artist: str, db_artist: str) -> float:
    """
    Artist score is intentionally forgiving because Last.fm often stores:
      title: Big Dog (with That Mexican OT & 2 Chainz)
      artist: Prof

    while the chart may store:
      artist: Prof f/ That Mexican OT & 2 Chainz
    """
    lastfm_norm = normalize_text(lastfm_artist)
    db_norm = normalize_text(db_artist)

    base = best_text_score(lastfm_artist, db_artist)

    # Give a strong boost if one artist string is contained in the other.
    if lastfm_norm and db_norm:
        if lastfm_norm in db_norm or db_norm in lastfm_norm:
            base = max(base, 92.0)

    return base


@dataclass
class LastfmTrack:
    lastfm_play_id: int
    lastfm_rank: int | None
    lastfm_track_name: str
    lastfm_artist_name: str
    song_title_key: str
    artist_key: str
    playcount: int


@dataclass
class Candidate:
    canonical_song_id: int
    song_title_display: str
    full_artist_display: str
    appearances: int
    best_rank: int | None
    latest_chart_date: str | None


@dataclass
class MatchResult:
    track: LastfmTrack
    candidate: Candidate
    total_score: float
    title_score: float
    artist_score: float
    reason: str


def ensure_alias_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lastfm_track_alias (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lastfm_track_name TEXT NOT NULL,
            lastfm_artist_name TEXT NOT NULL,
            song_title_key TEXT NOT NULL,
            artist_key TEXT NOT NULL,
            canonical_song_id INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(song_title_key, artist_key)
        )
        """
    )


def find_chart_week_id(conn: sqlite3.Connection, chart_date: str) -> int:
    row = conn.execute(
        """
        SELECT chart_week_id
        FROM chart_week
        WHERE chart_date = ?
        LIMIT 1
        """,
        (chart_date,),
    ).fetchone()

    if not row:
        raise SystemExit(f"No chart_week found for chart_date={chart_date}")

    return int(row[0])


def load_unmatched_lastfm_tracks(
    conn: sqlite3.Connection,
    chart_week_id: int,
    include_matched: bool = False,
) -> list[LastfmTrack]:
    where = "chart_week_id = ?"
    params: list[object] = [chart_week_id]

    if not include_matched:
        where += " AND canonical_song_id IS NULL"

    rows = conn.execute(
        f"""
        SELECT
            lastfm_play_id,
            lastfm_rank,
            lastfm_track_name,
            lastfm_artist_name,
            song_title_key,
            artist_key,
            playcount
        FROM lastfm_weekly_track_play
        WHERE {where}
        ORDER BY lastfm_rank, playcount DESC, lastfm_track_name
        """,
        params,
    ).fetchall()

    return [
        LastfmTrack(
            lastfm_play_id=int(row["lastfm_play_id"]),
            lastfm_rank=int(row["lastfm_rank"]) if row["lastfm_rank"] is not None else None,
            lastfm_track_name=row["lastfm_track_name"],
            lastfm_artist_name=row["lastfm_artist_name"],
            song_title_key=row["song_title_key"],
            artist_key=row["artist_key"],
            playcount=int(row["playcount"]),
        )
        for row in rows
    ]


def load_candidates(
    conn: sqlite3.Connection,
    chart_week_id: int,
    current_chart_only: bool,
) -> list[Candidate]:
    """
    Candidate universe.

    current_chart_only=True:
        Only songs on the selected chart week.

    current_chart_only=False:
        All canonical songs that have appeared in entry.
    """
    if current_chart_only:
        sql = """
            SELECT
                e.canonical_song_id,
                e.song_title_display,
                e.full_artist_display,
                COUNT(*) AS appearances,
                MIN(e.position) AS best_rank,
                MAX(cw.chart_date) AS latest_chart_date
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.chart_week_id = ?
              AND e.canonical_song_id IS NOT NULL
            GROUP BY e.canonical_song_id
            ORDER BY e.position
        """
        params = (chart_week_id,)
    else:
        sql = """
            SELECT
                e.canonical_song_id,
                e.song_title_display,
                e.full_artist_display,
                COUNT(*) AS appearances,
                MIN(e.position) AS best_rank,
                MAX(cw.chart_date) AS latest_chart_date
            FROM entry e
            JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
            WHERE e.canonical_song_id IS NOT NULL
            GROUP BY e.canonical_song_id, e.song_title_display, e.full_artist_display
            ORDER BY appearances DESC, latest_chart_date DESC
        """
        params = ()

    rows = conn.execute(sql, params).fetchall()

    candidates: list[Candidate] = []
    seen: set[tuple[int, str, str]] = set()

    for row in rows:
        key = (
            int(row["canonical_song_id"]),
            row["song_title_display"] or "",
            row["full_artist_display"] or "",
        )
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            Candidate(
                canonical_song_id=int(row["canonical_song_id"]),
                song_title_display=row["song_title_display"] or "",
                full_artist_display=row["full_artist_display"] or "",
                appearances=int(row["appearances"] or 0),
                best_rank=int(row["best_rank"]) if row["best_rank"] is not None else None,
                latest_chart_date=row["latest_chart_date"],
            )
        )

    return candidates


def score_match(track: LastfmTrack, candidate: Candidate) -> MatchResult:
    ts = title_score(track.lastfm_track_name, candidate.song_title_display)
    ars = artist_score(track.lastfm_artist_name, candidate.full_artist_display)

    # Weighted total. Title matters most; artist is secondary because Last.fm may place
    # featured artists in the title instead of artist field. This catches cases like
    # "Im Da Man" matching "I'm Da Man" even when the DB artist includes features.
    total = (ts * 0.84) + (ars * 0.16)

    reason_parts = []

    title_exact = normalize_title(track.lastfm_track_name) == normalize_title(candidate.song_title_display)
    compact_title_exact = compact_title(track.lastfm_track_name) == compact_title(candidate.song_title_display)

    if title_exact:
        total += 10
        reason_parts.append("title exact after cleanup")

    if compact_title_exact:
        total += 10
        reason_parts.append("compact title exact")

    lf_artist_norm = normalize_text(track.lastfm_artist_name)
    db_artist_norm = normalize_text(candidate.full_artist_display)
    artist_contained = False
    if lf_artist_norm and db_artist_norm and (lf_artist_norm in db_artist_norm or db_artist_norm in lf_artist_norm):
        artist_contained = True
        total += 5
        reason_parts.append("Last.fm artist contained in DB artist")

    # If title is extremely strong, don't punish artist mismatch too hard.
    # Still require some artist plausibility; otherwise all one-word titles like
    # "Hello" can become false positives across the full DB.
    if ts >= 98 and ars >= 35:
        total = max(total, 92)
        reason_parts.append("very strong title")

    # A near-exact title with a plausible artist should be accepted even if the
    # DB artist has featured credits Last.fm did not include.
    if ts >= 96 and ars >= 35:
        total = max(total, 90)
        reason_parts.append("strong title + plausible artist")

    # Short/common titles need stronger artist support when searching the whole DB.
    # This prevents cases like:
    #   "Hello" - LL Cool J
    # matching:
    #   "Hello" - Ice Cube feat. MC Ren; Dr. Dre
    # merely because the title is exact.
    cleaned_title = normalize_title(track.lastfm_track_name)
    title_token_count = len(cleaned_title.split())
    title_compact_len = len(compact_title(track.lastfm_track_name))
    short_or_generic_title = (
        title_token_count <= 1
        or title_compact_len <= 8
    )
    if short_or_generic_title and not artist_contained and ars < 60:
        total = min(total, 80.0)
        reason_parts.append("short/common title needs artist support")

    # But avoid absurd artist mismatches unless title is nearly exact.
    if ts < 94 and ars < 45:
        total -= 12
        reason_parts.append("weak artist + non-exact title")

    total = min(100.0, max(0.0, total))

    reason = ", ".join(reason_parts) if reason_parts else "weighted title/artist similarity"

    return MatchResult(
        track=track,
        candidate=candidate,
        total_score=total,
        title_score=ts,
        artist_score=ars,
        reason=reason,
    )


def find_best_match(track: LastfmTrack, candidates: list[Candidate]) -> MatchResult | None:
    best: MatchResult | None = None

    for candidate in candidates:
        result = score_match(track, candidate)
        if best is None or result.total_score > best.total_score:
            best = result

    return best


def backup_db(db_path: str) -> str:
    src = Path(db_path)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = src.with_name(f"{src.stem}.backup_lastfm_match_{stamp}{src.suffix}")
    shutil.copy2(src, backup)
    return str(backup)


def apply_match(conn: sqlite3.Connection, result: MatchResult) -> None:
    t = result.track
    c = result.candidate

    song_key = normalize_text(t.lastfm_track_name)
    artist_key = normalize_text(t.lastfm_artist_name)

    conn.execute(
        """
        INSERT INTO lastfm_track_alias (
            lastfm_track_name,
            lastfm_artist_name,
            song_title_key,
            artist_key,
            canonical_song_id
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(song_title_key, artist_key)
        DO UPDATE SET
            lastfm_track_name = excluded.lastfm_track_name,
            lastfm_artist_name = excluded.lastfm_artist_name,
            canonical_song_id = excluded.canonical_song_id
        """,
        (
            t.lastfm_track_name,
            t.lastfm_artist_name,
            song_key,
            artist_key,
            c.canonical_song_id,
        ),
    )

    conn.execute(
        """
        UPDATE lastfm_weekly_track_play
        SET canonical_song_id = ?
        WHERE lastfm_play_id = ?
        """,
        (c.canonical_song_id, t.lastfm_play_id),
    )


def print_result(result: MatchResult, accepted: bool | None = None) -> None:
    t = result.track
    c = result.candidate

    prefix = "MATCH" if accepted is True else "SKIP" if accepted is False else "CANDIDATE"

    print(
        f"\n[{prefix}] score={result.total_score:.1f} "
        f"title={result.title_score:.1f} artist={result.artist_score:.1f}"
    )
    print(
        f"  Last.fm #{t.lastfm_rank}: {t.lastfm_track_name} - {t.lastfm_artist_name} "
        f"({t.playcount} plays)"
    )
    print(
        f"  DB: canonical_song_id={c.canonical_song_id} | "
        f"{c.song_title_display} - {c.full_artist_display}"
    )
    print(
        f"  DB stats: appearances={c.appearances}, best_rank={c.best_rank}, "
        f"latest={c.latest_chart_date}"
    )
    print(f"  Reason: {result.reason}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fuzzy-match Last.fm imported tracks to canonical songs."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite DB.")
    parser.add_argument("--chart-date", required=True, help="Chart date, e.g. 2026-04-28.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=86.0,
        help="Minimum score to accept/write automatically. Default: 86.",
    )
    parser.add_argument(
        "--current-chart-only",
        action="store_true",
        help="Only match against songs on the selected chart week.",
    )
    parser.add_argument(
        "--all-db",
        action="store_true",
        help="Match against all entry songs in the DB. Default if --current-chart-only is not used.",
    )
    parser.add_argument(
        "--include-matched",
        action="store_true",
        help="Also review Last.fm rows that already have canonical_song_id.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Ask before accepting each proposed match.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write accepted matches to lastfm_track_alias and update lastfm rows.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of Last.fm tracks reviewed.",
    )

    args = parser.parse_args()

    current_chart_only = args.current_chart_only
    if args.all_db:
        current_chart_only = False

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        ensure_alias_table(conn)

        chart_week_id = find_chart_week_id(conn, args.chart_date)

        tracks = load_unmatched_lastfm_tracks(
            conn,
            chart_week_id=chart_week_id,
            include_matched=args.include_matched,
        )

        if args.limit:
            tracks = tracks[: args.limit]

        candidates = load_candidates(
            conn,
            chart_week_id=chart_week_id,
            current_chart_only=current_chart_only,
        )

        print(f"Chart date: {args.chart_date}")
        print(f"Chart week ID: {chart_week_id}")
        print(f"Last.fm rows to review: {len(tracks)}")
        print(f"Candidate universe: {len(candidates)} songs")
        print(f"Minimum score: {args.min_score}")
        print(f"Mode: {'WRITE' if args.write else 'DRY RUN'}")
        print(f"Candidate scope: {'current chart only' if current_chart_only else 'all DB entries'}")

        if args.write:
            backup_path = backup_db(args.db)
            print(f"Backup created: {backup_path}")

        accepted_count = 0
        skipped_count = 0
        no_candidate_count = 0

        for track in tracks:
            result = find_best_match(track, candidates)

            if result is None:
                no_candidate_count += 1
                continue

            auto_accept = result.total_score >= args.min_score
            accepted = auto_accept

            if args.interactive:
                print_result(result)
                answer = input("Accept this match? [y/N/q] ").strip().lower()

                if answer == "q":
                    print("Stopping interactive review.")
                    break

                accepted = answer == "y"

            else:
                # Print only strong candidates and notable misses.
                if result.total_score >= args.min_score or result.track.playcount >= 5:
                    print_result(result, accepted=auto_accept)

            if accepted:
                accepted_count += 1
                if args.write:
                    apply_match(conn, result)
            else:
                skipped_count += 1

        if args.write:
            conn.commit()

        print("\nSummary:")
        print(f"  Accepted matches: {accepted_count}")
        print(f"  Skipped:          {skipped_count}")
        print(f"  No candidate:     {no_candidate_count}")

        if not args.write:
            print("\nDry run only. Re-run with --write to save accepted matches.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
