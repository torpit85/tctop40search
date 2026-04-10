#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / 'db' / 'torreys_corner_top40.sqlite'
DATA_DIR = BASE_DIR / 'data'


def clean_text(value: object) -> str:
    if value is None:
        return ''
    text = str(value)
    text = text.replace('\ufeff', '')
    text = text.replace('\u2018', "'").replace('\u2019', "'")
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u2013', '-').replace('\u2014', '-')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def ascii_fold(text: str) -> str:
    text = unicodedata.normalize('NFKD', text)
    return ''.join(ch for ch in text if not unicodedata.combining(ch))


def normalize_title_key(value: object) -> str:
    text = ascii_fold(clean_text(value).lower())
    text = text.replace('&', ' and ')
    text = text.replace("'", '')
    text = re.sub(r'\bversus\b', 'vs', text)
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize_artist_key(value: object) -> str:
    text = ascii_fold(clean_text(value).lower())
    text = re.sub(r'\b(featuring|feat\.?|ft\.?|f/)\b', ' feat ', text)
    text = text.replace('&', ' and ')
    text = text.replace('+', ' and ')
    text = text.replace("'", '')
    text = re.sub(r'\bversus\b', 'vs', text)
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def canonical_group_key(song_title: object, artist: object) -> str:
    return f'{normalize_title_key(song_title)}|||{normalize_artist_key(artist)}'


def ensure_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}
    if column not in cols:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {decl}')


def ensure_schema(conn: sqlite3.Connection) -> None:
    ensure_column(conn, 'entry', 'canonical_song_id', 'INTEGER')
    ensure_column(conn, 'entry', 'canonical_title_key', 'TEXT')
    ensure_column(conn, 'entry', 'canonical_artist_key', 'TEXT')
    ensure_column(conn, 'entry', 'canonical_group_key', 'TEXT')
    ensure_column(conn, 'entry', 'derived_is_debut', 'INTEGER DEFAULT 0')
    ensure_column(conn, 'entry', 'derived_is_top_debut', 'INTEGER DEFAULT 0')
    ensure_column(conn, 'entry', 'derived_is_reentry', 'INTEGER DEFAULT 0')
    ensure_column(conn, 'entry', 'derived_marker', 'TEXT')

    conn.executescript(
        '''
        DROP TABLE IF EXISTS song_alias;
        DROP TABLE IF EXISTS canonical_song;

        CREATE TABLE canonical_song (
            canonical_song_id INTEGER PRIMARY KEY,
            canonical_title TEXT NOT NULL,
            canonical_artist TEXT NOT NULL,
            canonical_title_key TEXT NOT NULL,
            canonical_artist_key TEXT NOT NULL,
            canonical_group_key TEXT NOT NULL UNIQUE,
            entry_count INTEGER DEFAULT 0,
            alias_count INTEGER DEFAULT 0,
            first_chart_date TEXT,
            last_chart_date TEXT
        );

        CREATE TABLE song_alias (
            alias_id INTEGER PRIMARY KEY,
            alias_song_title TEXT NOT NULL,
            alias_artist TEXT NOT NULL,
            alias_title_key TEXT NOT NULL,
            alias_artist_key TEXT NOT NULL,
            alias_group_key TEXT NOT NULL,
            alias_display_key TEXT NOT NULL UNIQUE,
            canonical_song_id INTEGER NOT NULL,
            entry_count INTEGER DEFAULT 0,
            week_count INTEGER DEFAULT 0,
            first_chart_date TEXT,
            last_chart_date TEXT,
            FOREIGN KEY (canonical_song_id) REFERENCES canonical_song(canonical_song_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_entry_canonical_song_id ON entry(canonical_song_id);
        CREATE INDEX IF NOT EXISTS idx_entry_canonical_group_key ON entry(canonical_group_key);
        CREATE INDEX IF NOT EXISTS idx_entry_derived_marker ON entry(derived_marker);
        CREATE INDEX IF NOT EXISTS idx_canonical_song_group_key ON canonical_song(canonical_group_key);
        CREATE INDEX IF NOT EXISTS idx_song_alias_canonical_song_id ON song_alias(canonical_song_id);
        '''
    )
    conn.commit()


def pick_best_variant(variants: Iterable[tuple[str, str, int, str]]) -> tuple[str, str]:
    # variants: (song_title, artist, entry_count, last_chart_date)
    def sort_key(item: tuple[str, str, int, str]) -> tuple[int, str, int, int, str, str]:
        song, artist, count, last_date = item
        return (
            count,
            last_date or '',
            -song.count("'"),
            -(song != song.title()),
            song,
            artist,
        )

    best = max(variants, key=sort_key)
    return best[0], best[1]


def rebuild_canonical_data(conn: sqlite3.Connection) -> tuple[int, int, int]:
    ensure_schema(conn)

    rows = conn.execute(
        '''
        SELECT
            e.entry_id,
            e.song_title_display,
            e.full_artist_display,
            cw.chart_date
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        ORDER BY cw.chart_date, e.position
        '''
    ).fetchall()

    alias_groups: dict[tuple[str, str], dict[str, object]] = {}
    canonical_groups: dict[str, dict[str, object]] = defaultdict(lambda: {
        'aliases': set(),
        'variants': defaultdict(lambda: {'entry_count': 0, 'last_chart_date': ''}),
        'entry_count': 0,
        'first_chart_date': None,
        'last_chart_date': None,
    })

    for entry_id, song_title, artist, chart_date in rows:
        song_title = clean_text(song_title)
        artist = clean_text(artist)
        title_key = normalize_title_key(song_title)
        artist_key = normalize_artist_key(artist)
        group_key = f'{title_key}|||{artist_key}'

        alias_key = (song_title, artist)
        alias_info = alias_groups.setdefault(alias_key, {
            'title_key': title_key,
            'artist_key': artist_key,
            'group_key': group_key,
            'entry_count': 0,
            'weeks': set(),
            'first_chart_date': chart_date,
            'last_chart_date': chart_date,
        })
        alias_info['entry_count'] += 1
        alias_info['weeks'].add(chart_date)
        alias_info['first_chart_date'] = min(alias_info['first_chart_date'], chart_date)
        alias_info['last_chart_date'] = max(alias_info['last_chart_date'], chart_date)

        group = canonical_groups[group_key]
        group['aliases'].add(alias_key)
        group['entry_count'] += 1
        group['first_chart_date'] = chart_date if group['first_chart_date'] is None else min(group['first_chart_date'], chart_date)
        group['last_chart_date'] = chart_date if group['last_chart_date'] is None else max(group['last_chart_date'], chart_date)
        variant = group['variants'][alias_key]
        variant['entry_count'] += 1
        variant['last_chart_date'] = max(variant['last_chart_date'], chart_date)

    conn.execute('DELETE FROM song_alias')
    conn.execute('DELETE FROM canonical_song')
    conn.execute(
        '''
        UPDATE entry
        SET canonical_song_id = NULL,
            canonical_title_key = NULL,
            canonical_artist_key = NULL,
            canonical_group_key = NULL,
            derived_is_debut = 0,
            derived_is_top_debut = 0,
            derived_is_reentry = 0,
            derived_marker = NULL
        '''
    )

    canonical_id_by_group: dict[str, int] = {}
    alias_rows_inserted = 0
    for group_key in sorted(canonical_groups):
        group = canonical_groups[group_key]
        title_key, artist_key = group_key.split('|||', 1)
        variants = [
            (song, artist, meta['entry_count'], meta['last_chart_date'])
            for (song, artist), meta in group['variants'].items()
        ]
        canonical_title, canonical_artist = pick_best_variant(variants)
        cur = conn.execute(
            '''
            INSERT INTO canonical_song (
                canonical_title, canonical_artist,
                canonical_title_key, canonical_artist_key, canonical_group_key,
                entry_count, alias_count, first_chart_date, last_chart_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                canonical_title,
                canonical_artist,
                title_key,
                artist_key,
                group_key,
                group['entry_count'],
                len(group['aliases']),
                group['first_chart_date'],
                group['last_chart_date'],
            ),
        )
        canonical_song_id = cur.lastrowid
        canonical_id_by_group[group_key] = canonical_song_id

        for alias_key in sorted(group['aliases']):
            song_title, artist = alias_key
            alias_info = alias_groups[alias_key]
            conn.execute(
                '''
                INSERT INTO song_alias (
                    alias_song_title, alias_artist,
                    alias_title_key, alias_artist_key, alias_group_key, alias_display_key,
                    canonical_song_id, entry_count, week_count, first_chart_date, last_chart_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    song_title,
                    artist,
                    alias_info['title_key'],
                    alias_info['artist_key'],
                    alias_info['group_key'],
                    f'{song_title}|||{artist}',
                    canonical_song_id,
                    alias_info['entry_count'],
                    len(alias_info['weeks']),
                    alias_info['first_chart_date'],
                    alias_info['last_chart_date'],
                ),
            )
            alias_rows_inserted += 1

    entry_updates = []
    for entry_id, song_title, artist, _chart_date in rows:
        title_key = normalize_title_key(song_title)
        artist_key = normalize_artist_key(artist)
        group_key = f'{title_key}|||{artist_key}'
        entry_updates.append((canonical_id_by_group[group_key], title_key, artist_key, group_key, entry_id))

    conn.executemany(
        '''
        UPDATE entry
        SET canonical_song_id = ?,
            canonical_title_key = ?,
            canonical_artist_key = ?,
            canonical_group_key = ?
        WHERE entry_id = ?
        ''',
        entry_updates,
    )

    return len(canonical_groups), len(alias_groups), alias_rows_inserted


def derive_markers(conn: sqlite3.Connection) -> tuple[int, int, int]:
    rows = conn.execute(
        '''
        SELECT
            e.entry_id,
            e.canonical_song_id,
            e.chart_week_id,
            cw.chart_date,
            e.position
        FROM entry e
        JOIN chart_week cw ON cw.chart_week_id = e.chart_week_id
        WHERE e.canonical_song_id IS NOT NULL
        ORDER BY cw.chart_date, e.position, e.entry_id
        '''
    ).fetchall()

    week_order = [row[0] for row in conn.execute('SELECT chart_week_id FROM chart_week ORDER BY chart_date').fetchall()]
    prev_week_by_week_id: dict[int, int | None] = {}
    prev = None
    for week_id in week_order:
        prev_week_by_week_id[week_id] = prev
        prev = week_id

    seen_before: set[int] = set()
    song_to_weeks: dict[int, set[int]] = defaultdict(set)
    updates: list[tuple[int, int, int, str | None, int]] = []
    debut_positions_by_week: dict[int, list[tuple[int, int]]] = defaultdict(list)

    debut_count = 0
    reentry_count = 0
    for entry_id, canonical_song_id, chart_week_id, _chart_date, position in rows:
        is_debut = 0
        is_reentry = 0
        marker: str | None = None

        if canonical_song_id not in seen_before:
            is_debut = 1
            marker = 'DEBUT'
            debut_positions_by_week[chart_week_id].append((position, entry_id))
            debut_count += 1
        else:
            prev_week_id = prev_week_by_week_id[chart_week_id]
            prior_weeks = song_to_weeks[canonical_song_id]
            if prev_week_id is not None and prev_week_id not in prior_weeks:
                is_reentry = 1
                marker = 'RE-ENTRY'
                reentry_count += 1

        seen_before.add(canonical_song_id)
        song_to_weeks[canonical_song_id].add(chart_week_id)
        updates.append((is_debut, 0, is_reentry, marker, entry_id))

    updates_by_entry = {entry_id: [is_debut, is_top, is_re, marker] for is_debut, is_top, is_re, marker, entry_id in updates}
    top_debut_count = 0
    for _week_id, positions in debut_positions_by_week.items():
        if not positions:
            continue
        positions.sort()
        _, top_entry_id = positions[0]
        row = updates_by_entry[top_entry_id]
        row[1] = 1
        row[3] = 'TOP DEBUT'
        top_debut_count += 1

    final_updates = [
        (vals[0], vals[1], vals[2], vals[3], entry_id)
        for entry_id, vals in updates_by_entry.items()
    ]
    conn.executemany(
        '''
        UPDATE entry
        SET derived_is_debut = ?,
            derived_is_top_debut = ?,
            derived_is_reentry = ?,
            derived_marker = ?
        WHERE entry_id = ?
        ''',
        final_updates,
    )
    return debut_count, top_debut_count, reentry_count


def export_review_files(conn: sqlite3.Connection, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    groups_path = out_dir / 'canonical_song_groups.csv'
    review_path = out_dir / 'canonical_review_candidates.csv'

    group_rows = conn.execute(
        '''
        SELECT
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.canonical_group_key,
            cs.entry_count,
            cs.alias_count,
            cs.first_chart_date,
            cs.last_chart_date
        FROM canonical_song cs
        ORDER BY cs.alias_count DESC, cs.entry_count DESC, cs.canonical_title, cs.canonical_artist
        '''
    ).fetchall()

    with groups_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'canonical_song_id', 'canonical_title', 'canonical_artist', 'canonical_group_key',
            'entry_count', 'alias_count', 'first_chart_date', 'last_chart_date'
        ])
        writer.writerows(group_rows)

    review_rows = conn.execute(
        '''
        SELECT
            cs.canonical_song_id,
            cs.canonical_title,
            cs.canonical_artist,
            cs.entry_count AS canonical_entry_count,
            sa.alias_song_title,
            sa.alias_artist,
            sa.entry_count AS alias_entry_count,
            sa.week_count,
            sa.first_chart_date,
            sa.last_chart_date
        FROM canonical_song cs
        JOIN song_alias sa ON sa.canonical_song_id = cs.canonical_song_id
        WHERE cs.alias_count > 1
        ORDER BY cs.alias_count DESC, cs.entry_count DESC, cs.canonical_song_id, sa.entry_count DESC, sa.alias_song_title, sa.alias_artist
        '''
    ).fetchall()

    with review_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'canonical_song_id', 'canonical_title', 'canonical_artist', 'canonical_entry_count',
            'alias_song_title', 'alias_artist', 'alias_entry_count', 'week_count',
            'first_chart_date', 'last_chart_date'
        ])
        writer.writerows(review_rows)

    return groups_path, review_path


def main() -> None:
    parser = argparse.ArgumentParser(description='Canonicalize song identity and derive DEBUT / TOP DEBUT / RE-ENTRY markers.')
    parser.add_argument('--db', default=str(DB_PATH), help='SQLite database path')
    parser.add_argument('--out-dir', default=str(DATA_DIR), help='Folder for review CSV exports')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        canonical_count, alias_count, _alias_rows_inserted = rebuild_canonical_data(conn)
        debut_count, top_debut_count, reentry_count = derive_markers(conn)
        groups_path, review_path = export_review_files(conn, Path(args.out_dir))
        conn.commit()
        print(f'Canonical songs: {canonical_count}')
        print(f'Alias variants: {alias_count}')
        print(f'Debuts: {debut_count}')
        print(f'Top debuts: {top_debut_count}')
        print(f'Re-entries: {reentry_count}')
        print(f'Wrote: {groups_path}')
        print(f'Wrote: {review_path}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
