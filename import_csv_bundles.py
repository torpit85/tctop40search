#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import unicodedata
import zipfile
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'db' / 'tctop40.sqlite'
SCHEMA_PATH = BASE_DIR / 'schema.sql'

DATE_PATTERNS = [
    re.compile(r'(\d{4})-(\d{2})-(\d{2})'),
    re.compile(r'(\d{2})-(\d{2})-(\d{2})'),
]
COMBINED_ARTIST_KEYS = ('Unnamed: 2', '', 'Combined Artist', 'Combined', 'Song, Artist')


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


def normalize_key(value: object) -> str:
    text = clean_text(value).lower()
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r'\b(featuring|feat\.?|ft\.?)\b', 'feat', text)
    text = re.sub(r'[^a-z0-9&+/()\-\'.,;: ]+', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip(' -;,:')


def parse_chart_date(filename: str) -> str:
    for pat in DATE_PATTERNS:
        m = pat.search(filename)
        if not m:
            continue
        a, b, c = m.groups()
        if len(a) == 4:
            return f'{a}-{b}-{c}'
        year = int(c)
        year += 2000 if year <= 69 else 1900
        return f'{year:04d}-{int(a):02d}-{int(b):02d}'
    raise ValueError(f'Could not parse chart date from filename: {filename}')


def full_artist(artist: str, featured: str) -> str:
    artist = clean_text(artist)
    featured = clean_text(featured)
    if artist and featured:
        return f'{artist} feat. {featured}'
    return artist or featured


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding='utf-8'))
    conn.commit()


def get_combined_artist(row: dict[str, object], artist: str) -> str:
    for key in COMBINED_ARTIST_KEYS:
        value = clean_text(row.get(key))
        if value:
            return value
    return artist


def trim_rows(rows: list[dict[str, str]], zip_name: str, member_name: str, issues: list[str]) -> tuple[list[dict[str, str]], int, str | None]:
    source_row_count = len(rows)
    note: str | None = None

    if source_row_count == 100:
        trimmed: list[dict[str, str]] = []
        for row in rows:
            position_raw = clean_text(row.get('Position'))
            if not position_raw:
                continue
            try:
                position = int(float(position_raw))
            except ValueError:
                issues.append(f'{zip_name} :: {member_name} -> bad position {position_raw!r} while trimming 100-row chart')
                continue
            if position <= 40:
                trimmed.append(row)
        rows = trimmed
        note = f'Truncated 100-row source chart to Top 40 ({len(rows)} rows kept)'
        issues.append(f'{zip_name} :: {member_name} -> truncated 100-row chart to {len(rows)} rows')
    elif source_row_count != 40:
        note = f'Imported non-standard row count: {source_row_count}'
        issues.append(f'{zip_name} :: {member_name} -> row count {source_row_count}')

    return rows, source_row_count, note


def ingest_zip(conn: sqlite3.Connection, zip_path: Path) -> tuple[int, int, list[str]]:
    chart_count = 0
    entry_count = 0
    issues: list[str] = []

    with zipfile.ZipFile(zip_path) as zf:
        for name in sorted(zf.namelist()):
            if not name.lower().endswith('.csv'):
                continue
            raw = zf.read(name)
            text = raw.decode('utf-8-sig', errors='replace')
            reader = csv.DictReader(text.splitlines())
            rows = list(reader)
            if not rows:
                issues.append(f'{zip_path.name} :: {name} -> empty CSV')
                continue

            chart_date = parse_chart_date(Path(name).name)
            chart_id = clean_text(rows[0].get('Chart ID'))
            rows, source_row_count, note = trim_rows(rows, zip_path.name, name, issues)
            stored_row_count = len(rows)

            cur = conn.execute(
                '''
                INSERT OR REPLACE INTO chart_week
                    (chart_week_id, chart_date, chart_id, source_file, source_zip, row_count, notes)
                VALUES (
                    COALESCE((SELECT chart_week_id FROM chart_week WHERE chart_date = ?), NULL),
                    ?, ?, ?, ?, ?, ?
                )
                ''',
                (chart_date, chart_date, chart_id, Path(name).name, zip_path.name, stored_row_count, note),
            )
            chart_week_id = cur.lastrowid or conn.execute(
                'SELECT chart_week_id FROM chart_week WHERE chart_date = ?', (chart_date,)
            ).fetchone()[0]

            conn.execute('DELETE FROM entry WHERE chart_week_id = ?', (chart_week_id,))
            # Contentless FTS table: delete prior rows for this source file manually to avoid duplicates.
            conn.execute('DELETE FROM entry_fts WHERE source_file = ?', (Path(name).name,))

            for row in rows:
                position_raw = clean_text(row.get('Position'))
                if not position_raw:
                    continue
                try:
                    position = int(float(position_raw))
                except ValueError:
                    issues.append(f'{zip_path.name} :: {name} -> bad position {position_raw!r}')
                    continue

                artist = clean_text(row.get('Artist'))
                featured = clean_text(row.get('Featured'))
                song_title = clean_text(row.get('Song Title'))
                combined_artist = get_combined_artist(row, artist)
                slug = clean_text(row.get('Slug'))
                full_artist_display = full_artist(artist, featured)
                normalized_display = normalize_key(f'{song_title} {full_artist_display} {combined_artist} {slug}')

                conn.execute(
                    '''
                    INSERT INTO entry (
                        chart_week_id, position, chart_id,
                        raw_combined_artist, raw_artist, raw_featured, raw_song_title, raw_slug,
                        artist_display, featured_display, full_artist_display, song_title_display,
                        normalized_artist, normalized_featured, normalized_full_artist,
                        normalized_song_title, normalized_display
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        chart_week_id, position, chart_id,
                        combined_artist, artist, featured, song_title, slug,
                        artist, featured, full_artist_display, song_title,
                        normalize_key(artist), normalize_key(featured), normalize_key(full_artist_display),
                        normalize_key(song_title), normalized_display,
                    )
                )
                entry_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                conn.execute(
                    'INSERT INTO entry_fts(rowid, song_title_display, full_artist_display, normalized_display, raw_slug, source_file) VALUES (?, ?, ?, ?, ?, ?)',
                    (entry_id, song_title, full_artist_display, normalized_display, slug, Path(name).name),
                )
                entry_count += 1

            if source_row_count == 100 and stored_row_count != 40:
                issues.append(f'{zip_path.name} :: {name} -> expected 40 rows after truncation but kept {stored_row_count}')

            chart_count += 1

    return chart_count, entry_count, issues


def discover_zip_paths(paths: Iterable[str]) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            out.extend(sorted(p.glob('*.zip')))
        elif p.is_file() and p.suffix.lower() == '.zip':
            out.append(p)
    return sorted(dict.fromkeys(out))


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Torrey's Corner Top 40 CSV ZIP bundles into SQLite.")
    parser.add_argument('inputs', nargs='+', help='ZIP files and/or folders containing ZIP files')
    parser.add_argument('--db', default=str(DB_PATH), help='SQLite database path')
    parser.add_argument('--reset', action='store_true', help='Drop existing imported rows before import')
    args = parser.parse_args()

    zip_paths = discover_zip_paths(args.inputs)
    if not zip_paths:
        raise SystemExit('No ZIP files found in the supplied input paths.')

    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        if args.reset:
            conn.execute("INSERT INTO entry_fts(entry_fts) VALUES ('delete-all')")
            conn.execute('DELETE FROM entry')
            conn.execute('DELETE FROM chart_week')
            conn.commit()

        total_charts = 0
        total_entries = 0
        all_issues: list[str] = []
        for zip_path in zip_paths:
            charts, entries, issues = ingest_zip(conn, zip_path)
            total_charts += charts
            total_entries += entries
            all_issues.extend(issues)
            conn.commit()
            print(f'Imported {zip_path.name}: {charts} charts, {entries} entries')

        print(f'\nDone: {total_charts} charts, {total_entries} entries total')
        if all_issues:
            print('\nImport notes:')
            for issue in all_issues:
                print(f'- {issue}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
