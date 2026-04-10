# Torrey's Corner Top 40 Database / Search Engine

This project builds a local SQLite + FTS5 search engine for Torrey's Corner Top 40 CSV bundles.

## What it does
- imports weekly chart CSVs from ZIP bundles
- parses chart dates from filenames
- stores both raw values and cleaned display/search values
- creates a full-text search index for fast searching
- keeps track of source ZIP and source CSV filename
- canonicalizes song identity across light formatting variants
- derives `DEBUT`, `TOP DEBUT`, and `RE-ENTRY` markers from chart history

## Current assumptions
The importer is built around the CSV shape found in the uploaded bundles:
- `Chart ID`
- `Position`
- `Unnamed: 2` (combined artist field in most files)
- `Artist`
- `Featured`
- `Song Title`
- `Slug`

It also tolerates the leaner variant seen in a couple of 2011 files:
- `Chart ID`
- `Position`
- `Artist`
- `Featured`
- `Song Title`

## Setup
```bash
cd /mnt/data/torreys_corner_top40
python3 scripts/init_db.py
python3 scripts/import_csv_bundles.py --reset /mnt/data/TCTOP40_2003CSV.zip /mnt/data/TCTOP40_2004CSV.zip
```

Or import a whole folder of ZIP bundles:
```bash
python3 scripts/import_csv_bundles.py --reset /mnt/data
```

## Canonicalization and derived markers
After import, run:

```bash
python3 scripts/derive_canonical_songs.py
```

That script will:
- add canonical-song tables and columns if they do not exist yet
- group light title/artist variants into a shared `canonical_song_id`
- populate `entry.canonical_song_id`, `canonical_title_key`, `canonical_artist_key`, and `canonical_group_key`
- derive `entry.derived_is_debut`, `entry.derived_is_top_debut`, `entry.derived_is_reentry`, and `entry.derived_marker`
- export review files to `data/`

Review exports:
- `data/canonical_song_groups.csv`
- `data/canonical_review_candidates.csv`

The canonicalization is intentionally conservative. It handles punctuation, apostrophes, capitalization, accent-folding, and light artist-token differences like `feat.` vs `featuring`, but it does **not** automatically collapse remix/live/acoustic/alternate-version titles.

## Search examples
```bash
python3 scripts/search_cli.py "slow"
python3 scripts/search_cli.py --date 2004-01-06
python3 scripts/search_cli.py --song "slow jamz"
python3 scripts/search_cli.py --artist "janet jackson"
```

## Streamlit front end
A basic Streamlit search app is included. It supports:
- full-text search across songs, artists, slugs, and mixed text
- week-by-week chart browsing
- song history pages with peak / first / last-week stats
- artist history pages with distinct-song counts
- quick tables for #1s, Top 10s, top debuts, debut weeks, and biggest climbers

Run it like this:

```bash
cd "$(dirname "$0")"  # or cd /path/to/torreys_corner_top40
streamlit run app.py
```

If Streamlit is not installed yet:

```bash
pip install streamlit pandas
```

The app now also:
- accepts either `db/torreys_corner_top40.sqlite` or `db/tctop40.sqlite`
- tolerates the database file being in the project root
- safely quotes FTS searches so apostrophes like `player's remorse` do not break SQLite FTS5 parsing
