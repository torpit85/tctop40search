PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS chart_week (
    chart_week_id INTEGER PRIMARY KEY,
    chart_date TEXT NOT NULL UNIQUE,
    chart_id TEXT,
    source_file TEXT NOT NULL,
    source_zip TEXT,
    row_count INTEGER,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS entry (
    entry_id INTEGER PRIMARY KEY,
    chart_week_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    chart_id TEXT,

    raw_combined_artist TEXT,
    raw_artist TEXT,
    raw_featured TEXT,
    raw_song_title TEXT,
    raw_slug TEXT,

    artist_display TEXT,
    featured_display TEXT,
    full_artist_display TEXT,
    song_title_display TEXT,

    normalized_artist TEXT,
    normalized_featured TEXT,
    normalized_full_artist TEXT,
    normalized_song_title TEXT,
    normalized_display TEXT,

    canonical_song_id INTEGER,
    canonical_title_key TEXT,
    canonical_artist_key TEXT,
    canonical_group_key TEXT,
    derived_is_debut INTEGER DEFAULT 0,
    derived_is_top_debut INTEGER DEFAULT 0,
    derived_is_reentry INTEGER DEFAULT 0,
    derived_marker TEXT,

    FOREIGN KEY (chart_week_id) REFERENCES chart_week(chart_week_id) ON DELETE CASCADE,
    UNIQUE(chart_week_id, position)
);

CREATE TABLE IF NOT EXISTS canonical_song (
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

CREATE TABLE IF NOT EXISTS song_alias (
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

CREATE INDEX IF NOT EXISTS idx_chart_week_date ON chart_week(chart_date);
CREATE INDEX IF NOT EXISTS idx_entry_chart_week_id ON entry(chart_week_id);
CREATE INDEX IF NOT EXISTS idx_entry_position ON entry(position);
CREATE INDEX IF NOT EXISTS idx_entry_norm_song ON entry(normalized_song_title);
CREATE INDEX IF NOT EXISTS idx_entry_norm_artist ON entry(normalized_full_artist);
CREATE INDEX IF NOT EXISTS idx_entry_canonical_song_id ON entry(canonical_song_id);
CREATE INDEX IF NOT EXISTS idx_entry_canonical_group_key ON entry(canonical_group_key);
CREATE INDEX IF NOT EXISTS idx_entry_derived_marker ON entry(derived_marker);
CREATE INDEX IF NOT EXISTS idx_canonical_song_group_key ON canonical_song(canonical_group_key);
CREATE INDEX IF NOT EXISTS idx_song_alias_canonical_song_id ON song_alias(canonical_song_id);

CREATE VIRTUAL TABLE IF NOT EXISTS entry_fts USING fts5(
    song_title_display,
    full_artist_display,
    normalized_display,
    raw_slug,
    source_file,
    content=''
);
