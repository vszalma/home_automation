-- 004_curation_tables.sql
-- Step 4c: Human curation persistence (SHA-level, authoritative semantics)
-- Safe to run multiple times.

PRAGMA foreign_keys = ON;

----------------------------------------------------------------------
-- Curated tags (authoritative semantic layer)
-- One row per (sha256, tag)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS curated_tags (
    sha256        TEXT NOT NULL,
    tag           TEXT NOT NULL,
    source        TEXT NOT NULL,              -- 'ai_promoted' | 'human_added'
    run_id        INTEGER,                    -- originating ai_caption_runs.run_id
    confidence    REAL,                       -- AI score if promoted, NULL if human
    note          TEXT,                       -- optional reviewer notes
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (sha256, tag)
);

CREATE INDEX IF NOT EXISTS idx_curated_tags_sha256
    ON curated_tags (sha256);

CREATE INDEX IF NOT EXISTS idx_curated_tags_tag
    ON curated_tags (tag);


----------------------------------------------------------------------
-- AI tag overrides (sticky suppression / rejection)
-- Prevents repeated bad AI suggestions
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ai_tag_overrides (
    sha256        TEXT NOT NULL,
    tag           TEXT NOT NULL,
    action        TEXT NOT NULL,              -- 'suppress' (v1)
    run_id        INTEGER,
    note          TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (sha256, tag, action)
);

CREATE INDEX IF NOT EXISTS idx_ai_tag_overrides_sha256
    ON ai_tag_overrides (sha256);


----------------------------------------------------------------------
-- Curated captions (optional human-approved or human-edited captions)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS curated_captions (
    sha256        TEXT PRIMARY KEY,
    caption       TEXT NOT NULL,
    source        TEXT NOT NULL,              -- 'human' | 'ai_accepted'
    run_id        INTEGER,
    note          TEXT,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);


----------------------------------------------------------------------
-- Curation import audit (CSV → DB runs)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS curation_import_runs (
    import_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    csv_path         TEXT NOT NULL,
    accept_threshold REAL NOT NULL,
    started_at       TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at      TEXT,

    rows_total       INTEGER NOT NULL DEFAULT 0,
    rows_applied     INTEGER NOT NULL DEFAULT 0,
    rows_skipped     INTEGER NOT NULL DEFAULT 0,
    rows_errors      INTEGER NOT NULL DEFAULT 0
);
