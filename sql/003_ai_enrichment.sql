-- 003_ai_enrichment.sql
-- AI enrichment sidecar: captions + scored tags (non-authoritative)
-- Safe/idempotent: results keyed by (sha256, run_id)

PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS ai_models (
    ai_model_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    revision TEXT,
    runtime TEXT,               -- e.g. "cpu", "cuda"
    device TEXT,                -- e.g. "cpu", "cuda:0"
    params_json TEXT,           -- json string
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ai_caption_runs (
    run_id INTEGER PRIMARY KEY,
    ai_model_id INTEGER NOT NULL,
    prompt_template TEXT,
    decode_params_json TEXT,    -- json string
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    note TEXT,
    FOREIGN KEY (ai_model_id) REFERENCES ai_models(ai_model_id)
);

CREATE TABLE IF NOT EXISTS ai_captions (
    sha256 TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    caption TEXT NOT NULL,
    caption_alt_json TEXT,
    confidence REAL,
    source_file_id INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (sha256, run_id),
    FOREIGN KEY (run_id) REFERENCES ai_caption_runs(run_id)
);

CREATE TABLE IF NOT EXISTS ai_tag_vocab (
    tag TEXT PRIMARY KEY,
    category TEXT
);

CREATE TABLE IF NOT EXISTS ai_tags (
    sha256 TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    tag TEXT NOT NULL,
    score REAL NOT NULL,
    evidence TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (sha256, run_id, tag),
    FOREIGN KEY (run_id) REFERENCES ai_caption_runs(run_id),
    FOREIGN KEY (tag) REFERENCES ai_tag_vocab(tag)
);

-- Optional work queue for resumable processing
CREATE TABLE IF NOT EXISTS ai_queue (
    sha256 TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending|done|error|skipped
    priority INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_run_id INTEGER,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Thumbnail seam (stub): implemented later, but DB support can exist now
CREATE TABLE IF NOT EXISTS ai_thumbnails (
    sha256 TEXT NOT NULL,
    profile TEXT NOT NULL,                -- e.g. "orig" now; "512_jpg_q85" later
    thumb_path TEXT NOT NULL,
    width INTEGER,
    height INTEGER,
    format TEXT,
    source_file_id INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (sha256, profile)
);

CREATE INDEX IF NOT EXISTS idx_ai_tags_run ON ai_tags(run_id);
CREATE INDEX IF NOT EXISTS idx_ai_captions_run ON ai_captions(run_id);
