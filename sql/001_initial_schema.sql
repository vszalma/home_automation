-- 001_initial_schema.sql
-- Initial SQLite schema for media tagging pipeline (rules + AI + metadata write)
-- Safe to run multiple times (IF NOT EXISTS everywhere)

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- Migration tracking (recommended even for schema v1)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
  version      INTEGER PRIMARY KEY,
  filename     TEXT NOT NULL,
  applied_utc  TEXT NOT NULL
);


-- ------------------------------------------------------------
-- Runs (audit trail for every command invocation)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runs (
  run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  started_utc   TEXT NOT NULL,   -- ISO 8601 UTC
  ended_utc     TEXT,            -- ISO 8601 UTC
  command       TEXT NOT NULL,   -- e.g. "scan", "ai-analyze", "plan-tags", "apply-tags"
  args_json     TEXT,            -- JSON string snapshot of config/args (optional)
  status        TEXT NOT NULL DEFAULT 'running', -- running|ok|failed
  notes         TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_started_utc ON runs(started_utc);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);


-- ------------------------------------------------------------
-- Roots (Original / Library / Staging)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS roots (
  root_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  name        TEXT NOT NULL UNIQUE,     -- "Original", "Library", "Staging"
  base_path   TEXT NOT NULL,            -- absolute/UNC path
  type        TEXT NOT NULL,            -- original|library|staging
  is_active   INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_roots_type_active ON roots(type, is_active);


-- ------------------------------------------------------------
-- Files (each discovered path instance)
-- NOTE: choose path strategy: relative-to-root or full path; keep consistent.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS files (
  file_id                INTEGER PRIMARY KEY AUTOINCREMENT,
  root_id                INTEGER NOT NULL REFERENCES roots(root_id) ON DELETE CASCADE,

  path                   TEXT NOT NULL,       -- relative to root (recommended) OR full path
  filename               TEXT NOT NULL,
  ext                    TEXT,                -- ".jpg", ".nef", etc.
  media_type             TEXT NOT NULL,       -- image|video|other

  size_bytes             INTEGER NOT NULL,
  mtime_utc              TEXT,                -- ISO UTC
  ctime_utc              TEXT,                -- ISO UTC (optional)

  sha256                 TEXT,                -- null until computed
  phash                  TEXT,                -- optional future: perceptual hash

  exif_dt_original_utc   TEXT,                -- ISO UTC (optional)
  exif_make              TEXT,
  exif_model             TEXT,

  status                 TEXT NOT NULL DEFAULT 'active', -- active|missing|moved|deleted
  last_seen_run_id       INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,

  created_utc            TEXT NOT NULL DEFAULT (datetime('now')),
  updated_utc            TEXT NOT NULL DEFAULT (datetime('now'))
);

-- path unique within each root
CREATE UNIQUE INDEX IF NOT EXISTS idx_files_root_path_unique ON files(root_id, path);

CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_files_media_type ON files(media_type);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen_run_id);


-- ------------------------------------------------------------
-- Hash groups (one row per unique content hash)
-- canonical_library_file_id points to the "kept" file in the Library.
-- best_file_id can be used for heuristics if canonical isn't selected yet.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hash_groups (
  sha256                    TEXT PRIMARY KEY,
  canonical_library_file_id  INTEGER REFERENCES files(file_id) ON DELETE SET NULL,
  best_file_id              INTEGER REFERENCES files(file_id) ON DELETE SET NULL,
  first_seen_run_id         INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  last_seen_run_id          INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_hash_groups_canonical ON hash_groups(canonical_library_file_id);
CREATE INDEX IF NOT EXISTS idx_hash_groups_last_seen ON hash_groups(last_seen_run_id);


-- ------------------------------------------------------------
-- File-group membership (all files that share a hash)
-- role: original|library|staging|duplicate
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS file_group_members (
  sha256      TEXT NOT NULL REFERENCES hash_groups(sha256) ON DELETE CASCADE,
  file_id     INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  role        TEXT NOT NULL,
  PRIMARY KEY (sha256, file_id)
);

CREATE INDEX IF NOT EXISTS idx_fgm_file ON file_group_members(file_id);
CREATE INDEX IF NOT EXISTS idx_fgm_role ON file_group_members(role);


-- ------------------------------------------------------------
-- Canonical provenance (original files that contribute context to a canonical hash)
-- Useful when multiple original paths exist for the same hash group.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS canonical_provenance (
  sha256           TEXT NOT NULL REFERENCES hash_groups(sha256) ON DELETE CASCADE,
  original_file_id INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  weight           REAL NOT NULL DEFAULT 1.0,
  note             TEXT,
  PRIMARY KEY (sha256, original_file_id)
);

CREATE INDEX IF NOT EXISTS idx_prov_sha ON canonical_provenance(sha256);
CREATE INDEX IF NOT EXISTS idx_prov_orig_file ON canonical_provenance(original_file_id);


-- ------------------------------------------------------------
-- Tags dictionary
-- tag = your final keyword string, e.g. "Year/2019", "Trip/Italy", "Object/Dog"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tags (
  tag_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  tag         TEXT NOT NULL UNIQUE,
  tag_type    TEXT,                           -- year|event|place|object|person|etc
  is_allowed  INTEGER NOT NULL DEFAULT 1,
  created_utc TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tags_type ON tags(tag_type);
CREATE INDEX IF NOT EXISTS idx_tags_allowed ON tags(is_allowed);


-- ------------------------------------------------------------
-- Applied/planned tags per file (pipeline's intended/applied truth)
-- source: rule|ai|manual|import
-- applied_state: planned|applied|skipped|reverted
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS file_tags (
  file_id        INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  tag_id         INTEGER NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
  source         TEXT NOT NULL,
  confidence     REAL,                         -- nullable for rule/manual
  applied_state  TEXT NOT NULL DEFAULT 'planned',
  applied_run_id INTEGER REFERENCES runs(run_id) ON DELETE SET NULL,
  note           TEXT,
  created_utc    TEXT NOT NULL DEFAULT (datetime('now')),
  updated_utc    TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (file_id, tag_id, source)
);

CREATE INDEX IF NOT EXISTS idx_file_tags_file ON file_tags(file_id);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_file_tags_state ON file_tags(applied_state);
CREATE INDEX IF NOT EXISTS idx_file_tags_source ON file_tags(source);


-- ------------------------------------------------------------
-- AI tag suggestions (raw model output; not the same as applied tags)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tag_suggestions (
  file_id     INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  tag_id      INTEGER NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
  model       TEXT NOT NULL,           -- e.g. "clip-vit-b32"
  score       REAL NOT NULL,
  run_id      INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  created_utc TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (file_id, tag_id, model, run_id)
);

CREATE INDEX IF NOT EXISTS idx_suggest_file ON tag_suggestions(file_id);
CREATE INDEX IF NOT EXISTS idx_suggest_tag ON tag_suggestions(tag_id);
CREATE INDEX IF NOT EXISTS idx_suggest_model ON tag_suggestions(model);
CREATE INDEX IF NOT EXISTS idx_suggest_run ON tag_suggestions(run_id);
CREATE INDEX IF NOT EXISTS idx_suggest_score ON tag_suggestions(score);


-- ------------------------------------------------------------
-- Captions (optional AI output)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS captions (
  file_id     INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  model       TEXT NOT NULL,           -- e.g. "blip-base"
  caption     TEXT NOT NULL,
  run_id      INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  created_utc TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (file_id, model, run_id)
);

CREATE INDEX IF NOT EXISTS idx_captions_file ON captions(file_id);
CREATE INDEX IF NOT EXISTS idx_captions_run ON captions(run_id);


-- ------------------------------------------------------------
-- Analysis state (optional accelerator to skip re-analysis)
-- You could derive this from tag_suggestions/captions, but this makes it cheap.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analysis_state (
  file_id              INTEGER PRIMARY KEY REFERENCES files(file_id) ON DELETE CASCADE,
  last_analyzed_run_id  INTEGER REFERENCES runs(run_id) ON DELETE SET NULL,
  last_caption_model    TEXT,
  last_tag_model        TEXT,
  updated_utc           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_analysis_last_run ON analysis_state(last_analyzed_run_id);


-- ------------------------------------------------------------
-- Metadata write log (what you changed on disk)
-- before_keywords/after_keywords stored as JSON array strings for audit/debug.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_write_log (
  log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id         INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
  run_id          INTEGER NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  writer          TEXT NOT NULL,        -- e.g. "exiftool"
  before_keywords TEXT,                 -- JSON array string
  after_keywords  TEXT,                 -- JSON array string
  result          TEXT NOT NULL,        -- ok|error
  error           TEXT,
  created_utc     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_mwl_file ON metadata_write_log(file_id);
CREATE INDEX IF NOT EXISTS idx_mwl_run ON metadata_write_log(run_id);
CREATE INDEX IF NOT EXISTS idx_mwl_result ON metadata_write_log(result);


-- ------------------------------------------------------------
-- Helpful views (for reporting/queues)
-- ------------------------------------------------------------

-- Canonical library files (one per hash group, when selected)
CREATE VIEW IF NOT EXISTS v_canonical_library_files AS
SELECT
  hg.sha256,
  f.file_id,
  r.name AS root_name,
  r.base_path,
  f.path,
  f.filename,
  f.ext,
  f.media_type,
  f.size_bytes,
  f.mtime_utc,
  f.exif_dt_original_utc
FROM hash_groups hg
JOIN files f ON f.file_id = hg.canonical_library_file_id
JOIN roots r ON r.root_id = f.root_id;

-- Files needing hashing (sha256 missing, active)
CREATE VIEW IF NOT EXISTS v_files_needing_hash AS
SELECT f.*
FROM files f
WHERE f.sha256 IS NULL
  AND f.status = 'active';

-- Planned tags queue (waiting for apply-tags)
CREATE VIEW IF NOT EXISTS v_planned_tags AS
SELECT
  f.file_id,
  r.name AS root_name,
  f.path,
  t.tag,
  ft.source,
  ft.confidence,
  ft.applied_state,
  ft.applied_run_id
FROM file_tags ft
JOIN files f ON f.file_id = ft.file_id
JOIN roots r ON r.root_id = f.root_id
JOIN tags t  ON t.tag_id = ft.tag_id
WHERE ft.applied_state = 'planned';

-- Low-confidence AI suggestions queue (tune threshold later)
CREATE VIEW IF NOT EXISTS v_ai_suggestions_low_conf AS
SELECT
  f.file_id,
  r.name AS root_name,
  f.path,
  t.tag,
  s.model,
  s.score,
  s.run_id
FROM tag_suggestions s
JOIN files f ON f.file_id = s.file_id
JOIN roots r ON r.root_id = f.root_id
JOIN tags t  ON t.tag_id = s.tag_id
WHERE s.score < 0.30;


-- ------------------------------------------------------------
-- Timestamp maintenance triggers
-- ------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_files_set_updated
AFTER UPDATE ON files
FOR EACH ROW
BEGIN
  UPDATE files SET updated_utc = datetime('now') WHERE file_id = NEW.file_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_file_tags_set_updated
AFTER UPDATE ON file_tags
FOR EACH ROW
BEGIN
  UPDATE file_tags
  SET updated_utc = datetime('now')
  WHERE file_id = NEW.file_id AND tag_id = NEW.tag_id AND source = NEW.source;
END;

CREATE TRIGGER IF NOT EXISTS trg_analysis_state_set_updated
AFTER UPDATE ON analysis_state
FOR EACH ROW
BEGIN
  UPDATE analysis_state
  SET updated_utc = datetime('now')
  WHERE file_id = NEW.file_id;
END;
