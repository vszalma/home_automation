-- 002_seed_config.sql
-- Seed placeholder roots and default settings for media_pipeline.
-- Base paths are placeholders; update to your environment before production use.

-- Ensure settings table exists.
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);

-- Seed roots with placeholder paths (safe, idempotent).
INSERT OR IGNORE INTO roots (name, type, base_path, is_active) VALUES
    ('Original', 'original', 'N:\\CathyK', 1),
    ('Library', 'library',  'D:\\MediaArchive', 1),
    ('Staging', 'staging',  'D:\\MediaArchive_quarantine', 0);

-- Seed default settings (idempotent).
INSERT OR IGNORE INTO settings (key, value, updated_utc) VALUES
    ('hash_chunk_bytes', '8388608', datetime('now')),
    ('ai_default_threshold', '0.32', datetime('now')),
    ('scan_skip_dirs', '["$RECYCLE.BIN","System Volume Information",".git",".svn","@eaDir"]', datetime('now')),
    ('path_storage_mode', 'relative', datetime('now'));
