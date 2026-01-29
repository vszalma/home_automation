# ai_analyze.py
# Step 4a: AI enrichment sidecar (captions + scored tags), SHA-level
#
# CPU-friendly default: Salesforce/blip-image-captioning-base (BLIP).
# BLIP-2 is structured to be swappable later (GPU recommended).  See notes below.
#
# Requires:
#   pip install transformers torch pillow tqdm
#
# Usage examples:
#   python ai_analyze.py --db C:\path\home_automation.db --where-tag needs_review --limit 200
#   python ai_analyze.py --db ... --roles staging --only-new
#
# Notes:
# - Non-destructive: never writes to rule_tags/hash_group_rule_tags.
# - Idempotent: skips sha256 already captioned for same run_id.
# - Thumbnail seam is stubbed: currently always uses original image path.

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

import torch
from PIL import Image, ImageOps
from tqdm import tqdm

import logging
import structlog
import home_automation_common


def _init_logger(module_name: str, verbose: bool = False):
    home_automation_common.create_logger(module_name)  # side effects
    logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)
    return structlog.get_logger().bind(module=module_name)

def _init_logger(module_name: str, verbose: bool = False):
    home_automation_common.create_logger(module_name)  # side effects
    logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)
    return structlog.get_logger().bind(module=module_name)

@dataclass(frozen=True)
class ImageSource:
    sha256: str
    file_id: int
    abs_path: Path
    profile: str = "orig"  # thumbnail profile; "orig" for now


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AI enrichment: captions + scored tags (SHA-level, non-destructive).")
    p.add_argument("--db", required=True, help="Path to SQLite DB.")
    p.add_argument("--model", default="Salesforce/blip-image-captioning-base",
                   help="HF model name. CPU default is BLIP image-captioning base.")
    p.add_argument("--revision", default=None, help="Optional HF revision/tag/commit.")
    p.add_argument("--device", default="cpu", help="cpu | cuda | cuda:0 (future). CPU recommended for this machine.")
    p.add_argument("--roles", default="library,staging,original",
                   help="Comma-separated roles to choose representative file from if canonical is absent.")
    p.add_argument("--prefer-canonical", action="store_true",
                   help="Prefer hash_groups.canonical_library_file_id when available.")
    p.add_argument("--where-tag", default=None,
                   help="Process only sha256 that have this rule tag (e.g., needs_review).")
    p.add_argument("--only-new", action="store_true",
                   help="Skip sha256 already processed for this run_id.")
    p.add_argument("--limit", type=int, default=0, help="Max sha256 to process (0 = no limit).")
    p.add_argument("--batch-size", type=int, default=32, help="Commit every N items.")
    p.add_argument("--dry-run", action="store_true", help="Do not write captions/tags; just log what would happen.")
    return p.parse_args()


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # keep writes fast for batch inserts
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def ensure_ai_schema_present(conn: sqlite3.Connection) -> None:
    # Minimal runtime check: fail fast if user forgot to run migration.
    required = {"ai_models", "ai_caption_runs", "ai_captions", "ai_tags", "ai_tag_vocab"}
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    present = {r["name"] for r in rows}
    missing = required - present
    if missing:
        raise RuntimeError(
            f"Missing AI tables: {sorted(missing)}. Run 003_ai_enrichment.sql migration first."
        )


def upsert_model_and_run(conn: sqlite3.Connection, model_name: str, revision: Optional[str],
                         device: str, decode_params: dict, prompt_template: Optional[str]) -> int:
    params_json = json.dumps({"model": model_name, "revision": revision}, sort_keys=True)
    runtime = "cuda" if "cuda" in device else "cpu"

    # Try find existing model row (simple match on name+revision+device)
    row = conn.execute(
        "SELECT ai_model_id FROM ai_models WHERE name=? AND IFNULL(revision,'')=IFNULL(?, '') AND IFNULL(device,'')=IFNULL(?, '')",
        (model_name, revision, device)
    ).fetchone()

    if row:
        ai_model_id = int(row["ai_model_id"])
    else:
        conn.execute(
            "INSERT INTO ai_models(name, revision, runtime, device, params_json) VALUES (?,?,?,?,?)",
            (model_name, revision, runtime, device, params_json)
        )
        ai_model_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    conn.execute(
        "INSERT INTO ai_caption_runs(ai_model_id, prompt_template, decode_params_json, note) VALUES (?,?,?,?)",
        (ai_model_id, prompt_template, json.dumps(decode_params, sort_keys=True), "ai_analyze.py run")
    )
    run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    return run_id


def list_target_sha256(conn: sqlite3.Connection, where_tag: Optional[str], limit: int) -> list[str]:
    sql = "SELECT DISTINCT sha256 FROM hash_groups"
    params: list = []
    if where_tag:
        sql = """
            SELECT DISTINCT hgrt.sha256
            FROM hash_group_rule_tags hgrt
            WHERE hgrt.tag = ?
        """
        params = [where_tag]
    if limit and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [r["sha256"] for r in rows]


def already_done(conn: sqlite3.Connection, run_id: int, sha256: str) -> bool:
    row = conn.execute("SELECT 1 FROM ai_captions WHERE sha256=? AND run_id=? LIMIT 1", (sha256, run_id)).fetchone()
    return row is not None


def resolve_file_abs_path(conn: sqlite3.Connection, file_id: int) -> Path:
    """
    Assumes you have a roots table mapping root_id -> absolute root path.
    If your schema differs, adjust this function accordingly.

    Expected:
      roots(root_id PK, base_path TEXT)
      files(file_id PK, root_id, path, filename)
    """
    row = conn.execute("""
        SELECT f.path, f.filename, r.base_path
        FROM files f
        JOIN roots r ON r.root_id = f.root_id
        WHERE f.file_id = ?
    """, (file_id,)).fetchone()

    if not row:
        raise RuntimeError(f"Could not resolve file_id={file_id} (missing files/roots row).")

    rel = Path(row["path"]) / row["filename"]
    # row["path"] is stored with '\' separator. Path will tolerate it on Windows.
    return Path(row["base_path"]) / rel


def pick_representative_file(conn: sqlite3.Connection, sha256: str, roles: Sequence[str],
                             prefer_canonical: bool) -> Optional[int]:
    if prefer_canonical:
        row = conn.execute(
            "SELECT canonical_library_file_id FROM hash_groups WHERE sha256=?",
            (sha256,)
        ).fetchone()
        if row and row["canonical_library_file_id"]:
            return int(row["canonical_library_file_id"])

    # Otherwise pick smallest file_id among requested roles (stable + deterministic)
    qmarks = ",".join(["?"] * len(roles))
    row = conn.execute(f"""
        SELECT MIN(fgm.file_id) AS file_id
        FROM file_group_members fgm
        WHERE fgm.sha256 = ?
          AND fgm.role IN ({qmarks})
    """, [sha256, *roles]).fetchone()

    if row and row["file_id"] is not None:
        return int(row["file_id"])
    return None


def get_image_for_ai(conn: sqlite3.Connection, sha256: str, roles: Sequence[str],
                     prefer_canonical: bool) -> Optional[ImageSource]:
    """
    Thumbnail seam: today returns original path.
    Later: check ai_thumbnails for profile, build/store thumb, return thumb.
    """
    file_id = pick_representative_file(conn, sha256, roles, prefer_canonical)
    if file_id is None:
        return None

    abs_path = resolve_file_abs_path(conn, file_id)
    if not abs_path.exists():
        # if canonical missing on disk, try fallback to any role
        return None

    return ImageSource(sha256=sha256, file_id=file_id, abs_path=abs_path, profile="orig")


def caption_image_blip(model, processor, image_path: Path, decode_params: dict) -> str:
    """
    Generate a caption using BLIP in CPU-only mode.
    """
    image = Image.open(image_path).convert("RGB")
    image = ImageOps.exif_transpose(image)

    inputs = processor(images=image, return_tensors="pt")

    # Map decode params with safe defaults
    gen_kwargs = {
        "max_new_tokens": decode_params.get("max_new_tokens", decode_params.get("max_length")),
        "num_beams": decode_params.get("num_beams", 1),
        "do_sample": False,
    }
    # remove None to avoid HF warnings
    gen_kwargs = {k: v for k, v in gen_kwargs.items() if v is not None}

    with torch.inference_mode():
        generated_ids = model.generate(**inputs, **gen_kwargs)

    caption = processor.decode(generated_ids[0], skip_special_tokens=True)
    return caption.strip()


def simple_tagger_from_caption(caption: str) -> list[Tuple[str, float, str]]:
    """
    Very simple, deterministic, and cheap. Replace/extend later.
    Returns list of (tag, score, evidence).
    """
    c = caption.lower()
    vocab = {
        # docs/screenshots
        "document": ["document", "paper", "text", "letter", "invoice", "receipt", "form"],
        "receipt": ["receipt"],
        "screenshot": ["screenshot", "screen", "monitor", "laptop"],
        # people/animals
        "person": ["person", "man", "woman", "child", "boy", "girl", "people"],
        "dog": ["dog", "puppy"],
        "cat": ["cat", "kitten"],
        # common scenes
        "beach": ["beach", "ocean", "sea"],
        "snow": ["snow", "winter"],
        "car": ["car", "vehicle"],
        "food": ["food", "pizza", "cake", "burger"],
    }

    tags: list[Tuple[str, float, str]] = []
    for tag, keys in vocab.items():
        for k in keys:
            if k in c:
                # score heuristic: longer/more specific keyword hits get slightly higher score
                score = 0.85 if len(k) >= 6 else 0.80
                tags.append((tag, score, f"keyword:{k}"))
                break
    return tags


def ensure_vocab(conn: sqlite3.Connection, tags: Iterable[str]) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO ai_tag_vocab(tag, category) VALUES (?, NULL)",
        [(t,) for t in tags]
    )


def main() -> None:
    args = parse_args()
    
    logger = _init_logger("ai_analyze", verbose=getattr(args, "verbose", False))

    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    if not roles:
        raise ValueError("No roles provided.")

    conn = connect(args.db)
    ensure_ai_schema_present(conn)

    # Import transformers lazily so the script can error early on schema issues.
    from transformers import BlipForConditionalGeneration, BlipProcessor  # type: ignore

    # CPU-safe decode params (deterministic-ish)
    # NOTE: some models may ignore some params; that's OK.
    decode_params = {
        "max_new_tokens": 40,
        "do_sample": False,
        "num_beams": 1,
    }

    logger.info("Loading BLIP model", 
                model=args.model, 
                device=args.device, 
                revision=args.revision
                )
    torch.set_num_threads(2)
    processor = BlipProcessor.from_pretrained(args.model, revision=args.revision)
    model = BlipForConditionalGeneration.from_pretrained(
        args.model,
        revision=args.revision,
        torch_dtype=torch.float32
    )
    model.eval()

    run_id = upsert_model_and_run(
        conn=conn,
        model_name=args.model,
        revision=args.revision,
        device=args.device,
        decode_params=decode_params,
        prompt_template=None
    )
    conn.commit()

    sha_list = list_target_sha256(conn, args.where_tag, args.limit)
    logger.info("Targets selected", 
                count=len(sha_list), 
                where_tag=args.where_tag, 
                run_id=run_id
                )

    processed = 0
    wrote = 0
    errors = 0

    pending_inserts_captions = []
    pending_inserts_tags = []
    pending_queue_updates = []

    for sha in tqdm(sha_list, desc="AI analyze"):
        processed += 1

        if args.only_new and already_done(conn, run_id, sha):
            continue

        src = get_image_for_ai(conn, sha, roles, args.prefer_canonical)
        if src is None:
            errors += 1
            pending_queue_updates.append((sha, "error", "No resolvable file for sha256 (missing candidate or missing on disk)", run_id))
            continue

        try:
            caption = caption_image_blip(model, processor, src.abs_path, decode_params)
            if not caption:
                pending_queue_updates.append((sha, "error", "Empty caption output", run_id))
                errors += 1
                continue

            # Tags (cheap deterministic v1)
            tags = simple_tagger_from_caption(caption)
            tag_names = [t[0] for t in tags]

            if not args.dry_run:
                pending_inserts_captions.append((sha, run_id, caption, None, None, src.file_id))
                if tag_names:
                    ensure_vocab(conn, tag_names)
                    for tag, score, evidence in tags:
                        pending_inserts_tags.append((sha, run_id, tag, float(score), evidence))

                pending_queue_updates.append((sha, "done", None, run_id))
                wrote += 1

        except Exception as e:
            errors += 1
            pending_queue_updates.append((sha, "error", repr(e), run_id))

        # batch commit
        if not args.dry_run and (len(pending_inserts_captions) >= args.batch_size):
            conn.executemany(
                "INSERT OR REPLACE INTO ai_captions(sha256, run_id, caption, caption_alt_json, confidence, source_file_id) VALUES (?,?,?,?,?,?)",
                pending_inserts_captions
            )
            conn.executemany(
                "INSERT OR REPLACE INTO ai_tags(sha256, run_id, tag, score, evidence) VALUES (?,?,?,?,?)",
                pending_inserts_tags
            )
            # upsert queue
            conn.executemany("""
                INSERT INTO ai_queue(sha256, status, last_error, last_run_id, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(sha256) DO UPDATE SET
                    status=excluded.status,
                    last_error=excluded.last_error,
                    last_run_id=excluded.last_run_id,
                    updated_at=datetime('now')
            """, pending_queue_updates)

            conn.commit()
            pending_inserts_captions.clear()
            pending_inserts_tags.clear()
            pending_queue_updates.clear()

    # flush remainder
    if not args.dry_run and pending_inserts_captions:
        conn.executemany(
            "INSERT OR REPLACE INTO ai_captions(sha256, run_id, caption, caption_alt_json, confidence, source_file_id) VALUES (?,?,?,?,?,?)",
            pending_inserts_captions
        )
        conn.executemany(
            "INSERT OR REPLACE INTO ai_tags(sha256, run_id, tag, score, evidence) VALUES (?,?,?,?,?)",
            pending_inserts_tags
        )
        conn.executemany("""
            INSERT INTO ai_queue(sha256, status, last_error, last_run_id, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(sha256) DO UPDATE SET
                status=excluded.status,
                last_error=excluded.last_error,
                last_run_id=excluded.last_run_id,
                updated_at=datetime('now')
        """, pending_queue_updates)
        conn.commit()

    # close run
    conn.execute("UPDATE ai_caption_runs SET finished_at=datetime('now') WHERE run_id=?", (run_id,))
    conn.commit()

    logger.info("Done", 
        run_id=run_id,
        processed=processed,
        wrote=wrote,
        errors=errors,
        model=args.model,
        device=args.device,
        )


if __name__ == "__main__":
    main()
