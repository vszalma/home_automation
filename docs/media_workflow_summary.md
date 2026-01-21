# Media Organization, Verification, and Cleanup Workflow

This document summarizes the **three-script workflow** used to safely organize, verify, deduplicate, and clean up large media collections (images and videos).  
The design prioritizes **data safety**, **auditability**, and **phased execution**.

---

## High-Level Workflow Overview

1. **Organize (Copy/Report/Move)**
   - Scan source data for media files
   - Normalize and organize into a structured archive
   - Produce a detailed CSV manifest

2. **Verify (Hash & Validate)**
   - Verify that each source file has a byte-identical copy in the archive
   - Generate verification manifests
   - Designed for long-running, resumable execution

3. **Apply Deletions (Quarantine/Delete)**
   - Use verified manifests to safely remove originals
   - Default behavior is *quarantine*, not permanent deletion
   - Includes strong safeguards (run_id validation)

Each stage is independent, repeatable, and auditable.

---

## Script 1: organize_media_by_date.py

### Purpose
Scans a source directory for image and video files and **copies, moves, or reports** them into a year-based archive structure.

This script is **non-destructive by default** and is intended to be run first.

### Key Responsibilities
- Discover media files (images and videos)
- Extract creation date (EXIF for images, filesystem or ffprobe for videos)
- Organize files into `destination_root/<YEAR>/`
- Handle filename collisions safely
- Produce a CSV report mapping source → destination

### Modes
- `report` (default): CSV only, no file changes
- `copy`: Copy files to archive
- `move`: Move files to archive (use with caution)

### Important CLI Parameters
```
--source <path>                 Source directory or drive
--destination-root <path>       Root of organized archive

--mode {report,copy,move}       Operation mode (default: report)
--dry-run                       Simulate copy/move without file changes

--media-kind {images,videos,both}
--types <.ext,.ext>             Override default media extensions

--date-from YYYY-MM-DD
--date-to YYYY-MM-DD             Filter by date range

--report-csv <path>             Output CSV path
```

---

## Script 2: verify_media_archive.py

### Purpose
Verifies that each source file listed in the report CSV has an **identical copy** in the archive.

This script performs **content hashing** and produces verification manifests.

### Important CLI Parameters
```
--input-csv <path>
--verified-out <path>
--unverified-out <path>

--hash {sha256}
--limit <N>
--offset <N>
--state-file <path>
```

---

## Script 3: apply_deletion_manifest.py

### Purpose
Safely removes original files **only after verification**, using a deletion manifest.

### Important CLI Parameters
```
--manifest <path>
--expected-run-id <uuid|auto>
--quarantine-root <path>

--limit <N>
--offset <N>
--state-file <path>

--dry-run
--delete-permanently
--yes-really-delete
```

---

## Recommended Safe Execution Order

1. Run Script 1 in **report mode**
2. Review CSV output
3. Run Script 1 in **copy mode**
4. Run Script 2 (verify) in batches
5. Run Script 3 in **quarantine mode**
6. Permanently delete (optional, later)

---

## Summary

This workflow provides a **safe, repeatable, and auditable** approach to cleaning and organizing large media collections.
