#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${1:-${WA_ARCHIVE_DB_PATH:-$ROOT_DIR/wa_archive.db}}"
LIMIT="${2:-${VERIFY_LIMIT:-300}}"
MEDIA_DIR="${3:-${GREENAPI_MEDIA_STORE_DIR:-$ROOT_DIR/data/media}}"
KEEP_MEDIA_FILES_RAW="${GREENAPI_KEEP_MEDIA_FILES:-0}"

python3 - "$DB_PATH" "$LIMIT" "$MEDIA_DIR" "$KEEP_MEDIA_FILES_RAW" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

if len(sys.argv) < 5:
    print("usage: verify_media_transcript.sh <db_path> [limit] [media_dir]", file=sys.stderr)
    sys.exit(2)


def truthy(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v or "").strip().lower() in {"1", "true", "yes", "y"}


def is_placeholder(txt: str) -> bool:
    t = str(txt or "").strip()
    return t.startswith("<media:") and t.endswith(">")


db = Path(sys.argv[1])
limit = int(sys.argv[2])
media_dir = Path(sys.argv[3])
env_keep = truthy(sys.argv[4])

if not db.exists():
    print(json.dumps({"ok": False, "error": f"db not found: {db}"}, ensure_ascii=False))
    sys.exit(2)

conn = sqlite3.connect(str(db))
has_messages = conn.execute(
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages' LIMIT 1"
).fetchone()
if not has_messages:
    print(json.dumps({"ok": False, "error": "table messages not found", "db": str(db)}, ensure_ascii=False))
    sys.exit(2)

rows = conn.execute(
    """
    SELECT id, ts, peer, text, raw_json
    FROM messages
    WHERE source_type IN ('greenapi','greenapi-history')
    ORDER BY id DESC
    LIMIT ?
    """,
    (limit,),
).fetchall()

checked = 0
bad_text = []
file_leaks = []
storage_violations = []
text_hits = []

for rid, ts, peer, text, raw_json in rows:
    try:
        parsed = json.loads(raw_json or "{}")
    except Exception:
        continue

    diag = parsed.get("waArchiveIngestDiag") if isinstance(parsed, dict) else None
    if not isinstance(diag, dict):
        continue

    is_audio = truthy(diag.get("isAudio"))
    is_image = truthy(diag.get("isImage"))
    if not (is_audio or is_image):
        continue

    checked += 1

    txt = str(text or "").strip()
    if not txt or is_placeholder(txt):
        bad_text.append({
            "id": rid,
            "ts": ts,
            "peer": peer,
            "text": txt[:120],
            "kind": "audio" if is_audio else "image",
        })
    else:
        text_hits.append(
            {
                "id": rid,
                "ts": ts,
                "peer": peer,
                "kind": "audio" if is_audio else "image",
                "text_preview": txt[:120],
            }
        )

    storage = diag.get("mediaStorage") if isinstance(diag.get("mediaStorage"), dict) else {}
    row_keep = truthy(storage.get("keepMediaFiles")) if "keepMediaFiles" in storage else env_keep

    if not row_keep:
        if truthy(storage.get("binaryStored")):
            storage_violations.append({
                "id": rid,
                "reason": "binaryStored=true while keep=0",
                "storage": storage,
            })

        media_download = diag.get("mediaDownload") if isinstance(diag.get("mediaDownload"), dict) else {}
        path_raw = str(media_download.get("path") or storage.get("binaryPath") or "").strip()
        if path_raw:
            p = Path(path_raw)
            if p.exists():
                file_leaks.append({"id": rid, "path": str(p)})

media_dir_file_count = 0
media_dir_files_sample = []
if media_dir.exists() and media_dir.is_dir():
    for p in media_dir.rglob("*"):
        if p.is_file():
            media_dir_file_count += 1
            if len(media_dir_files_sample) < 20:
                media_dir_files_sample.append(str(p))

ok = (
    checked > 0
    and len(bad_text) == 0
    and len(file_leaks) == 0
    and len(storage_violations) == 0
    and (env_keep or media_dir_file_count == 0)
)

summary = {
    "ok": ok,
    "checked_rows": len(rows),
    "checked_image_audio_rows": checked,
    "text_hits": text_hits[:20],
    "bad_text": bad_text[:20],
    "file_leaks": file_leaks[:20],
    "storage_violations": storage_violations[:20],
    "env_keep_media_files": env_keep,
    "media_dir": str(media_dir),
    "media_dir_file_count": media_dir_file_count,
    "media_dir_files_sample": media_dir_files_sample,
}
print(json.dumps(summary, ensure_ascii=False, indent=2))
sys.exit(0 if ok else 1)
PY
