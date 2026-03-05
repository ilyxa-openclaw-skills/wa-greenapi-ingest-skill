#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${1:-${WA_ARCHIVE_DB_PATH:-$ROOT_DIR/wa_archive.db}}"
LIMIT="${VERIFY_LIMIT:-300}"

python3 - "$DB_PATH" "$LIMIT" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

if len(sys.argv) < 3:
    print("usage: verify_media_transcript.sh <db_path> [limit]", file=sys.stderr)
    sys.exit(2)

db = Path(sys.argv[1])
limit = int(sys.argv[2])

if not db.exists():
    print(json.dumps({"ok": False, "error": f"db not found: {db}"}, ensure_ascii=False))
    sys.exit(2)

conn = sqlite3.connect(str(db))
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

hits = []
for row in rows:
    rid, ts, peer, text, raw_json = row
    try:
        parsed = json.loads(raw_json or "{}")
    except Exception:
        continue

    diag = parsed.get("waArchiveIngestDiag") if isinstance(parsed, dict) else None
    if not isinstance(diag, dict):
        continue

    media = diag.get("mediaDownload")
    tr = diag.get("transcription")
    if not isinstance(media, dict) or not isinstance(tr, dict):
        continue
    if not tr.get("ok"):
        continue

    media_path = Path(str(media.get("path") or ""))
    if not media_path.exists():
        continue

    txt = str(text or "").strip()
    if not txt or (txt.startswith("<media:") and txt.endswith(">")):
        continue

    hits.append(
        {
            "id": rid,
            "ts": ts,
            "peer": peer,
            "text_preview": txt[:120],
            "media_path": str(media_path),
            "engine": tr.get("engine"),
        }
    )

summary = {
    "ok": bool(hits),
    "checked_rows": len(rows),
    "hits": hits[:20],
}
print(json.dumps(summary, ensure_ascii=False, indent=2))
sys.exit(0 if hits else 1)
PY
