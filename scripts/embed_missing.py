#!/usr/bin/env python3
"""
Backfill embeddings for rows in wa_archive.db.messages that have no vector yet.

Minimal helper for greenapi-ingest pipeline where ingestion and embedding are decoupled.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import urllib.request
from pathlib import Path

DEFAULT_DB_PATH = Path(os.getenv("WA_ARCHIVE_DB_PATH", "./wa_archive.db"))
DEFAULT_MODEL = os.getenv("WA_EMBED_MODEL", "text-embedding-3-small")
DEFAULT_TIMEOUT_SEC = int(os.getenv("WA_EMBED_TIMEOUT_SEC", "45"))


def ensure_embeddings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS embeddings (
          message_id INTEGER PRIMARY KEY,
          model TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_model ON embeddings(model)")
    conn.commit()


def fetch_embedding(text: str, model: str, timeout_sec: int) -> list[float]:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    body = json.dumps({"model": model, "input": text}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/embeddings",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=max(5, int(timeout_sec))) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    vec = (((data or {}).get("data") or [{}])[0]).get("embedding")
    if not isinstance(vec, list) or not vec:
        raise RuntimeError("OpenAI returned empty embedding")
    return vec


def run_backfill(db_path: Path, batch: int, model: str, timeout_sec: int, dry_run: bool = False) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_embeddings_table(conn)

        rows = list(
            conn.execute(
                """
                SELECT m.id, m.text
                FROM messages m
                LEFT JOIN embeddings e ON e.message_id = m.id
                WHERE e.message_id IS NULL
                  AND m.text IS NOT NULL
                  AND length(trim(m.text)) > 0
                ORDER BY m.id ASC
                LIMIT ?
                """,
                (max(1, int(batch)),),
            )
        )

        processed = 0
        for row in rows:
            if dry_run:
                processed += 1
                continue

            vec = fetch_embedding(str(row["text"]), model=model, timeout_sec=timeout_sec)
            conn.execute(
                "INSERT OR REPLACE INTO embeddings(message_id, model, vector_json) VALUES(?,?,?)",
                (int(row["id"]), model, json.dumps(vec, ensure_ascii=False)),
            )
            processed += 1

        if not dry_run:
            conn.commit()

        missing_after = conn.execute(
            """
            SELECT COUNT(*)
            FROM messages m
            LEFT JOIN embeddings e ON e.message_id = m.id
            WHERE e.message_id IS NULL
            """
        ).fetchone()[0]

        return {
            "selected": len(rows),
            "embedded": processed,
            "missing_after": int(missing_after),
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing embeddings in wa_archive.db")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to wa_archive.db")
    parser.add_argument("--batch", type=int, default=50, help="Max rows to embed per run")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL, help=f"Embedding model (default: {DEFAULT_MODEL})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC, help="HTTP timeout (sec)")
    parser.add_argument("--dry-run", action="store_true", help="Only count candidates, do not call OpenAI")
    args = parser.parse_args()

    stats = run_backfill(
        db_path=args.db,
        batch=args.batch,
        model=str(args.model or DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        timeout_sec=max(5, int(args.timeout)),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
