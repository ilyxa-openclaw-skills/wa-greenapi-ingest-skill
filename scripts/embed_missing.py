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
DEFAULT_SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("WA_EMBED_SQLITE_BUSY_TIMEOUT_MS", "30000"))


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


def connect_db(db_path: Path, busy_timeout_ms: int) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=max(1.0, busy_timeout_ms / 1000.0))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {max(1000, int(busy_timeout_ms))}")
    return conn


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


def run_backfill(
    db_path: Path,
    batch: int,
    model: str,
    timeout_sec: int,
    dry_run: bool = False,
    sqlite_busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
) -> dict[str, int]:
    conn = connect_db(db_path, sqlite_busy_timeout_ms)
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
            # Keep the SQLite write transaction short so ingest/enrich can continue
            # writing while we wait on the embedding provider for the next row.
            conn.execute(
                "INSERT OR REPLACE INTO embeddings(message_id, model, vector_json) VALUES(?,?,?)",
                (int(row["id"]), model, json.dumps(vec, ensure_ascii=False)),
            )
            conn.commit()
            processed += 1

        missing_after = conn.execute(
            """
            SELECT COUNT(*)
            FROM messages m
            LEFT JOIN embeddings e ON e.message_id = m.id
            WHERE e.message_id IS NULL
              AND m.text IS NOT NULL
              AND length(trim(m.text)) > 0
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
    parser.add_argument(
        "--sqlite-busy-timeout-ms",
        type=int,
        default=DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
        help=f"SQLite busy timeout in ms (default: {DEFAULT_SQLITE_BUSY_TIMEOUT_MS})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only count candidates, do not call OpenAI")
    args = parser.parse_args()

    stats = run_backfill(
        db_path=args.db,
        batch=args.batch,
        model=str(args.model or DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        timeout_sec=max(5, int(args.timeout)),
        dry_run=bool(args.dry_run),
        sqlite_busy_timeout_ms=max(1000, int(args.sqlite_busy_timeout_ms)),
    )
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
