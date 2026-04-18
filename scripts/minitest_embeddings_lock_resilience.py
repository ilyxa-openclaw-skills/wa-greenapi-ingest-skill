#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import embed_missing


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "wa_archive.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE messages (
              id INTEGER PRIMARY KEY,
              text TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO messages(id, text) VALUES(?, ?)",
            [
                (1, "first row"),
                (2, "second row"),
            ],
        )
        conn.commit()
        conn.close()

        state = {"calls": 0, "side_write_ok": False}
        original_fetch = embed_missing.fetch_embedding

        def fake_fetch_embedding(text: str, model: str, timeout_sec: int) -> list[float]:
            state["calls"] += 1
            if state["calls"] == 2:
                other = sqlite3.connect(db_path, timeout=0.1)
                try:
                    other.execute("INSERT INTO messages(id, text) VALUES(?, ?)", (99, "side write"))
                    other.commit()
                finally:
                    other.close()
                state["side_write_ok"] = True
            return [float(state["calls"]), float(len(text))]

        embed_missing.fetch_embedding = fake_fetch_embedding
        try:
            stats = embed_missing.run_backfill(
                db_path=db_path,
                batch=2,
                model="fake-model",
                timeout_sec=5,
                sqlite_busy_timeout_ms=1000,
            )
        finally:
            embed_missing.fetch_embedding = original_fetch

        if not state["side_write_ok"]:
            raise SystemExit("concurrent side write failed while backfill was running")

        verify = sqlite3.connect(db_path)
        try:
            embedded_rows = verify.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
            side_row = verify.execute("SELECT text FROM messages WHERE id = 99").fetchone()
        finally:
            verify.close()

        if embedded_rows != 2:
            raise SystemExit(f"expected 2 embedded rows, got {embedded_rows}")
        if side_row != ("side write",):
            raise SystemExit(f"expected concurrent side write row, got {side_row!r}")
        if stats["embedded"] != 2 or stats["missing_after"] != 1:
            raise SystemExit(f"unexpected stats: {stats}")

    print("ok embeddings lock resilience")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
