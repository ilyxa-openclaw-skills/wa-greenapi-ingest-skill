#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path


class FakeClient:
    def __init__(self) -> None:
        self.media_url = "https://example.invalid/media"
        self._retry_stats = {
            "retries_total": 0,
            "retries_429": 0,
            "retries_5xx": 0,
            "retries_network": 0,
        }

    def get_retry_stats(self) -> dict[str, int]:
        return dict(self._retry_stats)


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-media-backfill-") as td:
        tmp_dir = Path(td)
        db_path = tmp_dir / "wa_archive.db"
        media_dir = tmp_dir / "media"
        client = FakeClient()

        history_event = {
            "chatId": "77000000001@c.us",
            "idMessage": "hist-audio-1",
            "timestamp": 1777000001,
            "typeMessage": "audioMessage",
            "fileName": "voice-note.oga",
            "mimeType": "audio/ogg; codecs=opus",
            "downloadUrl": "https://example.invalid/media/voice-note.oga",
            "ptt": True,
        }

        normalized = ingest.normalize_history_event(
            history_event,
            media_url=client.media_url,
            direction_hint="incoming",
            source_type=ingest.SOURCE_TYPE_CHAT_HISTORY,
        )
        assert normalized is not None
        ingest._enrich_media_and_transcript(
            client=client,
            row=normalized,
            media_dir=media_dir,
            transcribe_audio=True,
            transcribe_model=ingest.DEFAULT_TRANSCRIBE_MODEL,
            transcribe_language="ru",
            describe_images=False,
            describe_model=ingest.DEFAULT_DESCRIBE_MODEL,
            keep_media_files=False,
            download_media=False,
            no_analyze_docs=True,
        )

        conn = ingest.ensure_db(db_path)
        try:
            ingest._ensure_embeddings_table(conn)
            action = ingest.upsert_message(conn, normalized)
            row_id = int(
                conn.execute(
                    "SELECT id FROM messages WHERE source_message_id=? LIMIT 1",
                    ("hist-audio-1",),
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT OR REPLACE INTO embeddings(message_id, model, vector_json) VALUES(?,?,?)",
                (row_id, "text-embedding-3-small", "[0.1,0.2,0.3]"),
            )
            conn.commit()
        finally:
            conn.close()

        orig_download = ingest.download_media_file
        orig_transcribe = ingest.transcribe_with_fallback

        def fake_download_media_file(client, row, probe, media_root):
            media_root.mkdir(parents=True, exist_ok=True)
            path = media_root / "voice-note.ogg"
            path.write_bytes(b"OggSfake-opus-payload")
            return {
                "ok": True,
                "path": str(path),
                "mimeType": "audio/ogg; codecs=opus",
                "fileName": "voice-note.ogg",
            }

        def fake_transcribe(path, model, language):
            return "голосовое сообщение из истории", "fake:stt", []

        ingest.download_media_file = fake_download_media_file
        ingest.transcribe_with_fallback = fake_transcribe
        try:
            stats = ingest.reprocess_skipped_media_once(
                client=client,
                db_path=db_path,
                media_dir=media_dir,
                batch=5,
                dry_run=False,
                transcribe_audio=True,
                transcribe_model=ingest.DEFAULT_TRANSCRIBE_MODEL,
                transcribe_language="ru",
                describe_images=False,
                describe_model=ingest.DEFAULT_DESCRIBE_MODEL,
                keep_media_files=False,
                no_analyze_docs=True,
                peer="77000000001",
                audio_only=True,
            )
        finally:
            ingest.download_media_file = orig_download
            ingest.transcribe_with_fallback = orig_transcribe

        conn = sqlite3.connect(db_path)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, text, raw_json FROM messages WHERE source_message_id=? LIMIT 1",
                ("hist-audio-1",),
            ).fetchone()
            embedding_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM embeddings WHERE message_id=?",
                    (int(row["id"]),),
                ).fetchone()[0]
            )
        finally:
            conn.close()

        diag = json.loads(str(row["raw_json"]))["waArchiveIngestDiag"]

    checks = {
        "seed_row_inserted": action == "inserted",
        "audio_row_transcribed": str(row["text"]).startswith("[audio transcript] голосовое сообщение из истории"),
        "transcription_diag_ok": diag["transcription"]["ok"] is True and diag["transcription"]["engine"] == "fake:stt",
        "selected_one_candidate": stats["selected"] == 1,
        "updated_one_row": stats["updated"] == 1,
        "transcribed_one_row": stats["transcribed"] == 1,
        "remaining_after_zero": stats["remaining_after"] == 0,
        "embedding_invalidated": stats["embeddings_invalidated"] == 1 and embedding_count == 0,
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "stats": stats,
                "text": row["text"],
                "diag": diag,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
