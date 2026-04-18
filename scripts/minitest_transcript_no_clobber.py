#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


def _build_row(*, text: str, raw_json: str, source_message_id: str = "audio-1") -> dict:
    return {
        "ts": "2026-04-18T10:54:08+00:00",
        "direction": "in",
        "peer": "77772133027@c.us",
        "text": text,
        "raw_json": raw_json,
        "source_line": "",
        "source_type": "greenapi-history",
        "source_message_id": source_message_id,
    }


def _audio_diag(*, ok: bool, error: str | None = None, engine: str | None = None) -> str:
    transcription = {"ok": ok}
    if error:
        transcription["error"] = error
    if engine:
        transcription["engine"] = engine
    return json.dumps({"waArchiveIngestDiag": {"transcription": transcription}}, ensure_ascii=False)


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-no-clobber-") as td:
        db_path = Path(td) / "wa_archive.db"
        conn = ingest.ensure_db(db_path)

        initial_success = _build_row(
            text="[audio transcript] Голосовое сообщение для Данила 123.",
            raw_json=_audio_diag(ok=True, engine=ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE),
        )
        failed_retry = _build_row(
            text="[media:audio] type=audioMessage mime=audio/ogg; codecs=opus file=test.oga transcript_unavailable",
            raw_json=_audio_diag(
                ok=False,
                error="openclaw:capability-audio: No transcript returned for audio",
            ),
        )
        successful_retry = _build_row(
            text="[audio transcript] Голосовое сообщение для Данила 123. Подтверждаю повторно.",
            raw_json=_audio_diag(ok=True, engine=ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE),
            source_message_id="audio-2",
        )
        failed_initial = _build_row(
            text="[media:audio] type=audioMessage mime=audio/ogg; codecs=opus file=test2.oga transcript_unavailable",
            raw_json=_audio_diag(ok=False, error="mock fail"),
            source_message_id="audio-2",
        )

        action_insert = ingest.upsert_message(conn, initial_success)
        action_downgrade = ingest.upsert_message(conn, failed_retry)
        stored_success = conn.execute(
            "SELECT text, raw_json FROM messages WHERE source_message_id=?",
            ("audio-1",),
        ).fetchone()

        action_insert_failed = ingest.upsert_message(conn, failed_initial)
        action_upgrade = ingest.upsert_message(conn, successful_retry)
        stored_upgrade = conn.execute(
            "SELECT text, raw_json FROM messages WHERE source_message_id=?",
            ("audio-2",),
        ).fetchone()

        stored_success_raw = json.loads(stored_success[1])
        stored_upgrade_raw = json.loads(stored_upgrade[1])
        conn.close()

    checks = {
        "insert_success_row": action_insert == "inserted",
        "failed_retry_does_not_clobber": action_downgrade == "duplicate"
        and stored_success[0] == "[audio transcript] Голосовое сообщение для Данила 123."
        and stored_success_raw["waArchiveIngestDiag"]["transcription"]["ok"] is True,
        "insert_failed_row": action_insert_failed == "inserted",
        "successful_retry_replaces_failed_audio": action_upgrade == "updated"
        and stored_upgrade[0].startswith("[audio transcript] Голосовое сообщение для Данила 123.")
        and stored_upgrade_raw["waArchiveIngestDiag"]["transcription"]["ok"] is True,
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "audio_1_text": stored_success[0],
                "audio_1_diag": stored_success_raw["waArchiveIngestDiag"]["transcription"],
                "audio_2_text": stored_upgrade[0],
                "audio_2_diag": stored_upgrade_raw["waArchiveIngestDiag"]["transcription"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
