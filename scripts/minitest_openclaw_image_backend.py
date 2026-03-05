#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


def _make_tiny_png(path: Path) -> None:
    tiny_png_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO3Z6fQAAAAASUVORK5CYII="
    path.write_bytes(base64.b64decode(tiny_png_b64))


def _base_row(tag: str) -> dict:
    return {
        "source_message_id": f"minitest-{tag}",
        "text": "<media:image>",
        "raw_obj": {"waArchiveIngestDiag": {}},
        "_payload": {
            "messageData": {
                "typeMessage": "imageMessage",
            }
        },
        "_media_meta": {
            "isMedia": True,
            "isAudio": False,
            "isImage": True,
            "isVoice": False,
            "messageType": "imageMessage",
            "mediaLabel": "image",
            "mimeType": "image/png",
            "fileName": "tiny.png",
        },
    }


def _run_case(ingest, *, should_describe_ok: bool) -> dict:
    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-") as td:
        td_path = Path(td)
        media_dir = td_path / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        img_path = media_dir / "tiny.png"
        _make_tiny_png(img_path)

        row = _base_row("ok" if should_describe_ok else "fail")

        orig_download = ingest.download_media_file
        orig_describe = ingest.describe_image_with_fallback

        def fake_download_media_file(**kwargs):
            return {
                "ok": True,
                "path": str(img_path),
                "size": int(img_path.stat().st_size),
                "sha256": "test",
                "mimeType": "image/png",
                "sourceUrl": "mock://tiny.png",
                "attempts": 1,
                "sourceCandidates": ["mock://tiny.png"],
            }

        def fake_describe_image_with_fallback(path, mime_type, model, backend):
            if should_describe_ok:
                return "на изображении тестовый пиксель", "mock:image", []
            raise RuntimeError("mock describe backend failure")

        ingest.download_media_file = fake_download_media_file
        ingest.describe_image_with_fallback = fake_describe_image_with_fallback
        try:
            stats = ingest._enrich_media_and_transcript(
                client=SimpleNamespace(),
                row=row,
                media_dir=media_dir,
                transcribe_audio=False,
                transcribe_model="whisper-1",
                transcribe_language=None,
                describe_images=True,
                describe_model="gpt-4o-mini",
                keep_media_files=False,
            )
        finally:
            ingest.download_media_file = orig_download
            ingest.describe_image_with_fallback = orig_describe

        diag = row["raw_obj"]["waArchiveIngestDiag"]["imageDescription"]
        storage = row["raw_obj"]["waArchiveIngestDiag"]["mediaStorage"]

        return {
            "text": row.get("text"),
            "pending_reprocess": bool(diag.get("pending_reprocess")),
            "binary_deleted": bool(storage.get("binaryDeleted")),
            "path_exists": img_path.exists(),
            "stats": stats,
        }


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    success_case = _run_case(ingest, should_describe_ok=True)
    failed_case = _run_case(ingest, should_describe_ok=False)

    checks = {
        "success_has_plain_description": success_case["text"] == "на изображении тестовый пиксель",
        "success_not_pending_reprocess": success_case["pending_reprocess"] is False,
        "success_cleanup_done": success_case["binary_deleted"] is True and success_case["path_exists"] is False,
        "fail_has_marker_only": failed_case["text"] == "[image description failed]",
        "fail_pending_reprocess_true": failed_case["pending_reprocess"] is True,
        "fail_cleanup_done": failed_case["binary_deleted"] is True and failed_case["path_exists"] is False,
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "success_case": success_case,
                "failed_case": failed_case,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
