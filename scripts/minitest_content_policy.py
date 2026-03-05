#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


def _make_fake_pdf(path: Path, pages: int) -> None:
    chunks = [b"%PDF-1.4\n"]
    for i in range(max(1, int(pages))):
        chunks.append(f"{i + 1} 0 obj\n<< /Type /Page >>\nendobj\n".encode("utf-8"))
    chunks.append(b"%%EOF\n")
    path.write_bytes(b"".join(chunks))


def _base_row(*, msg_id: str, file_name: str, mime_type: str, label: str = "document") -> dict:
    return {
        "source_message_id": msg_id,
        "text": "<media:file>",
        "raw_obj": {"waArchiveIngestDiag": {}},
        "_payload": {
            "messageData": {
                "typeMessage": "fileMessage",
            }
        },
        "_media_meta": {
            "isMedia": True,
            "isAudio": False,
            "isImage": False,
            "isVoice": False,
            "messageType": "fileMessage",
            "mediaLabel": label,
            "mimeType": mime_type,
            "fileName": file_name,
        },
    }


def _run_pdf_case(ingest, *, pages: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-pdf-") as td:
        td_path = Path(td)
        media_dir = td_path / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        pdf_path = media_dir / f"sample-{pages}.pdf"
        _make_fake_pdf(pdf_path, pages=pages)

        row = _base_row(
            msg_id=f"minitest-pdf-{pages}",
            file_name=pdf_path.name,
            mime_type="application/pdf",
            label="document",
        )

        called = {"analyze": 0}

        orig_download = ingest.download_media_file
        orig_analyze = ingest.analyze_content_with_fallback

        def fake_download_media_file(**kwargs):
            return {
                "ok": True,
                "path": str(pdf_path),
                "size": int(pdf_path.stat().st_size),
                "sha256": "test",
                "mimeType": "application/pdf",
                "sourceUrl": "mock://sample.pdf",
                "attempts": 1,
                "sourceCandidates": ["mock://sample.pdf"],
            }

        def fake_analyze_content_with_fallback(path, mime_type, *, kind, model, backend):
            called["analyze"] += 1
            return "БЛОК 1 — SUMMARY:\nТестовый PDF\n\nБЛОК 2 — VISIBLE_TEXT:\nстрока 1\nстрока 2", "mock:pdf", []

        ingest.download_media_file = fake_download_media_file
        ingest.analyze_content_with_fallback = fake_analyze_content_with_fallback

        try:
            stats = ingest._enrich_media_and_transcript(
                client=SimpleNamespace(),
                row=row,
                media_dir=media_dir,
                transcribe_audio=False,
                transcribe_model="whisper-1",
                transcribe_language=None,
                describe_images=False,
                describe_model="gpt-4o-mini",
                keep_media_files=False,
            )
        finally:
            ingest.download_media_file = orig_download
            ingest.analyze_content_with_fallback = orig_analyze

        diag = row["raw_obj"]["waArchiveIngestDiag"].get("documentAnalysis", {})
        storage = row["raw_obj"]["waArchiveIngestDiag"].get("mediaStorage", {})

        return {
            "pages": pages,
            "text": row.get("text"),
            "diag": diag,
            "analyze_called": called["analyze"],
            "binary_deleted": bool(storage.get("binaryDeleted")),
            "path_exists": pdf_path.exists(),
            "stats": stats,
        }


def _run_text_case(ingest) -> dict:
    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-text-") as td:
        td_path = Path(td)
        media_dir = td_path / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        text_path = media_dir / "sample.txt"
        source_text = "line-1\nline-2\nline-3"
        text_path.write_text(source_text, encoding="utf-8")

        row = _base_row(
            msg_id="minitest-text",
            file_name=text_path.name,
            mime_type="text/plain",
            label="file",
        )

        orig_download = ingest.download_media_file

        def fake_download_media_file(**kwargs):
            return {
                "ok": True,
                "path": str(text_path),
                "size": int(text_path.stat().st_size),
                "sha256": "test",
                "mimeType": "text/plain",
                "sourceUrl": "mock://sample.txt",
                "attempts": 1,
                "sourceCandidates": ["mock://sample.txt"],
            }

        ingest.download_media_file = fake_download_media_file
        try:
            stats = ingest._enrich_media_and_transcript(
                client=SimpleNamespace(),
                row=row,
                media_dir=media_dir,
                transcribe_audio=False,
                transcribe_model="whisper-1",
                transcribe_language=None,
                describe_images=False,
                describe_model="gpt-4o-mini",
                keep_media_files=False,
            )
        finally:
            ingest.download_media_file = orig_download

        diag = row["raw_obj"]["waArchiveIngestDiag"].get("documentAnalysis", {})
        storage = row["raw_obj"]["waArchiveIngestDiag"].get("mediaStorage", {})

        return {
            "source_text": source_text,
            "text": row.get("text"),
            "diag": diag,
            "binary_deleted": bool(storage.get("binaryDeleted")),
            "path_exists": text_path.exists(),
            "stats": stats,
        }


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    pdf_small = _run_pdf_case(ingest, pages=2)
    pdf_large = _run_pdf_case(ingest, pages=21)
    text_case = _run_text_case(ingest)

    checks = {
        "pdf_le_20_full_processed": (
            bool(pdf_small["diag"].get("ok"))
            and pdf_small["diag"].get("kind") == "pdf"
            and pdf_small["analyze_called"] == 1
            and isinstance(pdf_small.get("text"), str)
            and "БЛОК 1" in str(pdf_small.get("text"))
        ),
        "pdf_gt_20_skipped": (
            pdf_large["diag"].get("status") == "skip"
            and pdf_large["diag"].get("reason") == "too_many_pages"
            and bool(pdf_large["diag"].get("pending_reprocess"))
            and bool(pdf_large["diag"].get("manual"))
            and pdf_large["analyze_called"] == 0
            and str(pdf_large.get("text")) == "[pdf skipped: too_many_pages]"
        ),
        "text_file_full_analyzed": (
            text_case.get("text") == text_case.get("source_text")
            and bool(text_case["diag"].get("ok"))
            and text_case["diag"].get("kind") == "text"
            and not bool(text_case["diag"].get("bytesTruncated"))
            and not bool(text_case["diag"].get("charsTruncated"))
        ),
        "cleanup_keep_0": (
            pdf_small["binary_deleted"]
            and not pdf_small["path_exists"]
            and pdf_large["binary_deleted"]
            and not pdf_large["path_exists"]
            and text_case["binary_deleted"]
            and not text_case["path_exists"]
        ),
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "pdf_small": pdf_small,
                "pdf_large": pdf_large,
                "text_case": text_case,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
