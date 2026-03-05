#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


def _base_row(*, msg_id: str) -> dict:
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
            "mediaLabel": "document",
            "mimeType": "application/vnd.ms-excel",
            "fileName": "legacy.xls",
        },
    }


def _run_case(ingest, *, fallback_should_fail: bool) -> dict:
    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-xls-fallback-") as td:
        td_path = Path(td)
        media_dir = td_path / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        xls_path = media_dir / "legacy.xls"
        xls_path.write_bytes(b"D0CF11E0-mock-legacy-xls-content")

        row = _base_row(msg_id=f"minitest-xls-fallback-{'fail' if fallback_should_fail else 'ok'}")

        calls = {"extract": 0, "fallback": 0}

        orig_download = ingest.download_media_file
        orig_extract = ingest._extract_office_text
        orig_fallback = ingest.analyze_document_via_openclaw

        def fake_download_media_file(**kwargs):
            return {
                "ok": True,
                "path": str(xls_path),
                "size": int(xls_path.stat().st_size),
                "sha256": "test",
                "mimeType": "application/vnd.ms-excel",
                "sourceUrl": "mock://legacy.xls",
                "attempts": 1,
                "sourceCandidates": ["mock://legacy.xls"],
            }

        def fake_extract_office_text(path, *, min_chars):
            calls["extract"] += 1
            return {
                "ok": False,
                "text": "",
                "engine": "",
                "format": ".xls",
                "bytes": int(Path(path).stat().st_size),
                "usedBytes": 0,
                "errors": ["legacy .xls text extractor is unavailable"],
                "error": "legacy .xls text extractor is unavailable",
                "extractedChars": 0,
                "minCharsThreshold": int(min_chars),
                "heuristicApplied": False,
                "enginesTried": [],
            }

        def fake_analyze_document_via_openclaw(path, mime_type="", *, kind="document", gateway_url=None, timeout_sec=None):
            calls["fallback"] += 1
            if fallback_should_fail:
                raise RuntimeError("mock openclaw gateway failure")
            return (
                "БЛОК 1 — SUMMARY:\nLegacy XLS extracted via gateway\n\nБЛОК 2 — VISIBLE_TEXT:\nЯчейка A1: test",
                "mock:openclaw-gateway",
            )

        ingest.download_media_file = fake_download_media_file
        ingest._extract_office_text = fake_extract_office_text
        ingest.analyze_document_via_openclaw = fake_analyze_document_via_openclaw

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
            ingest._extract_office_text = orig_extract
            ingest.analyze_document_via_openclaw = orig_fallback

        diag_root = row["raw_obj"]["waArchiveIngestDiag"]
        diag_doc = diag_root.get("documentAnalysis", {})
        storage = diag_root.get("mediaStorage", {})

        return {
            "text": row.get("text"),
            "diag_root": diag_root,
            "diag_doc": diag_doc,
            "stats": stats,
            "calls": calls,
            "binary_deleted": bool(storage.get("binaryDeleted")),
            "path_exists": xls_path.exists(),
        }


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    success_case = _run_case(ingest, fallback_should_fail=False)
    fail_case = _run_case(ingest, fallback_should_fail=True)

    checks = {
        "fallback_success_route_triggered": (
            success_case["calls"]["extract"] == 1
            and success_case["calls"]["fallback"] == 1
            and success_case["diag_doc"].get("office_fallback") == "openclaw_vision"
            and success_case["diag_root"].get("office_fallback") == "openclaw_vision"
            and bool(success_case["diag_doc"].get("ok"))
            and str(success_case.get("text") or "") != "[office extraction failed]"
            and "БЛОК 1" in str(success_case.get("text") or "")
        ),
        "fallback_failure_sets_marker_only_after_attempt": (
            fail_case["calls"]["extract"] == 1
            and fail_case["calls"]["fallback"] == 1
            and not bool(fail_case["diag_doc"].get("ok"))
            and fail_case["diag_doc"].get("office_fallback") == "openclaw_vision"
            and bool(str(fail_case["diag_doc"].get("office_fallback_error") or "").strip())
            and str(fail_case.get("text") or "") == "[office extraction failed]"
        ),
        "cleanup_keep_0": (
            success_case["binary_deleted"]
            and not success_case["path_exists"]
            and fail_case["binary_deleted"]
            and not fail_case["path_exists"]
        ),
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "success_case": success_case,
                "fail_case": fail_case,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
