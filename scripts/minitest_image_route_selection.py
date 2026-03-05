#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import sys
import tempfile
from pathlib import Path


def _make_tiny_png(path: Path) -> None:
    tiny_png_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO3Z6fQAAAAASUVORK5CYII="
    path.write_bytes(base64.b64decode(tiny_png_b64))


def _case_path_priority(ingest, image_path: Path) -> dict:
    calls = {"path": 0, "image_url": 0}

    orig_path = ingest._analyze_file_via_openclaw_path
    orig_chat = ingest._openclaw_chat_completions_text

    def fake_path(*args, **kwargs):
        calls["path"] += 1
        return "БЛОК 1 — SUMMARY:\npath ok\n\nБЛОК 2 — VISIBLE_TEXT:\nhello", "mock:path"

    def fake_chat(*args, **kwargs):
        calls["image_url"] += 1
        return "БЛОК 1 — SUMMARY:\nimage_url ok\n\nБЛОК 2 — VISIBLE_TEXT:\nhello", "mock-auth"

    ingest._analyze_file_via_openclaw_path = fake_path
    ingest._openclaw_chat_completions_text = fake_chat
    try:
        text, engine = ingest.describe_image_via_openclaw(image_path, mime_type="image/png")
    finally:
        ingest._analyze_file_via_openclaw_path = orig_path
        ingest._openclaw_chat_completions_text = orig_chat

    return {
        "text": text,
        "engine": engine,
        "calls": calls,
    }


def _case_image_url_degraded_retry_path(ingest, image_path: Path) -> dict:
    calls = {"path": 0, "image_url": 0}

    orig_path = ingest._analyze_file_via_openclaw_path
    orig_chat = ingest._openclaw_chat_completions_text

    def fake_path(*args, **kwargs):
        calls["path"] += 1
        if calls["path"] == 1:
            raise RuntimeError("primary path transient failure")
        return "БЛОК 1 — SUMMARY:\npath retry ok\n\nБЛОК 2 — VISIBLE_TEXT:\nretry text", "mock:path-retry"

    def fake_chat(*args, **kwargs):
        calls["image_url"] += 1
        return "БЛОК 1 — SUMMARY:\nвложение недоступно\n\nБЛОК 2 — VISIBLE_TEXT:\n[текст не обнаружен]", "mock-auth"

    ingest._analyze_file_via_openclaw_path = fake_path
    ingest._openclaw_chat_completions_text = fake_chat
    try:
        text, engine = ingest.describe_image_via_openclaw(image_path, mime_type="image/png")
    finally:
        ingest._analyze_file_via_openclaw_path = orig_path
        ingest._openclaw_chat_completions_text = orig_chat

    return {
        "text": text,
        "engine": engine,
        "calls": calls,
    }


def _case_conflict_prefers_path(ingest) -> dict:
    path_candidate = ingest._build_image_analysis_candidate(
        text="БЛОК 1 — SUMMARY:\npath summary\n\nБЛОК 2 — VISIBLE_TEXT:\npath text",
        engine="mock:path",
        route="path",
    )
    image_url_candidate = ingest._build_image_analysis_candidate(
        text="БЛОК 1 — SUMMARY:\nimage_url summary\n\nБЛОК 2 — VISIBLE_TEXT:\nimage text",
        engine="mock:image_url",
        route="image_url",
    )
    chosen = ingest._select_best_image_analysis_candidate(path_candidate, image_url_candidate)
    return {
        "chosen_route": chosen.get("route") if isinstance(chosen, dict) else None,
        "chosen_engine": chosen.get("engine") if isinstance(chosen, dict) else None,
    }


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-img-route-") as td:
        tmp_dir = Path(td)
        image_path = tmp_dir / "tiny.png"
        _make_tiny_png(image_path)

        case1 = _case_path_priority(ingest, image_path)
        case2 = _case_image_url_degraded_retry_path(ingest, image_path)
        case3 = _case_conflict_prefers_path(ingest)

    checks = {
        "path_primary_no_image_url_when_path_ok": (
            case1["calls"]["path"] == 1
            and case1["calls"]["image_url"] == 0
            and "path" in str(case1["engine"])
        ),
        "image_url_degraded_triggers_path_retry": (
            case2["calls"]["path"] == 2
            and case2["calls"]["image_url"] == 1
            and "path" in str(case2["engine"])
            and "retry" in str(case2["text"]).lower()
        ),
        "conflict_prefers_path": case3["chosen_route"] == "path",
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "case1": case1,
                "case2": case2,
                "case3": case3,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
