#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


def _dummy_audio(path: Path) -> None:
    # Для unit-style теста содержимое не важно: transcribe функции будут замоканы.
    path.write_bytes(b"RIFF....WAVEfmt ")


def _case_primary_model_success(ingest, audio_path: Path) -> dict:
    calls = {"openai": [], "local": 0}

    orig_openai = ingest.transcribe_openai
    orig_local = ingest.transcribe_local_whisper

    def fake_openai(path, model, language):
        calls["openai"].append(model)
        return "тест транскрипта", f"openai:{model}"

    def fake_local(path, language):
        calls["local"] += 1
        return "локальный транскрипт", "local:whisper"

    ingest.transcribe_openai = fake_openai
    ingest.transcribe_local_whisper = fake_local
    try:
        text, engine, errs = ingest.transcribe_with_fallback(
            audio_path,
            model=ingest.DEFAULT_TRANSCRIBE_MODEL,
            language="ru",
        )
    finally:
        ingest.transcribe_openai = orig_openai
        ingest.transcribe_local_whisper = orig_local

    return {"text": text, "engine": engine, "errs": errs, "calls": calls}


def _case_openai_model_fallback_to_whisper1(ingest, audio_path: Path) -> dict:
    calls = {"openai": [], "local": 0}

    orig_openai = ingest.transcribe_openai
    orig_local = ingest.transcribe_local_whisper

    def fake_openai(path, model, language):
        calls["openai"].append(model)
        if model == ingest.DEFAULT_TRANSCRIBE_MODEL:
            raise RuntimeError("mock model unavailable")
        if model == ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL:
            return "fallback whisper1 transcript", f"openai:{model}"
        raise RuntimeError("unexpected model")

    def fake_local(path, language):
        calls["local"] += 1
        return "локальный транскрипт", "local:whisper"

    ingest.transcribe_openai = fake_openai
    ingest.transcribe_local_whisper = fake_local
    try:
        text, engine, errs = ingest.transcribe_with_fallback(
            audio_path,
            model=ingest.DEFAULT_TRANSCRIBE_MODEL,
            language="ru",
        )
    finally:
        ingest.transcribe_openai = orig_openai
        ingest.transcribe_local_whisper = orig_local

    return {"text": text, "engine": engine, "errs": errs, "calls": calls}


def _case_openai_exhausted_then_local(ingest, audio_path: Path) -> dict:
    calls = {"openai": [], "local": 0}

    orig_openai = ingest.transcribe_openai
    orig_local = ingest.transcribe_local_whisper

    def fake_openai(path, model, language):
        calls["openai"].append(model)
        raise RuntimeError("mock openai failure")

    def fake_local(path, language):
        calls["local"] += 1
        return "local transcript ok", "local:whisper"

    ingest.transcribe_openai = fake_openai
    ingest.transcribe_local_whisper = fake_local
    try:
        text, engine, errs = ingest.transcribe_with_fallback(
            audio_path,
            model=ingest.DEFAULT_TRANSCRIBE_MODEL,
            language="ru",
        )
    finally:
        ingest.transcribe_openai = orig_openai
        ingest.transcribe_local_whisper = orig_local

    return {"text": text, "engine": engine, "errs": errs, "calls": calls}


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-audio-") as td:
        tmp_dir = Path(td)
        audio_path = tmp_dir / "sample.ogg"
        _dummy_audio(audio_path)

        case1 = _case_primary_model_success(ingest, audio_path)
        case2 = _case_openai_model_fallback_to_whisper1(ingest, audio_path)
        case3 = _case_openai_exhausted_then_local(ingest, audio_path)

    checks = {
        "default_model_is_upgraded": ingest.DEFAULT_TRANSCRIBE_MODEL == "gpt-4o-mini-transcribe",
        "primary_model_success_short_circuit": (
            case1["calls"]["openai"] == [ingest.DEFAULT_TRANSCRIBE_MODEL]
            and case1["calls"]["local"] == 0
            and case1["engine"] == f"openai:{ingest.DEFAULT_TRANSCRIBE_MODEL}"
        ),
        "fallback_to_whisper1_before_local": (
            case2["calls"]["openai"] == [
                ingest.DEFAULT_TRANSCRIBE_MODEL,
                ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL,
            ]
            and case2["calls"]["local"] == 0
            and case2["engine"] == f"openai:{ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL}"
            and len(case2["errs"]) == 1
        ),
        "local_whisper_as_last_resort": (
            case3["calls"]["openai"] == [
                ingest.DEFAULT_TRANSCRIBE_MODEL,
                ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL,
            ]
            and case3["calls"]["local"] == 1
            and case3["engine"] == "local:whisper"
            and len(case3["errs"]) == 2
        ),
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
