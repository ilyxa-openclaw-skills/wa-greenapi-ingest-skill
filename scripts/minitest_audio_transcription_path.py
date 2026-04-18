#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


def _dummy_audio(path: Path) -> None:
    path.write_bytes(b"RIFF....WAVEfmt ")


def _raise(message: str):
    raise RuntimeError(message)


def _run_case(
    ingest,
    audio_path: Path,
    *,
    openai_behavior,
    local_behavior,
    openclaw_behavior,
) -> dict:
    calls = {"openai": [], "local": 0, "openclaw": 0}

    orig_openai = ingest.transcribe_openai
    orig_local = ingest.transcribe_local_whisper
    orig_openclaw = ingest.transcribe_openclaw_capability

    def fake_openai(path, model, language):
        calls["openai"].append(model)
        return openai_behavior(path, model, language)

    def fake_local(path, language):
        calls["local"] += 1
        return local_behavior(path, language)

    def fake_openclaw(path, language=None, timeout_sec=None):
        calls["openclaw"] += 1
        return openclaw_behavior(path, language, timeout_sec)

    ingest.transcribe_openai = fake_openai
    ingest.transcribe_local_whisper = fake_local
    ingest.transcribe_openclaw_capability = fake_openclaw
    try:
        text, engine, errs = ingest.transcribe_with_fallback(
            audio_path,
            model=ingest.DEFAULT_TRANSCRIBE_MODEL,
            language="ru",
        )
    finally:
        ingest.transcribe_openai = orig_openai
        ingest.transcribe_local_whisper = orig_local
        ingest.transcribe_openclaw_capability = orig_openclaw

    return {"text": text, "engine": engine, "errs": errs, "calls": calls}


def _case_primary_model_success(ingest, audio_path: Path) -> dict:
    return _run_case(
        ingest,
        audio_path,
        openai_behavior=lambda path, model, language: ("test transcript", f"openai:{model}"),
        local_behavior=lambda path, language: ("local transcript", "local:whisper"),
        openclaw_behavior=lambda path, language, timeout_sec: (
            "openclaw transcript",
            ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE,
        ),
    )


def _case_openai_model_fallback_to_whisper1(ingest, audio_path: Path) -> dict:
    def openai_behavior(path, model, language):
        if model == ingest.DEFAULT_TRANSCRIBE_MODEL:
            _raise("mock model unavailable")
        if model == ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL:
            return "fallback whisper1 transcript", f"openai:{model}"
        _raise("unexpected model")

    return _run_case(
        ingest,
        audio_path,
        openai_behavior=openai_behavior,
        local_behavior=lambda path, language: ("local transcript", "local:whisper"),
        openclaw_behavior=lambda path, language, timeout_sec: (
            "openclaw transcript",
            ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE,
        ),
    )


def _case_openai_exhausted_then_local(ingest, audio_path: Path) -> dict:
    return _run_case(
        ingest,
        audio_path,
        openai_behavior=lambda path, model, language: _raise("mock openai failure"),
        local_behavior=lambda path, language: ("local transcript ok", "local:whisper"),
        openclaw_behavior=lambda path, language, timeout_sec: (
            "openclaw transcript",
            ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE,
        ),
    )


def _case_openai_and_local_exhausted_then_openclaw(ingest, audio_path: Path) -> dict:
    return _run_case(
        ingest,
        audio_path,
        openai_behavior=lambda path, model, language: _raise("mock openai failure"),
        local_behavior=lambda path, language: _raise("no local whisper executable found"),
        openclaw_behavior=lambda path, language, timeout_sec: (
            "openclaw transcript ok",
            ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE,
        ),
    )


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
        case4 = _case_openai_and_local_exhausted_then_openclaw(ingest, audio_path)

    checks = {
        "default_model_is_upgraded": ingest.DEFAULT_TRANSCRIBE_MODEL == "gpt-4o-mini-transcribe",
        "primary_model_success_short_circuit": (
            case1["calls"]["openai"] == [ingest.DEFAULT_TRANSCRIBE_MODEL]
            and case1["calls"]["local"] == 0
            and case1["calls"]["openclaw"] == 0
            and case1["engine"] == f"openai:{ingest.DEFAULT_TRANSCRIBE_MODEL}"
        ),
        "fallback_to_whisper1_before_local": (
            case2["calls"]["openai"] == [
                ingest.DEFAULT_TRANSCRIBE_MODEL,
                ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL,
            ]
            and case2["calls"]["local"] == 0
            and case2["calls"]["openclaw"] == 0
            and case2["engine"] == f"openai:{ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL}"
            and len(case2["errs"]) == 1
        ),
        "local_whisper_before_openclaw": (
            case3["calls"]["openai"] == [
                ingest.DEFAULT_TRANSCRIBE_MODEL,
                ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL,
            ]
            and case3["calls"]["local"] == 1
            and case3["calls"]["openclaw"] == 0
            and case3["engine"] == "local:whisper"
            and len(case3["errs"]) == 2
        ),
        "openclaw_after_openai_and_local": (
            case4["calls"]["openai"] == [
                ingest.DEFAULT_TRANSCRIBE_MODEL,
                ingest.OPENAI_TRANSCRIBE_FALLBACK_MODEL,
            ]
            and case4["calls"]["local"] == 1
            and case4["calls"]["openclaw"] == 1
            and case4["engine"] == ingest.OPENCLAW_AUDIO_TRANSCRIBE_ENGINE
            and len(case4["errs"]) == 3
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
                "case4": case4,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
