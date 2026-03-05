#!/usr/bin/env python3
"""Mini self-check for history direction mapping in greenapi_ingest.py."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    script_path = Path(__file__).with_name("greenapi_ingest.py")
    spec = importlib.util.spec_from_file_location("greenapi_ingest", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    mod = _load_module()

    direction_cases = [
        {
            "name": "hint_out_overrides_incoming_type",
            "event": {"type": "incomingMessageReceived"},
            "hint": "out",
            "expected": "out",
        },
        {
            "name": "hint_in_overrides_outgoing_type",
            "event": {"type": "outgoingMessageReceived"},
            "hint": "in",
            "expected": "in",
        },
        {
            "name": "type_outgoing",
            "event": {"type": "outgoing"},
            "hint": None,
            "expected": "out",
        },
        {
            "name": "typeWebhook_incoming",
            "event": {"typeWebhook": "incomingMessageReceived"},
            "hint": None,
            "expected": "in",
        },
        {
            "name": "direction_sent",
            "event": {"direction": "sent"},
            "hint": None,
            "expected": "out",
        },
        {
            "name": "fromMe_true",
            "event": {"fromMe": True},
            "hint": None,
            "expected": "out",
        },
        {
            "name": "nested_messageData_typeWebhook",
            "event": {"messageData": {"typeWebhook": "outgoingMessageReceived"}},
            "hint": None,
            "expected": "out",
        },
    ]

    failures: list[dict[str, str]] = []
    for case in direction_cases:
        got = mod._history_direction(case["event"], direction_hint=case["hint"])  # noqa: SLF001
        if got != case["expected"]:
            failures.append({
                "name": case["name"],
                "expected": case["expected"],
                "got": str(got),
            })

    synthetic_text = {
        "idMessage": "SYNTH-TEXT-OUT",
        "type": "incomingMessageReceived",
        "chatId": "77772133027@c.us",
        "typeMessage": "textMessage",
        "textMessage": "synthetic text",
        "timestamp": 1700000000,
    }
    normalized_text = mod.normalize_history_event(synthetic_text, media_url="https://example.test", direction_hint="out")
    if not normalized_text or normalized_text.get("direction") != "out":
        failures.append({
            "name": "normalize_history_text_out",
            "expected": "out",
            "got": str((normalized_text or {}).get("direction")),
        })

    synthetic_audio = {
        "idMessage": "SYNTH-AUDIO-OUT",
        "type": "incomingMessageReceived",
        "chatId": "77772133027@c.us",
        "typeMessage": "audioMessage",
        "mimeType": "audio/ogg",
        "downloadUrl": "https://example.test/audio.ogg",
        "timestamp": 1700000001,
    }
    normalized_audio = mod.normalize_history_event(synthetic_audio, media_url="https://example.test", direction_hint="out")
    if not normalized_audio or normalized_audio.get("direction") != "out":
        failures.append({
            "name": "normalize_history_audio_out",
            "expected": "out",
            "got": str((normalized_audio or {}).get("direction")),
        })

    summary = {
        "ok": not failures,
        "cases": len(direction_cases) + 2,
        "failures": failures,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
