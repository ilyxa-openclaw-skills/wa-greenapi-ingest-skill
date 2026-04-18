#!/usr/bin/env python3
from __future__ import annotations

import json
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

    def list_chats(self) -> list[dict[str, str]]:
        return [{"chatId": "77000000001@c.us"}]

    def fetch_history_messages(self) -> list[dict[str, object]]:
        return []

    def get_chat_history(
        self,
        chat_id: str,
        count: int,
        id_message: str | None = None,
        use_id_message: bool = False,
    ) -> list[dict[str, object]]:
        if not use_id_message:
            return [
                {
                    "chatId": chat_id,
                    "idMessage": "msg-4",
                    "timestamp": 1776000004,
                    "typeMessage": "textMessage",
                    "textMessage": "fourth row",
                },
                {
                    "chatId": chat_id,
                    "idMessage": "msg-3",
                    "timestamp": 1776000003,
                    "typeMessage": "textMessage",
                    "textMessage": "third row",
                },
            ]
        if id_message == "msg-3":
            return [
                {
                    "chatId": chat_id,
                    "idMessage": "msg-2",
                    "timestamp": 1776000002,
                    "typeMessage": "textMessage",
                    "textMessage": "second row",
                },
                {
                    "chatId": chat_id,
                    "idMessage": "msg-1",
                    "timestamp": 1776000001,
                    "typeMessage": "textMessage",
                    "textMessage": "first row",
                },
            ]
        raise RuntimeError(f"unexpected cursor: use_id_message={use_id_message} id_message={id_message}")

    def get_retry_stats(self) -> dict[str, int]:
        return dict(self._retry_stats)


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-batch-limit-") as td:
        tmp_dir = Path(td)
        db_path = tmp_dir / "wa_archive.db"
        state_path = tmp_dir / ".greenapi_ingest_state.json"
        media_dir = tmp_dir / "media"

        stats = ingest.ingest_full_history_once(
            client=FakeClient(),
            db_path=db_path,
            state_path=state_path,
            media_dir=media_dir,
            dry_run=True,
            transcribe_audio=False,
            describe_images=False,
            keep_media_files=False,
            download_media=False,
            no_analyze_docs=True,
            history_batch_size=2,
            max_chats=1,
            max_messages=50,
            max_batches_per_chat=2,
            refresh_chat_list=True,
            chat_history_pagination="auto",
        )

        state = json.loads(state_path.read_text(encoding="utf-8"))
        full = state["full_history"]
        completed = full.get("completed_chats") or []
        problematic = full.get("problematic_chats") or {}
        current_idx = full.get("current_chat_index")

    checks = {
        "single_chat_processed": stats["chats_processed"] == 1,
        "batch_limit_does_not_mark_completed": completed == [],
        "batch_limit_keeps_chat_pending": stats["remaining_chats_total"] == 1,
        "problematic_reason_recorded": problematic.get("77000000001@c.us", {}).get("reason") == "max_batches_per_chat_reached",
        "current_index_stays_on_same_chat": current_idx == 0,
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "stats": stats,
                "full_state": full,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
