#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


class FakeClient:
    def __init__(self) -> None:
        self._retry_stats = {
            "retries_total": 0,
            "retries_429": 0,
            "retries_5xx": 0,
            "retries_network": 0,
        }

    def list_chats(self) -> list[dict[str, str]]:
        return [
            {"chatId": "77000000001@c.us"},
            {"chatId": "77000000002@c.us"},
            {"chatId": "77000000003@c.us"},
            {"chatId": "77000000004@c.us"},
            {"chatId": "77000000005@c.us"},
        ]

    def fetch_history_messages(self) -> list[dict[str, object]]:
        return []

    def get_chat_history(
        self,
        chat_id: str,
        count: int,
        id_message: str | None = None,
        use_id_message: bool = False,
    ) -> list[dict[str, object]]:
        return []

    def get_retry_stats(self) -> dict[str, int]:
        return dict(self._retry_stats)


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-full-history-") as td:
        tmp_dir = Path(td)
        db_path = tmp_dir / "wa_archive.db"
        state_path = tmp_dir / ".greenapi_ingest_state.json"
        media_dir = tmp_dir / "media"

        conn = ingest.ensure_db(db_path)
        conn.execute(
            "INSERT INTO messages(ts, direction, peer, text, raw_json, source_line, source_type, source_message_id) VALUES(?,?,?,?,?,?,?,?)",
            (
                "2026-04-18T00:00:00+00:00",
                "in",
                "77009999999@c.us",
                "seed row",
                "{}",
                "",
                "greenapi-history",
                "seed-1",
            ),
        )
        conn.commit()
        conn.close()

        stats = ingest.ingest_full_history_once(
            client=FakeClient(),
            db_path=db_path,
            state_path=state_path,
            media_dir=media_dir,
            dry_run=False,
            transcribe_audio=False,
            describe_images=False,
            keep_media_files=False,
            download_media=False,
            no_analyze_docs=True,
            history_batch_size=50,
            max_chats=2,
            max_messages=200,
            max_batches_per_chat=2,
            refresh_chat_list=True,
            chat_history_pagination="auto",
        )

        state = json.loads(state_path.read_text(encoding="utf-8"))
        full = state["full_history"]
        chat_order = full["chat_order"]
        diag = full["diag"]

    checks = {
        "full_chat_order_not_truncated_to_processing_slice": len(chat_order) == 6,
        "stats_report_full_chat_order": stats["chat_order_total"] == 6,
        "only_two_chats_processed_this_run": stats["chats_processed"] == 2,
        "remaining_chats_are_visible": stats["remaining_chats_total"] == 4,
        "coverage_missing_chats_reported": stats["coverage_missing_chats_before"] == 5,
        "state_diag_keeps_coverage_counters": (
            diag["chatOrderTotal"] == 6
            and diag["coverageMissingChatsBefore"] == 5
            and diag["coverageMissingChatsNow"] == 5
        ),
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "stats": stats,
                "chat_order": chat_order,
                "diag": diag,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
