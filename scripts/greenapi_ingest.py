#!/usr/bin/env python3
"""
MVP ingest для GREEN-API -> wa_archive.db (таблица messages).

Фокус:
- получать уведомления из GREEN-API queue (receiveNotification)
- нормализовать события сообщений
- писать в совместимый формат wa_archive.db
- устойчиво переживать ошибки (лог + continue)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_DB_PATH = Path(os.getenv("WA_ARCHIVE_DB_PATH", "./wa_archive.db"))
DEFAULT_STATE_PATH = Path(os.getenv("GREENAPI_STATE_PATH", "./.greenapi_ingest_state.json"))
DEFAULT_RECEIVE_TIMEOUT = int(os.getenv("GREENAPI_RECEIVE_TIMEOUT", "5"))
DEFAULT_POLL_SLEEP = float(os.getenv("GREENAPI_POLL_SLEEP_SEC", "0.5"))


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _first_non_empty(*vals: Any) -> str:
    for v in vals:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v or "").strip().lower() in {"1", "true", "yes", "y"}


def _ts_to_iso(ts: Any) -> str:
    if ts is None or ts == "":
        return ""
    try:
        if isinstance(ts, str) and ts.strip().isdigit():
            ts = int(ts.strip())
        if isinstance(ts, (int, float)):
            return dt.datetime.fromtimestamp(float(ts), tz=dt.timezone.utc).isoformat()
    except Exception:
        pass
    # fallback: уже строка
    return str(ts)


def _load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("Не удалось прочитать state: %s", path)
    return dict(default)


def _save_json(path: Path, data: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logging.exception("Не удалось записать state: %s", path)


def ensure_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT,
          direction TEXT,
          peer TEXT,
          text TEXT,
          raw_json TEXT,
          source_line TEXT,
          source_type TEXT DEFAULT 'session_jsonl',
          source_message_id TEXT DEFAULT '',
          created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )

    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
    if "source_message_id" not in cols:
        conn.execute("ALTER TABLE messages ADD COLUMN source_message_id TEXT DEFAULT ''")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_peer ON messages(peer)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)")

    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_source_msg
            ON messages(source_type, source_message_id)
            WHERE source_message_id IS NOT NULL AND source_message_id <> ''
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_fallback
            ON messages(source_type, ts, direction, peer, text)
            WHERE source_message_id IS NULL OR source_message_id = ''
            """
        )
    except sqlite3.IntegrityError:
        # На старых БД могут быть дубликаты. Для MVP молча продолжаем.
        pass

    conn.commit()
    return conn


class GreenApiClient:
    def __init__(
        self,
        api_url: str,
        media_url: str,
        id_instance: str,
        api_token: str,
        http_timeout_sec: int = 30,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.media_url = media_url.rstrip("/")
        self.id_instance = id_instance
        self.api_token = api_token
        self.http_timeout_sec = http_timeout_sec

    def _request_json(self, method: str, url: str) -> Any:
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.http_timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace").strip()
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    logging.error("Некорректный JSON от GREENAPI: %s", raw[:300])
                    return None
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            logging.error("HTTP %s %s -> %s %s", method, url, e.code, body[:300])
            return None
        except Exception:
            logging.exception("Ошибка HTTP запроса: %s %s", method, url)
            return None

    def receive_notification(self, receive_timeout: int = 5) -> dict[str, Any] | None:
        timeout_val = max(5, min(60, int(receive_timeout)))
        query = urllib.parse.urlencode({"receiveTimeout": timeout_val})
        url = (
            f"{self.api_url}/waInstance{self.id_instance}/receiveNotification/"
            f"{self.api_token}?{query}"
        )
        data = self._request_json("GET", url)
        if isinstance(data, dict):
            return data
        return None

    def delete_notification(self, receipt_id: Any) -> bool:
        if receipt_id is None or str(receipt_id).strip() == "":
            return False
        url = (
            f"{self.api_url}/waInstance{self.id_instance}/deleteNotification/"
            f"{self.api_token}/{receipt_id}"
        )
        data = self._request_json("DELETE", url)
        return bool(isinstance(data, dict) and data.get("result") is True)


def _parse_instance_token(raw: str) -> tuple[str, str]:
    token = str(raw or "").strip()
    if not token:
        raise ValueError("GREENAPI_INSTANCE_TOKEN пустой")

    # Рекомендуемый формат: "idInstance:apiTokenInstance"
    if ":" in token:
        left, right = token.split(":", 1)
        if left.strip() and right.strip():
            return left.strip(), right.strip()

    # Дополнительно поддерживаем "idInstance/apiTokenInstance"
    if "/" in token:
        left, right = token.split("/", 1)
        if left.strip() and right.strip():
            return left.strip(), right.strip()

    raise ValueError(
        "GREENAPI_INSTANCE_TOKEN должен быть в формате idInstance:apiTokenInstance "
        "(или idInstance/apiTokenInstance)"
    )


def _extract_direction(type_webhook: str, body: dict[str, Any]) -> str:
    tw = str(type_webhook or "").strip().lower()
    if tw.startswith("incoming"):
        return "in"
    if tw.startswith("outgoing"):
        return "out"
    if _is_truthy(body.get("fromMe")):
        return "out"
    return "in"


def _extract_peer(payload: dict[str, Any], direction: str) -> str:
    sender_data = payload.get("senderData") if isinstance(payload.get("senderData"), dict) else {}
    recipient_data = payload.get("recipientData") if isinstance(payload.get("recipientData"), dict) else {}

    if direction == "out":
        peer = _first_non_empty(
            recipient_data.get("chatId"),
            recipient_data.get("recipient"),
            payload.get("chatId"),
            sender_data.get("chatId"),
            sender_data.get("sender"),
        )
    else:
        peer = _first_non_empty(
            sender_data.get("chatId"),
            sender_data.get("sender"),
            payload.get("chatId"),
            recipient_data.get("chatId"),
            recipient_data.get("recipient"),
        )

    return peer or "unknown"


def _extract_source_message_id(payload: dict[str, Any]) -> str:
    md = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}
    return _first_non_empty(
        payload.get("idMessage"),
        md.get("idMessage"),
        md.get("stanzaId"),
        md.get("id"),
    )


def _extract_text_and_media_flags(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    md = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}
    type_message = str(md.get("typeMessage") or "").strip()
    type_message_lc = type_message.lower()

    # 1) Текст
    text = _first_non_empty(
        ((md.get("textMessageData") or {}).get("textMessage") if isinstance(md.get("textMessageData"), dict) else ""),
        ((md.get("extendedTextMessageData") or {}).get("text") if isinstance(md.get("extendedTextMessageData"), dict) else ""),
        ((md.get("quotedMessage") or {}).get("stanzaBody") if isinstance(md.get("quotedMessage"), dict) else ""),
        payload.get("message"),
        payload.get("text"),
    )
    if text:
        return text, {
            "messageType": type_message or "textMessage",
            "isMedia": False,
            "isVoice": False,
            "mediaLabel": "text",
        }

    # 2) Медиа / войс placeholder (чтобы не ломать pipeline)
    audio_data = md.get("audioMessageData") if isinstance(md.get("audioMessageData"), dict) else {}
    file_data = md.get("fileMessageData") if isinstance(md.get("fileMessageData"), dict) else {}

    is_audio_like = (
        "audio" in type_message_lc
        or "voice" in type_message_lc
        or str(audio_data.get("mimeType") or "").lower().startswith("audio/")
        or str(file_data.get("mimeType") or "").lower().startswith("audio/")
    )

    is_voice = (
        _is_truthy(audio_data.get("ptt"))
        or _is_truthy(file_data.get("ptt"))
        or "voice" in type_message_lc
        or "ptt" in type_message_lc
    )

    if is_audio_like:
        # Важно для существующего voice-пайплайна: placeholder оставляем <media:audio>
        return "<media:audio>", {
            "messageType": type_message or "audioMessage",
            "isMedia": True,
            "isVoice": bool(is_voice),
            "mediaLabel": "voice" if is_voice else "audio",
        }

    media_kind = "media"
    if type_message_lc.endswith("message"):
        media_kind = type_message_lc[:-7] or "media"
    elif type_message_lc:
        media_kind = type_message_lc

    return f"<media:{media_kind}>", {
        "messageType": type_message or "unknown",
        "isMedia": True,
        "isVoice": False,
        "mediaLabel": media_kind,
    }


def normalize_notification(notification: dict[str, Any], media_url: str) -> dict[str, Any] | None:
    if not isinstance(notification, dict):
        return None

    payload = notification.get("body") if isinstance(notification.get("body"), dict) else {}
    if not payload:
        return None

    type_webhook = str(payload.get("typeWebhook") or "").strip()
    message_data = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}

    # Для MVP архивируем только message-события
    if not message_data and "message" not in type_webhook.lower():
        return None

    direction = _extract_direction(type_webhook, payload)
    peer = _extract_peer(payload, direction)
    ts = _ts_to_iso(payload.get("timestamp"))
    source_message_id = _extract_source_message_id(payload)

    text, media_meta = _extract_text_and_media_flags(payload)
    if not text:
        return None

    raw_obj = {
        "greenapi": notification,
        "waArchiveIngestDiag": {
            "pipeline": "greenapi_ingest.py",
            "source": "greenapi_queue",
            "typeWebhook": type_webhook,
            "directionInferred": direction,
            "mediaUrlConfigured": bool(media_url),
            **media_meta,
        },
    }

    source_line = json.dumps(notification, ensure_ascii=False)
    return {
        "ts": ts,
        "direction": direction,
        "peer": peer,
        "text": text,
        "raw_json": json.dumps(raw_obj, ensure_ascii=False),
        "source_line": source_line[:2000],
        "source_type": "greenapi",
        "source_message_id": source_message_id,
    }


def _should_replace_text(existing_text: str, incoming_text: str) -> bool:
    old = str(existing_text or "").strip()
    new = str(incoming_text or "").strip()
    if not new or new == old:
        return False

    old_is_placeholder = old.startswith("<media:") and old.endswith(">")
    new_is_placeholder = new.startswith("<media:") and new.endswith(">")

    if not old or old_is_placeholder:
        return True
    if new_is_placeholder:
        return False
    return len(new) >= len(old)


def upsert_message(conn: sqlite3.Connection, row: dict[str, Any]) -> str:
    source_type = row.get("source_type") or "greenapi"
    source_message_id = str(row.get("source_message_id") or "").strip()

    if source_message_id:
        existing = conn.execute(
            """
            SELECT id, ts, direction, peer, text
            FROM messages
            WHERE source_type=? AND source_message_id=?
            ORDER BY id ASC LIMIT 1
            """,
            (source_type, source_message_id),
        ).fetchone()
        if existing:
            row_id, old_ts, old_direction, old_peer, old_text = existing

            new_text = old_text
            if _should_replace_text(str(old_text or ""), str(row.get("text") or "")):
                new_text = row.get("text")

            new_ts = old_ts or row.get("ts")
            new_direction = old_direction or row.get("direction")
            new_peer = old_peer if str(old_peer or "").strip() not in {"", "unknown"} else row.get("peer")

            if (
                new_text != old_text
                or new_ts != old_ts
                or new_direction != old_direction
                or new_peer != old_peer
            ):
                conn.execute(
                    """
                    UPDATE messages
                    SET ts=?, direction=?, peer=?, text=?, raw_json=?, source_line=?
                    WHERE id=?
                    """,
                    (
                        new_ts,
                        new_direction,
                        new_peer,
                        new_text,
                        row.get("raw_json"),
                        row.get("source_line"),
                        row_id,
                    ),
                )
                return "updated"
            return "duplicate"

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO messages(
          ts, direction, peer, text, raw_json, source_line, source_type, source_message_id
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            row.get("ts"),
            row.get("direction"),
            row.get("peer"),
            row.get("text"),
            row.get("raw_json"),
            row.get("source_line"),
            source_type,
            source_message_id,
        ),
    )
    return "inserted" if cur.rowcount else "duplicate"


def ingest_once(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    state = _load_json(state_path, default={})
    result = {
        "received": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "deleted": 0,
        "errors": 0,
        "dry_run": bool(dry_run),
    }

    conn: sqlite3.Connection | None = None
    try:
        if not dry_run:
            conn = ensure_db(db_path)

        event = client.receive_notification(receive_timeout=receive_timeout)
        if not event:
            return result

        result["received"] += 1
        receipt_id = event.get("receiptId") if isinstance(event, dict) else None

        try:
            normalized = normalize_notification(event, media_url=client.media_url)
            if normalized is None:
                result["skipped"] += 1
            elif dry_run:
                result["inserted"] += 1
                logging.info(
                    "[dry-run] normalized: direction=%s peer=%s text=%s",
                    normalized.get("direction"),
                    normalized.get("peer"),
                    str(normalized.get("text") or "")[:120],
                )
            else:
                action = upsert_message(conn, normalized)
                if action == "inserted":
                    result["inserted"] += 1
                elif action == "updated":
                    result["updated"] += 1
                else:
                    result["skipped"] += 1
        except Exception:
            result["errors"] += 1
            logging.exception("Ошибка обработки уведомления")

        # В рабочем режиме всегда удаляем событие из очереди, чтобы не зациклиться на битом payload.
        if receipt_id is not None and not dry_run:
            if client.delete_notification(receipt_id):
                result["deleted"] += 1
            else:
                logging.warning("Не удалось удалить notification receiptId=%s", receipt_id)

        state["last_run_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
        state["last_receipt_id"] = event.get("receiptId") if isinstance(event, dict) else None
        _save_json(state_path, state)

        if conn is not None:
            conn.commit()

        return result
    finally:
        if conn is not None:
            conn.close()


def run_loop(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    poll_sleep_sec: float,
    dry_run: bool,
    max_iterations: int,
) -> None:
    i = 0
    while True:
        i += 1
        try:
            stats = ingest_once(
                client=client,
                db_path=db_path,
                state_path=state_path,
                receive_timeout=receive_timeout,
                dry_run=dry_run,
            )
            logging.info("cycle=%s stats=%s", i, json.dumps(stats, ensure_ascii=False))
        except KeyboardInterrupt:
            logging.info("Остановлено пользователем")
            break
        except Exception:
            logging.exception("Критическая ошибка цикла ingest (continue)")

        if max_iterations > 0 and i >= max_iterations:
            break

        if poll_sleep_sec > 0:
            time.sleep(poll_sleep_sec)


def _build_client_from_env() -> GreenApiClient:
    api_url = str(os.getenv("GREENAPI_API_URL", "")).strip()
    media_url = str(os.getenv("GREENAPI_MEDIA_URL", "")).strip()
    instance_token = str(os.getenv("GREENAPI_INSTANCE_TOKEN", "")).strip()

    if not api_url:
        raise ValueError("Не задан GREENAPI_API_URL")
    if not media_url:
        raise ValueError("Не задан GREENAPI_MEDIA_URL")
    if not instance_token:
        raise ValueError("Не задан GREENAPI_INSTANCE_TOKEN")

    id_instance, api_token = _parse_instance_token(instance_token)
    return GreenApiClient(
        api_url=api_url,
        media_url=media_url,
        id_instance=id_instance,
        api_token=api_token,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="MVP GREENAPI ingest -> wa_archive.db")
    sub = parser.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Путь до wa_archive.db")
    common.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH, help="Путь до state JSON")
    common.add_argument(
        "--receive-timeout",
        type=int,
        default=DEFAULT_RECEIVE_TIMEOUT,
        help="receiveNotification timeout (5..60)",
    )
    common.add_argument("--dry-run", action="store_true", help="Ничего не писать в БД и не удалять queue-событие")
    common.add_argument("--verbose", action="store_true", help="Подробные логи")

    sub.add_parser("ingest-once", parents=[common], help="Принять максимум 1 уведомление")

    p_run = sub.add_parser("run", parents=[common], help="Запустить непрерывный polling")
    p_run.add_argument("--poll-sleep", type=float, default=DEFAULT_POLL_SLEEP, help="Пауза между циклами")
    p_run.add_argument("--max-iterations", type=int, default=0, help="0 = бесконечно")

    args = parser.parse_args()
    _setup_logging(verbose=bool(args.verbose))

    client = _build_client_from_env()

    if args.cmd == "ingest-once":
        stats = ingest_once(
            client=client,
            db_path=args.db,
            state_path=args.state,
            receive_timeout=args.receive_timeout,
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(stats, ensure_ascii=False))
        return

    if args.cmd == "run":
        run_loop(
            client=client,
            db_path=args.db,
            state_path=args.state,
            receive_timeout=args.receive_timeout,
            poll_sleep_sec=args.poll_sleep,
            dry_run=bool(args.dry_run),
            max_iterations=int(args.max_iterations),
        )


if __name__ == "__main__":
    main()
