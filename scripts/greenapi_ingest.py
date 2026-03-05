#!/usr/bin/env python3
"""
Phase 2 ingest для GREEN-API -> wa_archive.db (таблица messages).

Что добавлено поверх MVP:
- media download (GREENAPI_MEDIA_URL + robust fallback по URL/path/endpoint)
- voice/audio транскрипция (OpenAI Whisper first, local whisper fallback)
- безопасные режимы тестового запуска (--max-events, --since, --dry-run)
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import logging
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
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
DEFAULT_MEDIA_DIR = Path(os.getenv("GREENAPI_MEDIA_STORE_DIR", "./data/media"))
DEFAULT_KEEP_MEDIA_FILES = str(os.getenv("GREENAPI_KEEP_MEDIA_FILES", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
DEFAULT_DESCRIBE_IMAGES = str(os.getenv("GREENAPI_DESCRIBE_IMAGES", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
DEFAULT_TRANSCRIBE_AUDIO = str(os.getenv("GREENAPI_TRANSCRIBE_AUDIO", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
DEFAULT_DESCRIBE_MODEL = str(os.getenv("GREENAPI_DESCRIBE_MODEL", "gpt-4o-mini")).strip() or "gpt-4o-mini"
DEFAULT_IMAGE_DESCRIBE_BACKEND = str(os.getenv("GREENAPI_IMAGE_DESCRIBE_BACKEND", "auto")).strip().lower() or "auto"
DEFAULT_OPENCLAW_GATEWAY_URL = str(os.getenv("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")).strip() or "http://127.0.0.1:18789"
DEFAULT_OPENCLAW_GATEWAY_PASSWORD = str(os.getenv("OPENCLAW_GATEWAY_PASSWORD", "")).strip()
DEFAULT_OPENCLAW_GATEWAY_TOKEN = str(os.getenv("OPENCLAW_GATEWAY_TOKEN", "")).strip()
DEFAULT_OPENCLAW_DESCRIBE_TIMEOUT = int(os.getenv("OPENCLAW_IMAGE_DESCRIBE_TIMEOUT_SEC", "120"))
DEFAULT_TRANSCRIBE_MODEL = str(os.getenv("GREENAPI_TRANSCRIBE_MODEL", "whisper-1")).strip() or "whisper-1"
DEFAULT_TRANSCRIBE_LANGUAGE = str(os.getenv("GREENAPI_TRANSCRIBE_LANGUAGE", "")).strip() or None
DEFAULT_HTTP_TIMEOUT = int(os.getenv("GREENAPI_HTTP_TIMEOUT_SEC", "45"))

SOURCE_QUEUE = "queue"
SOURCE_HISTORY = "history"
SOURCE_AUTO = "auto"

SOURCE_TYPE_QUEUE = "greenapi"
SOURCE_TYPE_HISTORY = "greenapi-history"

URL_KEY_CANDIDATES = {
    "downloadurl",
    "urlfile",
    "fileurl",
    "mediaurl",
    "url",
    "filepath",
    "downloadpath",
    "directpath",
    "path",
}

MEDIA_BLOCK_KEYS = (
    "audioMessageData",
    "fileMessageData",
    "imageMessageData",
    "videoMessageData",
    "documentMessageData",
    "stickerMessageData",
    "extendedTextMessageData",
)


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


def _safe_json_loads(raw: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return None


def _ts_to_iso(ts: Any) -> str:
    if ts is None or ts == "":
        return ""
    try:
        if isinstance(ts, str) and ts.strip().isdigit():
            ts = int(ts.strip())
        if isinstance(ts, (int, float)):
            t = float(ts)
            # millis support
            if t > 1e12:
                t = t / 1000.0
            return dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).isoformat()
    except Exception:
        pass
    return str(ts)


def _iso_to_epoch(iso_or_any: Any) -> float | None:
    s = str(iso_or_any or "").strip()
    if not s:
        return None
    if s.isdigit():
        t = float(s)
        return t / 1000.0 if t > 1e12 else t
    try:
        z = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(z).timestamp()
    except Exception:
        return None


def _sanitize_for_filename(v: str, fallback: str = "file") -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(v or "").strip())
    s = s.strip("._-")
    return s[:120] if s else fallback


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


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
        http_timeout_sec: int = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.media_url = media_url.rstrip("/")
        self.id_instance = id_instance
        self.api_token = api_token
        self.http_timeout_sec = int(http_timeout_sec)

    def _request_json(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
        quiet_errors: bool = False,
    ) -> Any:
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, headers=headers, data=data, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.http_timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace").strip()
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    if not quiet_errors:
                        logging.error("Некорректный JSON от GREENAPI: %s", raw[:300])
                    return None
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            if not quiet_errors:
                logging.error("HTTP %s %s -> %s %s", method, url, e.code, body[:300])
            return None
        except Exception:
            if not quiet_errors:
                logging.exception("Ошибка HTTP JSON запроса: %s %s", method, url)
            return None

    def _request_bytes(self, method: str, url: str, quiet_errors: bool = False) -> tuple[bytes, str] | None:
        req = urllib.request.Request(url, headers={"Accept": "*/*"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.http_timeout_sec) as resp:
                ctype = str(resp.headers.get("Content-Type") or "").strip()
                return resp.read(), ctype
        except urllib.error.HTTPError as e:
            if not quiet_errors:
                body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
                logging.error("HTTP %s %s -> %s %s", method, url, e.code, body[:300])
            return None
        except Exception:
            if not quiet_errors:
                logging.exception("Ошибка HTTP bytes запроса: %s %s", method, url)
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

    def request_download_url_via_api(self, chat_id: str, id_message: str) -> list[str]:
        """Fallback: спросить downloadFile у API и вытащить URL/path из ответа."""
        if not chat_id or not id_message:
            return []
        url = f"{self.api_url}/waInstance{self.id_instance}/downloadFile/{self.api_token}"
        payload = {"chatId": chat_id, "idMessage": id_message}
        data = self._request_json("POST", url, payload=payload, quiet_errors=True)
        if data is None:
            return []
        return _collect_candidate_refs(data)

    def _extract_history_items(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in ("messages", "data", "items", "result", "list"):
                v = data.get(key)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
            if any(k in data for k in ("idMessage", "timestamp", "typeMessage", "chatId")):
                return [data]
        return []

    def last_outgoing_messages(self) -> list[dict[str, Any]]:
        url = f"{self.api_url}/waInstance{self.id_instance}/lastOutgoingMessages/{self.api_token}"
        data = self._request_json("GET", url, quiet_errors=True)
        return self._extract_history_items(data)

    def last_incoming_messages(self) -> list[dict[str, Any]]:
        url = f"{self.api_url}/waInstance{self.id_instance}/lastIncomingMessages/{self.api_token}"
        data = self._request_json("GET", url, quiet_errors=True)
        return self._extract_history_items(data)

    def fetch_history_messages(self) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        for item in self.last_outgoing_messages():
            merged.append({"direction_hint": "out", "event": item})
        for item in self.last_incoming_messages():
            merged.append({"direction_hint": "in", "event": item})
        return merged


class SinceFilter:
    def __init__(self, since_raw: str | None) -> None:
        self.since_raw = str(since_raw or "").strip()
        self.mode: str | None = None
        self.since_epoch: float | None = None
        self.since_id: str | None = None
        self.id_unlocked: bool = False

        if not self.since_raw:
            return

        maybe_epoch = _iso_to_epoch(self.since_raw)
        if maybe_epoch is not None:
            self.mode = "ts"
            self.since_epoch = maybe_epoch
            return

        self.mode = "id"
        self.since_id = self.since_raw

    def describe(self) -> str:
        if self.mode == "ts":
            return f"ts>={self.since_epoch}"
        if self.mode == "id":
            return f"id>={self.since_id} (queue-order gate)"
        return "disabled"

    def allow(self, row: dict[str, Any]) -> bool:
        if not self.mode:
            return True

        if self.mode == "ts":
            event_epoch = _iso_to_epoch(row.get("ts"))
            if event_epoch is None:
                return False
            assert self.since_epoch is not None
            return event_epoch >= self.since_epoch

        if self.mode == "id":
            msg_id = str(row.get("source_message_id") or "").strip()
            if self.id_unlocked:
                return True
            if msg_id and msg_id == self.since_id:
                self.id_unlocked = True
                return True
            return False

        return True


def _parse_instance_token(raw: str) -> tuple[str, str]:
    token = str(raw or "").strip()
    if not token:
        raise ValueError("GREENAPI_INSTANCE_TOKEN пустой")

    if ":" in token:
        left, right = token.split(":", 1)
        if left.strip() and right.strip():
            return left.strip(), right.strip()

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


def _collect_candidate_refs(obj: Any, *, max_depth: int = 5) -> list[str]:
    out: list[str] = []

    def walk(x: Any, depth: int) -> None:
        if depth > max_depth:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                key = str(k).strip().lower()
                if key in URL_KEY_CANDIDATES and isinstance(v, str) and v.strip():
                    out.append(v.strip())
                walk(v, depth + 1)
        elif isinstance(x, list):
            for item in x:
                walk(item, depth + 1)

    walk(obj, 0)

    uniq: list[str] = []
    seen: set[str] = set()
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _extract_text_and_media_flags(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    md = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}
    type_message = str(md.get("typeMessage") or "").strip()
    type_message_lc = type_message.lower()

    text = _first_non_empty(
        ((md.get("textMessageData") or {}).get("textMessage") if isinstance(md.get("textMessageData"), dict) else ""),
        ((md.get("extendedTextMessageData") or {}).get("text") if isinstance(md.get("extendedTextMessageData"), dict) else ""),
        ((md.get("quotedMessage") or {}).get("stanzaBody") if isinstance(md.get("quotedMessage"), dict) else ""),
        payload.get("message"),
        payload.get("text"),
    )

    audio_data = md.get("audioMessageData") if isinstance(md.get("audioMessageData"), dict) else {}
    file_data = md.get("fileMessageData") if isinstance(md.get("fileMessageData"), dict) else {}
    image_data = md.get("imageMessageData") if isinstance(md.get("imageMessageData"), dict) else {}

    mime_type = _first_non_empty(
        audio_data.get("mimeType"),
        image_data.get("mimeType"),
        file_data.get("mimeType"),
        md.get("mimeType"),
    )
    file_name = _first_non_empty(
        audio_data.get("fileName"),
        image_data.get("fileName"),
        file_data.get("fileName"),
        md.get("fileName"),
    )

    is_audio_like = (
        "audio" in type_message_lc
        or "voice" in type_message_lc
        or str(mime_type).lower().startswith("audio/")
    )
    is_image_like = (
        "image" in type_message_lc
        or str(mime_type).lower().startswith("image/")
    )

    is_voice = (
        _is_truthy(audio_data.get("ptt"))
        or _is_truthy(file_data.get("ptt"))
        or "voice" in type_message_lc
        or "ptt" in type_message_lc
    )

    media_kind = "media"
    if type_message_lc.endswith("message"):
        media_kind = type_message_lc[:-7] or "media"
    elif type_message_lc:
        media_kind = type_message_lc

    explicit_text_types = {"textmessage", "extendedtextmessage"}
    is_media_hint = bool(
        is_audio_like
        or is_image_like
        or (type_message_lc and type_message_lc not in explicit_text_types and type_message_lc != "")
    )

    if text:
        return text, {
            "messageType": type_message or "textMessage",
            "isMedia": bool(is_media_hint),
            "isAudio": bool(is_audio_like),
            "isImage": bool(is_image_like),
            "isVoice": bool(is_voice),
            "mediaLabel": media_kind if is_media_hint else "text",
            "mimeType": mime_type,
            "fileName": file_name,
        }

    if is_audio_like:
        return "<media:audio>", {
            "messageType": type_message or "audioMessage",
            "isMedia": True,
            "isAudio": True,
            "isImage": False,
            "isVoice": bool(is_voice),
            "mediaLabel": "voice" if is_voice else "audio",
            "mimeType": mime_type,
            "fileName": file_name,
        }

    return f"<media:{media_kind}>", {
        "messageType": type_message or "unknown",
        "isMedia": True,
        "isAudio": False,
        "isImage": bool(is_image_like),
        "isVoice": False,
        "mediaLabel": media_kind,
        "mimeType": mime_type,
        "fileName": file_name,
    }


def _extract_media_probe(payload: dict[str, Any], media_meta: dict[str, Any], source_message_id: str) -> dict[str, Any] | None:
    if not _is_truthy(media_meta.get("isMedia")):
        return None

    md = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}

    nodes: list[Any] = [payload, md]
    for key in MEDIA_BLOCK_KEYS:
        v = md.get(key)
        if isinstance(v, dict):
            nodes.append(v)

    refs: list[str] = []
    for n in nodes:
        refs.extend(_collect_candidate_refs(n))

    # unique, keep order
    uniq_refs: list[str] = []
    seen: set[str] = set()
    for r in refs:
        if r not in seen:
            seen.add(r)
            uniq_refs.append(r)

    sender_data = payload.get("senderData") if isinstance(payload.get("senderData"), dict) else {}
    recipient_data = payload.get("recipientData") if isinstance(payload.get("recipientData"), dict) else {}

    chat_id = _first_non_empty(
        payload.get("chatId"),
        sender_data.get("chatId"),
        recipient_data.get("chatId"),
        sender_data.get("sender"),
        recipient_data.get("recipient"),
    )

    return {
        "isMedia": bool(media_meta.get("isMedia")),
        "isAudio": bool(media_meta.get("isAudio")),
        "isImage": bool(media_meta.get("isImage")),
        "isVoice": bool(media_meta.get("isVoice")),
        "messageType": media_meta.get("messageType") or "",
        "mediaLabel": media_meta.get("mediaLabel") or "media",
        "mimeType": media_meta.get("mimeType") or "",
        "fileName": media_meta.get("fileName") or "",
        "chatId": chat_id,
        "idMessage": source_message_id,
        "refs": uniq_refs,
    }


def _build_download_candidates(client: GreenApiClient, probe: dict[str, Any]) -> list[str]:
    refs = [str(x).strip() for x in (probe.get("refs") or []) if str(x).strip()]

    urls: list[str] = []

    def add(u: str) -> None:
        s = str(u or "").strip()
        if not s:
            return
        if s not in seen:
            seen.add(s)
            urls.append(s)

    seen: set[str] = set()

    for ref in refs:
        if ref.startswith("http://") or ref.startswith("https://"):
            add(ref)
            continue
        if ref.startswith("//"):
            add("https:" + ref)
            continue

        cleaned = ref.lstrip("/")
        if not cleaned:
            continue
        cleaned_q = urllib.parse.quote(cleaned, safe="/:@?&=._-%")

        # Варианты для разных форматов path из webhook.
        add(f"{client.media_url}/{cleaned_q}")
        add(f"{client.api_url}/{cleaned_q}")
        add(f"{client.media_url}/waInstance{client.id_instance}/downloadFile/{client.api_token}/{cleaned_q}")
        add(f"{client.api_url}/waInstance{client.id_instance}/downloadFile/{client.api_token}/{cleaned_q}")

    chat_id = str(probe.get("chatId") or "").strip()
    id_message = str(probe.get("idMessage") or "").strip()
    file_name = str(probe.get("fileName") or "").strip()

    if chat_id and id_message:
        q_chat = urllib.parse.quote(chat_id, safe="@._:-")
        q_msg = urllib.parse.quote(id_message, safe="@._:-")
        for base in (client.media_url, client.api_url):
            add(f"{base}/waInstance{client.id_instance}/downloadFile/{client.api_token}/{q_chat}/{q_msg}")
            if file_name:
                q_name = urllib.parse.quote(file_name, safe="@._:-")
                add(
                    f"{base}/waInstance{client.id_instance}/downloadFile/"
                    f"{client.api_token}/{q_chat}/{q_msg}/{q_name}"
                )

    return urls


def _guess_extension(file_name: str, mime_type: str, source_url: str) -> str:
    ext = Path(str(file_name or "").strip()).suffix
    if ext:
        return ext[:10]

    mime = str(mime_type or "").strip().lower()
    if mime:
        guessed = mimetypes.guess_extension(mime) or ""
        if guessed:
            return guessed

    p = urllib.parse.urlparse(str(source_url or "").strip())
    path_ext = Path(p.path).suffix
    if path_ext:
        return path_ext[:10]

    return ".bin"


def _media_target_path(
    media_root: Path,
    row: dict[str, Any],
    probe: dict[str, Any],
    source_url: str,
    mime_type: str,
) -> Path:
    ts = _first_non_empty(row.get("ts"), dt.datetime.now(tz=dt.timezone.utc).isoformat())
    d = ts[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ts[:10]) else dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%d")
    yyyy, mm, dd = d.split("-")

    out_dir = media_root / yyyy / mm / dd
    out_dir.mkdir(parents=True, exist_ok=True)

    msg_id = _sanitize_for_filename(str(row.get("source_message_id") or "")[:64], fallback="msg")
    peer = _sanitize_for_filename(str(row.get("peer") or "peer")[:40], fallback="peer")

    ext = _guess_extension(str(probe.get("fileName") or ""), mime_type, source_url)
    base = f"{msg_id}_{peer}"

    p = out_dir / f"{base}{ext}"
    i = 1
    while p.exists():
        p = out_dir / f"{base}_{i}{ext}"
        i += 1
    return p


def _extract_json_download_candidates(blob: bytes) -> list[str]:
    if not blob:
        return []
    small = blob[:200000]
    try:
        raw = small.decode("utf-8", errors="replace").strip()
    except Exception:
        return []
    if not raw or (not raw.startswith("{") and not raw.startswith("[")):
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    return _collect_candidate_refs(parsed)


def download_media_file(
    client: GreenApiClient,
    row: dict[str, Any],
    probe: dict[str, Any],
    media_root: Path,
) -> dict[str, Any] | None:
    candidates = _build_download_candidates(client, probe)

    # Доп. fallback через downloadFile POST {chatId,idMessage}.
    api_refs = client.request_download_url_via_api(
        chat_id=str(probe.get("chatId") or "").strip(),
        id_message=str(probe.get("idMessage") or "").strip(),
    )
    if api_refs:
        probe2 = dict(probe)
        probe2["refs"] = list((probe.get("refs") or [])) + api_refs
        candidates = _build_download_candidates(client, probe2)

    tried: list[str] = []
    queue = list(candidates)
    seen: set[str] = set()

    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        tried.append(url)

        resp = client._request_bytes("GET", url, quiet_errors=True)
        if resp is None:
            continue
        blob, ctype = resp

        # Иногда endpoint может вернуть JSON с downloadUrl/path вместо файла.
        if "application/json" in str(ctype).lower() or (blob[:1] in {b"{", b"["} and len(blob) < 200000):
            extra_refs = _extract_json_download_candidates(blob)
            if extra_refs:
                extra_probe = dict(probe)
                extra_probe["refs"] = list((probe.get("refs") or [])) + extra_refs
                extra_urls = _build_download_candidates(client, extra_probe)
                for u in extra_urls:
                    if u not in seen:
                        queue.append(u)
                continue

        if not blob:
            continue

        mime_type = str(probe.get("mimeType") or "").strip() or str(ctype or "").split(";", 1)[0].strip()
        out_path = _media_target_path(media_root, row=row, probe=probe, source_url=url, mime_type=mime_type)
        out_path.write_bytes(blob)

        return {
            "ok": True,
            "path": str(out_path.resolve()),
            "size": int(len(blob)),
            "sha256": _sha256_bytes(blob),
            "mimeType": mime_type,
            "sourceUrl": url,
            "attempts": len(tried),
            "sourceCandidates": tried[:12],
        }

    logging.warning(
        "Media download failed for source_message_id=%s tried=%s. "
        "TODO: verify GREENAPI media endpoint format for this webhook payload.",
        row.get("source_message_id"),
        len(tried),
    )
    return {
        "ok": False,
        "path": "",
        "size": 0,
        "sha256": "",
        "mimeType": str(probe.get("mimeType") or ""),
        "sourceUrl": "",
        "attempts": len(tried),
        "sourceCandidates": tried[:12],
        "todo": "Check GREENAPI webhook media URL/path format; fallback list exhausted",
    }


def transcribe_openai(path: Path, model: str = "whisper-1", language: str | None = None, timeout_sec: int = 240) -> tuple[str, str]:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    if not shutil.which("curl"):
        raise RuntimeError("curl is not available")

    cmd = [
        "curl",
        "-sS",
        "--fail-with-body",
        "https://api.openai.com/v1/audio/transcriptions",
        "-H",
        f"Authorization: Bearer {key}",
        "-F",
        f"file=@{path}",
        "-F",
        f"model={model}",
        "-F",
        "response_format=text",
        "-F",
        "temperature=0",
    ]
    if language:
        cmd.extend(["-F", f"language={language}"])

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"OpenAI transcription failed: {err[:500]}")

    txt = (proc.stdout or "").strip()
    if not txt:
        raise RuntimeError("OpenAI transcription returned empty text")
    return txt, f"openai:{model}"


def transcribe_local_whisper(path: Path, language: str | None = None, timeout_sec: int = 600) -> tuple[str, str]:
    whisper_bin = shutil.which("whisper")
    whisper_cli_bin = shutil.which("whisper-cli")

    if whisper_bin:
        with tempfile.TemporaryDirectory(prefix="greenapi-whisper-") as td:
            out_dir = Path(td)
            cmd = [
                whisper_bin,
                str(path),
                "--model",
                "base",
                "--output_format",
                "txt",
                "--output_dir",
                str(out_dir),
                "--fp16",
                "False",
            ]
            if language:
                cmd.extend(["--language", language])
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "").strip()
                raise RuntimeError(f"local whisper failed: {err[:500]}")

            out_file = out_dir / f"{path.stem}.txt"
            txt = out_file.read_text(encoding="utf-8", errors="ignore").strip() if out_file.exists() else ""
            if not txt:
                raise RuntimeError("local whisper returned empty text")
            return txt, "local:whisper"

    if whisper_cli_bin:
        with tempfile.TemporaryDirectory(prefix="greenapi-whispercpp-") as td:
            out_base = Path(td) / path.stem
            cmd = [
                whisper_cli_bin,
                "-f",
                str(path),
                "-otxt",
                "-of",
                str(out_base),
            ]
            if language:
                cmd.extend(["-l", language])
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "").strip()
                raise RuntimeError(f"local whisper-cli failed: {err[:500]}")

            out_file = Path(str(out_base) + ".txt")
            txt = out_file.read_text(encoding="utf-8", errors="ignore").strip() if out_file.exists() else ""
            if not txt:
                raise RuntimeError("local whisper-cli returned empty text")
            return txt, "local:whisper-cli"

    raise RuntimeError("no local whisper executable found")


def transcribe_with_fallback(path: Path, model: str, language: str | None) -> tuple[str, str, list[str]]:
    errs: list[str] = []
    try:
        txt, engine = transcribe_openai(path, model=model, language=language)
        return txt, engine, errs
    except Exception as e:
        errs.append(str(e))

    try:
        txt, engine = transcribe_local_whisper(path, language=language)
        return txt, engine, errs
    except Exception as e:
        errs.append(str(e))
        raise RuntimeError(" | ".join(errs))


def _build_media_meta_text(meta: dict[str, Any], suffix: str = "") -> str:
    label = str(meta.get("mediaLabel") or "media").strip() or "media"
    message_type = str(meta.get("messageType") or "").strip()
    mime_type = str(meta.get("mimeType") or "").strip()
    file_name = str(meta.get("fileName") or "").strip()

    parts = [f"[media:{label}]"]
    if message_type:
        parts.append(f"type={message_type}")
    if mime_type:
        parts.append(f"mime={mime_type}")
    if file_name:
        parts.append(f"file={file_name}")
    if suffix:
        parts.append(str(suffix).strip())
    return " ".join(p for p in parts if p)


def _is_media_placeholder_text(text: str) -> bool:
    t = str(text or "").strip()
    return bool(t and t.startswith("<media:") and t.endswith(">"))


def _set_image_description_failed_text(row: dict[str, Any], marker: str = "[image description failed]") -> None:
    normalized_marker = str(marker or "").strip() or "[image description failed]"
    row["text"] = normalized_marker


def _cleanup_downloaded_file(path_raw: str) -> tuple[bool, str]:
    raw = str(path_raw or "").strip()
    if not raw:
        return False, "empty path"
    p = Path(raw).expanduser()
    if not p.exists():
        return False, "already missing"
    try:
        p.unlink()
        return True, "deleted"
    except Exception as e:
        return False, f"unlink failed: {e}"


def _normalize_image_describe_backend(raw: str | None) -> str:
    mode = str(raw or "").strip().lower()
    if mode in {"openclaw", "openai", "auto"}:
        return mode
    return "auto"


def _looks_like_image_not_seen_response(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return True

    if "не вижу" in s and "изображ" in s:
        return True
    if "пришли" in s and ("фото" in s or "изображ" in s):
        return True

    markers = (
        "не могу увидеть изображ",
        "не могу просмотреть изображ",
        "изображение не",
        "can't see",
        "cannot see",
        "cannot view",
        "no image",
        "image not provided",
        "image was not provided",
        "send the image",
    )
    return any(m in s for m in markers)


def _gateway_auth_candidates(password: str | None = None, token: str | None = None) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    p = str(password if password is not None else DEFAULT_OPENCLAW_GATEWAY_PASSWORD).strip()
    t = str(token if token is not None else DEFAULT_OPENCLAW_GATEWAY_TOKEN).strip()

    if p:
        out.append(("password", p))
    if t and t != p:
        out.append(("token", t))

    out.append(("none", ""))
    return out


def _openclaw_chat_completions_text(
    payload: dict[str, Any],
    gateway_url: str = DEFAULT_OPENCLAW_GATEWAY_URL,
    timeout_sec: int = DEFAULT_OPENCLAW_DESCRIBE_TIMEOUT,
    password: str | None = None,
    token: str | None = None,
) -> tuple[str, str]:
    base_url = str(gateway_url or DEFAULT_OPENCLAW_GATEWAY_URL).strip().rstrip("/")
    if not base_url:
        raise RuntimeError("OPENCLAW_GATEWAY_URL is empty")

    url = f"{base_url}/v1/chat/completions"
    req_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    auth_errors: list[str] = []

    for auth_mode, secret in _gateway_auth_candidates(password=password, token=token):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if secret:
            headers["Authorization"] = f"Bearer {secret}"

        req = urllib.request.Request(url, data=req_body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=max(5, int(timeout_sec))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            if e.code == 401:
                auth_errors.append(f"{auth_mode}: unauthorized")
                continue
            raise RuntimeError(f"OpenClaw HTTP {e.code}: {body[:400]}")
        except Exception as e:
            raise RuntimeError(f"OpenClaw request failed: {e}")

        parsed = _safe_json_loads(raw)
        if not parsed:
            raise RuntimeError(f"OpenClaw returned non-JSON response: {raw[:400]}")

        choices = parsed.get("choices") if isinstance(parsed, dict) else None
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("OpenClaw returned empty choices")

        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        txt = str(msg.get("content") if isinstance(msg, dict) else "").strip()
        if not txt:
            raise RuntimeError("OpenClaw returned empty text")

        return txt, auth_mode

    if auth_errors:
        raise RuntimeError("; ".join(auth_errors))
    raise RuntimeError("OpenClaw request failed: no auth method succeeded")


def describe_image_via_openclaw(
    path: Path,
    mime_type: str = "",
    gateway_url: str = DEFAULT_OPENCLAW_GATEWAY_URL,
    timeout_sec: int = DEFAULT_OPENCLAW_DESCRIBE_TIMEOUT,
) -> tuple[str, str]:
    img_bytes = path.read_bytes()
    if not img_bytes:
        raise RuntimeError("image file is empty")

    img_mime = str(mime_type or "").strip().lower()
    if not img_mime:
        guessed, _ = mimetypes.guess_type(str(path))
        img_mime = str(guessed or "image/jpeg")

    b64 = base64.b64encode(img_bytes).decode("ascii")
    data_uri = f"data:{img_mime};base64,{b64}"

    prompt_common = (
        "Опиши изображение кратко и по фактам для полнотекстового поиска в архиве сообщений. "
        "1-2 предложения, без домыслов."
    )

    attempts: list[str] = []

    # Attempt 1: OpenAI-compatible image_url content (если endpoint поддерживает multimodal блоки).
    payload_image_url = {
        "model": "openclaw",
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt_common},
                    {"type": "image_url", "image_url": {"url": data_uri}},
                ],
            }
        ],
    }
    try:
        txt, auth_mode = _openclaw_chat_completions_text(
            payload_image_url,
            gateway_url=gateway_url,
            timeout_sec=timeout_sec,
        )
        if not _looks_like_image_not_seen_response(txt):
            return txt, f"openclaw-gateway:chat-completions:image_url:{auth_mode}"
        attempts.append("image_url content was accepted but image seems ignored")
    except Exception as e:
        attempts.append(f"image_url attempt failed: {e}")

    # Attempt 2: path adapter (работает с текущим OpenClaw endpoint, где chat payload сводится к text).
    local_path = str(path.resolve())
    payload_path_prompt = {
        "model": "openclaw",
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": (
                    f"{prompt_common}\n"
                    f"Локальный путь к изображению: {local_path}\n"
                    "Используй доступные инструменты анализа изображения, если нужно."
                ),
            }
        ],
    }

    try:
        txt, auth_mode = _openclaw_chat_completions_text(
            payload_path_prompt,
            gateway_url=gateway_url,
            timeout_sec=timeout_sec,
        )
        return txt, f"openclaw-gateway:chat-completions:path:{auth_mode}"
    except Exception as e:
        attempts.append(f"path adapter failed: {e}")
        raise RuntimeError(" | ".join(attempts))


def describe_image_openai(path: Path, mime_type: str = "", model: str = DEFAULT_DESCRIBE_MODEL, timeout_sec: int = 240) -> tuple[str, str]:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    if not shutil.which("curl"):
        raise RuntimeError("curl is not available")

    img_bytes = path.read_bytes()
    if not img_bytes:
        raise RuntimeError("image file is empty")

    img_mime = str(mime_type or "").strip().lower()
    if not img_mime:
        guessed, _ = mimetypes.guess_type(str(path))
        img_mime = str(guessed or "image/jpeg")

    b64 = base64.b64encode(img_bytes).decode("ascii")
    data_uri = f"data:{img_mime};base64,{b64}"

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Опиши изображение кратко и по фактам для полнотекстового поиска в архиве сообщений. "
                            "1-2 предложения, без домыслов."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": data_uri},
                    },
                ],
            }
        ],
    }

    cmd = [
        "curl",
        "-sS",
        "--fail-with-body",
        "https://api.openai.com/v1/chat/completions",
        "-H",
        f"Authorization: Bearer {key}",
        "-H",
        "Content-Type: application/json",
        "-d",
        json.dumps(payload, ensure_ascii=False),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"OpenAI vision failed: {err[:500]}")

    parsed = _safe_json_loads(proc.stdout or "")
    if not parsed:
        raise RuntimeError("OpenAI vision returned invalid JSON")

    choices = parsed.get("choices") if isinstance(parsed, dict) else None
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("OpenAI vision returned empty choices")

    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    txt = ""
    if isinstance(msg, dict):
        txt = str(msg.get("content") or "").strip()

    if not txt:
        raise RuntimeError("OpenAI vision returned empty text")

    return txt, f"openai-vision:{model}"


def describe_image_with_fallback(
    path: Path,
    mime_type: str,
    model: str = DEFAULT_DESCRIBE_MODEL,
    backend: str = DEFAULT_IMAGE_DESCRIBE_BACKEND,
) -> tuple[str, str, list[str]]:
    errs: list[str] = []
    backend_mode = _normalize_image_describe_backend(backend)

    if backend_mode in {"auto", "openclaw"}:
        try:
            txt, engine = describe_image_via_openclaw(path, mime_type=mime_type)
            return txt, engine, errs
        except Exception as e:
            errs.append(f"openclaw: {e}")

    if backend_mode in {"auto", "openai"}:
        try:
            txt, engine = describe_image_openai(path, mime_type=mime_type, model=model)
            return txt, engine, errs
        except Exception as e:
            errs.append(f"openai: {e}")

    if errs:
        raise RuntimeError(" | ".join(errs))
    raise RuntimeError(f"No image describe backend available for mode={backend_mode}")


def normalize_notification(notification: dict[str, Any], media_url: str) -> dict[str, Any] | None:
    if not isinstance(notification, dict):
        return None

    payload = notification.get("body") if isinstance(notification.get("body"), dict) else {}
    if not payload:
        return None

    type_webhook = str(payload.get("typeWebhook") or "").strip()
    message_data = payload.get("messageData") if isinstance(payload.get("messageData"), dict) else {}

    # Архивируем только message-события.
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
            "phase": "phase2",
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
        "raw_obj": raw_obj,
        "raw_json": json.dumps(raw_obj, ensure_ascii=False),
        "source_line": source_line[:2000],
        "source_type": SOURCE_TYPE_QUEUE,
        "source_message_id": source_message_id,
        "_payload": payload,
        "_media_meta": media_meta,
    }


def _normalize_direction_value(value: Any) -> str | None:
    s = str(value or "").strip().lower()
    if not s:
        return None

    direct_map = {
        "out": "out",
        "outgoing": "out",
        "outbound": "out",
        "sent": "out",
        "send": "out",
        "fromme": "out",
        "from_me": "out",
        "in": "in",
        "incoming": "in",
        "inbound": "in",
        "received": "in",
        "recv": "in",
        "to_me": "in",
    }
    if s in direct_map:
        return direct_map[s]

    compact = re.sub(r"[^a-z0-9]+", "", s)
    if not compact:
        return None

    if (
        compact.startswith(("outgoing", "outbound", "messagesent", "sent"))
        or "outgoing" in compact
        or "outbound" in compact
        or compact.endswith("sent")
    ):
        return "out"

    if (
        compact.startswith(("incoming", "inbound", "messagereceived", "received"))
        or "incoming" in compact
        or "inbound" in compact
        or "received" in compact
    ):
        return "in"

    return None


def _history_candidate_values(event: dict[str, Any]) -> list[Any]:
    keys = (
        "direction",
        "messageDirection",
        "chatDirection",
        "folder",
        "typeWebhook",
        "type",
        "eventType",
        "event",
        "status",
        "statusMessage",
        "messageType",
        "typeMessage",
    )

    values: list[Any] = []
    nodes: list[dict[str, Any]] = [event]
    for nested_key in ("body", "payload", "messageData", "data"):
        nested = event.get(nested_key)
        if isinstance(nested, dict):
            nodes.append(nested)

    for node in nodes:
        for k in keys:
            if k in node:
                values.append(node.get(k))

    return values


def _history_direction(event: dict[str, Any], direction_hint: str | None = None) -> str:
    hint_dir = _normalize_direction_value(direction_hint)
    if hint_dir in {"in", "out"}:
        return hint_dir

    from_me_false_seen = False
    for node in [event, event.get("body"), event.get("payload"), event.get("messageData"), event.get("data")]:
        if not isinstance(node, dict):
            continue
        for key in ("fromMe", "isFromMe", "from_me"):
            if key not in node:
                continue
            if _is_truthy(node.get(key)):
                return "out"
            from_me_false_seen = True

    for value in _history_candidate_values(event):
        direction = _normalize_direction_value(value)
        if direction in {"in", "out"}:
            return direction

    if from_me_false_seen:
        return "in"

    return "in"


def _history_timestamp_raw(event: dict[str, Any]) -> Any:
    return _first_non_empty(
        event.get("timestamp"),
        event.get("timestampSend"),
        event.get("time"),
        event.get("createdAt"),
        event.get("created"),
        event.get("date"),
    )


def _history_sort_key(item: dict[str, Any]) -> tuple[float, str]:
    event = item.get("event") if isinstance(item.get("event"), dict) else {}
    ts_raw = _history_timestamp_raw(event)
    epoch = _iso_to_epoch(ts_raw)
    if epoch is None:
        epoch = 0.0
    msg_id = _first_non_empty(event.get("idMessage"), event.get("id"), event.get("stanzaId"))
    return (epoch, msg_id)


def _build_history_payload(event: dict[str, Any], direction: str) -> dict[str, Any]:
    type_message = _first_non_empty(event.get("typeMessage"), event.get("messageType"), event.get("type"))

    chat_id = _first_non_empty(event.get("chatId"), event.get("senderId"), event.get("recipientId"))
    sender_id = _first_non_empty(event.get("senderId"), event.get("sender"), event.get("from"))
    recipient_id = _first_non_empty(event.get("recipientId"), event.get("recipient"), event.get("to"), chat_id)

    text = _first_non_empty(
        event.get("textMessage"),
        event.get("text"),
        event.get("message"),
        ((event.get("textMessageData") or {}).get("textMessage") if isinstance(event.get("textMessageData"), dict) else ""),
    )
    caption = _first_non_empty(event.get("caption"), event.get("fileCaption"))

    payload: dict[str, Any] = {
        "typeWebhook": "outgoingMessageReceived" if direction == "out" else "incomingMessageReceived",
        "timestamp": _history_timestamp_raw(event),
        "idMessage": _first_non_empty(event.get("idMessage"), event.get("id"), event.get("stanzaId")),
        "chatId": chat_id,
        "fromMe": direction == "out",
        "message": text or caption,
        "senderData": {
            "chatId": sender_id if direction == "in" else chat_id,
            "sender": sender_id,
        },
        "recipientData": {
            "chatId": recipient_id if direction == "out" else chat_id,
            "recipient": recipient_id,
        },
        "messageData": {
            "typeMessage": type_message,
        },
    }

    md = payload["messageData"]
    if text:
        md["textMessageData"] = {"textMessage": text}
    elif caption:
        md["extendedTextMessageData"] = {"text": caption}

    media_block: dict[str, Any] = {}
    for key in (
        "downloadUrl",
        "urlFile",
        "fileUrl",
        "filePath",
        "downloadPath",
        "path",
        "directPath",
        "url",
        "mediaUrl",
    ):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            media_block[key] = value.strip()

    file_name = _first_non_empty(event.get("fileName"), event.get("name"))
    if file_name:
        media_block["fileName"] = file_name

    mime_type = _first_non_empty(event.get("mimeType"), event.get("mime"), event.get("fileMimeType"))
    if mime_type:
        media_block["mimeType"] = mime_type

    ptt = event.get("ptt")
    if ptt is not None:
        media_block["ptt"] = ptt

    type_lc = str(type_message or "").strip().lower()
    is_audio_like = "audio" in type_lc or "voice" in type_lc or "ptt" in type_lc or str(mime_type).lower().startswith("audio/")

    if media_block:
        if is_audio_like:
            md["audioMessageData"] = dict(media_block)
        else:
            md["fileMessageData"] = dict(media_block)

        for key, value in media_block.items():
            payload.setdefault(key, value)

    return payload


def normalize_history_event(
    history_event: dict[str, Any],
    media_url: str,
    direction_hint: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(history_event, dict):
        return None

    direction = _history_direction(history_event, direction_hint=direction_hint)
    payload = _build_history_payload(history_event, direction=direction)

    type_webhook = str(payload.get("typeWebhook") or "").strip()
    source_message_id = _extract_source_message_id(payload)
    ts = _ts_to_iso(payload.get("timestamp"))
    peer = _extract_peer(payload, direction)

    text, media_meta = _extract_text_and_media_flags(payload)
    if not text:
        return None

    raw_obj = {
        "greenapi": {
            "history": history_event,
        },
        "waArchiveIngestDiag": {
            "pipeline": "greenapi_ingest.py",
            "phase": "phase2",
            "source": "greenapi_history",
            "typeWebhook": type_webhook,
            "directionInferred": direction,
            "mediaUrlConfigured": bool(media_url),
            "historyDirectionHint": direction_hint,
            **media_meta,
        },
    }

    source_line = json.dumps(history_event, ensure_ascii=False)
    return {
        "ts": ts,
        "direction": direction,
        "peer": peer,
        "text": text,
        "raw_obj": raw_obj,
        "raw_json": json.dumps(raw_obj, ensure_ascii=False),
        "source_line": source_line[:2000],
        "source_type": SOURCE_TYPE_HISTORY,
        "source_message_id": source_message_id,
        "_payload": payload,
        "_media_meta": media_meta,
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
    source_type = row.get("source_type") or SOURCE_TYPE_QUEUE
    source_message_id = str(row.get("source_message_id") or "").strip()

    if source_message_id:
        existing = conn.execute(
            """
            SELECT id, source_type, ts, direction, peer, text
            FROM messages
            WHERE source_message_id=?
            ORDER BY CASE WHEN source_type=? THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """,
            (source_message_id, source_type),
        ).fetchone()
        if existing:
            row_id, old_source_type, old_ts, old_direction, old_peer, old_text = existing

            new_text = old_text
            if _should_replace_text(str(old_text or ""), str(row.get("text") or "")):
                new_text = row.get("text")

            new_ts = old_ts or row.get("ts")
            incoming_direction = str(row.get("direction") or "").strip().lower()
            if incoming_direction in {"in", "out"}:
                new_direction = incoming_direction
            else:
                new_direction = old_direction
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
                    SET ts=?, direction=?, peer=?, text=?, raw_json=?, source_line=?, source_message_id=?
                    WHERE id=?
                    """,
                    (
                        new_ts,
                        new_direction,
                        new_peer,
                        new_text,
                        row.get("raw_json"),
                        row.get("source_line"),
                        source_message_id,
                        row_id,
                    ),
                )
                if old_source_type != source_type:
                    logging.debug(
                        "Dedup hit: source_message_id=%s existing_source=%s incoming_source=%s",
                        source_message_id,
                        old_source_type,
                        source_type,
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


def _enrich_media_and_transcript(
    client: GreenApiClient,
    row: dict[str, Any],
    media_dir: Path,
    transcribe_audio: bool,
    transcribe_model: str,
    transcribe_language: str | None,
    describe_images: bool,
    describe_model: str,
    keep_media_files: bool,
) -> dict[str, Any]:
    meta = row.get("_media_meta") if isinstance(row.get("_media_meta"), dict) else {}
    payload = row.get("_payload") if isinstance(row.get("_payload"), dict) else {}
    raw_obj = row.get("raw_obj") if isinstance(row.get("raw_obj"), dict) else {}
    diag = raw_obj.get("waArchiveIngestDiag") if isinstance(raw_obj.get("waArchiveIngestDiag"), dict) else {}

    result = {
        "media_downloaded": 0,
        "transcribed": 0,
        "images_described": 0,
        "media_deleted": 0,
    }

    probe = _extract_media_probe(payload, meta, str(row.get("source_message_id") or ""))
    if not probe:
        row["raw_obj"] = raw_obj
        row["raw_json"] = json.dumps(raw_obj, ensure_ascii=False)
        return result

    is_audio = _is_truthy(meta.get("isAudio"))
    is_image = _is_truthy(meta.get("isImage"))
    needs_download = bool(is_audio or is_image)

    diag["textFirstPolicy"] = {
        "keepMediaFiles": bool(keep_media_files),
        "describeImages": bool(describe_images),
        "transcribeAudio": bool(transcribe_audio),
        "imageDescribeBackend": _normalize_image_describe_backend(DEFAULT_IMAGE_DESCRIBE_BACKEND),
        "openclawGatewayUrl": DEFAULT_OPENCLAW_GATEWAY_URL,
    }

    media_info: dict[str, Any] | None = None
    if needs_download:
        media_info = download_media_file(client=client, row=row, probe=probe, media_root=media_dir)
        if media_info:
            diag["mediaDownload"] = media_info
        else:
            diag["mediaDownload"] = {"ok": False, "reason": "download_failed_unknown"}
    else:
        diag["mediaDownload"] = {
            "ok": False,
            "skipped": True,
            "reason": "text-first: non-image/non-audio media is not downloaded",
        }

    if isinstance(media_info, dict) and media_info.get("ok"):
        result["media_downloaded"] = 1

    media_path = Path(str(media_info.get("path"))) if isinstance(media_info, dict) and media_info.get("ok") and media_info.get("path") else None

    if is_audio:
        if transcribe_audio and media_path is not None:
            try:
                transcript, engine, fallback_errors = transcribe_with_fallback(
                    media_path,
                    model=transcribe_model,
                    language=transcribe_language,
                )
                transcript = transcript.strip()
                if transcript:
                    prev = str(row.get("text") or "").strip()
                    if prev and not (prev.startswith("<media:") and prev.endswith(">")):
                        row["text"] = f"{prev}\n[audio transcript] {transcript}"
                    else:
                        row["text"] = f"[audio transcript] {transcript}"
                    diag["transcription"] = {
                        "ok": True,
                        "engine": engine,
                        "chars": len(transcript),
                        "language": transcribe_language,
                        "fallbackErrors": fallback_errors,
                    }
                    result["transcribed"] = 1
                else:
                    diag["transcription"] = {
                        "ok": False,
                        "error": "empty transcript",
                    }
            except Exception as e:
                diag["transcription"] = {
                    "ok": False,
                    "error": str(e),
                }
                logging.warning("Transcription failed for %s: %s", row.get("source_message_id"), e)
        elif transcribe_audio:
            diag["transcription"] = {
                "ok": False,
                "error": "audio media not downloaded",
            }
        else:
            diag["transcription"] = {
                "ok": False,
                "skipped": True,
                "reason": "disabled by flag",
            }

        current = str(row.get("text") or "").strip()
        if not current or (current.startswith("<media:") and current.endswith(">")):
            row["text"] = _build_media_meta_text(meta, suffix="transcript_unavailable")

    if is_image:
        image_mime = str(meta.get("mimeType") or "").strip()
        if describe_images and media_path is not None:
            try:
                desc, engine, fallback_errors = describe_image_with_fallback(
                    media_path,
                    mime_type=image_mime,
                    model=describe_model,
                    backend=DEFAULT_IMAGE_DESCRIBE_BACKEND,
                )
                desc = str(desc or "").strip()
                if desc:
                    prev = str(row.get("text") or "").strip()
                    if prev and not _is_media_placeholder_text(prev):
                        row["text"] = f"{prev}\n{desc}"
                    else:
                        row["text"] = desc
                    diag["imageDescription"] = {
                        "ok": True,
                        "engine": engine,
                        "chars": len(desc),
                        "fallbackErrors": fallback_errors,
                        "pending_reprocess": False,
                    }
                    result["images_described"] = 1
                else:
                    diag["imageDescription"] = {
                        "ok": False,
                        "error": "empty image description",
                        "pending_reprocess": True,
                    }
                    _set_image_description_failed_text(row)
            except Exception as e:
                diag["imageDescription"] = {
                    "ok": False,
                    "error": str(e),
                    "pending_reprocess": True,
                }
                _set_image_description_failed_text(row)
                logging.warning("Image description failed for %s: %s", row.get("source_message_id"), e)
        elif describe_images:
            diag["imageDescription"] = {
                "ok": False,
                "error": "image media not downloaded",
                "pending_reprocess": True,
            }
            _set_image_description_failed_text(row)
        else:
            diag["imageDescription"] = {
                "ok": False,
                "skipped": True,
                "reason": "disabled by flag",
                "pending_reprocess": False,
            }

    if not needs_download:
        current = str(row.get("text") or "").strip()
        if not current or (current.startswith("<media:") and current.endswith(">")):
            row["text"] = _build_media_meta_text(meta, suffix="binary_not_saved")

    storage_meta: dict[str, Any] = {
        "policy": "text-first",
        "keepMediaFiles": bool(keep_media_files),
        "binaryStored": False,
        "binaryDeleted": False,
        "binaryPath": "",
    }

    if media_path is None:
        storage_meta["note"] = "no_file_downloaded"
    elif keep_media_files:
        storage_meta.update(
            {
                "binaryStored": True,
                "binaryDeleted": False,
                "binaryPath": str(media_path),
                "pathExistsAfterProcessing": media_path.exists(),
                "note": "kept_by_policy",
            }
        )
    else:
        deleted, cleanup_note = _cleanup_downloaded_file(str(media_path))
        if deleted:
            result["media_deleted"] = 1
        storage_meta.update(
            {
                "binaryStored": False,
                "binaryDeleted": bool(deleted),
                "binaryPath": str(media_path),
                "cleanup": cleanup_note,
                "pathExistsAfterProcessing": media_path.exists(),
                "note": "deleted_after_processing" if deleted else "cleanup_failed",
            }
        )

    diag["mediaStorage"] = storage_meta

    if isinstance(diag.get("mediaDownload"), dict) and media_path is not None:
        diag["mediaDownload"]["pathExistsAfterProcessing"] = media_path.exists()

    raw_obj["waArchiveIngestDiag"] = diag
    row["raw_obj"] = raw_obj
    row["raw_json"] = json.dumps(raw_obj, ensure_ascii=False)
    return result


def _empty_stats(dry_run: bool) -> dict[str, Any]:
    return {
        "received": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "skipped_since": 0,
        "deleted": 0,
        "errors": 0,
        "media_downloaded": 0,
        "transcribed": 0,
        "images_described": 0,
        "media_deleted": 0,
        "dry_run": bool(dry_run),
    }


def _process_normalized_row(
    *,
    client: GreenApiClient,
    conn: sqlite3.Connection | None,
    result: dict[str, Any],
    normalized: dict[str, Any] | None,
    dry_run: bool,
    media_dir: Path,
    transcribe_audio: bool,
    transcribe_model: str,
    transcribe_language: str | None,
    describe_images: bool,
    describe_model: str,
    keep_media_files: bool,
    since_filter: SinceFilter | None,
) -> None:
    if normalized is None:
        result["skipped"] += 1
        return

    if since_filter is not None and not since_filter.allow(normalized):
        result["skipped_since"] += 1
        return

    if dry_run:
        result["inserted"] += 1
        logging.info(
            "[dry-run] normalized: id=%s direction=%s peer=%s text=%s",
            normalized.get("source_message_id"),
            normalized.get("direction"),
            normalized.get("peer"),
            str(normalized.get("text") or "")[:120],
        )
        return

    assert conn is not None
    enrich = _enrich_media_and_transcript(
        client=client,
        row=normalized,
        media_dir=media_dir,
        transcribe_audio=bool(transcribe_audio),
        transcribe_model=transcribe_model,
        transcribe_language=transcribe_language,
        describe_images=bool(describe_images),
        describe_model=describe_model,
        keep_media_files=bool(keep_media_files),
    )
    result["media_downloaded"] += int(enrich.get("media_downloaded") or 0)
    result["transcribed"] += int(enrich.get("transcribed") or 0)
    result["images_described"] += int(enrich.get("images_described") or 0)
    result["media_deleted"] += int(enrich.get("media_deleted") or 0)

    action = upsert_message(conn, normalized)
    if action == "inserted":
        result["inserted"] += 1
    elif action == "updated":
        result["updated"] += 1
    else:
        result["skipped"] += 1


def ingest_queue_once(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    media_dir: Path,
    dry_run: bool = False,
    transcribe_audio: bool = True,
    transcribe_model: str = DEFAULT_TRANSCRIBE_MODEL,
    transcribe_language: str | None = DEFAULT_TRANSCRIBE_LANGUAGE,
    describe_images: bool = True,
    describe_model: str = DEFAULT_DESCRIBE_MODEL,
    keep_media_files: bool = DEFAULT_KEEP_MEDIA_FILES,
    since_filter: SinceFilter | None = None,
) -> dict[str, Any]:
    state = _load_json(state_path, default={})
    result = _empty_stats(dry_run)

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
            _process_normalized_row(
                client=client,
                conn=conn,
                result=result,
                normalized=normalized,
                dry_run=dry_run,
                media_dir=media_dir,
                transcribe_audio=transcribe_audio,
                transcribe_model=transcribe_model,
                transcribe_language=transcribe_language,
                describe_images=describe_images,
                describe_model=describe_model,
                keep_media_files=keep_media_files,
                since_filter=since_filter,
            )
        except Exception:
            result["errors"] += 1
            logging.exception("Ошибка обработки queue-уведомления")

        # Важно: dry-run не должен удалять queue-событие.
        if receipt_id is not None and not dry_run:
            if client.delete_notification(receipt_id):
                result["deleted"] += 1
            else:
                logging.warning("Не удалось удалить notification receiptId=%s", receipt_id)

        state["last_run_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
        state["last_receipt_id"] = event.get("receiptId") if isinstance(event, dict) else None
        state["last_source_mode"] = SOURCE_QUEUE
        if since_filter is not None:
            state["since_filter"] = since_filter.describe()
        _save_json(state_path, state)

        if conn is not None:
            conn.commit()

        return result
    finally:
        if conn is not None:
            conn.close()


def ingest_history_once(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    media_dir: Path,
    dry_run: bool = False,
    transcribe_audio: bool = True,
    transcribe_model: str = DEFAULT_TRANSCRIBE_MODEL,
    transcribe_language: str | None = DEFAULT_TRANSCRIBE_LANGUAGE,
    describe_images: bool = True,
    describe_model: str = DEFAULT_DESCRIBE_MODEL,
    keep_media_files: bool = DEFAULT_KEEP_MEDIA_FILES,
    since_filter: SinceFilter | None = None,
    max_events: int = 50,
) -> dict[str, Any]:
    state = _load_json(state_path, default={})
    result = _empty_stats(dry_run)

    wrapped_events = client.fetch_history_messages()
    if not wrapped_events:
        return result

    wrapped_events = sorted(wrapped_events, key=_history_sort_key)
    limit = max(1, int(max_events))
    wrapped_events = wrapped_events[-limit:]

    result["received"] = len(wrapped_events)

    conn: sqlite3.Connection | None = None
    try:
        if not dry_run:
            conn = ensure_db(db_path)

        for item in wrapped_events:
            event = item.get("event") if isinstance(item.get("event"), dict) else {}
            direction_hint = item.get("direction_hint")
            try:
                normalized = normalize_history_event(
                    event,
                    media_url=client.media_url,
                    direction_hint=str(direction_hint or "") or None,
                )
                _process_normalized_row(
                    client=client,
                    conn=conn,
                    result=result,
                    normalized=normalized,
                    dry_run=dry_run,
                    media_dir=media_dir,
                    transcribe_audio=transcribe_audio,
                    transcribe_model=transcribe_model,
                    transcribe_language=transcribe_language,
                    describe_images=describe_images,
                    describe_model=describe_model,
                    keep_media_files=keep_media_files,
                    since_filter=since_filter,
                )
            except Exception:
                result["errors"] += 1
                logging.exception("Ошибка обработки history-сообщения")

        state["last_run_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
        state["last_history_received"] = len(wrapped_events)
        state["last_source_mode"] = SOURCE_HISTORY
        if wrapped_events:
            last_event = wrapped_events[-1].get("event") if isinstance(wrapped_events[-1].get("event"), dict) else {}
            state["last_history_message_id"] = _first_non_empty(last_event.get("idMessage"), last_event.get("id"), last_event.get("stanzaId"))
        if since_filter is not None:
            state["since_filter"] = since_filter.describe()
        _save_json(state_path, state)

        if conn is not None:
            conn.commit()

        return result
    finally:
        if conn is not None:
            conn.close()


def ingest_once_by_source(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    media_dir: Path,
    dry_run: bool,
    transcribe_audio: bool,
    transcribe_model: str,
    transcribe_language: str | None,
    describe_images: bool,
    describe_model: str,
    keep_media_files: bool,
    since_filter: SinceFilter | None,
    source: str,
    max_history_events: int,
) -> dict[str, Any]:
    source_mode = str(source or SOURCE_AUTO).strip().lower()

    if source_mode == SOURCE_QUEUE:
        return ingest_queue_once(
            client=client,
            db_path=db_path,
            state_path=state_path,
            receive_timeout=receive_timeout,
            media_dir=media_dir,
            dry_run=dry_run,
            transcribe_audio=transcribe_audio,
            transcribe_model=transcribe_model,
            transcribe_language=transcribe_language,
            describe_images=describe_images,
            describe_model=describe_model,
            keep_media_files=keep_media_files,
            since_filter=since_filter,
        )

    if source_mode == SOURCE_HISTORY:
        return ingest_history_once(
            client=client,
            db_path=db_path,
            state_path=state_path,
            media_dir=media_dir,
            dry_run=dry_run,
            transcribe_audio=transcribe_audio,
            transcribe_model=transcribe_model,
            transcribe_language=transcribe_language,
            describe_images=describe_images,
            describe_model=describe_model,
            keep_media_files=keep_media_files,
            since_filter=since_filter,
            max_events=max_history_events,
        )

    queue_stats = ingest_queue_once(
        client=client,
        db_path=db_path,
        state_path=state_path,
        receive_timeout=receive_timeout,
        media_dir=media_dir,
        dry_run=dry_run,
        transcribe_audio=transcribe_audio,
        transcribe_model=transcribe_model,
        transcribe_language=transcribe_language,
        describe_images=describe_images,
        describe_model=describe_model,
        keep_media_files=keep_media_files,
        since_filter=since_filter,
    )

    if int(queue_stats.get("received") or 0) > 0:
        return queue_stats

    return ingest_history_once(
        client=client,
        db_path=db_path,
        state_path=state_path,
        media_dir=media_dir,
        dry_run=dry_run,
        transcribe_audio=transcribe_audio,
        transcribe_model=transcribe_model,
        transcribe_language=transcribe_language,
        describe_images=describe_images,
        describe_model=describe_model,
        keep_media_files=keep_media_files,
        since_filter=since_filter,
        max_events=max_history_events,
    )



def _merge_stats(dst: dict[str, Any], src: dict[str, Any]) -> None:
    for k, v in src.items():
        if isinstance(v, bool) or isinstance(dst.get(k), bool):
            continue
        if isinstance(v, (int, float)) and isinstance(dst.get(k), (int, float)):
            dst[k] = dst[k] + v


def ingest_batch(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    media_dir: Path,
    dry_run: bool,
    transcribe_audio: bool,
    transcribe_model: str,
    transcribe_language: str | None,
    describe_images: bool,
    describe_model: str,
    keep_media_files: bool,
    since_filter: SinceFilter | None,
    max_events: int,
    source: str = SOURCE_AUTO,
) -> dict[str, Any]:
    n = max(1, int(max_events))
    source_mode = str(source or SOURCE_AUTO).strip().lower()

    agg = _empty_stats(dry_run)

    if source_mode == SOURCE_HISTORY:
        return ingest_history_once(
            client=client,
            db_path=db_path,
            state_path=state_path,
            media_dir=media_dir,
            dry_run=dry_run,
            transcribe_audio=transcribe_audio,
            transcribe_model=transcribe_model,
            transcribe_language=transcribe_language,
            describe_images=describe_images,
            describe_model=describe_model,
            keep_media_files=keep_media_files,
            since_filter=since_filter,
            max_events=n,
        )

    for _ in range(n):
        stats = ingest_queue_once(
            client=client,
            db_path=db_path,
            state_path=state_path,
            receive_timeout=receive_timeout,
            media_dir=media_dir,
            dry_run=dry_run,
            transcribe_audio=transcribe_audio,
            transcribe_model=transcribe_model,
            transcribe_language=transcribe_language,
            describe_images=describe_images,
            describe_model=describe_model,
            keep_media_files=keep_media_files,
            since_filter=since_filter,
        )
        _merge_stats(agg, stats)

        # Если очередь пуста, дальше не крутим.
        if int(stats.get("received") or 0) == 0:
            break

    if source_mode == SOURCE_AUTO and int(agg.get("received") or 0) == 0:
        hist = ingest_history_once(
            client=client,
            db_path=db_path,
            state_path=state_path,
            media_dir=media_dir,
            dry_run=dry_run,
            transcribe_audio=transcribe_audio,
            transcribe_model=transcribe_model,
            transcribe_language=transcribe_language,
            describe_images=describe_images,
            describe_model=describe_model,
            keep_media_files=keep_media_files,
            since_filter=since_filter,
            max_events=n,
        )
        _merge_stats(agg, hist)

    return agg



def run_loop(
    client: GreenApiClient,
    db_path: Path,
    state_path: Path,
    receive_timeout: int,
    poll_sleep_sec: float,
    media_dir: Path,
    dry_run: bool,
    max_iterations: int,
    max_events: int,
    transcribe_audio: bool,
    transcribe_model: str,
    transcribe_language: str | None,
    describe_images: bool,
    describe_model: str,
    keep_media_files: bool,
    since_filter: SinceFilter | None,
    source: str = SOURCE_AUTO,
) -> None:
    i = 0
    total_received = 0
    source_mode = str(source or SOURCE_AUTO).strip().lower()
    history_fallback_ready = True

    while True:
        if max_events > 0 and total_received >= max_events:
            logging.info("Достигнут --max-events=%s, остановка", max_events)
            break

        i += 1
        try:
            remaining = (max_events - total_received) if max_events > 0 else 50
            history_limit = max(1, int(remaining))

            if source_mode == SOURCE_QUEUE:
                stats = ingest_queue_once(
                    client=client,
                    db_path=db_path,
                    state_path=state_path,
                    receive_timeout=receive_timeout,
                    media_dir=media_dir,
                    dry_run=dry_run,
                    transcribe_audio=transcribe_audio,
                    transcribe_model=transcribe_model,
                    transcribe_language=transcribe_language,
                    describe_images=describe_images,
                    describe_model=describe_model,
                    keep_media_files=keep_media_files,
                    since_filter=since_filter,
                )
            elif source_mode == SOURCE_HISTORY:
                stats = ingest_history_once(
                    client=client,
                    db_path=db_path,
                    state_path=state_path,
                    media_dir=media_dir,
                    dry_run=dry_run,
                    transcribe_audio=transcribe_audio,
                    transcribe_model=transcribe_model,
                    transcribe_language=transcribe_language,
                    describe_images=describe_images,
                    describe_model=describe_model,
                    keep_media_files=keep_media_files,
                    since_filter=since_filter,
                    max_events=history_limit,
                )
            else:
                queue_stats = ingest_queue_once(
                    client=client,
                    db_path=db_path,
                    state_path=state_path,
                    receive_timeout=receive_timeout,
                    media_dir=media_dir,
                    dry_run=dry_run,
                    transcribe_audio=transcribe_audio,
                    transcribe_model=transcribe_model,
                    transcribe_language=transcribe_language,
                    describe_images=describe_images,
                    describe_model=describe_model,
                    keep_media_files=keep_media_files,
                    since_filter=since_filter,
                )
                if int(queue_stats.get("received") or 0) > 0:
                    stats = queue_stats
                    history_fallback_ready = True
                elif history_fallback_ready:
                    stats = ingest_history_once(
                        client=client,
                        db_path=db_path,
                        state_path=state_path,
                        media_dir=media_dir,
                        dry_run=dry_run,
                        transcribe_audio=transcribe_audio,
                        transcribe_model=transcribe_model,
                        transcribe_language=transcribe_language,
                        describe_images=describe_images,
                        describe_model=describe_model,
                        keep_media_files=keep_media_files,
                        since_filter=since_filter,
                        max_events=history_limit,
                    )
                    history_fallback_ready = False
                else:
                    stats = queue_stats

            total_received += int(stats.get("received") or 0)
            logging.info("cycle=%s total_received=%s stats=%s", i, total_received, json.dumps(stats, ensure_ascii=False))
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
        http_timeout_sec=DEFAULT_HTTP_TIMEOUT,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="GREENAPI ingest -> wa_archive.db (phase2)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Путь до wa_archive.db")
    common.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH, help="Путь до state JSON")
    common.add_argument("--media-dir", type=Path, default=DEFAULT_MEDIA_DIR, help="Куда сохранять скачанные media")
    common.add_argument(
        "--receive-timeout",
        type=int,
        default=DEFAULT_RECEIVE_TIMEOUT,
        help="receiveNotification timeout (5..60)",
    )
    common.add_argument(
        "--dry-run",
        action="store_true",
        help="Только read/normalize: не писать в БД, не скачивать media, не делать transcript/description, не deleteNotification",
    )
    common.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Максимум событий за запуск (queue/history; ingest-once: default 1, run: default 0=без лимита)",
    )
    common.add_argument(
        "--source",
        choices=[SOURCE_QUEUE, SOURCE_HISTORY, SOURCE_AUTO],
        default=SOURCE_AUTO,
        help="Источник событий: queue | history | auto (auto: сначала queue, если 0 — history)",
    )
    common.add_argument(
        "--since",
        type=str,
        default="",
        help="Best-effort фильтр: unix-ts/ISO timestamp или source_message_id (queue-order gate)",
    )
    common.add_argument("--no-transcribe-audio", action="store_true", help="Отключить авто-транскрипт voice/audio")
    common.add_argument("--no-describe-images", action="store_true", help="Отключить авто-описание изображений")
    common.add_argument("--keep-media-files", action="store_true", help="Не удалять временно скачанные image/audio файлы")
    common.add_argument("--transcribe-model", default=DEFAULT_TRANSCRIBE_MODEL, help="OpenAI model (default whisper-1)")
    common.add_argument("--describe-model", default=DEFAULT_DESCRIBE_MODEL, help="Vision model for image description (default gpt-4o-mini)")
    common.add_argument("--transcribe-language", default=DEFAULT_TRANSCRIBE_LANGUAGE, help="Опциональный язык (ru/en/...) ")
    common.add_argument("--verbose", action="store_true", help="Подробные логи")

    sub.add_parser("ingest-once", parents=[common], help="Принять до --max-events уведомлений и выйти")

    p_run = sub.add_parser("run", parents=[common], help="Запустить polling")
    p_run.add_argument("--poll-sleep", type=float, default=DEFAULT_POLL_SLEEP, help="Пауза между циклами")
    p_run.add_argument("--max-iterations", type=int, default=0, help="0 = бесконечно")

    args = parser.parse_args()
    _setup_logging(verbose=bool(args.verbose))

    client = _build_client_from_env()

    since_filter = SinceFilter(args.since)
    if since_filter.mode:
        logging.info("Since filter enabled: %s", since_filter.describe())

    transcribe_audio = bool(DEFAULT_TRANSCRIBE_AUDIO) and not bool(args.no_transcribe_audio)
    describe_images = bool(DEFAULT_DESCRIBE_IMAGES) and not bool(args.no_describe_images)
    keep_media_files = bool(DEFAULT_KEEP_MEDIA_FILES) or bool(args.keep_media_files)

    if args.dry_run:
        transcribe_audio = False
        describe_images = False

    if args.cmd == "ingest-once":
        stats = ingest_batch(
            client=client,
            db_path=args.db,
            state_path=args.state,
            receive_timeout=args.receive_timeout,
            media_dir=args.media_dir,
            dry_run=bool(args.dry_run),
            transcribe_audio=transcribe_audio,
            transcribe_model=str(args.transcribe_model or "whisper-1"),
            transcribe_language=str(args.transcribe_language or "").strip() or None,
            describe_images=describe_images,
            describe_model=str(args.describe_model or DEFAULT_DESCRIBE_MODEL),
            keep_media_files=keep_media_files,
            since_filter=since_filter,
            max_events=max(1, int(args.max_events if args.max_events is not None else 1)),
            source=str(args.source or SOURCE_AUTO).strip().lower(),
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
            media_dir=args.media_dir,
            dry_run=bool(args.dry_run),
            max_iterations=int(args.max_iterations),
            max_events=max(0, int(args.max_events if args.max_events is not None else 0)),
            transcribe_audio=transcribe_audio,
            transcribe_model=str(args.transcribe_model or "whisper-1"),
            transcribe_language=str(args.transcribe_language or "").strip() or None,
            describe_images=describe_images,
            describe_model=str(args.describe_model or DEFAULT_DESCRIBE_MODEL),
            keep_media_files=keep_media_files,
            since_filter=since_filter,
            source=str(args.source or SOURCE_AUTO).strip().lower(),
        )


if __name__ == "__main__":
    main()
