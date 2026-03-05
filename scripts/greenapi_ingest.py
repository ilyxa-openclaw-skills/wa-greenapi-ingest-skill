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
DEFAULT_TRANSCRIBE_AUDIO = str(os.getenv("GREENAPI_TRANSCRIBE_AUDIO", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
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

    mime_type = _first_non_empty(
        audio_data.get("mimeType"),
        file_data.get("mimeType"),
        md.get("mimeType"),
    )
    file_name = _first_non_empty(
        audio_data.get("fileName"),
        file_data.get("fileName"),
        md.get("fileName"),
    )

    is_audio_like = (
        "audio" in type_message_lc
        or "voice" in type_message_lc
        or str(mime_type).lower().startswith("audio/")
    )

    is_voice = (
        _is_truthy(audio_data.get("ptt"))
        or _is_truthy(file_data.get("ptt"))
        or "voice" in type_message_lc
        or "ptt" in type_message_lc
    )

    if text:
        return text, {
            "messageType": type_message or "textMessage",
            "isMedia": False,
            "isAudio": False,
            "isVoice": False,
            "mediaLabel": "text",
            "mimeType": mime_type,
            "fileName": file_name,
        }

    if is_audio_like:
        return "<media:audio>", {
            "messageType": type_message or "audioMessage",
            "isMedia": True,
            "isAudio": True,
            "isVoice": bool(is_voice),
            "mediaLabel": "voice" if is_voice else "audio",
            "mimeType": mime_type,
            "fileName": file_name,
        }

    media_kind = "media"
    if type_message_lc.endswith("message"):
        media_kind = type_message_lc[:-7] or "media"
    elif type_message_lc:
        media_kind = type_message_lc

    return f"<media:{media_kind}>", {
        "messageType": type_message or "unknown",
        "isMedia": True,
        "isAudio": False,
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
        "isVoice": bool(media_meta.get("isVoice")),
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


def _history_direction(event: dict[str, Any], direction_hint: str | None = None) -> str:
    t = _first_non_empty(event.get("typeWebhook"), event.get("type"), event.get("eventType")).lower()
    if t.startswith("out") or "outgoing" in t:
        return "out"
    if t.startswith("in") or "incoming" in t:
        return "in"
    if _is_truthy(event.get("fromMe")):
        return "out"
    if str(direction_hint or "").strip().lower() in {"in", "out"}:
        return str(direction_hint).strip().lower()
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
) -> dict[str, Any]:
    meta = row.get("_media_meta") if isinstance(row.get("_media_meta"), dict) else {}
    payload = row.get("_payload") if isinstance(row.get("_payload"), dict) else {}
    raw_obj = row.get("raw_obj") if isinstance(row.get("raw_obj"), dict) else {}
    diag = raw_obj.get("waArchiveIngestDiag") if isinstance(raw_obj.get("waArchiveIngestDiag"), dict) else {}

    result = {
        "media_downloaded": 0,
        "transcribed": 0,
    }

    probe = _extract_media_probe(payload, meta, str(row.get("source_message_id") or ""))
    if not probe:
        row["raw_obj"] = raw_obj
        row["raw_json"] = json.dumps(raw_obj, ensure_ascii=False)
        return result

    media_info = download_media_file(client=client, row=row, probe=probe, media_root=media_dir)
    if media_info:
        diag["mediaDownload"] = media_info
    else:
        diag["mediaDownload"] = {"ok": False, "reason": "download_failed_unknown"}

    if media_info and media_info.get("ok"):
        result["media_downloaded"] = 1

    if (
        transcribe_audio
        and _is_truthy(meta.get("isAudio"))
        and isinstance(media_info, dict)
        and media_info.get("ok")
        and media_info.get("path")
    ):
        media_path = Path(str(media_info.get("path")))
        try:
            transcript, engine, fallback_errors = transcribe_with_fallback(
                media_path,
                model=transcribe_model,
                language=transcribe_language,
            )
            transcript = transcript.strip()
            if transcript:
                if str(row.get("text") or "").strip().startswith("<media:"):
                    diag["placeholderText"] = row.get("text")
                    row["text"] = transcript
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
    elif _is_truthy(meta.get("isAudio")) and transcribe_audio:
        diag["transcription"] = {
            "ok": False,
            "error": "audio media not downloaded",
        }

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
    )
    result["media_downloaded"] += int(enrich.get("media_downloaded") or 0)
    result["transcribed"] += int(enrich.get("transcribed") or 0)

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
        help="Только read/normalize: не писать в БД, не скачивать media, не делать transcript, не deleteNotification",
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
    common.add_argument("--transcribe-model", default=DEFAULT_TRANSCRIBE_MODEL, help="OpenAI model (default whisper-1)")
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
    if args.dry_run:
        transcribe_audio = False

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
            since_filter=since_filter,
            source=str(args.source or SOURCE_AUTO).strip().lower(),
        )


if __name__ == "__main__":
    main()
