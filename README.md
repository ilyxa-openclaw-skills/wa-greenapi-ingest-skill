# wa-greenapi-ingest-skill (Phase 2+ history fallback)

Отдельный ingest-проект для импорта WhatsApp событий из **GREEN-API** в SQLite-архив `wa_archive.db`.

## Что делает сейчас

- читает переменные окружения:
  - `GREENAPI_API_URL`
  - `GREENAPI_MEDIA_URL`
  - `GREENAPI_INSTANCE_TOKEN`
- поддерживает источники событий:
  - `queue` → `receiveNotification`
  - `history` → `lastOutgoingMessages` + `lastIncomingMessages`
  - `auto` (default) → сначала `queue`, если 0 событий — fallback на `history`
- нормализует queue/history события в одну схему таблицы `messages`
- помечает источник в `source_type`:
  - queue: `greenapi`
  - history: `greenapi-history`
- дедуплицирует по `source_message_id` (`idMessage`) и уже существующим записям в БД
- умеет **скачивать media локально** (`data/media/...`)
- для **voice/audio**:
  - скачивает media
  - делает транскрипцию (OpenAI Whisper API → fallback local `whisper`/`whisper-cli`)
  - сохраняет transcript в `messages.text` (вместо placeholder `<media:audio>`)
  - пишет диагностику в `raw_json.waArchiveIngestDiag`
- удаляет queue-уведомление (`deleteNotification`) после обработки (кроме dry-run)

> Текущий embeddings/pipeline подхватывает transcript автоматически, т.к. он лежит в `messages.text`.

---

## Структура

```text
.
├── .env.example
├── README.md
└── scripts
    ├── greenapi_ingest.py
    ├── smoke_check.sh
    └── verify_media_transcript.sh
```

---

## Подготовка

```bash
cd /home/openclaw/.openclaw/workspace/integrations/wa-greenapi-ingest-skill
cp .env.example .env
# заполни .env
```

Если нужен OpenAI-транскриб:

```bash
export OPENAI_API_KEY=...
```

(Если ключа нет, скрипт попробует local `whisper`/`whisper-cli`.)

---

## Быстрый smoke

```bash
./scripts/smoke_check.sh
```

Smoke проверяет:
- синтаксис Python
- dry-run ingest 1 события
- наличие verify-скрипта

---

## CLI режимы источника (`--source`)

```bash
--source queue|history|auto
```

- `queue` — только queue (`receiveNotification`)
- `history` — только history API:
  - `GET /waInstance{idInstance}/lastOutgoingMessages/{token}`
  - `GET /waInstance{idInstance}/lastIncomingMessages/{token}`
- `auto` (default) — сначала queue, если `received=0`, то history

---

## Тестовый прогон для нашего кейса (text + audio из history)

### 1) Dry-run через history (без записи в БД)

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --source history \
  --dry-run \
  --max-events 20 \
  --verbose
```

### 2) Реальный ingest через history (записать text + audio с транскриптом)

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --source history \
  --max-events 20 \
  --verbose
```

### 3) Проверка, что есть media + transcript

```bash
./scripts/verify_media_transcript.sh ./wa_archive.db
```

Скрипт ищет записи `source_type IN ('greenapi','greenapi-history')` и проверяет:
- media-файл физически существует
- `waArchiveIngestDiag.transcription.ok=true`
- `messages.text` уже не `<media:...>` placeholder

---

## Авто-режим (default)

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --source auto \
  --max-events 10 \
  --verbose
```

Логика:
- сначала queue
- если queue пустая (`received=0`) — автоматом берёт history

---

## Непрерывный режим

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py run \
  --source auto \
  --poll-sleep 0.5 \
  --max-events 20 \
  --verbose
```

`--max-events 0` в `run` = без лимита.

---

## Дополнительные флаги

```bash
--max-events N
--since <unix-ts|iso|source_message_id>
--dry-run
--no-transcribe-audio
--transcribe-model whisper-1
--transcribe-language ru
```

---

## Совместимость с `wa_archive.db`

Пишется в те же поля `messages`:

- `ts`
- `direction`
- `peer`
- `text`
- `raw_json`
- `source_line`
- `source_type`
- `source_message_id`

Bridge/автоответы не затрагиваются (изменения только в ingest-репозитории).
