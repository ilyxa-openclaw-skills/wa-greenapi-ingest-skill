# wa-greenapi-ingest-skill (text-first media policy)

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
- history direction mapping:
  - `lastOutgoingMessages` -> `direction=out`
  - `lastIncomingMessages` -> `direction=in`
- дедуплицирует по `source_message_id` (`idMessage`) и fallback-ключу

## Новая политика хранения media (text-first)

По умолчанию бинарные media-файлы **не накапливаются**.

### Image
- временно скачивает image
- делает описание содержимого через настраиваемый backend:
  - `GREENAPI_IMAGE_DESCRIBE_BACKEND=auto` (default): **OpenClaw gateway** -> OpenAI
  - `GREENAPI_IMAGE_DESCRIBE_BACKEND=openclaw`: только OpenClaw
  - `GREENAPI_IMAGE_DESCRIBE_BACKEND=openai`: только OpenAI
- при успехе сохраняет в `messages.text` нормальный description-текст (без meta-prefix)
- при ошибке backend **не** пишет meta-fallback в `text`; ставит только `[image description failed]` и `raw_json.waArchiveIngestDiag.imageDescription.pending_reprocess=true`
- удаляет локальный файл при `GREENAPI_KEEP_MEDIA_FILES=0`

### Audio/Voice
- временно скачивает audio/voice
- делает транскрипцию (OpenAI Whisper → fallback local `whisper`/`whisper-cli`)
- сохраняет в `messages.text` финальный текст транскрипта
- удаляет локальный файл при `GREENAPI_KEEP_MEDIA_FILES=0`

### Остальные media (video/docs/stickers/...)
- бинарь не сохраняет
- пишет мета-текст в `messages.text` + диагностику в `raw_json`

### Диагностика в `raw_json`
В `raw_json.waArchiveIngestDiag` добавляются поля:
- `textFirstPolicy`
- `mediaDownload`
- `mediaStorage` (в т.ч. `binaryDeleted` / `binaryStored` / `pathExistsAfterProcessing`)
- `transcription` (для audio)
- `imageDescription` (для image, включая `pending_reprocess` при ошибке describe)

---

## Структура

```text
.
├── .env.example
├── README.md
└── scripts
    ├── greenapi_ingest.py
    ├── history_direction_selfcheck.py
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

Если нужен OpenAI-транскриб/vision (fallback или backend=openai):

```bash
export OPENAI_API_KEY=...
```

Для OpenClaw-native image describe (без отдельного OpenAI key для картинок):

```bash
export OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789
# auth: укажи либо password, либо token (в зависимости от gateway.auth.mode)
export OPENCLAW_GATEWAY_PASSWORD=...
# export OPENCLAW_GATEWAY_TOKEN=...
```

---

## Ключевые env

```bash
GREENAPI_KEEP_MEDIA_FILES=0                # default: удалять временные файлы
GREENAPI_DESCRIBE_IMAGES=1                 # default: включено
GREENAPI_TRANSCRIBE_AUDIO=1                # default: включено
GREENAPI_IMAGE_DESCRIBE_BACKEND=auto       # auto|openclaw|openai
OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789
OPENCLAW_GATEWAY_PASSWORD=
OPENCLAW_GATEWAY_TOKEN=
```

Дополнительно:

```bash
GREENAPI_DESCRIBE_MODEL=gpt-4o-mini
GREENAPI_TRANSCRIBE_MODEL=whisper-1
GREENAPI_TRANSCRIBE_LANGUAGE=
```

---

## Быстрый smoke

```bash
./scripts/smoke_check.sh
```

Отдельный mini-test backend failure handling:

```bash
python3 scripts/minitest_openclaw_image_backend.py
```

Smoke проверяет:
- синтаксис Python
- self-check direction mapping (`scripts/history_direction_selfcheck.py`)
- minitest OpenClaw image backend: при недоступном gateway нет meta-fallback и ставится marker failed
- dry-run ingest 1 события
- наличие verify-скрипта

---

## Запуск ingest

### ingest-once

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --source auto \
  --max-events 20 \
  --verbose
```

### run

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py run \
  --source auto \
  --poll-sleep 0.5 \
  --max-events 20 \
  --verbose
```

### Полезные флаги CLI

```bash
--no-describe-images
--no-transcribe-audio
--keep-media-files
--describe-model gpt-4o-mini
--transcribe-model whisper-1
--transcribe-language ru
--dry-run
--since <unix-ts|iso|source_message_id>
```

---

## Verify text-first

```bash
./scripts/verify_media_transcript.sh ./wa_archive.db 300 ./data/media
```

Проверяет:
- есть текстовые записи для `image/audio` (без `<media:...>` placeholder)
- при `GREENAPI_KEEP_MEDIA_FILES=0` нет сохранённых файлов (по DB-диагностике и по фактическим файлам в media-dir)

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
