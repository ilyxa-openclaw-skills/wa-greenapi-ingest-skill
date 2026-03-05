# wa-greenapi-ingest-skill (Phase 2 test-run)

Отдельный ingest-проект для уведомлений из **GREEN-API** в SQLite-архив `wa_archive.db`.

## Что делает Phase 2

- читает переменные окружения:
  - `GREENAPI_API_URL`
  - `GREENAPI_MEDIA_URL`
  - `GREENAPI_INSTANCE_TOKEN`
- принимает уведомления из queue (`receiveNotification`)
- нормализует message-события в таблицу `messages`
- умеет **скачивать media локально** в `data/media/...`
- для **voice/audio**:
  - определяет audio/voice media
  - делает транскрипцию (приоритет: OpenAI Whisper API → fallback local whisper/whisper-cli)
  - записывает transcript в `messages.text` (заменяет placeholder `<media:audio>`)
  - сохраняет диагностические метаданные в `raw_json.waArchiveIngestDiag`
- удаляет уведомление (`deleteNotification`) после обработки (кроме dry-run)
- добавлены safe-флаги:
  - `--max-events N`
  - `--since <ts|id>` (best-effort)
  - `--dry-run` (без write/delete/media/transcribe)

> Текущий embeddings-пайплайн подхватывает transcript автоматически, т.к. он лежит в `messages.text`.

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

Если нужен OpenAI-транскриб, экспортируй ключ:

```bash
export OPENAI_API_KEY=...
```

(Если ключа нет, скрипт попробует local `whisper` / `whisper-cli`.)

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

## Тестовый прогон на малом объёме (5–20 событий)

### 1) Dry-run (без удаления queue)

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --dry-run \
  --max-events 5 \
  --verbose
```

### 2) Реальный ingest 5–20 событий

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once \
  --max-events 10 \
  --verbose
```

или (например, limit 20):

```bash
python3 scripts/greenapi_ingest.py ingest-once --max-events 20 --verbose
```

### 3) Опционально ограничить по времени/ID

```bash
# по timestamp (unix или ISO)
python3 scripts/greenapi_ingest.py ingest-once --max-events 20 --since 1700000000
python3 scripts/greenapi_ingest.py ingest-once --max-events 20 --since 2026-03-05T00:00:00Z

# по source_message_id (best-effort queue-order gate)
python3 scripts/greenapi_ingest.py ingest-once --max-events 20 --since BAE5F3...
```

---

## Проверка media + transcript в БД

```bash
./scripts/verify_media_transcript.sh ./wa_archive.db
```

Скрипт возвращает JSON:
- `ok=true`, если нашёл хотя бы одну строку `messages` (source_type=`greenapi`) где:
  - есть `raw_json.waArchiveIngestDiag.mediaDownload.path`, файл физически существует
  - есть `raw_json.waArchiveIngestDiag.transcription.ok=true`
  - `messages.text` уже не placeholder `<media:...>`

Exit code:
- `0` — проверка успешна
- `1` — подходящих строк не найдено
- `2` — ошибка (например, не найдена БД)

---

## Непрерывный режим

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py run \
  --poll-sleep 0.5 \
  --max-events 20 \
  --verbose
```

`--max-events 0` в `run` = без лимита.

---

## Важные детали реализации

- Media download делает robust fallback:
  1. URL/path из webhook (`downloadUrl`, `urlFile`, `filePath`, `path`, и т.д.)
  2. сборка вариантов через `GREENAPI_MEDIA_URL` и `GREENAPI_API_URL`
  3. fallback через `downloadFile` API (`POST /downloadFile/{token}` с `chatId` + `idMessage`)
- Если media API у конкретного payload-формата не совпал, скрипт пишет явный warning + TODO в diag (`raw_json`), не падает.
- `--dry-run` специально **не удаляет notification из очереди**.

---

## Ограничения Phase 2

- `--since <id>` реализован как queue-order gate (best-effort), не как random access по истории.
- Для транскриба нужен либо `OPENAI_API_KEY`, либо доступный локальный `whisper`/`whisper-cli`.
- Некоторые редкие форматы media payload GREEN-API могут потребовать отдельного endpoint-паттерна (логируется как TODO).

---

## Совместимость

Таблица `messages` остаётся совместимой с текущим `wa_archive.db` pipeline:

- `ts`
- `direction`
- `peer`
- `text`
- `raw_json`
- `source_line`
- `source_type`
- `source_message_id`

Bridge/автоответы не затрагиваются (изменения только в ingest-репозитории).
