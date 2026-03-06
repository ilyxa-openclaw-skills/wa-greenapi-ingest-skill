# wa-greenapi-ingest-skill (text-first media policy)

Импорт WhatsApp событий из **GREEN-API** в SQLite `wa_archive.db`.

## Что делает

- читает queue/history из GREEN-API
- нормализует в таблицу `messages`
- дедуплицирует по `source_message_id`
- обогащает media по text-first политике

## Политика анализа контента (обновлено)

### 1) Единый backend image/doc анализа + image stability fix

Приоритет backend: **OpenClaw gateway** (`GREENAPI_CONTENT_ANALYZE_BACKEND=auto|openclaw|openai`).

Для **WhatsApp image** внутри OpenClaw backend теперь действует route policy:

- первичный маршрут: **path-анализ** через gateway (локальный файл)
- `image_url` используется как вспомогательный fallback
- если `image_url` вернул деградированный/служебный ответ (например `вложение недоступно` / `текст не обнаружен`), выполняется обязательный retry через `path`
- эвристика выбора лучшего результата: **path > image_url** при конфликте валидных ответов

Промпт анализа для image/doc строго двухблочный:

1. **БЛОК 1 — SUMMARY** (кратко, факты)
2. **БЛОК 2 — VISIBLE_TEXT** (максимально полный видимый текст, дословно, с маркером `[неразборчиво]`)

### 2) PDF политика

- `pages <= GREENAPI_PDF_MAX_PAGES` (default 20): обрабатывается **весь документ**
- `pages > GREENAPI_PDF_MAX_PAGES`: полный анализ **не выполняется**, ставится:
  - `status=skip`
  - `reason=too_many_pages`
  - `pending_reprocess=true`
  - `manual=true`
- без постраничного распила в этом шаге

### 3) Text/code файлы

Для `txt/md/json/yaml/py/js/ts/sh/log/csv/xml/html` и `text/*`:

- файл читается целиком (в пределах safeguard)
- итоговый текст сохраняется в `messages.text` (для поиска/эмбеддингов)
- safeguard (по умолчанию permissive):
  - `GREENAPI_TEXT_ANALYZE_MAX_BYTES=8388608`
  - `GREENAPI_TEXT_ANALYZE_MAX_CHARS=2000000`

### 4) Office (doc/docx/xls/xlsx)

Улучшен office extraction pipeline:

- **DOCX**: надёжный ZIP/XML parse (`document + header* + footer* + footnotes/endnotes/comments`, включая текст из таблиц)
- **XLSX**: извлечение по листам с нормализацией `sharedStrings`, `inlineStr`, формул и значений (`A1: ...`, `B3: =... => ...`)
- legacy **XLS**: если локальный extractor недоступен или вернул пустой текст, включается fallback через **OpenClaw gateway document analysis** (тот же 2-блочный SUMMARY + VISIBLE_TEXT)
- авто-детект формата по ZIP-сигнатуре (даже если файл локально сохранён с неправильным расширением, например `.bin`)
- fallback в локальные CLI-инструменты только если они реально доступны в системе
- устойчивость к битым XML/архивам + частичным ошибкам
- эвристика: если извлечено достаточно текста (`GREENAPI_OFFICE_MIN_CHARS`, default 24), fail-marker не ставится

### 5) Audio transcription quality (RU-friendly default)

По умолчанию используется более сильная модель:

- `GREENAPI_TRANSCRIBE_MODEL=gpt-4o-mini-transcribe`
- fallback цепочка: `<GREENAPI_TRANSCRIBE_MODEL> -> whisper-1 -> local whisper`

Как переключить модель:

```bash
GREENAPI_TRANSCRIBE_MODEL=whisper-1
# или любая доступная в окружении OpenAI transcribe-модель
```

### 6) Бинарники media не храним (по умолчанию)

`GREENAPI_KEEP_MEDIA_FILES=0` → временно скачанный файл удаляется после обработки.

## Диагностика в `raw_json.waArchiveIngestDiag`

Основные блоки:

- `textFirstPolicy`
- `mediaDownload`
- `mediaStorage`
- `transcription` (audio)
- `imageDescription` (image)
- `documentAnalysis` (pdf/text/office)
  - для office всегда пишутся диагностические поля: `engine`, `bytes`, `error`, `extracted_chars`
  - для legacy `.xls` fallback: `office_fallback=openclaw_vision`
- `contentAnalysis` (унифицированная диагностика image/doc backend)

## Ключевые env

```bash
GREENAPI_KEEP_MEDIA_FILES=0
GREENAPI_DESCRIBE_IMAGES=1
GREENAPI_TRANSCRIBE_AUDIO=1
GREENAPI_TRANSCRIBE_MODEL=gpt-4o-mini-transcribe
# fallback: -> whisper-1 -> local whisper

GREENAPI_CONTENT_ANALYZE_BACKEND=auto   # auto|openclaw|openai
GREENAPI_IMAGE_DESCRIBE_BACKEND=auto    # legacy fallback var
OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789
OPENCLAW_CONTENT_ANALYZE_TIMEOUT_SEC=120
OPENCLAW_GATEWAY_PASSWORD=
OPENCLAW_GATEWAY_TOKEN=

GREENAPI_PDF_MAX_PAGES=20
GREENAPI_TEXT_ANALYZE_MAX_BYTES=8388608
GREENAPI_TEXT_ANALYZE_MAX_CHARS=2000000
GREENAPI_OFFICE_MIN_CHARS=24
```

## Smoke / minitest

```bash
./scripts/smoke_check.sh
```

Внутри smoke:

- `python3 -m py_compile scripts/greenapi_ingest.py`
- `python3 scripts/history_direction_selfcheck.py`
- `python3 scripts/minitest_openclaw_image_backend.py`
- `python3 scripts/minitest_image_route_selection.py`
- `python3 scripts/minitest_audio_transcription_path.py`
- `python3 scripts/minitest_content_policy.py`
- `python3 scripts/minitest_office_extraction.py`
- `python3 scripts/minitest_xls_fallback.py`
- dry-run ingest-once

## Отдельные тесты новой политики

```bash
python3 scripts/minitest_image_route_selection.py
python3 scripts/minitest_audio_transcription_path.py
python3 scripts/minitest_content_policy.py
python3 scripts/minitest_office_extraction.py
python3 scripts/minitest_xls_fallback.py
```

Проверяет:

- image route: path-first + retry path при деградированном `image_url`
- конфликт результатов: приоритет `path > image_url`
- audio: default `gpt-4o-mini-transcribe` + fallback `whisper-1` + local whisper
- PDF <=20 страниц: full processed
- PDF >20 страниц: skipped (`too_many_pages`) + `pending_reprocess/manual=true`
- text file: full analyzed
- office DOCX/XLSX: table/sharedStrings/formula extraction + heuristic no-fail при достаточном тексте
- legacy XLS: local-fail/empty -> OpenClaw fallback (`office_fallback=openclaw_vision`), fail-marker только если fallback тоже упал
- keep=0 cleanup временных файлов

## Запуск ingest

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once --source auto --max-events 20 --verbose
```

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py run --source auto --poll-sleep 0.5 --max-events 20 --verbose
```

## Verify text-first

```bash
./scripts/verify_media_transcript.sh ./wa_archive.db 300 ./data/media
```

## Embeddings backfill (новое)

`greenapi_ingest.py` пишет в `messages`, а embeddings считаются отдельным шагом.

```bash
# dry-run: сколько кандидатов без векторов
python3 scripts/embed_missing.py --db ./wa_archive.db --batch 50 --dry-run

# реальный прогон (нужен OPENAI_API_KEY)
python3 scripts/embed_missing.py --db ./wa_archive.db --batch 50
```

Рекомендуется запускать периодически (cron/systemd timer), например раз в 2-5 минут.

## Быстрый ручной прогон (1 image + 1 voice)

```bash
set -a && source .env && set +a

# 1) пришли в WhatsApp одно изображение (или дождись его в queue), затем:
python3 scripts/greenapi_ingest.py ingest-once --source queue --max-events 1 --verbose

# 2) пришли одно voice/audio сообщение, затем:
python3 scripts/greenapi_ingest.py ingest-once --source queue --max-events 1 --verbose

# Проверка, что тексты появились и keep=0 cleanup соблюдается:
./scripts/verify_media_transcript.sh ./wa_archive.db 100 ./data/media
```
