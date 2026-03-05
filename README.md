# wa-greenapi-ingest-skill (MVP)

Отдельный проект для ingest уведомлений из **GREEN-API** в SQLite-архив `wa_archive.db`.

Цель: убрать рассинхрон с основным fork'ом и вести ingest как изолированный репозиторий.

## Что делает MVP

- читает переменные окружения:
  - `GREENAPI_API_URL`
  - `GREENAPI_MEDIA_URL`
  - `GREENAPI_INSTANCE_TOKEN`
- забирает уведомление из очереди GREEN-API (`receiveNotification`)
- нормализует событие сообщения в формат `messages` (совместимый с текущим `wa_archive.db`)
- поддерживает:
  - обычный текст
  - media placeholders (`<media:...>`)
  - voice/audio marker (для совместимости voice-пайплайна используется `<media:audio>` + флаг `isVoice` в `raw_json`)
- удаляет уведомление из очереди (`deleteNotification`) после обработки
- устойчив к ошибкам: логирует и продолжает работу

## Структура

```text
.
├── .env.example
├── README.md
└── scripts
    ├── greenapi_ingest.py
    └── smoke_check.sh
```

## Быстрый старт

1. Подготовь окружение:

```bash
cd /home/openclaw/.openclaw/workspace/integrations/wa-greenapi-ingest-skill
cp .env.example .env
# заполни .env
```

2. Проверка smoke:

```bash
./scripts/smoke_check.sh
```

3. Одноразовый ingest (1 уведомление):

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py ingest-once
```

4. Непрерывный polling:

```bash
set -a && source .env && set +a
python3 scripts/greenapi_ingest.py run
```

## Совместимость с существующим pipeline

Скрипт создает/использует таблицу `messages` в формате:

- `ts`
- `direction` (`in` / `out`)
- `peer`
- `text`
- `raw_json`
- `source_line`
- `source_type`
- `source_message_id`

Это совместимо с текущей моделью `wa_archive.db` и embeddings-процессом (который читает `messages.text`).

## Важные детали MVP

- MVP архивирует только message-события (не все service webhook-и).
- Дедупликация:
  - по `source_type + source_message_id` (если есть `idMessage`)
  - fallback-уникальность по `(source_type, ts, direction, peer, text)`
- Для `audio/voice` используется placeholder `<media:audio>`.
- Для остальных медиа — `<media:<type>>`.

## Ограничения MVP

- Нет скачивания и локального хранения media-файлов.
- Нет ретраев с backoff и продвинутого dead-letter механизма.
- Нет systemd/supervisor unit в этом репо (запуск вручную/через ваш оркестратор).

## Безопасность

- Не коммить `.env` и реальные токены.
- Рекомендуется ограничить права доступа к папке и БД.
