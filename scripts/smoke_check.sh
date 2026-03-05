#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INGEST_SCRIPT="$ROOT_DIR/scripts/greenapi_ingest.py"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

required=(GREENAPI_API_URL GREENAPI_MEDIA_URL GREENAPI_INSTANCE_TOKEN)
missing=()
for key in "${required[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    missing+=("$key")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "[smoke] Не заданы обязательные переменные: ${missing[*]}"
  echo "[smoke] Скопируй .env.example -> .env и заполни значения"
  exit 1
fi

echo "[smoke] Python синтаксис"
python3 -m py_compile "$INGEST_SCRIPT"

echo "[smoke] history direction self-check"
python3 "$ROOT_DIR/scripts/history_direction_selfcheck.py"

echo "[smoke] Пробный прием 1 уведомления в dry-run (без записи в БД / media / transcript и без deleteNotification)"
python3 "$INGEST_SCRIPT" ingest-once --dry-run --max-events 1 --verbose

echo "[smoke] verify script presence"
[[ -x "$ROOT_DIR/scripts/verify_media_transcript.sh" ]]

echo "[smoke] OK"
