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

echo "[smoke] minitest: image describe success/failure + no meta fallback + cleanup keep=0"
python3 "$ROOT_DIR/scripts/minitest_openclaw_image_backend.py"

echo "[smoke] minitest: image route selection (path priority + degraded image_url retry path)"
python3 "$ROOT_DIR/scripts/minitest_image_route_selection.py"

echo "[smoke] minitest: audio transcription path (default model + fallback chain)"
GREENAPI_TRANSCRIBE_MODEL=gpt-4o-mini-transcribe python3 "$ROOT_DIR/scripts/minitest_audio_transcription_path.py"

echo "[smoke] minitest: content policy (PDF<=20 full, PDF>20 skip, text full)"
python3 "$ROOT_DIR/scripts/minitest_content_policy.py"

echo "[smoke] minitest: office extraction (DOCX/XLSX, tables, heuristic, diagnostics)"
python3 "$ROOT_DIR/scripts/minitest_office_extraction.py"

echo "[smoke] Пробный прием 1 уведомления в dry-run (без записи в БД / media / transcript и без deleteNotification)"
python3 "$INGEST_SCRIPT" ingest-once --dry-run --max-events 1 --verbose

echo "[smoke] verify script presence"
[[ -x "$ROOT_DIR/scripts/verify_media_transcript.sh" ]]

echo "[smoke] OK"
