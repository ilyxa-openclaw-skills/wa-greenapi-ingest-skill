#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

wa_greenapi_require_env
# Serialize queue and history imports against the same GreenAPI/SQLite pipeline.
wa_greenapi_acquire_lock "wa-greenapi-ingest"
wa_greenapi_export_http_defaults

cd "${WA_GREENAPI_SKILL_ROOT}"
exec "${WA_GREENAPI_PYTHON_BIN}" "${WA_GREENAPI_SKILL_ROOT}/scripts/greenapi_ingest.py" \
  ingest-full-history \
  --history-batch-size "${WA_GREENAPI_HISTORY_BATCH_SIZE:-80}" \
  --max-chats "${WA_GREENAPI_HISTORY_MAX_CHATS_PER_RUN:-120}" \
  --max-messages "${WA_GREENAPI_HISTORY_MAX_MESSAGES_PER_RUN:-9600}" \
  --max-batches-per-chat "${WA_GREENAPI_HISTORY_MAX_BATCHES_PER_CHAT:-3}" \
  --refresh-chat-list \
  --chat-history-pagination "${WA_GREENAPI_HISTORY_CHAT_PAGINATION:-auto}" \
  --keep-media-files \
  --no-download-media \
  --no-transcribe-audio \
  --no-describe-images \
  --no-analyze-docs \
  "$@"
