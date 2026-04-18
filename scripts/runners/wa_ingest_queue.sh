#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

wa_greenapi_require_env
wa_greenapi_acquire_lock "wa-greenapi-ingest"
wa_greenapi_export_http_defaults

cd "${WA_GREENAPI_SKILL_ROOT}"
exec "${WA_GREENAPI_PYTHON_BIN}" "${WA_GREENAPI_SKILL_ROOT}/scripts/greenapi_ingest.py" \
  ingest-once \
  --source queue \
  --max-events "${WA_GREENAPI_QUEUE_MAX_EVENTS:-40}" \
  --chat-history-pagination off \
  --keep-media-files \
  --no-transcribe-audio \
  --no-describe-images \
  --no-analyze-docs \
  "$@"
