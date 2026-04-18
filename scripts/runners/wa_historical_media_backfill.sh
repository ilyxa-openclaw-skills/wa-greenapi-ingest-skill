#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

wa_greenapi_require_env
wa_greenapi_acquire_lock "wa-greenapi-ingest"
wa_greenapi_export_http_defaults

cd "${WA_GREENAPI_SKILL_ROOT}"
exec "${WA_GREENAPI_PYTHON_BIN}" "${WA_GREENAPI_SKILL_ROOT}/scripts/greenapi_ingest.py" \
  reprocess-skipped-media \
  --batch "${WA_GREENAPI_MEDIA_BACKFILL_BATCH:-4}" \
  --audio-only \
  "$@"
