#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

wa_greenapi_require_env
wa_greenapi_acquire_lock "wa-greenapi-embeddings-backfill"

cd "${WA_GREENAPI_SKILL_ROOT}"
exec "${WA_GREENAPI_PYTHON_BIN}" "${WA_GREENAPI_SKILL_ROOT}/scripts/embed_missing.py" \
  --db "$(wa_greenapi_db_path)" \
  --batch "${WA_EMBED_BATCH:-120}" \
  "$@"
