#!/usr/bin/env bash
set -euo pipefail

readonly WA_GREENAPI_RUNNER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WA_GREENAPI_SKILL_ROOT="$(cd "${WA_GREENAPI_RUNNER_DIR}/../.." && pwd)"
readonly WA_GREENAPI_LOCK_DIR_DEFAULT="${WA_GREENAPI_SKILL_ROOT}/.locks"
readonly WA_GREENAPI_PYTHON_BIN="${PYTHON_BIN:-python3}"

wa_greenapi_log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

wa_greenapi_require_env() {
  local env_file="${WA_GREENAPI_SKILL_ROOT}/.env"
  if [[ ! -f "${env_file}" ]]; then
    printf 'Missing required env file: %s\n' "${env_file}" >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}

wa_greenapi_acquire_lock() {
  local lock_name="$1"
  local lock_dir="${WA_GREENAPI_LOCK_DIR:-${WA_GREENAPI_LOCK_DIR_DEFAULT}}"
  local lock_path

  mkdir -p "${lock_dir}"
  lock_path="${lock_dir}/${lock_name}.lock"
  exec {WA_GREENAPI_LOCK_FD}>"${lock_path}"
  if ! flock -n "${WA_GREENAPI_LOCK_FD}"; then
    wa_greenapi_log "Skip: ${lock_name} already running"
    exit 0
  fi
}

wa_greenapi_export_http_defaults() {
  export GREENAPI_HTTP_MIN_INTERVAL_SEC="${GREENAPI_HTTP_MIN_INTERVAL_SEC:-1.05}"
  export GREENAPI_HTTP_MAX_RETRIES="${GREENAPI_HTTP_MAX_RETRIES:-4}"
  export GREENAPI_HTTP_BACKOFF_BASE_SEC="${GREENAPI_HTTP_BACKOFF_BASE_SEC:-1}"
  export GREENAPI_HTTP_BACKOFF_MAX_SEC="${GREENAPI_HTTP_BACKOFF_MAX_SEC:-12}"
  export GREENAPI_HTTP_BACKOFF_JITTER_SEC="${GREENAPI_HTTP_BACKOFF_JITTER_SEC:-0.3}"
}

wa_greenapi_db_path() {
  if [[ -n "${WA_ARCHIVE_DB_PATH:-}" ]]; then
    printf '%s\n' "${WA_ARCHIVE_DB_PATH}"
    return 0
  fi
  printf '%s\n' "${WA_GREENAPI_SKILL_ROOT}/wa_archive.db"
}
