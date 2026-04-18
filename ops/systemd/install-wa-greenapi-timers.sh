#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root or through sudo." >&2
  exit 1
fi

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SKILL_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
readonly GATEWAY_ENV_FILE="${WA_GREENAPI_GATEWAY_ENV_FILE:-/etc/openclaw/openclaw.env}"
readonly TEMPLATES=(
  "wa-greenapi-ingest-queue.service.template"
  "wa-greenapi-ingest-queue.timer.template"
  "wa-greenapi-history-reconcile.service.template"
  "wa-greenapi-history-reconcile.timer.template"
  "wa-greenapi-media-backfill.service.template"
  "wa-greenapi-media-backfill.timer.template"
  "wa-greenapi-embeddings-backfill.service.template"
  "wa-greenapi-embeddings-backfill.timer.template"
  "wa-greenapi-enrich-media.service.template"
  "wa-greenapi-enrich-media.timer.template"
)
readonly TIMERS=(
  "wa-greenapi-ingest-queue.timer"
  "wa-greenapi-history-reconcile.timer"
  "wa-greenapi-media-backfill.timer"
  "wa-greenapi-embeddings-backfill.timer"
  "wa-greenapi-enrich-media.timer"
)

render_template() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|__SKILL_ROOT__|${SKILL_ROOT}|g" \
    -e "s|__OPENCLAW_ENV_FILE__|${GATEWAY_ENV_FILE}|g" \
    "${src}" > "${dst}"
}

for template_name in "${TEMPLATES[@]}"; do
  template_path="${SCRIPT_DIR}/${template_name}"
  if [[ ! -f "${template_path}" ]]; then
    echo "Missing template: ${template_path}" >&2
    exit 1
  fi
  unit_name="${template_name%.template}"
  render_template "${template_path}" "${UNIT_DIR}/${unit_name}"
  chmod 0644 "${UNIT_DIR}/${unit_name}"
done

systemctl daemon-reload
systemctl enable --now "${TIMERS[@]}"
systemctl reset-failed "${TIMERS[@]}" || true

systemctl list-timers 'wa-greenapi-*' --no-pager
