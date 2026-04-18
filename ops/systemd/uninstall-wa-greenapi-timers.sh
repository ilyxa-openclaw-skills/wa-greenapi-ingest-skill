#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root or through sudo." >&2
  exit 1
fi

readonly UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
readonly UNITS=(
  "wa-greenapi-ingest-queue.timer"
  "wa-greenapi-ingest-queue.service"
  "wa-greenapi-embeddings-backfill.timer"
  "wa-greenapi-embeddings-backfill.service"
  "wa-greenapi-enrich-media.timer"
  "wa-greenapi-enrich-media.service"
)

systemctl disable --now \
  wa-greenapi-ingest-queue.timer \
  wa-greenapi-embeddings-backfill.timer \
  wa-greenapi-enrich-media.timer || true

for unit_name in "${UNITS[@]}"; do
  rm -f "${UNIT_DIR}/${unit_name}"
done

systemctl daemon-reload
systemctl reset-failed wa-greenapi-ingest-queue.service \
  wa-greenapi-embeddings-backfill.service \
  wa-greenapi-enrich-media.service || true
