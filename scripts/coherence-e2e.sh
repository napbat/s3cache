#!/usr/bin/env bash
# End-to-end tests: cross-node coherence + parity with direct S3.
#
# Spins up MinIO (S3 origin) and Valkey in containers, builds s3cache, and runs both
# scripts/parity_e2e.py (every operation through the cache matches talking to S3
# directly) and scripts/coherence_e2e.py (a write on one node is seen by another).
# Requires: podman (or docker), python3 + boto3.
#
#   scripts/coherence-e2e.sh          # start infra, run, tear infra down
#   KEEP=1 scripts/coherence-e2e.sh   # leave the containers running afterwards
set -euo pipefail
cd "$(dirname "$0")/.."

RUNTIME="${RUNTIME:-podman}"
command -v "$RUNTIME" >/dev/null || { echo "need podman or docker (set RUNTIME)"; exit 1; }

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    "$RUNTIME" rm -f s3cache-e2e-valkey s3cache-e2e-minio >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> starting Valkey + MinIO"
"$RUNTIME" rm -f s3cache-e2e-valkey s3cache-e2e-minio >/dev/null 2>&1 || true
"$RUNTIME" run -d --name s3cache-e2e-valkey -p 6379:6379 docker.io/valkey/valkey:8-alpine >/dev/null
"$RUNTIME" run -d --name s3cache-e2e-minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio:latest server /data >/dev/null

echo "==> waiting for MinIO"
for _ in $(seq 1 50); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/minio/health/live || true)" = "200" ] && break
  sleep 0.2
done

# Start from a clean commit log so the run is deterministic.
"$RUNTIME" exec s3cache-e2e-valkey valkey-cli DEL s3cache:index:log >/dev/null 2>&1 || true

echo "==> building s3cache (release)"
cargo build --release

export MINIO_ENDPOINT="http://127.0.0.1:9000" VALKEY_URL="redis://127.0.0.1:6379"

echo "==> running parity checks (cache vs direct S3)"
python3 scripts/parity_e2e.py

echo "==> running cross-node coherence checks"
"$RUNTIME" exec s3cache-e2e-valkey valkey-cli DEL s3cache:index:log >/dev/null 2>&1 || true
python3 scripts/coherence_e2e.py

echo "==> running resilience checks (Valkey outage != data-plane outage)"
"$RUNTIME" exec s3cache-e2e-valkey valkey-cli DEL s3cache:index:log >/dev/null 2>&1 || true
VALKEY_CONTAINER=s3cache-e2e-valkey RUNTIME="$RUNTIME" python3 scripts/resilience_e2e.py
