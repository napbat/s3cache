#!/usr/bin/env bash
# Build the pinned MinIO source used by the S3 origin tests.
set -euo pipefail
cd "$(dirname "$0")/.."

runtime="${RUNTIME:-docker}"
"$runtime" build \
  --file tests/minio/Dockerfile \
  --tag s3cache-minio-test:7aac2a2c \
  tests/minio
