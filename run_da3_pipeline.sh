#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
if [ -n "${DA3_ENV_PYTHON:-}" ]; then
  exec "$DA3_ENV_PYTHON" da3_remote_pipeline.py "$@"
fi
exec python3 da3_remote_pipeline.py "$@"
