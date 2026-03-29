#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
exec python da3_remote_pipeline.py "$@"
