#!/usr/bin/env bash
set -euo pipefail

cd /root/OpenWPM
source /root/miniconda3/etc/profile.d/conda.sh
conda activate openwpm

export PYTHONPATH=/root/OpenWPM:${PYTHONPATH:-}
export CRAWLER_API_HOST=${CRAWLER_API_HOST:-127.0.0.1}
export CRAWLER_API_PORT=${CRAWLER_API_PORT:-8080}

exec /root/miniconda3/envs/openwpm/bin/python -m uvicorn api.server:app \
  --host "$CRAWLER_API_HOST" \
  --port "$CRAWLER_API_PORT"
