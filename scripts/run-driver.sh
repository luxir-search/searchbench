#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CLIENT_CORES=${CLIENT_CORES:-$(python3 "$ROOT/python/cpu_layout.py" client)}
SERVER_CORES=${SERVER_CORES:-$(python3 "$ROOT/python/cpu_layout.py" server)}
exec taskset -c "$CLIENT_CORES" python3 "$ROOT/python/driver.py" "$@" \
  --client-cores "$CLIENT_CORES" --server-cores "$SERVER_CORES"
