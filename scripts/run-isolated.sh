#!/usr/bin/env bash
# Run a complete campaign in a fresh, loopback-only network namespace.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
exec python3 "$ROOT/python/network_namespace.py" "$@"
