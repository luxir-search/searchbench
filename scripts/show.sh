#!/usr/bin/env bash
# Show the exact request a task class sends and the response it gets back.
# Usage: show.sh <engine> <task> [--query "text"] [--lane exact|skip] [--full] [--dry] ...
# The engine must already be running (quick.sh leaves engines up).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
exec python3 "$ROOT/python/show_request.py" "$@"
