#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 5 ]]; then
  echo "usage: $0 ENGINE TASK LANE SERVER_PID OUTPUT_DIR [driver options...]" >&2
  exit 2
fi
ENGINE=$1; TASK=$2; LANE=$3; SERVER_PID=$4; OUTPUT_DIR=$5
shift 5
ROOT=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$OUTPUT_DIR"
for pass in 1 2; do
  "$ROOT/scripts/run-driver.sh" "$ENGINE" "$TASK" --lane "$LANE" \
    --server-pid "$SERVER_PID" --label "aa-$pass" \
    --output "$OUTPUT_DIR/$ENGINE-$TASK-$LANE-aa-$pass.json" "$@"
done
