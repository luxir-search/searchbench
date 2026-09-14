#!/usr/bin/env bash
# Run the retained Search Benchmark Game-derived query suite as a secondary campaign.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TASKS=$(PYTHONPATH="$ROOT/python" python3 -c \
  "from presets import BENCHMARK_GAME_TASKS; print(' '.join(BENCHMARK_GAME_TASKS))")
export BASELINE_QUERIES=${BASELINE_QUERIES:-$ROOT/queries/benchmark-game/queries.txt}
export BASELINE_TASKS=${BASELINE_TASKS:-$TASKS}
export BASELINE_OUTDIR=${BASELINE_OUTDIR:-$ROOT/results/benchmark-game-${BASELINE_LANE:-exact}}
export BASELINE_LABEL=${BASELINE_LABEL:-benchmark-game}
export BASELINE_TITLE=${BASELINE_TITLE:-Searchbench secondary benchmark-game: ${BASELINE_LANE:-exact}}
exec "$ROOT/scripts/run-baseline.sh"
