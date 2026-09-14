#!/usr/bin/env bash
# Rebuild the default query set from exact-count agreement across engines.
#
# SELECT_CORPORA names the corpus lanes that gate admission (space-separated,
# from: standard scale). The default is standard only; add scale when
# scale-corpus agreement should gate the set again. Every named lane feeds its
# corpus into all three engines; each corpus has its own data directory, so the
# scale lane costs its own feed but leaves the standard indexes in place.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUTDIR="$ROOT/results/query-selection"
SOURCE="$ROOT/queries/luceneutil/queries-all.txt"
CORPORA=${SELECT_CORPORA:-standard}

[[ -s "$SOURCE" ]] || {
  echo "missing derived luceneutil query source: $SOURCE" >&2
  exit 1
}

source "$ROOT/scripts/engine-common.sh"
archive_existing "$OUTDIR"

RESULT_ARGS=()
for corpus_lane in $CORPORA; do
  case "$corpus_lane" in
    standard) corpus="$ROOT/corpus/corpus-10m-searchbench.ndjson" ;;
    scale) corpus="$ROOT/corpus/corpus-33m-searchbench.ndjson" ;;
    *) echo "unknown corpus lane: $corpus_lane" >&2; exit 1 ;;
  esac
  "$ROOT/scripts/prepare-corpus.sh" "$corpus_lane"
  BASELINE_ENGINES="luxir opensearch elasticsearch" \
  BASELINE_CORPUS="$corpus" \
  BASELINE_TASKS=COUNT \
  BASELINE_LANE=exact \
  BASELINE_DURATION=0 \
  BASELINE_REPETITIONS=1 \
  BASELINE_OUTDIR="$OUTDIR/$corpus_lane" \
  BASELINE_QUERIES="$SOURCE" \
    "$ROOT/scripts/run-baseline.sh"
  RESULT_ARGS+=(--results "$corpus_lane=$OUTDIR/$corpus_lane")
done

python3 "$ROOT/python/select_queries.py" \
  "${RESULT_ARGS[@]}" \
  --source "$SOURCE" \
  --output "$ROOT/queries/luceneutil/queries.txt" \
  --manifest "$ROOT/queries/luceneutil/selection.json"
