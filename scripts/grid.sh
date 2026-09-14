#!/usr/bin/env bash
# Fixed selectivity x field grid checks against EXISTING indexes. Each cell is
# one validated warmup plus one short measured repetition. Engines are started
# when needed and LEFT RUNNING.
#
# -g selects WHICH grid (python/presets.py GRIDS): every grid sweeps the same
# rows and columns over the same corpus and differs only in what a cell asks
# for. Takes a comma list, or `all`; several grids in one invocation share the
# engine start, count check and cool-down.
#   facet               bucket counts, facet_limit 10, no documents (default)
#   facet-metric        + avg(price_i) per bucket
#   facet-metrics       + avg/max(price_i) and avg(sort_i) per bucket
#   facet-metric-sort   metric, buckets ordered by avg(price_i) desc
#   facet-metrics-sort  metrics, buckets ordered by avg(price_i) desc
#   facet-selected      + the column field's designated head and tail values
#                       selected (pinned buckets; luxir-only)
#   facet-top10         + top 10 matching ids retrieved (prices retrieval)
#   facet-selected-top10  selected AND retrieving, so the refiner is consumed
#                       (delta vs facet-top10 = cost of live selection)
#   sort10              sort by the column field, top 10 ids
#   sort10k             sort by the column field, top 10,000 ids
# The facet family differs only in the per-bucket payload, so the same cell of
# two of them differs by exactly that payload's cost. Each grid keeps its own
# results/<grid>-grid tree and rendered report, so cells can never collide and
# a re-render never mixes operations.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/engine-common.sh"
set +e

ENGINES="luxir"
GRID=facet          # comma list, or `all` (see the header)
ALL_GRIDS=$(python3 -c "import sys; sys.path.insert(0, '$ROOT/python'); \
from presets import GRID_NAMES; print(','.join(GRID_NAMES))")
# Stop each engine after its leg instead of leaving it running. Off by default
# because iteration wants the engine warm and up; on for a full board, where
# several idle engines holding 8G heaps and page cache alongside the one under
# test is not the posture the numbers claim to describe.
STOP_AFTER_LEG=${STOP_AFTER_LEG:-0}
# Dev-tier defaults: fast and rough on purpose. A cell ends at whichever of
# DURATION or MAX_REQUESTS comes first (0 = time only). Slow cells get few
# samples and coarse resolution - the accepted trade, since these runs guide
# development and a change believed to be 10x does not need 9x vs 11x resolved.
# Zoom in with -d 3 -R 0 when a specific cell matters.
#
# Neither bound is in param_hash (both live in run_config), so a fast grid
# still tabulates against longer baseline runs and against other engines.
#
# MAX_REQUESTS defaults OFF, having been measured and dropped. A 1000-request
# cap ends the fastest cells in 8ms, and over three legs each that cost
# resolution in both metrics actually read off this grid: median run-to-run
# spread 7.2% -> 3.7% on qps and 11.7% -> 7.3% on p50 latency when the cap is
# removed, worst-cell 33.6% -> 23.0% and 32.0% -> 25.6%. The 100% row gains
# most (28-34% -> 3.6-9.2% on five of its six cells). The cap bought 4s of a
# 20s leg. Set -R 1000 to have it back.
#
# ACCURACY TIER: -d 1, not -d 3. Swept 0.25/1/3s at three legs each, run-to-run
# spread over the whole grid was 7.0/5.7/4.6% median on qps and 12.2/8.4/7.8%
# on p50, for legs of 20/52/136s. Past 1s the curve is flat and the worst cell
# gets WORSE (qps 17.2% -> 20.6%), because what is left is between-run variance
# rather than sampling error - a longer single window cannot average it out.
# To tighten a specific cell, sample it more times (-r) rather than longer.
DURATION=0.25
MAX_REQUESTS=${MAX_REQUESTS:-0}
REPETITIONS=${REPETITIONS:-1}
LANE=exact
SELS=all
CARDS=all
WARMUP_QUERIES=32   # identical match-all requests; 0 = full pool (record tier)
                    # NB sizes the QUERY POOL (and so the untimed validation
                    # pass), not a warmup pass. Cheap: 0.01-0.47s per cell.
# Per-cell warmup is socket bring-up and nothing more: the leg has already been
# warmed across every shape, so all this owes is one request per client on its
# own connection before recording starts (WARMUP_REQUESTS below). Bounded by
# requests, not time, for the same reason as the measured window - so it costs
# the same on a fast engine as a slow one.
# The driver's own default is 1.0s, which was a fixed second on all 42 cells and
# the largest slice of per-cell overhead. Measured on the hardest case for it -
# elasticsearch at 100%/2Mu, 40 qps, fewest warmup requests per second and most
# JIT to provoke - 1.0s vs 0.1s gave median 41.2 vs 40.9 qps with the first
# repetition fastest in both. Workload presets keep the 1.0s default: they run a
# varied query pool, where "the leg pre-warmed every shape" does not hold.
REPLAY_WARMUP=${REPLAY_WARMUP:-0}
# Once-per-leg passes over EVERY shape the leg will measure. See warm_engine()
# in python/grid.py: without it the first cell after an engine start runs
# 7-14% slow, and it is always the same cell. Every engine needs it, not just
# the JVMs - luxir for first-use structures and mmap page faults. 0 disables.
WARM_PASSES=${WARM_PASSES:-2}
CONCURRENCY=8       # -C 32/64 for saturation runs (recorded in run_config, not param_hash)
MAXPAR=1            # -P 0 = parallel lane; 1 (default) = no intra-request
                    # parallelism but arena-dispatched (carries idle-arena churn
                    # on fast cells); -1 = serial inline on the transport thread
                    # - the clean engine-work cpu lane (keep CONCURRENCY <=
                    # luxir http threads). See README.md "MEASUREMENT CAVEAT"
                    # before trusting -P 1 cpu_ms/request on fast cells.
while getopts "e:g:d:l:s:c:w:C:P:R:W:r:x" opt; do
  case $opt in
    e) ENGINES=$OPTARG ;;
    g) GRID=$OPTARG ;;           # which grid(s) - see the header
    d) DURATION=$OPTARG ;;
    R) MAX_REQUESTS=$OPTARG ;;   # 0 = time-bounded only (accuracy tier)
    r) REPETITIONS=$OPTARG ;;    # resample each cell; beats a longer -d
    x) STOP_AFTER_LEG=1 ;;       # one engine up at a time (full-board hygiene)
    W) WARM_PASSES=$OPTARG ;;
    l) LANE=$OPTARG ;;
    s) SELS=$OPTARG ;;
    c) CARDS=$OPTARG ;;
    w) WARMUP_QUERIES=$OPTARG ;;
    C) CONCURRENCY=$OPTARG ;;
    P) MAXPAR=$OPTARG ;;
    *) exit 2 ;;
  esac
done

# One request per client, so it resolves after -C. Overridable by env.
WARMUP_REQUESTS=${WARMUP_REQUESTS:-$CONCURRENCY}

# Dev tier defaults to the text-free facet corpus in its own collection;
# claims-tier grids override both back to the full-text corpus/collection. Every
# grid uses the same corpus - that is the point of sharing the domain matrix.
CORPUS=${GRID_CORPUS:-$ROOT/corpus/corpus-10m-facet.ndjson}
DEFAULT_CORPUS="$ROOT/corpus/corpus-10m-facet.ndjson"
COLLECTION=${GRID_COLLECTION:-searchbench_facet}
# The facet corpus has its own data directory, so it neither carries nor is
# carried by the full-text campaign index. GRID_LAYOUT selects which layout of
# the named corpus to serve when a claims-tier grid points at another one.
#
# Default `merged`: query grids measure on a deterministic single-segment
# index, matching the standard campaign posture. An as-fed layout is whatever
# topology that feed's merge cascade happened to leave (luxir compacted the
# facet corpus to 2 uneven segments; ES/OS settled near 30), which is neither
# reproducible across refeeds nor a controlled variable across engines - it
# invalidated engine-vs-engine cells and same-engine regression comparisons
# alike. Multi-segment behavior is measured on the constructed topologies
# (run-cache-first-multisegment.sh), not on feed accidents; GRID_LAYOUT=as-fed
# remains available for deliberate ingest-shaped experiments.
GRID_LAYOUT=${GRID_LAYOUT:-merged}
SEARCHBENCH_DATASET=$(dataset_name "$CORPUS" "$GRID_LAYOUT") || exit 2
# GRID_FILTER_MODE=query makes the selectivity predicate the main query on
# every engine (query context, uncached) instead of the default filter-domain
# posture. It produces a distinct parameter hash.
# One result tree per grid. GRID_OUTDIR pins a single directory, so set it
# only when running one grid.
grid_outdir() { echo "${GRID_OUTDIR:-$ROOT/results/$1-grid}"; }
if [[ "$CORPUS" == "$DEFAULT_CORPUS" ]]; then
  "$ROOT/scripts/prepare-corpus.sh" facet
fi
[[ -s "$CORPUS" ]] || { echo "missing corpus: $CORPUS" >&2; exit 1; }
CORPUS_SHA256=$(corpus_sha256 "$CORPUS") || exit 2
ensure_driver || exit 1
CORPUS_DOCS=$(wc -l < "$CORPUS")
[[ "$GRID" == all ]] && GRID=$ALL_GRIDS
for grid in ${GRID//,/ }; do mkdir -p "$(grid_outdir "$grid")"; done
python3 "$ROOT/python/grid.py" validate-report --corpus "$CORPUS" \
  --documents "$CORPUS_DOCS" || exit 1
for grid in ${GRID//,/ }; do
  python3 "$ROOT/python/grid.py" prepare --engines "$ENGINES" --grid "$grid" \
    --corpus "$CORPUS" --documents "$CORPUS_DOCS" \
    --lane "$LANE" --outdir "$(grid_outdir "$grid")" --selectivities "$SELS" \
    --cardinalities "$CARDS" --max-parallel "$MAXPAR" || exit 1
done

status=0
for engine in ${ENGINES//,/ }; do
  port=$(engine_port "$engine") || { status=1; continue; }
  # Build (feed + force-merge) the selected layout if this box has never
  # served it; an existing verified dataset is reused untouched.
  DATASET_COLLECTION="$COLLECTION" \
    ensure_dataset "$engine" "$CORPUS" "$GRID_LAYOUT" "$CORPUS_SHA256" \
    "$CORPUS_DOCS" || { status=1; continue; }
  ensure_running "$engine" || { status=1; continue; }
  # Only if the package is actually hot - see wait_cool(). Sustained grid load
  # sits near 78C here and holds the pinned clock there, so the common case is
  # to wait nothing at all.
  wait_cool "${GRID_COOL_C:-78}" "${GRID_COOL_TIMEOUT:-120}"
  verify_count "$engine" "$port" "$CORPUS_DOCS" "$COLLECTION" || { status=1; continue; }
  pid=$(cat "$(engine_pidfile "$engine")")
  # Grids inside the engine loop: an engine is started, verified and cooled
  # once and then measured across every grid it was asked for.
  for grid in ${GRID//,/ }; do
    python3 "$ROOT/python/grid.py" run-engine "$engine" --grid "$grid" --port "$port" \
      --collection "$COLLECTION" \
      --server-pid "$pid" --duration "$DURATION" --lane "$LANE" \
      --corpus "$CORPUS" --documents "$CORPUS_DOCS" --outdir "$(grid_outdir "$grid")" \
      --selectivities "$SELS" --cardinalities "$CARDS" \
      --max-source-queries "$WARMUP_QUERIES" --concurrency "$CONCURRENCY" \
      --replay-warmup "$REPLAY_WARMUP" --warm-passes "$WARM_PASSES" \
      --max-requests "$MAX_REQUESTS" --warmup-requests "$WARMUP_REQUESTS" \
      --repetitions "$REPETITIONS" --server-cores "$SERVER_CORES" \
      --max-parallel "$MAXPAR" \
      || status=1
  done
  if [[ "$STOP_AFTER_LEG" == 1 ]]; then
    "$ROOT/scripts/stop-$(engine_process "$engine").sh" >/dev/null 2>&1 \
      && echo "stopped $(engine_process "$engine")"
  fi
done

for grid in ${GRID//,/ }; do
  # No --output: the report filename is grid.report_name(), so the naming
  # lives in one place rather than in both python and here. -c selects the
  # rendered columns too, so a sub-grid over an extra rung (100, 10K) has
  # somewhere to appear instead of measuring invisible cells.
  python3 "$ROOT/python/grid.py" render --engines "$ENGINES" --grid "$grid" \
    --lane "$LANE" --corpus "$CORPUS" --documents "$CORPUS_DOCS" \
    --outdir "$(grid_outdir "$grid")" --max-parallel "$MAXPAR" \
    --collection "$COLLECTION" --cardinalities "$CARDS" || status=1
done
exit "$status"
