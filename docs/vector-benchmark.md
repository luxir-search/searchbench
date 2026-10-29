# Vector benchmark definition

This benchmark measures Luxir bulk ingestion and one-segment approximate kNN search.
OpenSearch and Elasticsearch remain future reference engines. All timed HTTP
requests use the same native replay executable as the full-text baseline.
Preparation uses Python's standard library and a standalone C++20 numeric tool.

## Corpus and preparation

The pinned source is luceneutil's scattered Cohere v3 English Wikipedia set:
1M document vectors and 200K query vectors, each 1024 little-endian float32
components without a header, with corresponding RFC 4180 CSV paragraphs.
The four URLs, byte sizes and SHA-256 values are in
`corpora/cohere/manifest.json`. Preparation reads only `corpus/cache/`; it
never downloads missing files. All four artifacts pass the shared size/hash
verification and `.sha256-ok` cache stamp check before use. The license note
records the supplied English Wikipedia/Cohere derivation; upstream license
terms, release identity and dump date have not been independently verified.

`-s 100k` selects the first 100,000 document rows; `-s 1m` selects all 1M.
Ids are zero-based decimal row ordinals. CSV quoting, embedded newlines and
UTF-8 content survive the transform. The fields are id, title, text, url and
embedding. Title and text are indexed with Luxir's Unicode-word/lowercase
analyzer, url is stored without an index or column, and embedding is a
1024-dimensional vector with metric `ip`. There is no normalization on write.
These vectors are approximately unit length, so cosine ranks identically to
float32 precision; the benchmark and exact truth both use raw dot product.

The numeric helper uses shortest round-tripping float32 decimals. Preparation
streams CSV rows and vector JSON arrays without materializing the corpus in
memory. Output is atomically published and SHA-256 checked on reuse. Corpus
identity includes source pins, row count, transform source, and numeric helper
source and binary hashes. Rebuild a stale helper before preparation.

Ground truth exhaustively scores the first 1,000 queries over the selected
prefix and caches exact top-100 ids next to the corpus. It uses double
accumulation of float32 products, descending raw inner product, and ascending
ordinal for ties. It does not call an engine. Sixteen independent query workers
share the document matrix (about 4.1 GB at 1M); no approximate pruning or
quantization occurs. The computation is O(documents * queries * dimensions).
The cache identity includes transformed corpus and complete query-source
hashes, query count, metric, and numeric helper source/binary. The source query
CSV is verified for provenance but is not needed by queries.

## Feed and maintenance

The default schema leaves `ann` at its IVF+PQ defaults. `--codec rabitq`
declares `ann: {"codec":"rabitq"}`, and `--codec rabitqN` also sets
`rabitq.bits: N` (1 to 9). Each codec has a separate dataset identity.
No other codec/training/quantization settings are changed.

After schema installation an empty commit selects the concrete overlay name
`vec.embedding`. This activates merge-time builds before ingestion, including
when no segment is yet large enough for training. The shared Luxir feeder
uses 16 record-aligned concurrent streams, allow_dups, and one final durable
commit selecting the same overlay with `wait_for_merges:true`. The activation
commit contains no documents and is outside the feeder's reported elapsed
interval. Small segments may legitimately remain flat during feed.

Feed wall time and docs/s include ingestion, merges and the final commit;
final commit time is also reported separately. The outer feed record includes
schema setup and activation overhead. Before/after `/_stats?segments=true`
snapshots preserve `service.vector_build` counts, outcome counts, duration
count and accumulated seconds, including attempts that skip ineligible small
segments. They are build-attempt metrics, not a count of trained overlays.
Accumulated duration is task time and can overlap wall time. CPU seconds and
sampled peak total/anonymous RSS use the existing /proc collection. The server
log is retained. Missing metric fields mean unavailable, never zero.

Force-merge is separately timed maintenance with max_segments=1,
wait_for_merges=true and the same overlay selection. Before every query cell
and after replay the harness asserts one committed, quiescent segment, the
expected document count, and its vec.embedding overlay. An absent overlay is
an error, even though Luxir could fall back to exact scanning.

Indexes are separated by corpus, schema/codec, physical layout, feeder source,
and selected Luxir binary SHA-256. `LUXIR_BIN` selects either A/B binary.
Use `--refeed` for ingestion comparisons; otherwise matching indexes can be
reused and the report explicitly omits a new feed measurement. Existing data
is archived on refeed. Result directories are mandatory and must be new.

## Queries and recall

Each cell requests k=10 or k=100 and returns ids only. It runs the complete
1,000-query prefix at concurrency 1 and the full-text default of 8, with one
native replay thread. The default search knobs form one cell; explicit nprobe
settings 16, 64 and 256 form the sweep. Override with `--nprobes LIST` and
retain that declared choice in result cell identities. `--refine LIST` adds a
`refine_candidates` axis: `default` keeps Luxir's k-derived pool, `k` pins the
pool to k, and a count pins it to that count. A `k` cell measures codes-only
recall, since rescoring the k returned hits only reorders them. Non-default
cells carry an `-rf` name suffix. `--concurrency LIST` (default 1,8) selects
the concurrency cells.

Measured recall@10 for the default IVF+PQ build:

| Vectors | nprobe 16 | nprobe 64 | nprobe 256 | default |
|---|---:|---:|---:|---:|
| 100k | 0.925 | 0.989 | 0.9998 | 0.984 |
| 1M | 0.88 | 0.97 | 0.997 | 0.98 |

The sweep spans the 0.85 to 0.99 range at both sizes, and the default nprobe
(relative to sqrt(live vectors)) holds about 0.98 from 100k to 1M. Recall is
not reproducible to the last digit across refeeds of the same binary. The
training sample is a fixed stride over ordinals in merge order, and merge
order depends on how the concurrent feed streams interleave. Three 1M refeeds
of one binary spread recall at nprobe 16 over about 0.6 points and the
higher-probe cells over at most 0.4 points. Treat smaller A/B recall differences as
noise unless repeated refeeds separate them.

The functional smoke explores the first 100 queries over 10k vectors, without
timing, at nprobe 4,8,16,32,64,100.

Before timing, every query response is parsed and checked for errors,
warnings, exactly k unique in-range ids, and ids-only projection. Recall@k is
set overlap with the first k ground-truth ids divided by k, averaged equally
over the 1,000 queries. Validation ids and per-query recalls are retained.
The native driver then checks HTTP status, engine failure markers and the
complete prevalidated ordered docs array on every response. Any changed,
missing or additional id fails the cell. Thus the reported recall applies to
the timed replies as well. SBWL version 2 carries per-request expected payload
markers; ordinary workloads retain version 1. Request lengths are uint32,
and 1024-float bodies are exercised by the smoke.

Defaults are two 20-second repetitions, sequential query order and a
one-second native connection warmup. Repetitions typically agree within 1 to
2% at these latencies, and a 1M campaign runs in about 15 minutes. QPS aggregates completed requests over
measured elapsed time; latency histograms include p50/p90/p99/p999. Raw native
repetitions retain client/server CPU. The report includes feed/maintenance,
recall, QPS and latency, and the shared c32/t4 health control after the search
cells in the same server session. Control QPS and provenance are checked.
Contexts record actual startup, binary hashes, CPU sets, OS, network namespace,
corpus identity and serving topology. Controls are transport diagnostics and
do not correct search QPS.

## Running

Build only Searchbench tools and prepare both sizes outside the isolated
network. No Luxir build or download is performed by these commands:

```bash
cmake --build driver/build --target bench_replay vector_tool -j2
scripts/run-vector.sh -s 100k --prepare
scripts/run-vector.sh -s 1m --prepare
```

For each size and binary use a separate new result directory, matching CPU,
network and cache settings. Set/restore CPU policy outside the wrapper using
the README workflow. The index follows `SEARCHBENCH_DATA_ROOT` (about 4.8 GB
at 1M), so point it at tmpfs and delete it after the run. For example:

```bash
LUXIR_BIN=/opt/code/searchbench-bins/luxir-20261028-fe83c142/luxir \
  scripts/run-isolated.sh scripts/run-vector.sh -s 100k --refeed \
  --outdir results/vector-100k-before
LUXIR_BIN=/opt/code/searchbench-bins/luxir-20261028-2a3c6877/luxir \
  scripts/run-isolated.sh scripts/run-vector.sh -s 100k --refeed \
  --outdir results/vector-100k-after
LUXIR_BIN=/opt/code/searchbench-bins/luxir-20261028-fe83c142/luxir \
  scripts/run-isolated.sh scripts/run-vector.sh -s 1m --refeed \
  --outdir results/vector-1m-before
LUXIR_BIN=/opt/code/searchbench-bins/luxir-20261028-2a3c6877/luxir \
  scripts/run-isolated.sh scripts/run-vector.sh -s 1m --refeed \
  --outdir results/vector-1m-after
```

The functional-only loopback smoke uses `--smoke --outdir NEW_DIRECTORY` with
the after binary. It uses a 10,000-row slice, exact truth for 1,000 queries,
an untimed probe exploration, and two request-count-limited replay cells
(k10/default and k100/nprobe64), followed by a report. Its timings are not
performance results. The server is stopped and its scratch index deleted.

## Not yet measured

Reference engines, hybrid/filter workloads, multi-segment search, updates,
vector dimensionality/codec tuning, cache-cold search, recall confidence
intervals and dataset-general ranking quality are not measured here.

## Decision log

- Keep RFC 4180 parsing in Python and float serialization/exhaustive scoring in
  a native helper, rather than parsing 1B floats or doing dot products in Python.
  This keeps the CSV path small and the numeric work practical without numpy.
- Use ordinal ids rather than wiki_id/paragraph_id: row ordinals directly join
  source vectors, transformed documents and engine-independent ground truth.
- Use double accumulation and ordinal tie breaking for reproducible exact
  ranking rather than inheriting an engine's SIMD rounding/tie behavior.
- Activate a concrete overlay selector on an empty commit rather than waiting
  for the final commit or relying on a wildcard to activate an unseen field.
  This enables the intended bulk-loader merge behavior from the start.
- Preserve ordinary feed topology and time force-merge separately rather than
  optimizing inside feed. This separates ingestion work from query preparation.
- Use a separate campaign entry point rather than adding vector branches to
  run-baseline.sh's text task board. It shares datasets, schema, feed, adapters,
  driver serialization/replay, samplers, contexts, controls and report modules;
  its task axis is k/probes/recall rather than text classes and count agreement.
- Add per-record expected payloads to the existing replay format rather than
  introducing a vector-only load generator. This validates every timed reply
  with the same transport and percentile machinery as full-text tasks.
- Use two 20-second repetitions and three probe settings rather than long
  windows over a fine sweep. Repetitions typically agree within 1 to 2% and the three
  settings span the useful recall range, so the shorter campaign keeps binary
  A/Bs routine. Concurrency/thread posture stays at the full-text default plus
  a serial cell; the functional smoke instead caps request count.
- Use binary-specific datasets and mandatory new output directories rather
  than sharing an index across the A/B. Reuse is useful for queries; --refeed
  is required to measure both arms' overlay construction and ingestion.
- Retain complete metric snapshots rather than inferring overlay build time
  from commit wall time. Build-attempt durations can overlap and include skips.
- Calibrate the probe sweep from 100k and 1M campaigns rather than the 10k
  smoke, whose list count is too small to predict larger recall.
- Keep the cell/context execution contract inside this entry point for now.
  A broader harness refactor should expose it as one reusable driver primitive;
  provenance and control matching belong to that contract alongside
  serialization and replay, so new workloads must keep those identities
  compatible.
