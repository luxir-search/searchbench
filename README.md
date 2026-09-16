# Searchbench

Searchbench is the benchmark suite used to drive Luxir development.
OpenSearch and Elasticsearch are included as reference implementations; they
are the only comparison engines in scope.

The harness runs every engine through the same native HTTP replay driver,
validates responses before timing, separates client and server CPU sets, and
records result provenance.

## Quick start

Searchbench supplies its own corpus and downloads the reference distributions
when requested. The only project-specific prerequisite is a built Luxir
server. A sibling `../luxir` release build is discovered automatically;
otherwise set its path:

```bash
export LUXIR_BIN=/path/to/luxir
scripts/setup.sh
scripts/smoke.sh
```

The first setup builds the replay driver and downloads, verifies, and
transforms the pinned luceneutil Wikipedia corpus. It downloads a 5.87 GiB
compressed artifact once and reuses the verified cache thereafter.

Linux, Python 3.9 or newer, CMake, a C++20 compiler, and curl are required.
The C++ dependencies are declared in `driver/vcpkg.json`. GnuPG is needed only
to download OpenSearch.

## Run the development baseline

The default commands run Luxir only. Prepare the standard 10M-document corpus,
build a fresh Luxir index, and run the baseline with:

```bash
scripts/setup.sh --standard
scripts/run-isolated.sh scripts/run-baseline.sh
```

Prepare downloads and the corpus before entering the isolated network (see
[isolated network runs](#isolated-network-runs)). An existing index is
reused whenever its recorded corpus hash and physical index layout version
match; `BASELINE_REFEED=1` forces a fresh feed (for example, to time
indexing itself). The standard query baseline force-merges each engine to
one segment and asserts that topology around every measured cell. Result JSON
records every segment's live and deleted document counts; the rendered report
includes the complete stable layout.

Each fed shape lives in its own data directory, `data/<engine>/<corpus>-<layout>`,
so the standard campaign, the acceptance corpus, the facet corpus, and any
constructed multi-segment topology coexist and stay individually reusable
rather than displacing one another. `BASELINE_TOPOLOGY` runs the same task
board over a declared topology instead of the force-merged default:

```bash
BASELINE_TOPOLOGY=tiered-45 scripts/run-isolated.sh scripts/run-baseline.sh
scripts/run-isolated.sh scripts/quick.sh -T tiered-45 -t HIGH_TERM_TOP_10
```

```bash
scripts/run-isolated.sh scripts/quick.sh \
  -t HIGH_PHRASE_TOP_10,MED_SLOPPY_PHRASE_TOP_10,AND_HIGH_LOW_COUNT
```

Experiment dimensions are declared variants (`python/variants.py`):
comma-separated `key=value` deviations from the campaign baseline. Client
posture keys (`concurrency`, `threads`) set the replay driver, server posture
keys (`heap`) restart the server per posture group, and any other key
overrides a task parameter. Every task cell runs once per variant, results
land in slug-suffixed files (`luxir-HIGH_TERM_TOP_10@c16.json`), and the
report pivots the variant axis into column groups while warning about any
recorded configuration difference no variant declares:

```bash
BASELINE_VARIANTS="concurrency=1 concurrency=8 concurrency=16" \
  scripts/run-isolated.sh scripts/run-baseline.sh
scripts/run-isolated.sh scripts/quick.sh -t HIGH_TERM_TOP_10 -v concurrency=1 -v - -v concurrency=16
scripts/run-isolated.sh scripts/quick.sh -t FACET_10 -v facet_limit=100
```

The default full-text cells use luceneutil's term, AND, OR, phrase, and sloppy
phrase tasks split by source-corpus frequency. The older broad AOL-derived
query-shape workload is retained as a separate secondary campaign:

```bash
scripts/run-isolated.sh scripts/run-benchmark-game.sh
```

Its results use a separate output directory and are not combined with the
primary luceneutil cells.

The opt-in cache-first gate builds the named `tiered-45` TieredMergePolicy
steady-state topology from the same prepared 10M corpus and runs
changing-query/stable-filter cells plus three ordinary COUNT controls. It
defaults to Luxir; select REST references with the same comma-separated `-e`
syntax as `quick.sh`. The smaller `tiered-5` mechanism check remains available
through `-T`:

```bash
scripts/run-isolated.sh scripts/run-cache-first-multisegment.sh -d 5 -r 1
scripts/run-isolated.sh scripts/run-cache-first-multisegment.sh -e elasticsearch \
  -t FILTERED_RANGE_90_AND_HIGH_MED_TOP_10,HIGH_TERM_COUNT -d 5 -r 1
scripts/run-isolated.sh scripts/run-cache-first-multisegment.sh -T tiered-5 -d 5 -r 1
```

The lane verifies the complete declared document distribution after every
range and before/after every cell and writes `results/cache-first-<topology>`.
The constructed topology is kept as its own dataset, so it neither displaces
the standard one-segment index nor is rebuilt by the next campaign over the
same shape.

## Add the reference engines

Download the pinned OpenSearch and Elasticsearch distributions, then include
them in a smoke or baseline campaign:

```bash
scripts/setup.sh --standard --references
SMOKE_ENGINES=luxir,opensearch,elasticsearch scripts/smoke.sh
BASELINE_ENGINES="luxir opensearch elasticsearch" \
  scripts/run-isolated.sh scripts/run-baseline.sh
```

The reference downloads are lazy, so `scripts/start-opensearch.sh` or
`scripts/start-elasticsearch.sh` also downloads its missing distribution.

## Isolated network runs

Use `scripts/run-isolated.sh COMMAND ...` for performance measurements. It
creates a fresh Linux user/network namespace, brings up only loopback, and
runs the command with the caller's existing UID/GID. There is no hardcoded UID
and no sudo requirement. Engines, replay clients, ingestion, and health checks
must all run inside this invocation. Their `127.0.0.1` is independent of the
host's loopback, firewall rules, and container NAT/connection-tracking hooks.
Files, host PID numbers, CPU affinity, and the existing indices are retained.

Requirements: Linux 5.3 or newer with unprivileged user/network namespaces enabled,
util-linux (`unshare`, `setpriv`), iproute2 (`ip`), and Python 3.9 or newer.
Setup fails if isolation is unavailable; it never falls back to host
networking. Host firewall/container services need no changes.

The namespace has no external network connection. Build and prepare the
needed corpora and distributions first, then select a new output directory:

```bash
scripts/setup.sh --standard --references
BASELINE_ENGINES="luxir opensearch elasticsearch" \
BASELINE_OUTDIR="$PWD/results/baseline-exact-netns-$(date -u +%Y%m%dT%H%M%SZ)" \
  scripts/run-isolated.sh scripts/run-baseline.sh
```

Other campaign environment variables work unchanged. Stop any existing
Searchbench engines first. Only one campaign may use a checkout at a time:
data, logs, configuration, and PID files are shared. The wrapper refuses
existing engine PID files and overlapping isolated invocations. On command
exit, failure, SIGINT, or SIGTERM it stops any remaining namespace processes
(SIGTERM, then SIGKILL after 10 seconds) and clears their engine PID files.
Once the last process exits, the namespace disappears. SIGKILL of the wrapper
cannot run cleanup; inspect lingering processes/PID files before another run.

`quick.sh` normally leaves engines running. Through the wrapper they stop
when the wrapped command exits. To reuse one engine for multiple commands,
enter `scripts/run-isolated.sh bash`, run the commands inside that shell, then
exit it. Wrapping only `run-driver.sh` cannot reach a host-network engine.

CPU/power policy is separate and must be set and restored on the host, outside
the namespace. For the development hosts that provide `agent_do`, use an
outer shell with a cleanup trap (after setup/downloads):

```bash
(
  set -e
  trap 'sudo -n /usr/local/sbin/agent_do idle_settings' EXIT
  sudo -n /usr/local/sbin/agent_do benchmark_settings
  BASELINE_ENGINES="luxir opensearch elasticsearch" \
  BASELINE_OUTDIR="$PWD/results/baseline-exact-netns-$(date -u +%Y%m%dT%H%M%SZ)" \
    scripts/run-isolated.sh scripts/run-baseline.sh
)
```

On other machines use their normal CPU-policy controls. Keep explicit core
sets and CPU/cache settings consistent across compared runs. Result context
JSON records `host.network.mode`, `host.network.client_namespace`, and
`host.network.server_namespace` from the observed client/server processes.
An unwrapped run is recorded as `unmanaged`, since it may itself be inside a
container or externally managed namespace. Namespace IDs are local runtime
identifiers, not portable configuration identities. Rerun every compared
engine under the same network setup before publishing an isolated-network
board; retain host-network diagnostics in separate result directories.
Reports warn when network modes differ, including a mixture of isolated
results and historical results with no recorded network mode.

## Luxir healthcheck control

Full Luxir query campaigns automatically measure `GET /health` after each
serving configuration finishes its search cells, before stopping the server.
This applies to the standard and benchmark-game baselines, cache-first
multi-segment campaigns, and each grid. The control uses **32 connections and
four replay threads**, one second of connection warmup, and **three 10-second
measured repetitions**. These settings are fixed independently of the search
cell variants and their duration. Quick iteration and smoke checks do not add
this control.

`REPORT.md` (or the grid's report) includes a **Luxir transport control
(c32/t4)** section with median QPS, the repetition min/max, replay CPU use,
network mode, CPU assignments, OS version, and replay/Luxir binary hashes.
Raw results and per-repetition data live under `controls/` beside the report.
A missing control is shown as not recorded; a failed control is marked FAIL.
The control must follow the reported cells and match their Luxir process
session, binary hashes, CPU assignments, OS, and network setup. Stale or
mismatched controls are marked UNMATCHED, with the reason in the report.
Control rows do not enter the search comparisons, variant columns, or
exact-count agreement tables.

Health requests use the same replay executable, HTTP connection handling,
CPU assignments, network namespace, and running Luxir server as the query
cells. The endpoint performs no index work. This provides a control for
client and kernel/network changes across runs, while also including Luxir's
HTTP handling cost. Compare the raw control QPS across runs alongside the
search results; it is not a universal scaling factor for CPU-bound search.
The report does not automatically rescale search timings.

For a custom full campaign, call the shared helper inside the same namespace
after its Luxir cells and before server shutdown:

```bash
source scripts/engine-common.sh
record_health_control luxir "$pid" 9400 "$outdir"
```

An optional fifth argument is the campaign's variant specification; only its
server settings are retained, since the control fixes the client to c32/t4.
The helper records failures and returns nonzero if the control fails. Existing
controls are archived before a rerun so an old successful QPS cannot mask a
new failure. As with every measurement, keep the host CPU policy fixed until
the control finishes.

## Choose a corpus

The normal commands prepare their default corpus automatically. They can also
be materialized directly:

```bash
scripts/prepare-corpus.sh smoke    # 100K full-text acceptance corpus
scripts/prepare-corpus.sh standard # 10M development baseline corpus
scripts/prepare-corpus.sh facet    # 10M text-free facet corpus
scripts/prepare-corpus.sh scale    # 33,332,620-doc query-selection corpus
scripts/prepare-corpus.sh all
```

Generated corpora and the download cache live under `corpus/` and are ignored
by Git.

After changing the corpus, analyzer, or parser, regenerate the canonical
count-equivalent query set with:

```bash
scripts/select-queries.sh
```

This indexes the standard and scale corpora in all three engines and writes
its validation reports below `results/query-selection/`.

## Inspect and vary a run

Inspect the exact request for a task without sending it:

```bash
scripts/show.sh elasticsearch AND_HIGH_MED_TOP_10 --dry
```

The lower-level entry point is `scripts/run-driver.sh`. Select `--lane exact`
or `--lane skip`, and use `--set key=value` for a parameter variation. A
variation receives a distinct parameter identity, so reports cannot silently
combine it with the preset.

By default, Searchbench reserves the highest-numbered physical core and its
SMT siblings for the client, and assigns the remaining allowed CPUs to the
server. Override this with `CLIENT_CORES` and `SERVER_CORES`.

Results are written below `results/`; campaign scripts render a Markdown
report beside the result JSON. Reports show total and anonymous resident
memory separately, the effective startup parameters observed for each engine,
and one captured wire request for each request shape. The examples come from
the replay workload itself rather than being recreated by the report. Server
data, logs, PID files, downloaded distributions, and driver build products are
also ignored local artifacts.

## Benchmark definitions

The README describes how to operate the repository. The settings and rationale
for a competitive run live with that benchmark definition:

- [`docs/full-text-benchmark.md`](docs/full-text-benchmark.md) defines the
  standard full-text corpus, per-engine indexing posture, query workload,
  process settings, and known non-equivalences.
- [`docs/get-shape-spec.md`](docs/get-shape-spec.md) defines document lookup.
- [`corpora/wikipedia/README.md`](corpora/wikipedia/README.md) records the
  source corpus provenance and transformation.
- [`queries/luceneutil/README.md`](queries/luceneutil/README.md) records the
  primary query source, derivation, and exact-count selection.
- [`queries/benchmark-game/README.md`](queries/benchmark-game/README.md)
  records the separately runnable secondary query suite.

Pinned reference versions, licenses, and artifact verification rules live in
`engines/versions.json`. Searchbench itself is Apache License 2.0.

## Repository layout

| path | purpose |
|---|---|
| `driver/` | native HTTP replay loop and workload format |
| `python/` | adapters, workload definitions, validation, and reporting |
| `scripts/` | setup, lifecycle, feed, and campaign entry points |
| `config/` | node and heap overlays for the reference distributions |
| `corpora/` | pinned corpus provenance and lane definitions |
| `queries/` | pinned query sets, their upstream sources, and cross-engine selection records |
| `docs/` | benchmark definitions and request-shape contracts |
