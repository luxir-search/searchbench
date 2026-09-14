# Full-text benchmark definition

This document defines the full-text portion of the standard Searchbench
baseline. It describes the settings implemented by `scripts/run-baseline.sh`
and its feed and request adapters, including settings that are similar rather
than physically identical across engines.

Searchbench exists to drive Luxir development. OpenSearch and Elasticsearch
are reference implementations. The comparison is an end-to-end search
measurement through each engine's public HTTP interface, including query
parsing, collection, response encoding, and transport.

## Run scope

The three-engine development baseline is started with:

```bash
scripts/setup.sh --references
BASELINE_ENGINES="luxir opensearch elasticsearch" scripts/run-baseline.sh
```

The default baseline also runs facet and collection tasks against the same
index. This document covers the 45 primary full-text cells. The executable presets in
`python/presets.py` and the result JSON are authoritative for an individual
run; environment or parameter overrides define a different run posture.

The default development run uses the `exact` lane, two 5-second measured
repetitions, eight concurrent HTTP connections, one replay event-loop thread,
sequential query order, and a one-second connection warmup after validation.
These short defaults are iteration settings, not publication-duration claims.

## Process and node settings

| Setting | Luxir | OpenSearch | Elasticsearch | Reason |
|---|---|---|---|---|
| Nodes | 1 | 1 | 1 | Compare a local, non-distributed search path. |
| Search partitions | one local collection | 1 primary shard | 1 primary shard | Avoid distributed fan-out and reduction. |
| Replicas | not implemented | 0 | 0 | Do not charge replication to a read benchmark. |
| Network | localhost | localhost | localhost | Keep transport local and reproducible. |
| Security | none | disabled | disabled | Authentication is outside the measured operation. |
| Storage | filesystem | Lucene filesystem index | Lucene filesystem index | Use each engine's normal persistent local backend. |
| Managed heap | none | 8 GiB fixed JVM heap | 8 GiB fixed JVM heap | Give both JVM references the same explicit heap; total and anonymous RSS are recorded for all engines. |
| Open files | at least 65,536 soft | at least 65,536 soft | at least 65,536 soft | Prevent the launching shell's limit from becoming a scale-ingest failure. |

The lifecycle scripts pin the server to `SERVER_CORES`. The replay driver and
sampler are pinned to the disjoint `CLIENT_CORES` set. The topology-derived
default reserves one complete physical core for the client; publication runs
must state explicit core sets and machine power policy.

Every result captures the effective live server executable and argv from
`/proc`, a whitelist of performance-relevant environment variables, observed
CPU affinity and open-file limits, and the exact Searchbench YAML/JVM-option
overlays used by the REST engines. This intentionally records the expanded JVM
process rather than reconstructing the launcher command after the fact.

Luxir is launched at warning log level with its filesystem backend, automatic
HTTP and gRPC thread counts, and the checked-directory sync verifier off. The
last setting removes the diagnostic wrapper around filesystem operations; it
does not disable the commit fsyncs themselves. Elasticsearch also disables its
machine-learning subsystem. Other unlisted server settings remain the pinned
distribution or Luxir binary defaults.

OpenSearch additionally sets
`search.concurrent_segment_search.mode: none` on the index. OpenSearch 3.x
concurrent segment search can make single-shard terms aggregations approximate
through slice-level truncation. The standard index is shared by full-text and
facet tasks, so the exact-count posture is fixed at index creation. It also
makes full-text execution serial within the shard, matching Elasticsearch's
native per-shard execution. A concurrent-segment-search comparison is a
separate A/B posture.

The current pinned reference versions and verified artifact identities are in
`engines/versions.json`. Every result records the reference version, Luxir
binary identity, observed CPU affinity, process file limits, corpus identity,
and host state.

The current reference run uses OpenSearch 3.7.0 from its Apache-2.0 official
Linux x64 distribution and Elasticsearch 9.4.2 from its Elastic License 2.0
official Linux x86_64 distribution, both with their bundled JDK. The Luxir arm
uses the locally built release binary and records its SHA-256 and repository
state. Elastic License 2.0 contains no benchmark-publication restriction;
Elasticsearch's free source is also available under AGPLv3. Updating a pinned
reference version changes the run definition.

## Corpus

The standard corpus is the first 10,000,000 rows of luceneutil's pinned
`enwiki-20120502-lines-1k-fixed-utf8-with-random-label` artifact. Luceneutil
split articles at spaces into chunks no longer than 1,024 characters. Title
and date repeat on each chunk.

Searchbench:

- uses the zero-based source row ordinal as the stable string `id`;
- preserves the decoded `body` value exactly;
- ignores the source random-label column; and
- adds deterministic string, numeric, date, sort, and selectivity fields used
  by the non-full-text tasks in the shared baseline.

It does not remove punctuation, Unicode, digits, or Wikipedia markup, and it
does not pre-tokenize or lowercase text. Those operations belong to the engine
analyzers.

The standard corpus is deliberately a shared, enriched index rather than a
text-only index. Consequently, indexing throughput reported from this feed
includes the cost of every baseline field, not only `body`. The 33,332,620-row
`scale` corpus is text-only, but it exists to select count-equivalent queries;
it is not the standard indexing-throughput corpus. Exact source URL, byte
count, SHA-256, lane sizes, and transformation details are in
`corpora/wikipedia/manifest.json` and `corpora/wikipedia/README.md`.

## Fields indexed

### Full-text field

| Engine | `body` definition | Why |
|---|---|---|
| Luxir | `type: text`, `stored: true`, tokenizer `unicode_word`, then `lowercase` | Unicode word boundaries plus lowercase is the closest explicit Luxir counterpart to the Lucene standard analyzer while retaining Luxir's normal retrievable text posture. |
| OpenSearch | `type: text`, index and search analyzer `standard` | Use the engine's Lucene standard-analysis path explicitly at both index and query time. |
| Elasticsearch | `type: text`, index and search analyzer `standard` | Same as OpenSearch. |

The analyzers are close counterparts, not byte-for-byte token-stream
equivalents. In particular, Unicode lowercasing details can differ. The query
selection procedure below prevents known count differences from entering the
timed set instead of pretending the analyzers are identical.

OpenSearch and Elasticsearch leave the `body` field's own `store` option off
and retain the input document through their default enabled `_source`. Luxir
stores the raw `body` field but has no whole-document `_source` equivalent.
Timed full-text requests do not retrieve either copy: Luxir projects `id` from
its column and the REST requests set `_source:false` and project `id` from doc
values. Disabling `_source` for an ingest or index-size experiment is supported
by `feed_rest.py --no-source`, but that is a different index posture from the
standard baseline.

### Identity field

| Engine | Corpus `id` | Internal identity and ingest mode | Why |
|---|---|---|---|
| Luxir | reserved `id`, exact match index, column enabled | `allow_dups:true` | Postings provide lookup and the column provides result projection; allowing duplicates disables overwrite deletes for this append-only corpus. |
| OpenSearch | `keyword`, `index:true`, `doc_values:true` | auto-generated `_id` | Supplying `_id` makes indexing overwrite-aware. An auto-generated internal ID preserves the append-only indexing fast path while the corpus field handles lookup and projection. |
| Elasticsearch | `keyword`, `index:true`, `doc_values:true` | auto-generated `_id` | Same as OpenSearch. |

REST search hits still contain the generated `_id` as protocol metadata.
Searchbench neither queries nor validates it. This is an unavoidable extra
stored identity in the REST engines under the append-only posture.

### Other standard-corpus fields

The shared 10M corpus also contains:

- exact string fields `cat_s`, `cat100_s`, `cat1k_s`, `cat10k_s`,
  `cat100k_s`, `cat1m_s`, `cat5m_s`, `catu2m_s`, `catid_s`, and the
  `sel*_s` selectivity ladder;
- integer fields `sort_i` and `price_i`; and
- date field `date_dt`.

Luxir resolves these through its suffix templates: `_s` is an exact-match
index plus column, while `_i` and `_dt` are columns without a points index.
OpenSearch and Elasticsearch map the strings as `keyword`, `sort_i` as
`long`, `price_i` as `integer`, and `date_dt` as `date`, using their normal
indexed and doc-value defaults. These physical structures are not identical,
but the full-text tasks do not query them. They remain present because one
baseline index supports the complete development suite.

Both REST mappings use `dynamic: strict`, so a corpus/schema mismatch fails
the feed instead of silently creating a new text field.

## Feed and segment posture

All engines receive 16 concurrent client-side ingest channels by default.
This aligns offered ingest concurrency, not internal indexing thread counts;
each server owns its own scheduling.

Sixteen is not equally kind to every engine, and the feed-throughput number
should be read with that in mind. A luxir feed is flat from one stream to
thirty-two, because the server batches and parallelizes a single connection;
the REST engines handle one bulk request on one write thread, so offered
connections are their ingest parallelism and their own optimum lies higher.
Sixteen is kept because equal offered concurrency is the comparable posture,
not because it is any engine's optimum; `scripts/ingest-streams.sh` sweeps the
dimension for whichever engine you point it at.

| Engine | Feed shape | Refresh and commit | Merge posture |
|---|---|---|---|
| Luxir | The corpus is split into 16 record-aligned ranges, one per streaming update connection, shipped as 4 MiB transport blocks. The server cuts its own update batches out of each stream (1 MiB or 10,000 documents) and keeps several in flight at once. | Updates remain unpublished until one final durable commit after all streams drain; the commit uses `wait_for_merges:true`. | Each inverter auto-flushes at 64 MiB or 8,388,608 documents, whichever comes first. The shared index RAM cap is unlimited and merge factor is 10. The timed feed does not request a one-segment optimization. |
| OpenSearch | 16 concurrent `_bulk` connections, 1,000 documents per bulk request. | `refresh_interval:-1` during feed, then restored to `1s`, followed by an explicit refresh. Default translog durability is `request`. | The normal indexing buffer is 10% of the 8 GiB heap. The default translog flush threshold is 512 MiB. The timed feed does not request a force merge. |
| Elasticsearch | 16 concurrent `_bulk` connections, 1,000 documents per bulk request. | `refresh_interval:-1` during feed, then restored to `1s`, followed by an explicit refresh. Default translog durability is `request`. | The normal indexing buffer is 10% of the 8 GiB heap. The default translog flush threshold is 10 GiB. The timed feed does not request a force merge. |

Transport block and bulk sizes are native API choices, not a claimed common
unit of indexing work. Each fed shape owns a data directory named after its
corpus and layout, `data/<engine>/<corpus>-<layout>`, because an engine opens
every index in its data directory: the standard force-merged campaign, a
constructed tiered topology, the acceptance corpus, and the facet corpus
coexist instead of displacing one another, and each stays reusable. A run
reuses its dataset whenever the recorded corpus hash and index-layout version
match; `BASELINE_REFEED=1` forces a fresh feed, which is also how indexing
itself is timed. Construction always runs in its own server session, so ingest
posture never reaches a measured one and a reused index is served by exactly
the posture a freshly fed one is.

After the feed measurement completes, the standard query baseline explicitly
force-merges every index to one segment: Luxir commits with `max_segments:1`
and `wait_for_merges:true`; OpenSearch and Elasticsearch run synchronous
`_forcemerge?max_num_segments=1`. This maintenance time is outside the native
feed-throughput interval. The driver requires exactly one committed serving
segment before and after every measured cell.

The scalar count is not the provenance record. Every result contains the
complete serving topology sampled immediately before and after measurement:
engine segment identity, shard, maximum documents, live documents, deleted
documents, and bytes where the engine exposes them. Luxir obtains this from
`/_stats?segments=true`; the REST engines use `/_segments`. A changed topology,
an active merge (sampled from the same Luxir response or REST `_stats/merge`),
an uncommitted segment, or a count differing from the campaign's declared
expectation fails the cell.

Multi-segment query campaigns are explicitly in scope. They will use a named,
deterministically constructed topology and a declared expected segment count;
the same artifact will show the document distribution across those segments.
They must not inherit whatever segment layout happened to exist when indexing
finished.

Named multi-segment topologies are opt-in campaigns over the standard 10M
corpus, and one shared declaration supplies each exact largest-first range
vector to all three engines. The feed rejects a corpus whose length is not
exactly the declaration's sum.

`tiered-45` is the default multi-segment campaign shape because production
indexes commonly remain at 50 or more serving segments and can approach 100.
It models a TieredMergePolicy steady state with nine segments in each of five
10x size bands: nine near 1M documents, then nine each at 100K, 10K, 1K, and
100 documents. Nine exact bands sum to 9,999,900, so the largest segment
absorbs the 100-document remainder: it is 1,000,100 documents beside eight
1,000,000-document peers. The result is exactly 10M documents in 45 segments
without adding an artificial sixth tier.

The earlier `tiered-5` shape remains available with `-T tiered-5`. Its ranges
are 4,000,000, 3,000,000, 1,500,000, 1,000,000, and 500,000. It is a fast
mechanism/correctness check, not the worst-case cache-first profile.

Luxir feeds every declared range through one serialized stream. A range may
hit the bounded inverter RAM cap; its boundary commit force-merges the complete
index to the number of ranges fed so far. Luxir's force merge combines the
smallest segments, so descending construction consolidates only the new range
and preserves every earlier, larger tier. The merge factor is raised to keep
background policy merges out of the construction. The live named-topology
probe asserts the exact declared prefix immediately after every commit,
including each of `tiered-45`'s nine 100-document tail streams.

OpenSearch and Elasticsearch create a one-primary, zero-replica index with
automatic refresh disabled. Each declared range may use concurrent bulk
connections, but every connection drains before the boundary. The feed then
refreshes, flushes, and synchronously force-merges the complete index with
`max_num_segments` equal to the number of ranges fed and `flush=true`. A raised
`segments_per_tier` keeps background TieredMergePolicy work from racing those
explicit boundaries. After every range, not just at completion, `_segments`
plus `_stats/merge` must show the exact declared prefix vector, one shard, zero
deletes, the declared live total, and no merge activity. The temporary translog,
merge-policy, and refresh settings are restored after construction; the final
shape is asserted again after restoration.

The driver repeats the complete named-topology assertion around every measured
cell and records the name, declaration, and observed per-segment distribution
in each result. `scripts/run-cache-first-multisegment.sh -e` accepts the same
comma-separated engine syntax as `quick.sh`; omitting it remains Luxir-only.
The script defaults to `tiered-45`; `-T` selects another declared campaign
shape.
A constructed topology is a dataset like any other: it is built into its own
`data/<engine>/10m-searchbench-<topology>` directory, so it never replaces the
standard index, and a later campaign over the same topology reuses it instead
of rebuilding it. `BASELINE_TOPOLOGY` runs the ordinary task board over one of
these shapes, which is how the same 66-task board is measured over one segment
and over `tiered-45` without either feed displacing the other.

Feed throughput is diagnostic in this baseline, not yet a competitive indexing
metric. Luxir publishes one commit at the end, while the REST engines use their
request-durable translogs throughout the feed. Luxir's final commit waits for
its scheduled merges; the REST feed performs a refresh but does not explicitly
wait for every background merge to finish. The later one-segment maintenance
step controls query topology but does not make the preceding feed rates a
competitive indexing comparison. Such a comparison must first choose and
document one durability contract and one end-of-feed merge boundary for all
three engines.

## Query workload

The primary source is luceneutil's pinned `wikimedium.10M.tasks`. Searchbench
uses its operator by source-frequency taxonomy instead of pooling queries by
syntax alone:

| Operator | High | Medium | Low |
|---|---|---|---|
| Term | `high_term` | `med_term` | `low_term` |
| AND, high plus tier | `and_high_high` | `and_high_med` | `and_high_low` |
| OR, high plus tier | `or_high_high` | `or_high_med` | `or_high_low` |
| Phrase | `high_phrase` | `med_phrase` | `low_phrase` |
| Sloppy phrase, slop 4 | `high_sloppy_phrase` | `med_sloppy_phrase` | `low_sloppy_phrase` |

Five constant-score multi-term classes join the operator taxonomy. Lucene
rewrites these query types under its constant-score rewrite by default, so
none of them measure ranking; what separates them is where the term-dictionary
work lands, between expanding many matching terms with heavy postings on one
side and scanning a large term range that yields few survivors on the other:

- `wildcard`: luceneutil's `Wildcard` tasks, high-frequency stems with an
  interior `*` (`th*e`). Expansion- and postings-dominated: on the 10M
  corpus `th*e` counts 8.3M documents in tens of milliseconds.
- `prefix3`: luceneutil's `Prefix3` tasks, three-character prefixes (`mos*`).
  Pure subtree expansion, no filtering.
- `wildcard_scan`: derived, a LowTerm's first letter and last four letters
  around a star (`husband` becomes `h*band`). The engine scans one
  first-letter subtree under a suffix accept and keeps little of it.
- `wildcard_lead`: derived, a leading star before a LowTerm's last five
  letters (`*sband`). No engine gets a seekable prefix, so the whole term
  dictionary is visited; on the 10M corpus these cost roughly 0.4-0.6 s
  almost independently of how few documents they match.
- `regex`: a curated pattern set (luceneutil has no regexp task), chosen so
  automaton structure decides how much of the scanned range the engine's
  term-dictionary intersection can skip. The ladder runs from
  every-position-constrained shapes (`[0-9]{4}`, and `(19|20)[0-9]{2}`, which
  reaches the same result mass twice as fast through two narrow prefixes),
  through anchored subtrees with acceptance tails (`(re|un)[a-z]*ing`), to
  unanchored full scans (`.*ing`, `.*[0-9]`). Each row records its intent.
  Cross-engine count agreement on these patterns doubles as the semantic
  check of each engine's regexp automaton; a wildcard-translation twin class
  was rejected because it would only repeat the wildcard measurements.

Each of these 20 classes is crossed with top 10, top 100, and count, producing
60 named cells. Reports keep the classes separate; they do not publish one
geomean that combines different operators, frequency bands, or the secondary
workload.

Unanchored patterns visit every term, so their counts aggregate any
difference between the engines' term dictionaries; they are structurally the
most likely rows for the cross-corpus agreement gate to reject, and that
rejection is information about the analyzers, not noise.

The stable-filter family adds four opt-in cells without changing those primary
definitions:

- `FILTERED_RANGE_90_AND_HIGH_MED_TOP_10` and `_COUNT`;
- `FILTERED_RANGE_1_AND_HIGH_MED_TOP_10` and `_COUNT`.

The main query cycles through the existing `and_high_med` luceneutil pool. Each
replay record wraps it in a scored optional disjunction with a unique exact-ID
term whose reserved non-numeric value cannot occur in the pinned corpus. The
extra alternative therefore changes neither matches nor scores, but makes the
composite logical membership key distinct for every record. The FILTER child
is fixed across the cell: a numeric `price_i` range `[0,90000)` or `[0,1000)`,
declaring the dense 90% or sparse 1% posture in both the name and parameters.
Luxir emits its Boolean FILTER plus numeric range syntax; OpenSearch and
Elasticsearch emit the equivalent `bool.filter` and `range` clause. The nonce
alternative is an exact `id` term in every adapter. All engines therefore
validate the complete response for the same filtered query before timing.

Each filtered cell materializes 200,000 unique requests, more than twelve times
the largest five-second volume observed in the initial multi-segment smoke. The
lane validates every request through the normal untimed pass,
disables the replay driver's additional connection warmup, uses one measured
repetition, caps it before the workload can wrap, and fails if the pool is
exhausted before the requested duration. Thus adjacent and later measured
requests do not revisit a whole-query key while the same filter-clause key is
continually reused. The campaign also runs ordinary HIGH_TERM,
AND_HIGH_MED, and HIGH_SLOPPY_PHRASE COUNT controls on the identical topology.

`queries/luceneutil/queries-all.txt` contains a deterministic 940-query
extraction: the first 50 source queries per class, except the two high phrase
classes, which contain only 36 each upstream, plus the derived scan classes
and the 18 curated regex patterns. The default `queries.txt` is
derived by requiring exact hit-count agreement across all three engines on
both the 10M standard corpus and 33.3M scale corpus. Every rejection, count,
analyzer, binary, version, and input hash is recorded in
`queries/luceneutil/selection.json`. The selection may lag the source by
whole classes while newly added classes await the next agreement run;
`queries.txt` contains no queries from a pending class, so those cells cannot
run against an unadjudicated workload by accident.

Luceneutil's generator draws literal terms from the body term dictionary. Some
contain query-parser metacharacters, for example `user:ed`. Passing that text
unescaped to a Lucene query parser changes it into a field query even though
the generator intended the single body term. Searchbench escapes
metacharacters within term positions while preserving the task's term, AND,
OR, phrase, or sloppy-phrase operator. The original source query is retained
beside every derived expression.

The resulting expression reaches each engine's parser intact:

- Luxir receives an expression query scoped as `body:(...)`.
- OpenSearch and Elasticsearch receive `query_string` with `body` as the
  default field and `OR` as the default operator.

This retains required clauses, disjunctions, exact phrases, and phrase slop.
Parsing is intentionally part of the measured request. Flattening the source
into `match` queries would define a different workload.

The multi-term classes need two exceptions. Luxir's query language keeps
mid-word `*` and `/re/` as ordinary characters by design and exposes those
query types as named functions, so the `wildcard` and `regex` classes call
`wildcard('th*e', field=body)` and `regex('th.*e', field=body)` directly;
trailing-star prefixes parse natively. On the REST engines, `wildcard` and
`prefix3` flow through `query_string` unchanged, while `regex` uses the
structured `regexp` query, which reaches the same Lucene `RegexpQuery` without
`query_string`'s `/re/` wrapping and its extra escaping rules.

The Search Benchmark Game-derived AOL query set remains runnable with
`scripts/run-benchmark-game.sh`. It supplies broader shapes such as negation,
mixed required and optional clauses, and boosts, but is a separately named
secondary campaign with separate results. Its provenance, prior exact-count
selection, and MIT license are under `queries/benchmark-game/`.

## Collection and validation

Top-10 and top-100 tasks rank with each engine's normal scoring path and return
only the corpus `id`. Luxir projects it from its column; OpenSearch and
Elasticsearch use `docvalue_fields` with `_source:false`. Searchbench does not
claim identical scoring implementations or identical top-doc order. These
tasks explicitly do not request a total match count: COUNT owns that operation,
and combining it with top-K disables pruning differently across engines.

Count tasks return no documents. In the `exact` lane, Luxir sets
`get_number:true` and the REST engines set `track_total_hits:true`. The REST
shard request cache is explicitly disabled for every benchmark search shape,
including top-N, count, facet, and GET. This prevents a repeated request from
returning a cached whole response; it does not disable Lucene's query/filter
cache. The cached-response posture remains available only as an explicit
`request_cache=true` A/B. The separate `skip` lane omits exact-total work where
the engine permits it and has a distinct parameter identity.

Before timed replay, every request in the workload receives an untimed request
whose complete response is parsed and checked. Timed responses are checked for
HTTP and engine failure markers. Reports only combine results whose resolved
parameters have the same comparable hash. Exact-count agreement applies to
operations that request totals; top-K cells validate their returned payload but
do not repeat COUNT's work merely to populate the agreement table.

While writing the opaque replay workload, Searchbench captures the first
request in every replay bucket. The result records its method, path, original
body, serialized headers, and a SHA-256 of the exact wire bytes. Reports render
these captures for each material request shape (for example TOP_10, TOP_100,
and COUNT); they do not rebuild examples from the current adapter and thereby
misrepresent an older result.

## Known non-equivalences

The benchmark aligns the requested operation, corpus values, offered
concurrency, and result validation. It does not claim identical engine
internals. Material differences are:

- Luxir's analyzer and the Lucene standard analyzer are close, not identical;
  canonical queries are filtered by exact-count agreement.
- REST engines store a generated internal `_id` in addition to corpus `id`.
- The shared synthetic fields use analogous native structures, not identical
  codecs or indexing strategies.
- Luxir is a native process while the reference engines run on fixed-heap
  bundled JVMs. Reports show total RSS, which includes resident file-backed
  mappings, and anonymous RSS, which approximates resident heap, allocator
  arenas, stacks, and other anonymous allocations.
- Ingest request framing and background merge scheduling are engine-native.
- Exact count agreement does not prove identical matching-document sets,
  scoring, or top-doc order.

These are boundaries of the comparison, not settings to erase silently. A run
that changes one of them should record a distinct posture rather than reuse the
standard baseline name.

Searchbench and the primary luceneutil query source are Apache License 2.0.
The secondary Search Benchmark Game-derived workload is MIT licensed, with its
license beside the workload. Wikipedia content retains its own attribution and
reuse terms, recorded with the corpus manifest.
