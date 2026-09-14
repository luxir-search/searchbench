# Secondary Search Benchmark Game workload

This is the retained broad query-shape suite derived from
[Search Benchmark Game](https://github.com/quickwit-oss/search-benchmark-game),
whose queries were sampled upstream from the AOL query log. It is
intentionally separate from Searchbench's primary luceneutil frequency
taxonomy and never enters the same headline aggregate. Run it with
`scripts/run-benchmark-game.sh`.

`queries-all.txt` is the upstream 962-query `queries.txt` verbatim, at the
commit pinned in `source.json`, followed by 48 queries added for Searchbench:
20 sloppy phrases and 28 boosted queries (11 boosting a mandatory clause, 17
an optional one). `queries.txt` contains the 978 queries whose exact hit
counts agreed across Luxir, OpenSearch, and Elasticsearch on both the 10M
standard and 33.3M scale corpora. `selection.json` preserves the complete
selection record.

The classes cover terms, conjunctions, disjunctions, phrases, sloppy phrases,
negation, mixed required and optional clauses, boosts, and a two-phase critic.
Classification precedence is `two_phase`, `boosted`, `sloppy_phrase`,
`negated`, `intersection_union`, `term`, `intersection`, `union`, then
`phrase`.

The workload is MIT licensed. See `LICENSE` in this directory.
