# Primary luceneutil full-text workload

`queries-all.txt` is a deterministic 940-query extraction from luceneutil's
`wikimedium.10M.tasks`. The extraction retains the first 50 queries in each of
17 source classes: 15 operator by frequency classes plus the constant-score
multi-term classes `Wildcard` and `Prefix3`. `HighPhrase` and
`HighSloppyPhrase` contain only 36 source queries, so both classes retain all
36. Source order is kept within every class.

Three multi-term classes are derived rather than read from the source.
`wildcard_scan` and `wildcard_lead` splice a LowTerm's affixes around a star
(`husband` becomes `h*band` and `*sband`), so each pattern is guaranteed at
least one matching term while term-dictionary scanning dominates the matched
set; the leading-star form denies every engine a seekable prefix. `regex` is
a curated 18-pattern set (luceneutil has no regexp task) whose automaton
structure decides how much of the scanned range the engine's term-dictionary
intersection can skip; each row records its intent.

The task generator draws literal terms from the index, including values such
as `user:ed`. A raw Lucene query parser instead reads the colon as a field
operator. Searchbench escapes expression-parser metacharacters in individual
term positions while preserving the source task's term, AND, OR, phrase,
sloppy-phrase, wildcard, or prefix operator. Each row retains the original
text as `source_query`. This executes the task generator's intended indexed
term against `body` rather than silently querying an unrelated field.

`source.json` pins the upstream commit, file, byte count, SHA-256, license, and
extraction policy. Run `scripts/update-luceneutil-queries.sh` to download and
verify that exact source and regenerate `queries-all.txt`. The checked-in
derived file means normal benchmark runs do not need that download.

`queries.txt` is selected from `queries-all.txt` by exact hit-count agreement
between Luxir, OpenSearch, and Elasticsearch on both the 10M standard corpus
and 33.3M scale corpus. `selection.json` records the corpora, analyzers,
engine versions, hashes, class counts, and every rejected query. Run
`scripts/select-queries.sh` after changing an analyzer, parser, or corpus.

The source is licensed under Apache License 2.0, the same license included at
the repository root.
