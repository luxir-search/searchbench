# Document lookup shape

`GET_1`, `GET_10`, and `GET_100` read deterministic IDs from the corpus and
project `id` plus `price_i`.

- Luxir uses an ID-field match for one document and a Boolean union for batches.
  A constant-score wrapper avoids BM25 work for these identity lookups.
- OpenSearch and Elasticsearch use an exact `term` query for one document and
  a `terms` query for batches against the custom `id` keyword field.

The corpus ID has postings and doc values on every engine. Lookup uses postings;
`id` and `price_i` are projected from columns/doc values with source retrieval
disabled. OpenSearch and Elasticsearch generate their internal `_id`, keeping
indexing on the append-only fast path. Luxir feeds with `allow_dups=true`, which
turns off overwrite deletes. REST search hits still contain the generated
internal `_id` as protocol metadata, but Searchbench neither queries nor
validates it.

Using `_doc`/`_mget` would avoid the extra custom-field lookup, but it would
require making the corpus ID the REST `_id` and would force ingestion through
the overwrite-aware path. Searchbench chooses append-only indexing plus an
ordinary indexed/column-backed identity field because that is the target Luxir
development posture. The unavoidable REST cost is storing and emitting the
generated `_id` in addition to the corpus `id`.

Validation checks document count, projected fields, and the multiset of
returned corpus IDs. Lookup responses deliberately use no body-substring
failure rules because arbitrary returned values may contain those markers;
HTTP status and full validation are the contract.
