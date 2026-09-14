# Wikipedia corpus

Searchbench downloads the pinned English Wikipedia line-doc archive described
by `manifest.json`. It is luceneutil's LZMA-compressed
`enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt` artifact.

`scripts/prepare-corpus.sh` verifies the compressed archive before transforming
it. The line-doc header declares title, date, body, and random-label columns.
Each data row is one document. Searchbench preserves the body, uses the
zero-based line ordinal as the stable document ID, ignores the random label,
and adds deterministic benchmark fields. Tokenization and lowercasing are
engine schema decisions; corpus preparation does not remove punctuation,
digits, non-ASCII letters, or Wikipedia markup.

Luceneutil produced this workload by splitting Wikipedia articles at spaces
into chunks of at most 1,024 characters. Title and date repeat for every chunk
from an article. The artifact contains 33,332,620 documents. Searchbench uses
the first 100,000 for smoke, the first 10,000,000 for its standard lane, and
all documents for its scale query-selection lane. The scale transform carries
only ID and body because unrelated facet and sort fields cannot change a
full-text hit count. Because every lane is a prefix, a source line has the same
ID and synthetic fields everywhere those fields are present.

Wikipedia article text is reused under Wikipedia's content licenses. See
<https://en.wikipedia.org/wiki/Wikipedia:Copyrights> for attribution and reuse
terms. The archive is a luceneutil-packaged derivative rather than an official
Wikimedia dump. Its byte count and SHA-256 pin the exact input used by
Searchbench; the luceneutil URL is retrieval infrastructure, not corpus
identity.
