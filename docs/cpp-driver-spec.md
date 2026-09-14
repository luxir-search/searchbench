# Native replay contract

Python owns benchmark semantics: task resolution, Lucene query taxonomy,
engine translation, validation, provenance, and result assembly. The C++
driver owns only the timed closed loop.

The workload file contains a small header followed by bucket labels and opaque,
length-prefixed HTTP/1.1 requests. Each connection replays one request at a
time. Responses are framed with llhttp; non-2xx status, malformed framing,
timeouts, connection failures, and adapter-supplied byte markers count as
errors.

Every measured repetition reports overall and per-bucket HDR histograms,
request/error counts, elapsed time, and server process CPU time. Python rejects
bucket-label drift and folds those metrics into the versioned result schema.

The validation pass is intentionally separate and untimed. It fully parses one
pass of responses, checks document/facet/stat payloads, and gathers exact-count
samples across all three engines before the native loop performs cheap response
checks.
