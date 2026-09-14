"""Canonical field and indexing posture for every engine."""

# Result and reuse boundary for physical field mappings plus ingest semantics,
# PER ENGINE: bump an engine's number only when THAT engine's on-disk
# expectations change (luxir: its physical index format; the REST engines:
# this file's declared mappings/settings posture for them). A luxir-only
# format change must not invalidate JVM indexes fed from an unchanged corpus
# and posture - deliberately comparing versions is normal harness use.
# Numbers start where the retired global constant left off.
INDEX_LAYOUT_VERSIONS = {
    "luxir": 3,  # sparse segment file table + single-file small segments (2026-08)
    "elasticsearch": 3,
    "opensearch": 3,
}


def index_layout_version(engine):
    return INDEX_LAYOUT_VERSIONS[engine]
ID_FIELD = "id"
BODY_FIELD = "body"
LUXIR_ANALYZER = {"tokenizer": "unicode_word", "filters": ["lowercase"]}
REST_ANALYZER = {"analyzer": "standard", "search_analyzer": "standard"}

LUXIR_ID_SCHEMA = {"type": "id", "index": "match", "column": True}
REST_ID_MAPPING = {"type": "keyword", "index": True, "doc_values": True}

LUXIR_SCHEMA = {
    "fields": {
        ID_FIELD: LUXIR_ID_SCHEMA,
        BODY_FIELD: {
            "type": "text",
            "stored": True,
            "analyzer": LUXIR_ANALYZER,
        }
    }
}

REST_BODY_MAPPING = {"type": "text", **REST_ANALYZER}

IDENTITY_POSTURE = {
    "luxir": {"field": ID_FIELD, **LUXIR_ID_SCHEMA,
              "internal_id": ID_FIELD, "ingest_mode": "allow_dups"},
    "opensearch": {"field": ID_FIELD, **REST_ID_MAPPING,
                   "internal_id": "auto_generated", "ingest_mode": "append_only"},
    "elasticsearch": {"field": ID_FIELD, **REST_ID_MAPPING,
                      "internal_id": "auto_generated", "ingest_mode": "append_only"},
}

ANALYZER_POSTURE = {
    "luxir": LUXIR_ANALYZER,
    "opensearch": REST_ANALYZER,
    "elasticsearch": REST_ANALYZER,
}
