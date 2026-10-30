"""Canonical field and indexing posture for every engine."""

import os

# Result and reuse boundary for physical field mappings plus ingest semantics,
# PER ENGINE: bump an engine's number only when THAT engine's on-disk
# expectations change (luxir: its physical index format; the REST engines:
# this file's declared mappings/settings posture for them). A luxir-only
# format change must not invalidate JVM indexes fed from an unchanged corpus
# and posture - deliberately comparing versions is normal harness use.
# Numbers start where the retired global constant left off.
INDEX_LAYOUT_VERSIONS = {
    "luxir": 4,  # current schema history + explicit long-term truncation (2026-09)
    "elasticsearch": 3,
    "opensearch": 4,  # numeric fields keep a doc-values skipper beside points (2026-10)
}


def index_layout_version(engine):
    return INDEX_LAYOUT_VERSIONS[engine]


ID_FIELD = "id"
BODY_FIELD = "body"
# Projectable fields with no REST doc values: a hit returns them from _source.
TEXT_FIELDS = frozenset({BODY_FIELD})
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
            "long_terms": "truncate",
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
    "luxir": {**LUXIR_ANALYZER,
              "long_terms": LUXIR_SCHEMA["fields"][BODY_FIELD]["long_terms"]},
    "opensearch": REST_ANALYZER,
    "elasticsearch": REST_ANALYZER,
}

# Numeric fields every corpus variant carries, with their REST types. Luxir
# resolves each through the suffix template its name ends in.
NUMERIC_FIELDS = {"sort_i": "long", "price_i": "integer", "date_dt": "date"}

# Numeric fields keep a doc-values skipper - per-block min/max that lets range
# filters, sort pruning and histograms skip blocks - wherever the engine can
# build one. This setting decides whether a points (BKD) index sits beside it:
#   default  each engine's own default: points on OpenSearch and
#            Elasticsearch, none on Luxir, whose numeric templates are columns
#   on       points everywhere; Luxir declares the fields index:"range"
#   off      no points anywhere. Every engine then does the same numeric
#            indexing work as Luxir's default column, which is what the ingest
#            benchmarks run.
# OpenSearch builds both structures (skip_list beside index:true).
# Elasticsearch cannot: it builds a skipper only for a field that is not
# indexed, and only when index.mapping.use_doc_values_skipper is set, so with
# points it has none. Luxir's skipper is the zone map every numeric column
# carries (exact min/max per 4096 values and per 512).
NUMERIC_POINTS_ENV = "SEARCHBENCH_NUMERIC_POINTS"
NUMERIC_POINTS_SETTINGS = ("default", "on", "off")
DEFAULT_NUMERIC_POINTS = {"luxir": False, "elasticsearch": True, "opensearch": True}


def numeric_points_setting(value=None):
    """VALUE, else $SEARCHBENCH_NUMERIC_POINTS, else "default"."""
    if value is None:
        value = os.environ.get(NUMERIC_POINTS_ENV) or "default"
    if value not in NUMERIC_POINTS_SETTINGS:
        raise ValueError(f"{NUMERIC_POINTS_ENV}={value!r}; "
                         f"expected one of {list(NUMERIC_POINTS_SETTINGS)}")
    return value


def numeric_posture(engine, setting):
    """What ENGINE's numeric fields physically carry under SETTING."""
    points = (DEFAULT_NUMERIC_POINTS[engine] if setting == "default"
              else setting == "on")
    return {"fields": sorted(NUMERIC_FIELDS), "setting": setting,
            "points": points, "skipper": engine != "elasticsearch" or not points}


def numeric_posture_label(posture):
    return "+".join(name for name in ("points", "skipper") if posture[name])


def rest_numeric_mapping(engine, points):
    """(properties, index settings) for a REST index's numeric fields."""
    properties = {}
    for field, rest_type in NUMERIC_FIELDS.items():
        mapping = {"type": rest_type}
        if not points:
            mapping["index"] = False
        if engine == "opensearch":
            mapping["skip_list"] = True
        properties[field] = mapping
    settings = ({"mapping.use_doc_values_skipper": True}
                if engine == "elasticsearch" and not points else {})
    return properties, settings


def luxir_schema(points):
    """LUXIR_SCHEMA, plus a points index on every numeric field when POINTS."""
    if not points:
        return LUXIR_SCHEMA
    fields = dict(LUXIR_SCHEMA["fields"])
    for field in NUMERIC_FIELDS:
        fields[field] = {"parent": "_" + field.rsplit("_", 1)[1], "index": "range"}
    return {**LUXIR_SCHEMA, "fields": fields}


def vector_ann(codec):
    """PQ defaults/explicit m, or RaBitQ defaults/explicit bits."""
    if codec.startswith("pq-m") and codec[4:].isdigit() and int(codec[4:]) > 0:
        return {"codec": "pq", "pq": {"m": int(codec[4:])}}
    if codec == "pq":
        return {}
    if codec == "rabitq":
        return {"codec": "rabitq"}
    bits = codec.removeprefix("rabitq")
    if bits != codec and bits.isdigit() and 1 <= int(bits) <= 9:
        return {"codec": "rabitq", "rabitq": {"bits": int(bits)}}
    raise ValueError(f"unknown vector codec: {codec}")


def luxir_vector_schema(codec="pq", dataset="cohere"):
    """Dataset columns and metric with the selected ANN codec settings."""
    from vector_datasets import dataset_manifest
    manifest = dataset_manifest(dataset)
    ann = vector_ann(codec)
    if "pq" in ann and manifest['dims'] % ann['pq']['m']:
        raise ValueError("PQ m must divide dimensions")
    schema = {"fields": {
        "id": dict(LUXIR_ID_SCHEMA),
        **{field: {"type": "text", "stored": True,
                   "analyzer": LUXIR_ANALYZER, "long_terms": "truncate"}
           for field in ("title", "text")},
        "url": {"type": "string", "stored": True, "index": "none", "column": False},
        "embedding": {"type": "vector", "dims": manifest["dims"], "metric": manifest["metric"], "ann": ann},
    }}
    if dataset != "cohere":
        schema['fields'] = {k: v for k, v in schema['fields'].items() if k in ('id', 'embedding')}
    return schema
