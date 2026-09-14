"""Full-text query taxonomies and workload loading."""

from dataclasses import dataclass
import json


# The primary workload follows luceneutil's operator by frequency taxonomy.
# These stable names are direct snake-case forms of the task labels in
# wikimedium.10M.tasks.
LUCENEUTIL_CLASS_MAP = {
    "HighTerm": "high_term",
    "MedTerm": "med_term",
    "LowTerm": "low_term",
    "AndHighHigh": "and_high_high",
    "AndHighMed": "and_high_med",
    "AndHighLow": "and_high_low",
    "OrHighHigh": "or_high_high",
    "OrHighMed": "or_high_med",
    "OrHighLow": "or_high_low",
    "HighPhrase": "high_phrase",
    "MedPhrase": "med_phrase",
    "LowPhrase": "low_phrase",
    "HighSloppyPhrase": "high_sloppy_phrase",
    "MedSloppyPhrase": "med_sloppy_phrase",
    "LowSloppyPhrase": "low_sloppy_phrase",
    # Constant-score multi-term shapes. Lucene rewrites wildcard and prefix
    # (and regexp) under the constant-score rewrite by default, so these cells
    # measure term-dictionary work plus unscored membership rather than
    # ranking. The luceneutil patterns are high-frequency stems, so this pair
    # is expansion- and postings-dominated.
    "Wildcard": "wildcard",
    "Prefix3": "prefix3",
}
# Classes the importer derives rather than reads from luceneutil, covering the
# multi-term cost axes its Wildcard tasks do not: wildcard_scan and
# wildcard_lead make term-dictionary scanning dominate (a large scanned range
# with few surviving terms), and regex is a curated set whose automaton
# structure decides how much of that scan an engine's intersection can skip.
DERIVED_QUERY_CLASSES = ("wildcard_scan", "wildcard_lead", "regex")
PRIMARY_QUERY_CLASSES = tuple(LUCENEUTIL_CLASS_MAP.values()) + DERIVED_QUERY_CLASSES

# The Search Benchmark Game-derived workload remains available as a separate
# secondary campaign. More specific tags precede structural fallbacks in
# classify().
BENCHMARK_GAME_QUERY_CLASSES = (
    "term",
    "intersection",
    "union",
    "phrase",
    "sloppy_phrase",
    "negated",
    "intersection_union",
    "boosted",
    "two_phase",
)
SUPPORTED_QUERY_CLASSES = PRIMARY_QUERY_CLASSES + BENCHMARK_GAME_QUERY_CLASSES


@dataclass(frozen=True)
class QueryItem:
    text: str
    query_class: str
    tags: tuple
    source: str
    ids: tuple = ()
    # Stable-filter campaigns wrap the source query with one guaranteed-miss
    # exact-ID alternative. This preserves matches and scores while making the
    # composite logical membership identity distinct for every replay record.
    nonce: str = ""


def classify(text, tags=(), declared=None):
    """Return one stable class from an explicit declaration, tags, or syntax."""
    if declared is not None:
        if declared not in SUPPORTED_QUERY_CLASSES:
            raise ValueError(f"unknown declared query class {declared!r}")
        return declared

    for tag in tags:
        if tag in SUPPORTED_QUERY_CLASSES:
            return tag
        if tag.startswith("luceneutil:"):
            source_class = tag.split(":", 1)[1]
            if source_class in LUCENEUTIL_CLASS_MAP:
                return LUCENEUTIL_CLASS_MAP[source_class]

    primary = {tag.split(":", 1)[0] for tag in tags}
    if "two-phase-critic" in primary:
        return "two_phase"
    if "boosted" in primary:
        return "boosted"
    if "sloppy_phrase" in primary:
        return "sloppy_phrase"
    if "negated" in primary:
        return "negated"
    if "intersection_union" in primary:
        return "intersection_union"
    for query_class in ("term", "intersection", "union", "phrase"):
        if query_class in primary:
            return query_class

    # Older workload entries did not tag most phrases. Syntax is an adequate
    # fallback only when the taxonomy is absent; explicit tags always win.
    if len(text) >= 2 and text[0] == '"' and text.rfind('"') > 0:
        return "sloppy_phrase" if text[text.rfind('"') + 1:].startswith("~") else "phrase"
    if text.startswith("+") and " -" in text:
        return "negated"
    if text.startswith("+") and " +" in text:
        return "intersection"
    return "term" if len(text.split()) == 1 else "union"


def read_queries(path):
    """Read an NDJSON query file, preserving its explicit class and tags."""
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            entry = json.loads(line)
            tags = tuple(entry.get("tags", ()))
            text = entry["query"]
            query_class = classify(text, tags, entry.get("class"))
            if query_class not in SUPPORTED_QUERY_CLASSES:
                raise ValueError(f"{path}:{line_number}: unknown query class {query_class!r}")
            yield QueryItem(text, query_class, tags, str(path))


def read_id_batches(path, batch_size, max_queries=0):
    """Read deterministic, all-hit get work items from the start of a corpus."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    items = []
    ids = []
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            doc_id = value.get("id")
            if not isinstance(doc_id, str) or not doc_id:
                raise ValueError(f"{path}:{line_number}: document has no string id")
            ids.append(doc_id)
            if len(ids) != batch_size:
                continue
            label = ids[0] if batch_size == 1 else f"{ids[0]}..{ids[-1]}"
            items.append(QueryItem(label, "get", (), str(path), tuple(ids)))
            ids.clear()
            if max_queries and len(items) == max_queries:
                break
    if not items:
        raise RuntimeError(f"corpus produced no complete batches of {batch_size} ids")
    return items


def searchbench_queries(queries_path, _commands_path=None):
    """Return the same tagged workload for ranked and count requests.

    The old harness read randomized command captures, which discarded tags
    and excluded entire query classes. Replay already randomizes deterministically,
    so the canonical query file is both simpler and preserves the taxonomy.
    """
    queries = list(read_queries(queries_path))
    if not queries:
        raise RuntimeError(f"query source is empty: {queries_path}")
    return {"top": queries, "count": queries}
