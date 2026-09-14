#!/usr/bin/env python3
"""Derive Searchbench NDJSON from a pinned luceneutil task file."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re

from query_source import LUCENEUTIL_CLASS_MAP


EXPECTED_SOURCE_COUNTS = {
    "HighTerm": 84,
    "MedTerm": 500,
    "LowTerm": 500,
    "AndHighHigh": 500,
    "AndHighMed": 500,
    "AndHighLow": 500,
    "OrHighHigh": 500,
    "OrHighMed": 500,
    "OrHighLow": 499,
    "HighPhrase": 36,
    "MedPhrase": 500,
    "LowPhrase": 500,
    "HighSloppyPhrase": 36,
    "MedSloppyPhrase": 500,
    "LowSloppyPhrase": 500,
    "Wildcard": 500,
    "Prefix3": 500,
}
# Wildcard and Prefix3 lines carry no " # freq=..." metadata in the source.
TASK_LINE = re.compile(r"^([A-Za-z0-9]+): (.*?)(?: # (.+))?$")
QUERY_PARSER_SPECIAL = frozenset("+-&|!(){}[]^\"~*?:\\/")
# In a multi-term pattern, * and ? are the operators; everything else is
# literal. The pinned source only uses [a-z0-9*], so this is a guard against a
# future source revision, not a live code path.
PATTERN_SPECIAL = QUERY_PARSER_SPECIAL - frozenset("*?")
# Derived scan patterns splice affixes of a real indexed term, so every
# pattern is guaranteed at least one matching term on the pinned corpus.
DERIVED_TERM = re.compile(r"[a-z0-9]{6,}")

# The curated regex class. luceneutil has no regexp task, and translating the
# Wildcard tasks would only repeat their automata. These patterns instead vary
# how much of the scanned term range the automaton lets an engine's
# term-dictionary intersection skip: from every-position-constrained shapes
# that seek precisely, through anchored subtrees with acceptance tails, to
# unanchored shapes that must visit the whole dictionary. Cross-engine count
# agreement on these is also the semantic check of each engine's regexp
# automaton; a wildcard-translation twin class would add no validation beyond
# that and was rejected.
REGEX_TASKS = (
    # Every position constrained: intersection can seek, never scan.
    ("[0-9]{4}", "ten sibling digit subtrees, exact depth; year-heavy result"),
    ("(19|20)[0-9]{2}", "the [0-9]{4} result mass through two narrow prefixes"),
    ("[0-9]{4}s", "digit subtrees with a literal tail position"),
    ("[0-9]{7}", "deep exact-depth digit constraint, sparse result"),
    ("[0-9]{1,3}", "bounded repetition, union of three exact depths"),
    ("(www|http|https)", "tiny alternation, term-lookup floor for the machinery"),
    ("(mon|tues|wednes|thurs|fri|satur|sun)day", "seven-way anchored alternation, exact terms"),
    ("colou?r", "single optional position, two-term lookup"),
    # Anchored subtrees with an acceptance tail: bounded scan, cheap accept.
    ("(inter|under)[a-z]*", "two prefix subtrees, free tail; pure enumeration"),
    ("(re|un)[a-z]*ing", "two shallow subtrees scanned under a suffix accept"),
    ("[jkqxz][a-z]*ess", "five rare single-letter subtrees, selective suffix"),
    ("[aeiou]{2}[a-z]*", "twenty-five two-vowel roots, free tail"),
    ("[a-f0-9]{8,}", "long bounded hex class; markup and identifier terms"),
    # Unanchored: the automaton forbids skipping, so the full dictionary is
    # visited and per-term acceptance cost is the whole measurement.
    (".*ing", "full scan, common suffix, many survivors"),
    (".*(tion|sion)", "full scan under an alternation accept"),
    (".*[0-9]", "full scan, digit tail, mixed survivors"),
    (".*qu.*", "full scan, containment accept"),
    (".*0th", "full scan, sparse survivors; scan cost with no postings work"),
)


def escape_term(term):
    """Escape a literal luceneutil term for the shared expression parsers."""
    return "".join("\\" + char if char in QUERY_PARSER_SPECIAL else char
                   for char in term)


def escape_pattern(pattern):
    """Escape a wildcard pattern, preserving its * and ? operators."""
    return "".join("\\" + char if char in PATTERN_SPECIAL else char
                   for char in pattern)


def expression(source_class, source_query):
    """Preserve the task operator while making source terms literal."""
    if source_class in ("Wildcard", "Prefix3"):
        return escape_pattern(source_query)
    if source_class.endswith("Term"):
        return escape_term(source_query)
    if source_class.startswith("And"):
        terms = source_query.split()
        if len(terms) != 2 or any(not term.startswith("+") for term in terms):
            raise ValueError(f"unexpected AND task expression: {source_query!r}")
        return " ".join("+" + escape_term(term[1:]) for term in terms)
    if source_class.startswith("Or"):
        terms = source_query.split()
        if len(terms) != 2:
            raise ValueError(f"unexpected OR task expression: {source_query!r}")
        return " ".join(escape_term(term) for term in terms)
    return source_query


def derive(source, limit_per_class):
    """Return deterministic rows and complete source counts for target tasks."""
    if limit_per_class < 1:
        raise ValueError("limit_per_class must be positive")
    candidates = {source_class: [] for source_class in LUCENEUTIL_CLASS_MAP}
    for line_number, line in enumerate(Path(source).read_text(encoding="utf-8").splitlines(), 1):
        match = TASK_LINE.fullmatch(line)
        if not match or match.group(1) not in candidates:
            continue
        source_class, source_query, metadata = match.groups()
        provenance = {"source_line": line_number, "source_query": source_query}
        if metadata is not None:
            provenance["metadata"] = metadata
        candidates[source_class].append({
            "query": expression(source_class, source_query),
            "class": LUCENEUTIL_CLASS_MAP[source_class],
            "tags": [f"luceneutil:{source_class}"],
            "luceneutil": provenance,
        })

    source_counts = {name: len(rows) for name, rows in candidates.items()}
    if source_counts != EXPECTED_SOURCE_COUNTS:
        raise ValueError(
            f"luceneutil task counts differ from pinned source: {source_counts!r}")

    selected = []
    seen = set()
    for source_class in LUCENEUTIL_CLASS_MAP:
        for row in candidates[source_class][:limit_per_class]:
            query = row["query"]
            if query in seen:
                raise ValueError(f"duplicate selected query text: {query!r}")
            seen.add(query)
            selected.append(row)
    # The luceneutil Wildcard tasks are high-frequency stems whose cost is
    # expansion and postings. The derived scan classes flip that ratio: a
    # LowTerm's affixes around a star force a large scanned term range with
    # few surviving terms, so term-dictionary scanning dominates. wildcard_scan
    # bounds the scan to one first-letter subtree; wildcard_lead's leading
    # star denies every engine a seekable prefix and visits the dictionary.
    derived = {"wildcard_scan": [], "wildcard_lead": []}
    for row in candidates["LowTerm"]:
        term = row["luceneutil"]["source_query"]
        if not DERIVED_TERM.fullmatch(term):
            continue
        for query_class, query in (
                ("wildcard_scan", f"{term[0]}*{term[-4:]}"),
                ("wildcard_lead", f"*{term[-5:]}")):
            rows = derived[query_class]
            if len(rows) >= limit_per_class or query in seen:
                continue
            seen.add(query)
            rows.append({
                "query": query,
                "class": query_class,
                "tags": [f"searchbench:{query_class}"],
                "luceneutil": dict(row["luceneutil"]),
            })
    for rows in derived.values():
        selected.extend(rows)
    for pattern, intent in REGEX_TASKS:
        if pattern in seen:
            raise ValueError(f"duplicate selected query text: {pattern!r}")
        seen.add(pattern)
        selected.append({
            "query": pattern,
            "class": "regex",
            "tags": ["searchbench:regex"],
            "searchbench": {"intent": intent},
        })
    return selected, source_counts


def encode(rows):
    return b"".join(
        (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        for row in rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("output")
    parser.add_argument("--limit-per-class", type=int, default=50)
    args = parser.parse_args()
    rows, source_counts = derive(args.source, args.limit_per_class)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(output.name + ".part")
    partial.write_bytes(encode(rows))
    partial.replace(output)
    print(json.dumps({
        "output": str(output),
        "queries": len(rows),
        "class_counts": dict(sorted(Counter(row["class"] for row in rows).items())),
        "source_counts": source_counts,
    }, indent=2))


if __name__ == "__main__":
    main()
