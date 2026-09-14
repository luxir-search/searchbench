#!/usr/bin/env python3
"""Stream the pinned luceneutil Wikipedia corpus and add benchmark fields."""

import argparse
from array import array
import bisect
import hashlib
import json
import lzma
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATE_START = datetime(2020, 1, 1, tzinfo=timezone.utc)
DATE_SECONDS = 5 * 365 * 24 * 60 * 60 + 24 * 60 * 60
ZIPF_EXPONENT = 1.15
LUCENEUTIL_HEADER = (
    "FIELDS_HEADER_INDICATOR###\tdoctitle\tdocdate\tbody\tRandomLabel"
)


def input_lines(files):
    """Yield text lines from plain or LZMA files, or stdin."""
    if not files:
        yield from sys.stdin
        return
    for name in files:
        if name == "-":
            yield from sys.stdin
            continue
        path = Path(name)
        opener = lzma.open if path.suffix == ".lzma" else open
        with opener(path, "rt", encoding="utf-8") as source:
            yield from source


def ordinal_sort_value(ordinal):
    """A stable 32-bit permutation of the luceneutil line ordinal.

    The odd multiplier makes this a bijection modulo 2^32. Consequently every
    source line has a distinct numeric sort key while input order is not sort
    order. All published corpus lanes are prefixes of the same permutation.
    """
    return (ordinal * 0x9E3779B1 + 0x85EBCA77) & 0xFFFFFFFF


def input_documents(files, source_format="luceneutil-line-docs"):
    """Yield canonical source documents from the pinned line-doc format."""
    lines = input_lines(files)
    if source_format != "luceneutil-line-docs":
        raise ValueError(f"unknown source format: {source_format}")

    try:
        header = next(lines).rstrip("\r\n")
    except StopIteration as error:
        raise ValueError("empty luceneutil line-doc source") from error
    if header != LUCENEUTIL_HEADER:
        raise ValueError(f"unexpected luceneutil line-doc header: {header!r}")
    for ordinal, line in enumerate(lines):
        columns = line.rstrip("\r\n").split("\t")
        if len(columns) != 4:
            raise ValueError(
                f"luceneutil source line {ordinal + 2} has {len(columns)} columns; expected 4"
            )
        title, _date, body, _random_label = columns
        yield {
            "id": str(ordinal),
            "title": title,
            "body": body,
            "sort_i": ordinal_sort_value(ordinal),
        }


def zipf_cdf(size, exponent=ZIPF_EXPONENT):
    # array('d') keeps the 1M-rank CDF compact without changing the double
    # precision arithmetic (and therefore existing synthetic field values).
    weights = array("d", (1.0 / math.pow(rank, exponent)
                           for rank in range(1, size + 1)))
    total = sum(weights)
    running = 0.0
    for index, weight in enumerate(weights):
        running += weight / total
        weights[index] = running
    weights[-1] = 1.0
    return weights


CAT10_CDF = zipf_cdf(10)
CAT100_CDF = zipf_cdf(100)
CAT1K_CDF = zipf_cdf(1000)
CAT10K_CDF = zipf_cdf(10_000)
CAT100K_CDF = zipf_cdf(100_000)
CAT1M_CDF = zipf_cdf(1_000_000)

# The 5M-rank CDF costs ~5s to build; lazy so importing this module (tests,
# tooling) stays fast when it is not needed.
_BIG_CDFS = {}
def get_cdf(size, exponent):
    key = (size, exponent)
    if key not in _BIG_CDFS:
        _BIG_CDFS[key] = zipf_cdf(size, exponent)
    return _BIG_CDFS[key]


M64 = (1 << 64) - 1

def _mix(x):
    # splitmix64 finalizer: deterministic, portable, no RNG state.
    x = (x + 0x9E3779B97F4A7C15) & M64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & M64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & M64
    return x ^ (x >> 31)


# Frequency rank -> emitted name-number, a deterministic shuffle per field.
# Without this, zipf value names embed the frequency rank: term-dictionary
# order becomes descending-docfreq order and doc-values ordinals cluster the
# hot head in the first cache lines - unrealistic for term walks AND counter
# locality (found 2026-07-19). Real corpora have no name<->frequency
# correlation; after the shuffle, dict order is frequency-random.
_PERMS = {}
def rank_perm(size, salt):
    key = (size, salt)
    if key not in _PERMS:
        base = int.from_bytes(hashlib.blake2b(salt.encode(), digest_size=8).digest(), "big")
        _PERMS[key] = array("i", sorted(range(size), key=lambda r: _mix(base ^ r)))
    return _PERMS[key]

# (field, nominal, distribution, exponent, value_prefix).
# Mixed distributions are deliberate: Zipf-1.15 leaves much of a large nominal
# domain unrealized, so the high-realized rungs use classic Zipf-1.0 and
# uniform distributions. Exact realized counts live in each corpus report.
FACET_CARDINALITIES = (
    ("cat_s", 10, "zipf", 1.15, "cat-"),
    ("cat100_s", 100, "zipf", 1.15, "cat100-"),
    ("cat1k_s", 1_000, "zipf", 1.15, "cat1k-"),
    ("cat10k_s", 10_000, "zipf", 1.15, "cat10k-"),
    ("cat100k_s", 100_000, "zipf", 1.15, "cat100k-"),
    ("cat1m_s", 1_000_000, "zipf", 1.15, "cat1m-"),
    ("cat5m_s", 5_000_000, "zipf", 1.0, "cat5m-"),
    ("catu2m_s", 2_000_000, "uniform", None, "catu2m-"),
)

# Filter-selectivity tiers in basis points of 100k; one hash word drives all
# tiers, so they NEST (every sel1 doc is also a sel10 doc, etc) - the same
# query progressively refined, comparable across tiers on identical docs.
# sel01=0.1% (100/100k), sel001=0.01% (10/100k): the shared basis word means
# adding a tier does not perturb any other field, so only the new column is new.
SEL_TIERS = (("sel99", 99_000), ("sel90", 90_000), ("sel50", 50_000),
             ("sel10", 10_000), ("sel1", 1_000), ("sel01", 100),
             ("sel001", 10))


def title_for_hash(doc_id, source):
    if source.get("title"):
        return str(source["title"])
    # Use the stable id as the deterministic fallback when a source row has no
    # title rather than deriving synthetic fields from the article body.
    return doc_id


def digest_words(doc_id, title):
    # blake2b for stdlib stability and distribution quality, not security -
    # there is no adversary here (2026-07-15). digest_size growth changes ALL
    # derived field values: bump person= and re-feed every engine together.
    digest = hashlib.blake2b(
        (doc_id + "\0" + title).encode("utf-8"), digest_size=64,
        person=b"luxir-srvbench2").digest()
    return [int.from_bytes(digest[n:n + 4], "big") for n in range(0, 64, 4)]


def zipf_value(word, cdf, prefix):
    unit = word / float(1 << 32)
    rank = bisect.bisect_left(cdf, unit)
    # Unpadded on purpose: term lengths vary and lexicographic order differs
    # from numeric order (zero-padded rank names were sorted-by-frequency).
    return f"{prefix}{rank_perm(len(cdf), prefix)[rank]}"


def synthetic_fields(doc_id, title):
    w = digest_words(doc_id, title)
    dt = DATE_START + timedelta(seconds=w[3] % DATE_SECONDS)
    fields = {
        "cat_s": zipf_value(w[0], CAT10_CDF, "cat-"),
        "cat1k_s": zipf_value(w[1], CAT1K_CDF, "cat1k-"),
        "catid_s": f"catid-{w[2] % 500_000}",
        "date_dt": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price_i": w[4] % 100_001,
        "cat100_s": zipf_value(w[5], CAT100_CDF, "cat100-"),
        "cat10k_s": zipf_value(w[6], CAT10K_CDF, "cat10k-"),
        "cat100k_s": zipf_value(w[7], CAT100K_CDF, "cat100k-"),
        # w[9..11] were previously unused, so these fields do not perturb any
        # existing derived value or require a digest person= bump.
        "cat1m_s": zipf_value(w[9], CAT1M_CDF, "cat1m-"),
        "cat5m_s": zipf_value(w[10], get_cdf(5_000_000, 1.0), "cat5m-"),
        "catu2m_s": f"catu2m-{w[11] % 2_000_000}",
    }
    basis = w[8] % 100_000
    for name, threshold in SEL_TIERS:
        fields[name + "_s"] = "t" if basis < threshold else "f"
    return fields


def transform_doc(source, variant="full"):
    doc_id = str(source.get("id") or "")
    if not doc_id:
        return None
    if variant == "text":
        return {"id": doc_id, "body": str(source["body"])}
    sort_value = source.get("sort_i")
    if sort_value is None:
        sort_value = digest_words(doc_id, title_for_hash(doc_id, source))[0] & 0xFFFFFFFF
    result = {"id": doc_id, "sort_i": int(sort_value)}
    if variant == "full":
        # The facet variant drops the body: facet/filter benchmarks never
        # touch it, and it dominates index size and feed time. Synthetic
        # fields are IDENTICAL across variants (derived from id+title only),
        # so cardinality reports and per-field values agree.
        result["body"] = str(source["body"])
    result.update(synthetic_fields(doc_id, title_for_hash(doc_id, source)))
    return result


def designate_values(counts, documents, prefix):
    """Benchmark values with exact counts: head, nearest to 10/1/0.1% of
    docs, and a tail value - so grid rows built on value filters carry
    honest, exact selectivities (the filter-family axis)."""
    head = max(range(len(counts)), key=lambda i: counts[i])
    named = {"head": head}
    for label, fraction in (("10pct", 0.10), ("1pct", 0.01), ("0.1pct", 0.001)):
        target = documents * fraction
        named[label] = min(range(len(counts)), key=lambda i: abs(counts[i] - target))
    tail = None
    for i, c in enumerate(counts):
        if c > 0 and (tail is None or c < counts[tail]):
            tail = i
            if c == 1:
                break
    if tail is not None:
        named["tail"] = tail
    return {label: {"value": f"{prefix}{rank}", "count": counts[rank]}
            for label, rank in named.items() if counts[rank] > 0}


def cardinality_report(counters, documents, corpus_sha256, variant):
    fields = {}
    if counters is not None:
        for field, nominal, distribution, exponent, prefix in FACET_CARDINALITIES:
            counts = counters[field]
            entry = {
                "distribution": distribution,
                "nominal_cardinality": nominal,
                "realized_cardinality": sum(1 for c in counts if c),
                "values": designate_values(counts, documents, prefix),
            }
            if exponent is not None:
                entry["exponent"] = exponent
            fields[field] = entry
        fields["sort_i"] = {
            "distribution": "permuted_ordinal",
            "value_space": 1 << 32,
            "nominal_cardinality": documents,
            "realized_cardinality": documents,
        }
    return {"schema_version": 3, "variant": variant, "documents": documents,
            "corpus_sha256": corpus_sha256, "fields": fields}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", help="source files; stdin when omitted")
    parser.add_argument("--source-format",
                        choices=("luceneutil-line-docs",),
                        default="luceneutil-line-docs")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--variant", choices=("full", "facet", "text"),
                        default="full",
                        help="facet drops body; text drops synthetic fields")
    parser.add_argument("--cardinality-report",
                        help="write exact realized facet cardinalities as JSON")
    args = parser.parse_args()
    emitted = 0
    seen = ({field: array("i", bytes(4 * nominal))
             for field, nominal, *_ in FACET_CARDINALITIES}
            if args.cardinality_report and args.variant != "text" else None)
    output_digest = hashlib.sha256() if args.cardinality_report else None
    for source in input_documents(args.files, args.source_format):
        doc = transform_doc(source, args.variant)
        if doc is None:
            continue
        encoded = (json.dumps(doc, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
        sys.stdout.buffer.write(encoded)
        emitted += 1
        if output_digest is not None:
            output_digest.update(encoded)
        if seen is not None:
            for field, *_rest in FACET_CARDINALITIES:
                seen[field][int(doc[field].rsplit("-", 1)[1])] += 1
        if args.limit and emitted >= args.limit:
            break
    if args.cardinality_report:
        report = cardinality_report(
            seen, emitted, output_digest.hexdigest(), args.variant)
        Path(args.cardinality_report).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
