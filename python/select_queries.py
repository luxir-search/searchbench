#!/usr/bin/env python3
"""Select queries whose exact hit counts agree across all engines."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from adapters import ENGINES
from query_source import classify
import results_io
from schema import ANALYZER_POSTURE, BODY_FIELD


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def replace_bytes(path, data):
    path = Path(path)
    partial = path.with_name(path.name + ".part")
    partial.write_bytes(data)
    partial.replace(path)


def read_source(path):
    raw = Path(path).read_bytes()
    rows = []
    seen = set()
    for line_number, line in enumerate(raw.splitlines(keepends=True), 1):
        if not line.strip():
            continue
        entry = json.loads(line)
        query = entry["query"]
        if query in seen:
            raise ValueError(f"{path}:{line_number}: duplicate query {query!r}")
        seen.add(query)
        rows.append((query, classify(query, tuple(entry.get("tags", ())),
                                     entry.get("class")), line))
    if not rows:
        raise ValueError(f"empty query source: {path}")
    return raw, rows


def load_result(path, engine, source_hash):
    result = results_io.load_result(path)
    if result.get("engine") != engine:
        raise ValueError(f"{path}: expected engine {engine!r}")
    if result.get("lane") != "exact" or result.get("task_class") != "COUNT":
        raise ValueError(f"{path}: query selection requires exact COUNT results")
    expected_analysis = {"field": BODY_FIELD, **ANALYZER_POSTURE[engine]}
    observed_analysis = result.get("engine_config", {}).get("full_text")
    if observed_analysis != expected_analysis:
        raise ValueError(
            f"{path}: result analyzer posture {observed_analysis!r} "
            f"differs from {expected_analysis!r}")
    observed_source = result.get("source_files", {}).get("queries", {}).get("sha256")
    if observed_source != source_hash:
        raise ValueError(
            f"{path}: result query source {observed_source!r} differs from {source_hash!r}")
    corpus = result.get("corpus", {})
    corpus_hash = corpus.get("sha256")
    if not corpus_hash:
        raise ValueError(f"{path}: result has no corpus SHA-256")
    samples = result.get("count_samples", {})
    if not samples:
        raise ValueError(f"{path}: result has no exact count samples")
    return result, corpus_hash, samples


def select(source, result_sets):
    source_raw, rows = read_source(source)
    source_hash = sha256(source_raw)
    expected_keys = {f"COUNT\t{query}" for query, _query_class, _line in rows}
    results = {}
    corpora = {}
    for corpus_name, result_paths in result_sets.items():
        corpus_results = {}
        corpus_hashes = set()
        corpus_documents = set()
        corpus_variants = set()
        for engine in ENGINES:
            result, corpus_hash, samples = load_result(
                result_paths[engine], engine, source_hash)
            observed_keys = set(samples)
            missing = expected_keys - observed_keys
            extra = observed_keys - expected_keys
            if missing or extra:
                raise ValueError(
                    f"{corpus_name}/{engine}: count samples differ from source "
                    f"(missing={len(missing)}, extra={len(extra)})")
            corpus_results[engine] = (result, samples)
            corpus_hashes.add(corpus_hash)
            corpus_documents.add(
                result.get("corpus", {}).get("facet_cardinalities", {}).get("documents"))
            corpus_variants.add(
                result.get("corpus", {}).get("facet_cardinalities", {}).get("variant"))
        if len(corpus_hashes) != 1:
            raise ValueError(
                f"{corpus_name}: engines use different corpora: {sorted(corpus_hashes)}")
        if len(corpus_documents) != 1 or not isinstance(next(iter(corpus_documents)), int):
            raise ValueError(f"{corpus_name}: results lack a common document count")
        if len(corpus_variants) != 1 or not isinstance(next(iter(corpus_variants)), str):
            raise ValueError(f"{corpus_name}: results lack a common corpus variant")
        results[corpus_name] = corpus_results
        corpora[corpus_name] = {
            "documents": next(iter(corpus_documents)),
            "variant": next(iter(corpus_variants)),
            "sha256": next(iter(corpus_hashes)),
        }

    selected = []
    rejected = []
    for query, query_class, line in rows:
        key = f"COUNT\t{query}"
        counts = {
            corpus_name: {
                engine: corpus_results[engine][1][key] for engine in ENGINES
            }
            for corpus_name, corpus_results in results.items()
        }
        mismatched = [
            corpus_name for corpus_name, corpus_counts in counts.items()
            if len(set(corpus_counts.values())) != 1
        ]
        if not mismatched:
            selected.append((query, query_class, line))
        else:
            rejected.append({"query": query, "class": query_class,
                             "mismatched_corpora": mismatched, "counts": counts})

    versions = {}
    for engine in ENGINES:
        if engine == "luxir":
            continue
        observed = {
            corpus_results[engine][0].get("engine_config", {}).get("version")
            for corpus_results in results.values()
        }
        if len(observed) != 1:
            raise ValueError(f"{engine}: result sets use different versions: {observed}")
        versions[engine] = next(iter(observed))
    luxir_hashes = {
        corpus_results["luxir"][0].get("engine_config", {}).get("binary_sha256")
        for corpus_results in results.values()
    }
    if len(luxir_hashes) != 1:
        raise ValueError("result sets use different Luxir binaries")
    selected_raw = b"".join(line for _query, _query_class, line in selected)
    manifest = {
        "schema_version": 2,
        "method": "exact hit-count agreement across all engines and corpora",
        "corpora": corpora,
        "analyzers": ANALYZER_POSTURE,
        "reference_versions": versions,
        "luxir_binary_sha256": next(iter(luxir_hashes)),
        "source": {
            "file": Path(source).name,
            "queries": len(rows),
            "sha256": source_hash,
            "class_counts": dict(sorted(Counter(row[1] for row in rows).items())),
        },
        "selection": {
            "queries": len(selected),
            "sha256": sha256(selected_raw),
            "class_counts": dict(sorted(Counter(row[1] for row in selected).items())),
        },
        "rejected": rejected,
    }
    return selected_raw, manifest


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", action="append", required=True,
                        metavar="LANE=DIRECTORY",
                        help="named directory containing ENGINE-COUNT.json results")
    parser.add_argument(
        "--source", default=root / "queries/luceneutil/queries-all.txt")
    parser.add_argument(
        "--output", default=root / "queries/luceneutil/queries.txt")
    parser.add_argument(
        "--manifest", default=root / "queries/luceneutil/selection.json")
    args = parser.parse_args()
    result_sets = {}
    for entry in args.results:
        name, separator, directory = entry.partition("=")
        if not separator or not name or not directory or name in result_sets:
            parser.error(f"invalid or duplicate --results value: {entry!r}")
        result_sets[name] = {
            engine: Path(directory) / f"{engine}-COUNT.json" for engine in ENGINES
        }
    selected, manifest = select(args.source, result_sets)
    corpus_manifest = json.loads(
        (root / "corpora/wikipedia/manifest.json").read_text(encoding="utf-8"))
    # Any subset of the declared corpus lanes may gate admission; each named
    # lane must be the exact corpus the manifest declares for it.
    known_lanes = tuple(corpus_manifest["lanes"])
    unknown = [lane for lane in result_sets if lane not in known_lanes]
    if not result_sets or unknown:
        raise ValueError(f"query selection lanes must be from {known_lanes}, "
                         f"got {tuple(result_sets)}")
    for lane in result_sets:
        expected_documents = corpus_manifest["lanes"][lane]["documents"]
        expected_variant = corpus_manifest["lanes"][lane]["variant"]
        if manifest["corpora"][lane]["documents"] != expected_documents:
            raise ValueError(
                f"query selection requires the {lane} {expected_documents}-document "
                f"corpus; results contain {manifest['corpora'][lane]['documents']}")
        if manifest["corpora"][lane]["variant"] != expected_variant:
            raise ValueError(
                f"query selection requires the {lane} {expected_variant!r} variant; "
                f"results contain {manifest['corpora'][lane]['variant']!r}")
    manifest_bytes = (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()
    replace_bytes(args.output, selected)
    replace_bytes(args.manifest, manifest_bytes)
    print(json.dumps({"output": str(args.output), **manifest["selection"],
                      "rejected": len(manifest["rejected"])}, indent=2))


if __name__ == "__main__":
    main()
