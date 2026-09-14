#!/usr/bin/env python3
"""Stream transformed corpus records into Luxir's NDJSON /update endpoint.

--streams N feeds N record-aligned slices of the corpus over N concurrent
connections; the commit is one separate request after every stream drains.

What a stream is NOT is a unit of indexing. Luxir cuts its own update batches
out of the byte stream (indexing.stream-batch-size / -docs) and keeps
indexing.max-inflight-batches of them moving through its update graph at once,
each taking whatever inverter is idle, so even one stream reaches every
indexing thread. What streams do parallelize is the per-connection work ahead
of that batching - NDJSON framing and JSON record parsing run serially on a
connection's io strand - plus client-side transport.
"""

import argparse
import json
import os
import threading
import time

from adapters import make_adapter
from http_sync import request_json, stream_request
from schema import LUXIR_SCHEMA
from topologies import TOPOLOGIES, assert_topology, resolve_topology
from topology_probe import index_topology

ALLOW_DUPS_LINE = b'{"_update_":{"allow_dups":true}}\n'


def commit_line(args, max_segments=None):
    commit = {"wait_for_merges": True}
    if args.optimize:
        commit["max_segments"] = 1
    elif max_segments is not None:
        commit["max_segments"] = max_segments
    return json.dumps({"_end_": {"commit": commit}}, separators=(",", ":")).encode() + b"\n"


def range_blocks(path, start, end, block_bytes=4 * 1024 * 1024):
    """Yield [start, end) as record-aligned byte blocks.

    Raw file blocks, never lines: the server's NDJSON framer finds the record
    boundaries, and big blocks keep client syscall and per-line Python cost out
    of the ingest measurement. A record straddling a boundary belongs to the
    range that starts it, so ranges partition the corpus exactly.
    """
    with open(path, "rb") as source:
        if start:
            source.seek(start - 1)
            if source.read(1) != b"\n":
                source.readline()  # the previous range owns this partial record
        remaining = end - source.tell()
        while remaining > 0:
            block = source.read(min(block_bytes, remaining))
            if not block:
                return
            remaining -= len(block)
            if not block.endswith(b"\n"):
                tail = source.readline()  # finish the record this block cut
                remaining -= len(tail)
                block += tail if tail.endswith(b"\n") else tail + b"\n"
            yield block


def feed_topology(args, topology):
    """Feed each declared document range through its own committed stream.

    Ranges are declared in documents, not bytes, so this one reads records
    rather than blocks - but it never retains a range in memory: the largest
    tier in the 10M topology is several GiB of NDJSON. A short source is caught
    at its exact range, and any extra non-empty record after the final range is
    rejected so a topology name always denotes one corpus size.
    """
    responses = []
    boundaries = []
    adapter = make_adapter("luxir", args.collection)
    source_docs = 0
    with open(args.corpus, "rb") as source:
        for range_index, expected_docs in enumerate(topology.segment_docs, 1):
            range_started = time.monotonic()
            emitted = 0

            def range_records():
                nonlocal emitted, source_docs
                yield ALLOW_DUPS_LINE
                block = bytearray()
                while emitted < expected_docs:
                    line = source.readline()
                    if not line:
                        raise RuntimeError(
                            f"topology {topology.name} range {range_index} expected "
                            f"{expected_docs} documents, source ended after {emitted}")
                    if not line.strip():
                        continue
                    emitted += 1
                    source_docs += 1
                    block += line if line.endswith(b"\n") else line + b"\n"
                    if len(block) >= 4 * 1024 * 1024:
                        yield bytes(block)
                        block.clear()
                if block:
                    yield bytes(block)
                # Each range may auto-flush into bounded pieces. Ranges are
                # largest-first, so forcing the total to the number of ranges
                # already fed merges only the newly added smaller pieces and
                # preserves every earlier tier as its own segment.
                yield commit_line(args, range_index)

            raw = stream_request(args.host, args.port, "POST",
                                 f"/collections/{args.collection}/_update",
                                 range_records(), "application/x-ndjson")
            responses.extend(json.loads(line) for line in raw.splitlines() if line)
            observed = index_topology(
                adapter, args.host, args.port, timeout=120)
            declared = topology.after_ranges(range_index)
            assert_topology(declared, observed)
            boundaries.append({
                "range": range_index,
                "range_documents": expected_docs,
                "total_documents": declared.documents,
                "commit_max_segments": range_index,
                "elapsed_s": round(time.monotonic() - range_started, 3),
                "observed": observed,
            })
        extra = next((line for line in source if line.strip()), None)
        if extra is not None:
            raise RuntimeError(
                f"topology {topology.name} consumes {topology.documents} documents "
                "but the corpus contains more")
    if source_docs != topology.documents:
        raise RuntimeError(
            f"topology {topology.name} consumed {source_docs} documents; "
            f"expected {topology.documents}")
    return responses, boundaries


def stream_records(path, start, end):
    yield ALLOW_DUPS_LINE
    yield from range_blocks(path, start, end)
    yield b'{"_end_":{}}\n'


def feed_streams(args):
    """Feed N record-aligned corpus ranges over N concurrent /update streams.

    Each stream reads its own range straight off disk - no shared producer, so
    one slow connection cannot stall the others and nothing is buffered in the
    client. --streams 1 is the degenerate case: the whole corpus, one firehose.
    """
    size = os.path.getsize(args.corpus)
    bounds = [size * i // args.streams for i in range(args.streams + 1)]
    results = [None] * args.streams

    def worker(i):
        try:
            raw = stream_request(args.host, args.port, "POST",
                                 f"/collections/{args.collection}/_update",
                                 stream_records(args.corpus, bounds[i], bounds[i + 1]),
                                 "application/x-ndjson")
            results[i] = [json.loads(line) for line in raw.splitlines() if line]
        except Exception as error:
            results[i] = error

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.streams)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        raise failures[0]
    lines = [line for r in results for line in r]
    if args.commit:
        commit = stream_request(args.host, args.port, "POST",
                                f"/collections/{args.collection}/_update",
                                iter([commit_line(args)]),
                                "application/x-ndjson")
        lines += [json.loads(line) for line in commit.splitlines() if line]
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("corpus")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9400)
    parser.add_argument("--collection", default="searchbench")
    parser.add_argument("--commit", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--streams", type=int, default=16,
                        help="concurrent /update connections (ingest-parallelism dimension)")
    parser.add_argument("--topology", choices=tuple(TOPOLOGIES),
                        help="named exact document-range topology")
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()
    if args.streams < 1:
        parser.error("--streams must be at least 1")
    if args.topology and not args.commit:
        parser.error("--topology requires committed range boundaries")
    if args.topology and args.optimize:
        parser.error("--topology cannot be combined with --optimize")
    request_json(args.host, args.port, "POST",
                 f"/collections/{args.collection}/_schema", LUXIR_SCHEMA)
    started = time.monotonic()
    topology = resolve_topology(args.topology) if args.topology else None
    boundaries = None
    if topology:
        lines, boundaries = feed_topology(args, topology)
    else:
        lines = feed_streams(args)
    elapsed = time.monotonic() - started
    bad = [line for line in lines if line.get("status") not in (None, "ok")]
    if bad:
        raise RuntimeError(f"Luxir update returned errors: {bad[:3]}")
    count = request_json(args.host, args.port, "POST",
                         f"/collections/{args.collection}/_search",
                         {"query": {"all": True}, "limit": 0, "get_number": True})
    found = count.get("found") if isinstance(count, dict) else None
    print(json.dumps({"responses": lines, "count_response": count,
                      "streams": args.streams,
                      "topology": None if topology is None else {
                          "name": topology.name,
                          "segment_docs": list(topology.segment_docs),
                          "expected_segments": topology.segment_count,
                          "documents": topology.documents,
                          "boundaries": boundaries,
                      },
                      "allow_dups": True,
                      "elapsed_s": round(elapsed, 3),
                      "docs_per_s": round(found / elapsed, 1) if found and elapsed else None},
                     indent=2))


if __name__ == "__main__":
    main()
