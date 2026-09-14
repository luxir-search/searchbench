#!/usr/bin/env python3
"""Show the exact request a task class sends and the response it gets back.

Examples:
  show_request.py luxir TOP_10                  # first pool query, summary + trimmed body
  show_request.py opensearch FACET_HC --full    # untrimmed response
  show_request.py luxir DEEP_COLLECT --query "philadelphia phillies"
  show_request.py elasticsearch TOP_10 --dry    # print request only, do not send
"""

import argparse
import json
from pathlib import Path
import socket
import sys

from adapters import DEFAULT_PORTS, ENGINES, make_adapter
from presets import TASKS, param_hash, parse_override, resolve
from query_source import QueryItem, classify, read_id_batches, searchbench_queries

DEFAULT_CORPUS = Path(__file__).resolve().parents[1] / "corpus" / \
    "corpus-100k-searchbench.ndjson"
DEFAULT_QUERIES = Path(__file__).resolve().parents[1] / "queries" / \
    "luceneutil" / "queries.txt"


def http_request(host, port, request, timeout, headers=()):
    extra = "".join(f"{name}: {value}\r\n" for name, value in headers)
    head = (f"{request.method} {request.path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Content-Type: application/json\r\n"
            "Connection: close\r\n"
            f"{extra}"
            f"Content-Length: {len(request.body)}\r\n\r\n").encode()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(head + request.body)
        raw = b""
        while chunk := sock.recv(65536):
            raw += chunk
    header, _, body = raw.partition(b"\r\n\r\n")
    status = int(header.split(None, 2)[1])
    if b"chunked" in header.lower():
        merged = bytearray()
        rest = body
        while rest:
            line, _, rest = rest.partition(b"\r\n")
            size = int(line.split(b";", 1)[0], 16)
            if size == 0:
                break
            merged += rest[:size]
            rest = rest[size + 2:]
        body = bytes(merged)
    return status, body


def trim(value, limit):
    """Truncate long lists so responses stay readable; -1 keeps everything."""
    if limit < 0:
        return value
    if isinstance(value, list):
        head = [trim(v, limit) for v in value[:limit]]
        if len(value) > limit:
            head.append(f"... {len(value) - limit} more")
        return head
    if isinstance(value, dict):
        return {k: trim(v, limit) for k, v in value.items()}
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("engine", choices=ENGINES)
    parser.add_argument("task", choices=TASKS)
    parser.add_argument("--lane", choices=("exact", "skip"), default="exact")
    parser.add_argument("--query", help="ad-hoc Lucene-syntax query text")
    parser.add_argument("--index", type=int, default=0, help="pool index when --query absent")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--collection", default="searchbench")
    parser.add_argument("--queries", default=str(DEFAULT_QUERIES))
    parser.add_argument("--corpus", default=str(DEFAULT_CORPUS))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="override a preset parameter (repeatable)")
    parser.add_argument("--full", action="store_true", help="do not trim long lists")
    parser.add_argument("--raw", action="store_true", help="print raw response bytes, no pretty JSON")
    parser.add_argument("--dry", action="store_true", help="print the request only, do not send")
    args = parser.parse_args()
    port = args.port or DEFAULT_PORTS[args.engine]
    adapter = make_adapter(args.engine, args.collection)
    overrides = dict(parse_override(text) for text in args.set or ())
    resolved = resolve(args.task, args.lane, overrides)

    if args.task == "MIX":
        sys.exit("MIX is a weighted blend of the other presets; show one of its components instead")
    if resolved["shape"] == "get":
        if args.query:
            sys.exit("get work items come from --corpus; select one with --index")
        items = read_id_batches(
            args.corpus, resolved["batch_size"], max_queries=args.index + 1)
        item = items[args.index]
    elif args.query:
        item = QueryItem(args.query, classify(args.query), (), "ad-hoc")
    else:
        pools = searchbench_queries(args.queries)
        pool = pools["count"] if resolved["limit"] == 0 else pools["top"]
        if resolved.get("query_class"):
            pool = [q for q in pool if q.query_class == resolved["query_class"]]
        item = pool[args.index]

    print(f"=== params (hash {param_hash(resolved)})")  # same hash the driver records
    print(json.dumps(resolved, indent=2))
    params = dict(resolved, name=args.task)
    request = adapter.build(params, item)
    print(f"\n=== request ({args.engine} {args.task} count_mode={args.lane} "
          f"query={item.query_class}:{item.text!r})")
    print(f"{request.method} {request.path}")
    if request.body:
        print(json.dumps(json.loads(request.body), indent=2))
    if args.dry:
        return

    status, body = http_request(args.host, port, request, args.timeout, adapter.headers)
    lines = [line for line in body.split(b"\n") if line.strip()]
    print(f"\n=== response (HTTP {status}, {len(body)} bytes, {len(lines)} json line(s))")
    if args.raw:
        sys.stdout.buffer.write(body)
        return
    limit = -1 if args.full else 3
    show = lines if (args.full or len(lines) <= 3) else lines[:1]
    for line in show:
        print(json.dumps(trim(json.loads(line), limit), indent=2))
    if len(show) < len(lines):
        def describe(i, line):
            value = json.loads(line)
            bits = [f"line {i}"]
            if "found" in value:
                bits.append(f"found={value['found']}")
            if value.get("docs") is not None:
                bits.append(f"docs={len(value['docs'])}")
            if value.get("more"):
                bits.append("more=true")
            if "ops" in value:
                bits.append(f"ops={list(value['ops'])}")
            print("  ".join(bits))
        for i in range(min(3, len(lines))):
            describe(i, lines[i])
        if len(lines) > 4:
            print(f"... {len(lines) - 4} more lines ...")
        describe(len(lines) - 1, lines[-1])
    count = adapter.count(body)
    if count is not None:
        print(f"\n=== total hits: {count}")


if __name__ == "__main__":
    main()
