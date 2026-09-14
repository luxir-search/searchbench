#!/usr/bin/env python3
"""Probe one live index and assert a named serving topology."""

import argparse
import json

from adapters import DEFAULT_PORTS, ENGINES, make_adapter
from topologies import TOPOLOGIES, assert_topology, resolve_topology
from topology_probe import assert_quiescent_topology, index_topology


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=ENGINES)
    parser.add_argument("topology", choices=tuple(TOPOLOGIES))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--collection", default="searchbench")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    declared = resolve_topology(args.topology)
    adapter = make_adapter(args.engine, args.collection)
    observed = index_topology(
        adapter, args.host, args.port or DEFAULT_PORTS[args.engine], args.timeout)
    assert_quiescent_topology(observed, declared.segment_count)
    assert_topology(declared, observed)
    print(json.dumps({
        "declared": {
            "name": declared.name,
            "description": declared.description,
            "documents": declared.documents,
            "expected_segments": declared.segment_count,
            "segment_docs": list(declared.segment_docs),
        },
        "observed": observed,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
