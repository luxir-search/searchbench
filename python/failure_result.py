#!/usr/bin/env python3
"""Write explicit failed smoke cells so reporting never silently drops them."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from cpu_layout import current_cpu_sets, format_cpu_list


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("engine")
    parser.add_argument("task")
    parser.add_argument("lane")
    parser.add_argument("message")
    parser.add_argument("output")
    parser.add_argument("--corpus")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    versions = json.loads((root / "engines" / "versions.json").read_text(encoding="utf-8"))
    ports = {"luxir": 9400, "opensearch": 9201, "elasticsearch": 9202}
    corpus = (Path(args.corpus) if args.corpus else
              root / "corpus" / "corpus-100k-searchbench.ndjson")
    checksum_path = Path(str(corpus) + ".sha256")
    default_server, default_client = current_cpu_sets()
    server_cores = os.environ.get("SERVER_CORES", format_cpu_list(default_server))
    client_cores = os.environ.get("CLIENT_CORES", format_cpu_list(default_client))
    value = {
        "schema_version": 3,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "engine": args.engine,
        "task_class": args.task,
        "lane": args.lane,
        "failure": args.message,
        "engine_config": {
            "network_host": "127.0.0.1", "port": ports[args.engine],
            "transport_port": ({"opensearch": 9301, "elasticsearch": 9302}.get(args.engine)),
            "shards": 1, "replicas": 0, "node_count": 1,
            "heap_min": "8g" if args.engine != "luxir" else None,
            "heap_max": "8g" if args.engine != "luxir" else None,
            "security_enabled": False, "server_cores": server_cores
        },
        "core_split": {"server": server_cores, "client": client_cores},
        "corpus": {"path": str(corpus), "exists": corpus.exists(),
                   "bytes": corpus.stat().st_size if corpus.exists() else None,
                   "sha256": checksum_path.read_text(encoding="utf-8").strip().split()[0]
                   if checksum_path.exists() else None},
        "count_samples": {}
    }
    if args.engine == "luxir":
        value["engine_config"].update({"grpc_port": 9401, "store_backend": "fs"})
    else:
        value["engine_config"]["version"] = versions[args.engine]["version"]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
