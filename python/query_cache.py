"""The query/filter-cache state a serving engine reports: "on", "off", or None.

Observed rather than taken from SEARCHBENCH_QUERY_CACHE, so a result records
what actually served it and ensure_running (engine-common.sh) can tell whether
a server left running matches the requested posture.

  python3 query_cache.py <engine> <port> [collection]   prints on|off|unknown
"""

import sys

from http_sync import request_json


def observed_query_cache(engine, host, port, collection="searchbench"):
    try:
        if engine == "luxir":
            # A disabled cache (--query-cache-bytes=0) reports an empty object.
            stats = request_json(host, port, "GET", "/_stats", timeout=30)
            states = {bool(shard["index"]["query_cache"].get("enabled"))
                      for coll in stats.get("collections", [])
                      for shard in coll.get("shards", [])
                      if "query_cache" in shard.get("index", {})}
        else:
            # An index without the setting uses the engine default (enabled).
            settings = request_json(
                host, port, "GET", f"/{collection}/_settings"
                "?filter_path=*.settings.index.queries.cache.enabled", timeout=30)
            states = {str(index["settings"]["index"]["queries"]["cache"]["enabled"])
                      .lower() == "true" for index in settings.values()} or {True}
    except (OSError, RuntimeError, ValueError, KeyError, TypeError):
        return None
    if len(states) != 1:
        return None
    return "on" if states.pop() else "off"


if __name__ == "__main__":
    engine, port = sys.argv[1], int(sys.argv[2])
    collection = sys.argv[3] if len(sys.argv) > 3 else "searchbench"
    print(observed_query_cache(engine, "127.0.0.1", port, collection) or "unknown")
