"""Live serving-topology probe shared by feeds, verification, and replay."""

import http.client


def index_topology(adapter, host, port, timeout):
    requests = adapter.build_topology_probe()
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    responses = []
    try:
        for request in requests:
            connection.request(request.method, request.path, body=request.body or None,
                               headers={"Accept": "*/*", **dict(adapter.headers)})
            response = connection.getresponse()
            raw = response.read()
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(
                    f"{request.task} returned HTTP {response.status}: "
                    f"{raw[:200].decode(errors='replace')}")
            responses.append(raw)
    finally:
        connection.close()
    return adapter.parse_topology(responses)


def assert_quiescent_topology(topology, expected_segments=None):
    if expected_segments is not None and topology["segment_count"] != expected_segments:
        raise RuntimeError(
            f"expected {expected_segments} serving segments, found "
            f"{topology['segment_count']}")
    if topology.get("active_merges", 0):
        raise RuntimeError(
            f"index has {topology['active_merges']} active merges before measurement")
    merging = [segment["id"] for segment in topology["segments"]
               if segment.get("merging", False)]
    if merging:
        raise RuntimeError(f"serving segments are actively merging: {merging}")
