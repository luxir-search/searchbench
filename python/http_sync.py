"""Small stdlib HTTP helpers for streaming feeders."""

import http.client
import json


def request_json(host, port, method, path, body=None, headers=None, timeout=120):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    merged = {"Accept": "application/json"}
    if payload is not None:
        merged["Content-Type"] = "application/json"
    if headers:
        merged.update(headers)
    conn.request(method, path, body=payload, headers=merged)
    response = conn.getresponse()
    raw = response.read()
    status = response.status
    conn.close()
    parsed = json.loads(raw) if raw else {}
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status} {method} {path}: {raw[:1000].decode(errors='replace')}")
    return parsed


def request_bytes(host, port, method, path, body, content_type, timeout=120, headers=None):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    merged = {"Content-Type": content_type, "Accept": "application/json"}
    if headers:
        merged.update(headers)
    conn.request(method, path, body=body, headers=merged)
    response = conn.getresponse()
    raw = response.read()
    status = response.status
    conn.close()
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status} {method} {path}: {raw[:2000].decode(errors='replace')}")
    return json.loads(raw) if raw else {}


def stream_request(host, port, method, path, chunks, content_type, timeout=3600):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    conn.request(method, path, body=chunks, headers={"Content-Type": content_type}, encode_chunked=True)
    response = conn.getresponse()
    raw = response.read()
    status = response.status
    conn.close()
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status} {method} {path}: {raw[:2000].decode(errors='replace')}")
    return raw
