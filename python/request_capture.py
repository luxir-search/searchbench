"""Capture and render requests at the replay serialization boundary."""

import hashlib
import json


_SENSITIVE_HEADERS = frozenset({
    "authorization", "proxy-authorization", "cookie", "set-cookie",
})


def capture_request(request, wire, parameters):
    """Record the request whose exact wire bytes enter the replay blob.

    The body stays as its original UTF-8 text. Headers are taken from the
    serialized wire request rather than reconstructed, with credential-bearing
    values redacted so a result remains publishable. The hash covers the exact,
    unredacted bytes consumed by the replay driver.
    """
    head, separator, wire_body = wire.partition(b"\r\n\r\n")
    if not separator or wire_body != (request.body or b""):
        raise ValueError("serialized request does not contain the adapter request body")
    lines = head.decode("ascii").split("\r\n")
    expected_line = f"{request.method} {request.path} HTTP/1.1"
    if not lines or lines[0] != expected_line:
        raise ValueError("serialized request line differs from the adapter request")
    headers = []
    for line in lines[1:]:
        name, separator, value = line.partition(":")
        if not separator:
            raise ValueError(f"malformed serialized request header: {line!r}")
        value = value.lstrip()
        if name.lower() in _SENSITIVE_HEADERS:
            value = "[REDACTED]"
        headers.append({"name": name, "value": value})
    return {
        "task": request.task,
        "query": request.query,
        "parameters": parameters,
        "method": request.method,
        "path": request.path,
        "headers": headers,
        "body": wire_body.decode("utf-8"),
        "wire_sha256": hashlib.sha256(wire).hexdigest(),
    }


_RESPONSE_KEEP = 2
_RESPONSE_TEXT = 160


def _shorten(value):
    if isinstance(value, dict):
        return {key: _shorten(item) for key, item in value.items()}
    if isinstance(value, list):
        short = [_shorten(item) for item in value[:_RESPONSE_KEEP]]
        if len(value) > _RESPONSE_KEEP:
            short.append(f"... {len(value) - _RESPONSE_KEEP} more")
        return short
    if isinstance(value, str) and len(value) > _RESPONSE_TEXT:
        return f"{value[:_RESPONSE_TEXT]}... {len(value) - _RESPONSE_TEXT} more chars"
    return value


def capture_response(raw):
    """Record a shortened form of the response to a captured request.

    What an engine puts in each hit is part of what a cell measures, and only
    the response shows it. Lists keep their first entries and say how many were
    dropped, and long strings are cut, so every key an engine returned stays
    visible without carrying a hundred hits per cell. The size and hash cover
    the complete body.
    """
    try:
        body = _shorten(json.loads(raw))
    except ValueError:
        body = _shorten(raw.decode("utf-8", errors="replace"))
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "body": body}


def request_shape_label(parameters):
    """Name the material request shape while ignoring its particular query."""
    shape = parameters.get("shape", "unknown")
    if shape == "top_docs":
        limit = parameters.get("limit")
        if parameters.get("sort_field"):
            return f"SORT_TOP_{limit}"
        return "COUNT" if limit == 0 else f"TOP_{limit}"
    if shape == "get":
        return f"GET_{parameters.get('batch_size', '?')}"
    if shape == "facet":
        metrics = len(parameters.get("metrics") or ())
        label = "FACET" if not metrics else ("FACET_METRIC" if metrics == 1
                                               else "FACET_METRICS")
        return label + ("_SORT" if parameters.get("facet_sort") else "")
    return {
        "nested_facet": "NESTED_FACET",
        "date_facet": "DATE_FACET",
        "multi": "MULTI",
    }.get(shape, shape.upper())


def captured_http_lines(capture):
    """Render captured request content; JSON is pretty-printed for inspection."""
    body = capture.get("body", "")
    try:
        body = json.dumps(json.loads(body), indent=2)
    except (TypeError, ValueError):
        pass
    lines = [f"{capture.get('method', '?')} {capture.get('path', '?')}"]
    if body:
        lines.append(body)
    return lines
