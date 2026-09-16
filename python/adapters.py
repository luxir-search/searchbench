"""Per-engine request construction; no load/timing logic lives here."""

from collections import Counter
from dataclasses import dataclass
import json
import math

from schema import BODY_FIELD, ID_FIELD

# Returned by a bucket-metric accessor when the engine did not emit the key at
# all, which is a failure; a null VALUE is legal (see check_bucket_metrics).
MISSING = object()


@dataclass(frozen=True)
class Request:
    method: str
    path: str
    body: bytes
    task: str
    query: str


def topology_summary(segments, shard_count, active_merges=None):
    """Normalize one serving topology and derive totals from its segment rows."""
    segments = sorted(segments, key=lambda segment: (segment["shard"], segment["id"]))
    identities = [(segment["shard"], segment["id"]) for segment in segments]
    if len(identities) != len(set(identities)):
        raise RuntimeError("segment topology contains duplicate shard/id pairs")
    sizes = [segment.get("size_bytes") for segment in segments]
    result = {
        "shard_count": shard_count,
        "segment_count": len(segments),
        "max_docs": sum(segment["max_docs"] for segment in segments),
        "live_docs": sum(segment["live_docs"] for segment in segments),
        "deleted_docs": sum(segment["deleted_docs"] for segment in segments),
        "size_bytes": sum(sizes) if sizes and all(size is not None for size in sizes) else None,
        "segments": segments,
    }
    if active_merges is not None:
        result["active_merges"] = active_merges
    return result


def expr_quote(text):
    """Quote a literal for a Luxir expression function argument."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def luxir_query_clause(item):
    """Run the benchmark's Lucene query syntax through Luxir's strict parser.

    Wildcard and regex are the exceptions: Luxir's query language keeps
    mid-word * and /re/ as ordinary characters by design and exposes both
    query types as named functions instead, so those classes call the
    functions directly. Trailing-star prefixes parse natively.
    """
    if item.query_class in ("wildcard", "wildcard_scan", "wildcard_lead"):
        query = {"expr": {"q": f"wildcard({expr_quote(item.text)}, field={BODY_FIELD})"}}
    elif item.query_class == "regex":
        query = {"expr": {"q": f"regex({expr_quote(item.text)}, field={BODY_FIELD})"}}
    else:
        query = {"expr": {"q": f"{BODY_FIELD}:({item.text})"}}
    if item.nonce:
        query = {"boolean": {
            "optional": [query, {"match": {"field": ID_FIELD, "val": item.nonce}}],
            "min_match": 1,
        }}
    return query


def rest_query_clause(item):
    """Use the Lucene query-string parser exposed by OpenSearch and Elasticsearch.

    Regex is the exception: query_string would need /re/ wrapping with its own
    escaping rules, so the class uses the structured regexp query, which is the
    same RegexpQuery the wildcard class reaches through query_string's * syntax.
    """
    if item.query_class == "regex":
        query = {"regexp": {BODY_FIELD: {"value": item.text}}}
    else:
        query = {"query_string": {"query": item.text,
                                  "default_field": BODY_FIELD,
                                  "default_operator": "OR"}}
    if item.nonce:
        query = {"bool": {
            "should": [query, {"term": {ID_FIELD: {"value": item.nonce}}}],
            "minimum_should_match": 1,
        }}
    return query


def luxir_filter_clause(params):
    kind = params.get("filter_kind", "term")
    if kind == "term":
        return {"match": {"field": params["filter_field"],
                          "val": params["filter_value"]}}
    if kind == "numeric_range":
        bounds = {name: params[f"filter_{name}"]
                  for name in ("gt", "gte", "lt", "lte")
                  if f"filter_{name}" in params}
        if not bounds:
            raise ValueError("numeric_range filter requires at least one bound")
        return {"range": {"field": params["filter_field"], **bounds}}
    raise ValueError(f"unknown filter_kind {kind!r}")


def rest_filter_clause(params):
    kind = params.get("filter_kind", "term")
    if kind == "term":
        return {"term": {
            params["filter_field"]: {"value": params["filter_value"]}}}
    if kind == "numeric_range":
        bounds = {name: params[f"filter_{name}"]
                  for name in ("gt", "gte", "lt", "lte")
                  if f"filter_{name}" in params}
        if not bounds:
            raise ValueError("numeric_range filter requires at least one bound")
        return {"range": {params["filter_field"]: bounds}}
    raise ValueError(f"unknown filter_kind {kind!r}")


def wants_total(params):
    """Does this request ask the engine for the total match count at all?

    Distinct from count_mode, which says how *accurate* a requested total must
    be. Asking for one is not free and is not just a reporting choice: it
    disables luxir's pruning and Elasticsearch's early termination, so a cell
    that does not read the total should not pay for it. Defaults true, which is
    what every preset did before the parameter existed.
    """
    return params.get("total_hits", True)


def facet_metrics(params):
    """Per-bucket numeric metrics on a `facet` shape: ({name, op, field}, ...).

    Distinct from `multi`'s `stats`, which are one scalar each over the whole
    domain: these are evaluated independently inside every bucket. Empty for
    every count-only request, so a cell that asks for none sends exactly the
    bytes it did before this parameter existed.
    """
    return tuple(params.get("metrics") or ())


def facet_order(params):
    """(metric_name, direction) the buckets are ordered by, or None for count.

    Count-descending is every engine's default and is left unstated rather than
    sent explicitly, again so count-ordered cells are byte-unchanged.
    """
    name = params.get("facet_sort")
    return (name, params.get("facet_sort_dir", "desc")) if name else None


class BaseAdapter:
    # Query shapes this engine can execute with a faithful single request.
    # The initial engine set has no restrictions.
    supported_shapes = None
    # ((name, value), ...) sent during validation and replay.
    headers = ()
    # Parameters this adapter provably never reads, so they cannot change the
    # request it builds. They stay in param_hash (the exact identity of a run)
    # but are excluded from comparable_hash, which is what report tooling
    # matches on. Without this, a parameter only one engine implements splits
    # every other engine's identical measurements into series that refuse to be
    # tabulated together, and the only way back is re-running cells to relabel
    # byte-identical work.
    inert_params = frozenset()
    _fail_rules = ()

    def __init__(self, collection):
        self.collection = collection
        # Max facet-count error bound observed across validated responses;
        # None = engine reports no bound (luxir counts exactly). 0 = exact.
        self.facet_error = None

    def encode(self, method, path, body, task, query):
        return Request(method, path, json.dumps(body, separators=(",", ":")).encode(), task, query)

    # wire_bytes() is what lands in the replay blob; connect() is the same HTTP
    # exchange used by the untimed validation pass. The two must agree byte for
    # byte or validation would cover a different request than measured replay.

    def wire_bytes(self, request, host, port):
        body = request.body or b""
        # Identity encoding keeps byte-level replay fail-marker scans valid.
        extra = "".join(f"{name}: {value}\r\n" for name, value in self.headers)
        head = (f"{request.method} {request.path} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Content-Type: application/json\r\n"
                "Accept: application/json, application/x-ndjson\r\n"
                "Connection: keep-alive\r\n"
                f"{extra}"
                f"Content-Length: {len(body)}\r\n\r\n").encode("ascii")
        return head + body

    def connect(self, host, port, timeout):
        from async_http import AsyncHttpConnection
        return AsyncHttpConnection(host, port, timeout, self.headers)

    def replay_args(self):
        return []

    def check(self, raw):
        raise NotImplementedError

    def fail_rules(self, shape):
        return () if shape == "get" else self._fail_rules

    def validate(self, params, raw, item=None):
        """Full response validation for the untimed validate pass: runs
        check(), asserts the shape's payload actually came back (facet
        buckets, docs, stats), and returns the total-hits count when present."""
        raise NotImplementedError

    @staticmethod
    def validate_get_docs(params, item, docs):
        if item is None or not item.ids:
            raise RuntimeError("get validation requires the requested ids")
        expected = list(item.ids)
        if len(expected) != params["batch_size"]:
            raise RuntimeError(
                f"get work item has {len(expected)} ids, expected {params['batch_size']}")
        if not isinstance(docs, list) or len(docs) != len(expected):
            actual = len(docs) if isinstance(docs, list) else "no"
            raise RuntimeError(f"get returned {actual} docs, expected {len(expected)}")
        fields = params["fields"]
        for doc in docs:
            if not isinstance(doc, dict):
                raise RuntimeError("get returned a non-object document")
            missing = [field for field in fields if field not in doc]
            if missing:
                raise RuntimeError(f"get document is missing projected fields {missing}")
        actual = [doc[ID_FIELD] for doc in docs]
        if Counter(actual) != Counter(expected):
            raise RuntimeError("get response ids differ from the requested ids")

    def build_field_probe(self, field):
        """Build an all-docs one-bucket facet that proves a field is indexed."""
        raise NotImplementedError

    def build_topology_probe(self):
        """Requests for committed serving topology and merge activity."""
        raise NotImplementedError

    @staticmethod
    def parse_topology(raw):
        raise NotImplementedError

    def validate_field_probe(self, raw, field):
        params = {"shape": "facet", "limit": 0, "name": f"field probe {field}"}
        count = self.validate(params, raw)
        if count is None or count <= 0:
            raise RuntimeError(f"field probe {field}: index is empty or did not return a count")
        return count

    def validate_selected_probe(self, params, raw, expected):
        """Assert a selected-cell probe returned exact designated counts."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement facet_selected")

    @staticmethod
    def expected_facets(params):
        """(name, ...) of facet results the response must carry."""
        shape = params["shape"]
        if shape in ("facet", "date_facet"):
            return ("facet",)
        if shape == "multi":
            return tuple(f["name"] for f in params["facets"])
        return ()

    @staticmethod
    def expected_stats(params):
        return tuple(s["name"] for s in params.get("stats", ())) if params["shape"] == "multi" else ()

    @staticmethod
    def check_bucket_metrics(params, name, buckets, get):
        """Per-bucket metrics came back, in the order the request asked for.

        `get(bucket, metric_name)` returns the metric's value, or MISSING when
        the engine emitted no such key. A null value is legal - a bucket whose
        documents all lack the column has no average - but an absent key is
        not: it means the engine answered a cheaper request than the one sent.

        The order check is the same guard applied to the metric-ordered
        variants, and it is what separates measuring the plan the cell asked
        for from measuring a count-ordered plan the engine quietly substituted.
        Missing values sort below every present one (luxir orders empty buckets
        that way; ES emits no empty buckets at all).
        """
        metrics = facet_metrics(params)
        if not metrics:
            return
        for bucket in buckets:
            absent = [metric["name"] for metric in metrics
                      if get(bucket, metric["name"]) is MISSING]
            if absent:
                raise RuntimeError(f"{name}: bucket is missing metrics {absent}")
        order = facet_order(params)
        if not order or len(buckets) < 2:
            return
        metric, direction = order
        values = []
        for bucket in buckets:
            value = get(bucket, metric)
            values.append(-math.inf if value is None or value is MISSING else value)
        pairs = list(zip(values, values[1:]))
        ordered = (all(a >= b for a, b in pairs) if direction == "desc"
                   else all(a <= b for a, b in pairs))
        if not ordered:
            raise RuntimeError(f"{name}: buckets are not ordered by {metric} "
                               f"{direction}: {values}")


class LuxirAdapter(BaseAdapter):
    # request_cache is an ES/OS shard-cache switch with no luxir analogue.
    inert_params = frozenset({"request_cache"})
    _fail_rules = (("present", b'"error"'),)

    def fail_rules(self, shape):
        if shape == "health":
            return self._fail_rules + (("absent", b'"status":"ok"'),)
        return super().fail_rules(shape)

    def build_field_probe(self, field):
        top = {"query": {"all": True}, "limit": 0, "get_number": True,
               "ops": {"facet": {"field_facet": {"field": field, "limit": 1}}}}
        return self.encode("POST", f"/collections/{self.collection}/_search",
                           {"ops": {"q": {"top_docs": top}}}, "field-probe", "*:*")

    def build_topology_probe(self):
        return (Request("GET", f"/collections/{self.collection}/_stats?segments=true",
                        b"", "topology-probe", ""),)

    @staticmethod
    def parse_topology(responses):
        raw, = responses
        value = json.loads(raw)
        collections = value.get("collections", [])
        if len(collections) != 1:
            raise RuntimeError(
                f"Luxir topology expected one collection, found {len(collections)}")
        shards = collections[0].get("shards", [])
        if len(shards) != 1:
            raise RuntimeError(f"Luxir topology expected one shard, found {len(shards)}")
        index = shards[0].get("index", {})
        source = index.get("segments", [])
        uncommitted = [segment for segment in source if not segment.get("committed", False)]
        if uncommitted:
            raise RuntimeError(
                f"Luxir topology contains {len(uncommitted)} uncommitted segments")
        segments = []
        shard = shards[0].get("shard_id", 0)
        for segment in source:
            max_docs = segment.get("max_doc", 0)
            live_docs = segment.get("live_docs", 0)
            deleted_docs = segment.get("deleted_docs", 0)
            if max_docs != live_docs + deleted_docs:
                raise RuntimeError(
                    f"Luxir segment {segment.get('seg')} has inconsistent doc counts")
            size_bytes = segment.get("bytes")
            segments.append({
                # "seg" is the segment's on-disk data-file prefix (e.g. "s0a").
                "id": segment.get("seg"),
                "shard": shard,
                "primary": True,
                "max_docs": max_docs,
                "live_docs": live_docs,
                "deleted_docs": deleted_docs,
                # Segment data + deletes + overlays on disk; the index-level
                # directory total also counts manifest/schema/in-flight files
                # and is deliberately not used here so the sum stays
                # comparable with the REST engines' per-segment sizes.
                "size_bytes": int(size_bytes) if size_bytes is not None else None,
                "committed": True,
                "searchable": True,
                "merging": segment.get("merging", False),
                "merge_level": segment.get("merge_level", 0),
            })
        expected = index.get("totals", {}).get("committed_segments", 0)
        if expected != len(segments):
            raise RuntimeError(
                f"Luxir reports {expected} committed segments but returned {len(segments)}")
        return topology_summary(
            segments, shard_count=1, active_merges=index.get("active_merges", 0))

    def build(self, params, item):
        if params["shape"] == "health":
            return Request("GET", "/health", b"", params.get("name", "HEALTH"), item.text)
        if params["shape"] == "get":
            return self.build_get(params, item)
        top = {"query": {"all": True} if params.get("match_all") else luxir_query_clause(item),
               "limit": params["limit"]}
        if params.get("filter_field"):
            fq = luxir_filter_clause(params)
            filter_mode = params.get("filter_mode", "top_docs")
            if filter_mode == "boolean":
                # Rewrite: filter as a non-scoring BooleanQuery clause instead
                # of a TopDocs domain filter (execution-strategy A/B).
                top["query"] = {"boolean": {"filter": [fq], "required": [top["query"]]}}
            elif filter_mode == "query":
                if not params.get("match_all"):
                    raise ValueError("filter_mode=query requires match_all=true")
                # The selected docs are the main query rather than a TopDocs
                # filter. This is semantically equivalent only when the query
                # it replaces is match-all.
                top["query"] = fq
            elif filter_mode == "top_docs":
                top["filter"] = [fq]
            else:
                raise ValueError(f"unknown luxir filter_mode {filter_mode!r}")
        if params.get("fields"):
            top["fields"] = params["fields"]
        if wants_total(params) and params["count_mode"] == "exact":
            top["get_number"] = True
        if params.get("sort_field"):
            top["sort"] = [{"expr": params["sort_field"],
                             "dir": params.get("sort_dir", "asc").lower()}]
        shape = params["shape"]
        if shape == "facet":
            facet = {"field": params["facet_field"], "limit": params["facet_limit"]}
            metrics = facet_metrics(params)
            if metrics:
                facet["ops"] = {
                    metric["name"]: f"{metric['op']}({metric['field']})"
                    for metric in metrics}
            order = facet_order(params)
            if order:
                # Luxir resolves a facet sort expression as the name of one of
                # the facet's own metric ops (one sort key, no document-value
                # expressions - see docs/guide/faceting.md).
                facet["sort"] = [{"expr": order[0], "dir": order[1]}]
            if params.get("facet_selected"):
                facet["selected"] = list(params["facet_selected"])
            top["ops"] = {"facet": {"field_facet": facet}}
        elif shape == "nested_facet":
            top["ops"] = {"facet": {"field_facet": {
                "field": params["facet_field"], "limit": params["facet_limit"],
                "ops": {"subfacet": {"field_facet": {
                    "field": params["subfacet_field"], "limit": params["subfacet_limit"]}}}}}}
        elif shape == "date_facet":
            top["ops"] = {"facet": {"range_facet": {"field": params["date_field"],
                                                    "start": params["date_start_ms"],
                                                    "end": params["date_end_ms"],
                                                    "gap": params["date_gap_ms"],
                                                    "mincount": params["mincount"]}}}
        elif shape == "multi":
            ops = {}
            for facet in params["facets"]:
                ops[facet["name"]] = {"field_facet": {"field": facet["field"],
                                                      "limit": facet["limit"]}}
            for stat in params["stats"]:
                ops[stat["name"]] = f"{stat['op']}({stat['field']})"
            top["ops"] = ops
        elif shape != "top_docs":
            raise ValueError(f"unknown shape {shape}")
        body = {"ops": {"q": {"top_docs": top}}}
        if params.get("max_parallel"):
            # Request-level intra-request parallelism cap (1 = serial on the
            # shared arena, -1 = unlimited there; 0, luxir's inline-serial
            # serving default since 2026-09-01, equals the wire default and is
            # correctly elided by the truthy check).  REST adapters have no
            # equivalent and ignore the param: ES is serial per shard
            # natively; OS is serial under the exact-count posture baked at
            # index creation.
            body["max_parallel"] = params["max_parallel"]
        return self.encode("POST", f"/collections/{self.collection}/_search", body,
                           params.get("name", shape), item.text)

    def build_get(self, params, item):
        impl = params.get("get_impl", "boolean")
        if impl != "boolean":
            raise ValueError(f"unknown luxir get_impl {impl!r}")
        if len(item.ids) != params["batch_size"]:
            raise ValueError("get work item does not match batch_size")
        matches = [{"match": {"field": ID_FIELD, "val": doc_id}} for doc_id in item.ids]
        query = matches[0] if len(matches) == 1 else {
            "boolean": {"optional": matches, "min_match": 1}}
        query = {"constant_score": {"query": query}}
        top = {"query": query, "limit": len(matches), "fields": params["fields"]}
        body = {"ops": {"q": {"top_docs": top}}}
        return self.encode("POST", f"/collections/{self.collection}/_search", body,
                           params.get("name", "get"), item.text)

    def count(self, raw):
        counts = []
        for line in raw.splitlines():
            if not line:
                continue
            value = json.loads(line)
            if isinstance(value.get("found"), int):
                counts.append(value["found"])
            q = value.get("ops", {}).get("q")
            if isinstance(q, dict):
                for key in ("matches", "found"):
                    if isinstance(q.get(key), int):
                        counts.append(q[key])
        return counts[0] if counts else None

    def check(self, raw):
        for line in raw.splitlines():
            if not line:
                continue
            value = json.loads(line)
            if value.get("error"):
                raise RuntimeError(f"Luxir response error: {value['error']}")

    def validate(self, params, raw, item=None):
        self.check(raw)
        if params["shape"] == "health":
            if json.loads(raw).get("status") != "ok":
                raise RuntimeError("Luxir health response is not ok")
            return None
        name = params.get("name", params["shape"])
        # A response may stream as multiple NDJSON lines (batched doc lists
        # with more:true); aggregate docs and ops across all of them.
        found = None
        docs = None
        ops = {}
        for line in raw.splitlines():
            if not line:
                continue
            value = json.loads(line)
            if found is None and isinstance(value.get("found"), int):
                found = value["found"]
            if isinstance(value.get("docs"), list):
                docs = (docs or []) + value["docs"]
            ops.update(value.get("ops", {}))

        if params["shape"] == "get":
            self.validate_get_docs(params, item, docs)
            return None
        if params["limit"]:
            if docs is None:
                raise RuntimeError(f"{name}: missing docs")
            if found and not docs:
                raise RuntimeError(f"{name}: empty docs with found={found}")
        for facet in self.expected_facets(params):
            result = ops.get(facet)
            if not isinstance(result, dict) or not isinstance(result.get("buckets"), list):
                raise RuntimeError(f"{name}: missing facet result '{facet}'")
            if found and not result["buckets"]:
                raise RuntimeError(f"{name}: empty '{facet}' buckets with found={found}")
            if params["shape"] == "nested_facet":
                for bucket in result["buckets"]:
                    subfacet = bucket.get("subfacet")
                    if not isinstance(subfacet, dict) or not isinstance(subfacet.get("buckets"), list):
                        raise RuntimeError(f"{name}: missing nested facet result 'subfacet'")
            # Luxir keeps each metric beside its bucket's val/count.
            self.check_bucket_metrics(params, name, result["buckets"],
                                      lambda bucket, metric: bucket.get(metric, MISSING))
            # Selected values must come back exactly once each - merged into
            # the natural page or appended after it, but never dropped and
            # never duplicated. This is what separates measuring selection
            # from measuring a plain facet the engine quietly substituted.
            if params.get("facet_selected") and params["shape"] == "facet":
                values = [bucket.get("val") for bucket in result["buckets"]]
                for value in params["facet_selected"]:
                    if values.count(value) != 1:
                        raise RuntimeError(
                            f"{name}: selected value {value!r} appears "
                            f"{values.count(value)} times in '{facet}' buckets")
        for stat in self.expected_stats(params):
            if stat not in ops:
                raise RuntimeError(f"{name}: missing stat '{stat}'")
        return self.count(raw)

    def validate_selected_probe(self, params, raw, expected):
        """Exact-count check for a selected-cell probe (unfiltered row, totals
        on). Selection refines sideways - documents, never the facet's own
        counts - so each pinned bucket must carry its value's designated
        whole-corpus count whether it merged into the natural page or appended
        after it, and the refined document total must equal the pins' sum
        (the ladder fields are single-valued, so selected postings are
        disjoint)."""
        name = params.get("name", "selected probe")
        buckets = {}
        for line in raw.splitlines():
            if not line:
                continue
            value = json.loads(line)
            result = value.get("ops", {}).get("facet")
            if isinstance(result, dict):
                for bucket in result.get("buckets", ()):
                    buckets[bucket.get("val")] = bucket.get("count")
        for value, count in expected.items():
            if buckets.get(value) != count:
                raise RuntimeError(
                    f"{name}: pinned bucket {value!r} carries count "
                    f"{buckets.get(value)!r}; the corpus report says {count}")
        found = self.count(raw)
        if found != sum(expected.values()):
            raise RuntimeError(
                f"{name}: refined total {found!r} != {sum(expected.values())}, "
                f"the sum of the pinned values' exact counts")


class RestAdapter(BaseAdapter):
    # ES is serial per shard natively and OS is serial under the exact-count
    # posture, so max_parallel never reaches a request here.
    inert_params = frozenset({"max_parallel"})
    _fail_rules = (("absent", b'"timed_out":false'), ("absent", b'"failed":0'))

    def __init__(self, collection, engine):
        super().__init__(collection)
        self.engine = engine

    def build_field_probe(self, field):
        body = {"query": {"match_all": {}}, "size": 0, "_source": False,
                "track_total_hits": True,
                "aggs": {"facet": self.terms_agg(field, 1)}}
        return self.encode("POST", f"/{self.collection}/_search", body,
                           "field-probe", "*:*")

    def search_path(self, params):
        path = f"/{self.collection}/_search"
        if params.get("request_cache") is not None:
            path += f"?request_cache={'true' if params['request_cache'] else 'false'}"
        return path

    def build_topology_probe(self):
        return (
            Request("GET", f"/{self.collection}/_segments",
                    b"", "topology-probe", ""),
            Request("GET", f"/{self.collection}/_stats/merge?level=shards",
                    b"", "merge-probe", ""),
        )

    @staticmethod
    def parse_topology(responses):
        raw, merge_raw = responses
        value = json.loads(raw)
        indices = value.get("indices", {})
        if len(indices) != 1:
            raise RuntimeError(
                f"REST topology expected one index, found {len(indices)}")
        shard_map = next(iter(indices.values())).get("shards", {})
        segments = []
        for shard_text, copies in shard_map.items():
            primaries = [copy for copy in copies
                         if copy.get("routing", {}).get("primary") is True]
            if len(copies) != 1 or len(primaries) != 1:
                raise RuntimeError(
                    f"REST topology expected one primary and zero replicas for shard "
                    f"{shard_text}, found {len(primaries)} primaries among "
                    f"{len(copies)} copies")
            copy = primaries[0]
            source = copy.get("segments", {})
            committed = copy.get("num_committed_segments", len(source))
            searchable = copy.get("num_search_segments", len(source))
            if committed != len(source) or searchable != len(source):
                raise RuntimeError(
                    f"REST shard {shard_text} has {len(source)} segment records, "
                    f"{committed} committed, and {searchable} searchable")
            for name, segment in source.items():
                if not segment.get("committed", False) or not segment.get("search", False):
                    raise RuntimeError(
                        f"REST segment {name} is not both committed and searchable")
                live_docs = segment.get("num_docs", 0)
                deleted_docs = segment.get("deleted_docs", 0)
                segments.append({
                    "id": name,
                    "shard": int(shard_text),
                    "primary": True,
                    "max_docs": live_docs + deleted_docs,
                    "live_docs": live_docs,
                    "deleted_docs": deleted_docs,
                    "size_bytes": segment.get("size_in_bytes"),
                    "committed": True,
                    "searchable": True,
                    "compound": segment.get("compound"),
                    "merging": bool(segment.get("merge_id")),
                    "merge_id": segment.get("merge_id"),
                })
        merge_value = json.loads(merge_raw)
        merge_indices = merge_value.get("indices", {})
        if len(merge_indices) != 1:
            raise RuntimeError(
                f"REST merge stats expected one index, found {len(merge_indices)}")
        active_merges = (next(iter(merge_indices.values()))
                         .get("primaries", {}).get("merges", {}).get("current", 0))
        return topology_summary(
            segments, shard_count=len(shard_map), active_merges=active_merges)

    def build(self, params, item):
        if params["shape"] == "get":
            return self.build_get(params, item)
        body = {"query": {"match_all": {}} if params.get("match_all")
                else rest_query_clause(item)}
        if params.get("filter_field"):
            predicate = rest_filter_clause(params)
            filter_mode = params.get("filter_mode", "top_docs")
            if filter_mode == "query":
                if not params.get("match_all"):
                    raise ValueError("filter_mode=query requires match_all=true")
                # Query context, uncached. constant_score or bool.filter would
                # reintroduce the cached-domain posture this mode avoids.
                body["query"] = predicate
            elif filter_mode in ("top_docs", "boolean"):
                body["query"] = {"bool": {"must": [body["query"]],
                                          "filter": [predicate]}}
            else:
                raise ValueError(f"unknown REST filter_mode {filter_mode!r}")
        if not wants_total(params):
            # Explicitly off, not merely absent: the ES/OS default caps at
            # 10000 and still counts up to it, which keeps early termination
            # disabled. False is what actually lets the collector stop early.
            body["track_total_hits"] = False
        elif params["count_mode"] == "exact":
            body["track_total_hits"] = True
        # Scalar projections come from doc values. _source stays off so hit
        # retrieval never decompresses the stored source blob.
        body.update(size=params["limit"], _source=False)
        if params.get("fields"):
            body["docvalue_fields"] = params["fields"]
        if params.get("sort_field"):
            body["sort"] = [{params["sort_field"]: params.get("sort_dir", "asc")}]
        shape = params["shape"]
        if shape == "facet":
            if params.get("facet_selected"):
                # Deliberately untranslated: the REST engines have no pinned
                # selection, and an emulation (post_filter plus a filters agg)
                # would measure a different mechanism under the same cell name.
                raise ValueError("facet_selected is luxir-only; the REST "
                                 "adapters refuse it rather than emulate it")
            facet = self.terms_agg(params["facet_field"], params["facet_limit"])
            metrics = facet_metrics(params)
            if metrics:
                facet["aggs"] = {metric["name"]: {metric["op"]: {"field": metric["field"]}}
                                 for metric in metrics}
            order = facet_order(params)
            if order:
                facet["terms"]["order"] = {order[0]: order[1]}
            body["aggs"] = {"facet": facet}
        elif shape == "nested_facet":
            body["aggs"] = {"facet": self.terms_agg(params["facet_field"], params["facet_limit"])}
            body["aggs"]["facet"]["aggs"] = {
                "subfacet": self.terms_agg(params["subfacet_field"], params["subfacet_limit"])}
        elif shape == "date_facet":
            gap_days = params["date_gap_ms"] // (24 * 60 * 60 * 1000)
            # +22d aligns REST epoch-anchored buckets to the 2020-01-01 start.
            body["aggs"] = {"facet": {"date_histogram": {
                "field": params["date_field"], "fixed_interval": f"{gap_days}d",
                "offset": "+22d", "min_doc_count": params["mincount"]}}}
        elif shape == "multi":
            aggs = {}
            for facet in params["facets"]:
                aggs[facet["name"]] = self.terms_agg(facet["field"], facet["limit"],
                                                     doc_count_error=False)
            for stat in params["stats"]:
                aggs[stat["name"]] = {stat["op"]: {"field": stat["field"]}}
            body["aggs"] = aggs
        elif shape != "top_docs":
            raise ValueError(f"unknown shape {shape}")
        return self.encode("POST", self.search_path(params), body,
                           params.get("name", shape), item.text)

    def build_get(self, params, item):
        if len(item.ids) != params["batch_size"]:
            raise ValueError("get work item does not match batch_size")
        task = params.get("name", "get")
        if len(item.ids) == 1:
            predicate = {"term": {ID_FIELD: {"value": item.ids[0]}}}
        else:
            predicate = {"terms": {ID_FIELD: list(item.ids)}}
        body = {
            "query": {"constant_score": {"filter": predicate}},
            "size": len(item.ids),
            "track_total_hits": False,
            "_source": False,
            "docvalue_fields": params["fields"],
        }
        return self.encode("POST", self.search_path(params), body, task, item.text)

    @staticmethod
    def projected_doc(hit, fields, name):
        values = hit.get("fields")
        if not isinstance(values, dict):
            raise RuntimeError(f"{name}: hit has no doc-value fields")
        result = {}
        for field in fields:
            value = values.get(field)
            if not isinstance(value, list) or len(value) != 1:
                raise RuntimeError(
                    f"{name}: projected scalar field {field!r} did not return one value")
            result[field] = value[0]
        return result

    @staticmethod
    def terms_agg(field, size, doc_count_error=True):
        agg = {"terms": {"field": field, "size": size, "shard_size": size}}
        if doc_count_error:
            agg["terms"]["show_term_doc_count_error"] = True
        return agg

    def count(self, raw):
        value = json.loads(raw)
        total = value.get("hits", {}).get("total")
        if isinstance(total, dict) and isinstance(total.get("value"), int):
            return total["value"]
        if isinstance(total, int):
            return total
        return None

    def check(self, raw):
        value = json.loads(raw)
        if value.get("timed_out"):
            raise RuntimeError("REST search response timed out")
        if value.get("_shards", {}).get("failed", 0):
            raise RuntimeError(f"REST shard failures: {value.get('_shards')}")

    def validate(self, params, raw, item=None):
        self.check(raw)
        name = params.get("name", params["shape"])
        value = json.loads(raw)
        if params["shape"] == "get":
            hits = value.get("hits", {}).get("hits")
            if not isinstance(hits, list):
                raise RuntimeError(f"{name}: missing search hits")
            docs = [self.projected_doc(hit, params["fields"], name) for hit in hits]
            self.validate_get_docs(params, item, docs)
            return None
        count = self.count(raw)
        aggs = value.get("aggregations", {})

        if params["limit"]:
            hits = value.get("hits", {}).get("hits")
            if not isinstance(hits, list):
                raise RuntimeError(f"{name}: missing hits")
            if count and not hits:
                raise RuntimeError(f"{name}: empty hits with total={count}")
            if params.get("fields"):
                for hit in hits:
                    self.projected_doc(hit, params["fields"], name)
        for facet in self.expected_facets(params):
            result = aggs.get(facet)
            if not isinstance(result, dict) or not isinstance(result.get("buckets"), list):
                raise RuntimeError(f"{name}: missing aggregation '{facet}'")
            if count and not result["buckets"]:
                raise RuntimeError(f"{name}: empty '{facet}' buckets with total={count}")
            if params["shape"] == "nested_facet":
                for bucket in result["buckets"]:
                    subfacet = bucket.get("subfacet")
                    if not isinstance(subfacet, dict) or not isinstance(subfacet.get("buckets"), list):
                        raise RuntimeError(f"{name}: missing nested aggregation 'subfacet'")
            # A sub-aggregation's result is {"value": x} beside the bucket.
            self.check_bucket_metrics(
                params, name, result["buckets"],
                lambda bucket, metric: bucket.get(metric, {}).get("value", MISSING))
            error = result.get("doc_count_error_upper_bound")
            if error is not None:
                self.facet_error = max(self.facet_error or 0, error)
        for stat in self.expected_stats(params):
            if "value" not in aggs.get(stat, {}):
                raise RuntimeError(f"{name}: missing stat '{stat}'")
        return count


# One definition per engine, shared by the driver and request-inspection tools.
ENGINES = ("luxir", "opensearch", "elasticsearch")
DEFAULT_PORTS = {"luxir": 9400, "opensearch": 9201, "elasticsearch": 9202}


def make_adapter(engine, collection):
    if engine == "luxir":
        return LuxirAdapter(collection)
    if engine in ("opensearch", "elasticsearch"):
        return RestAdapter(collection, engine)
    raise ValueError(f"unknown engine {engine!r}")
