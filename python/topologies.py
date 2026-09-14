"""Named, deterministic serving-segment layouts for query campaigns."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Topology:
    name: str
    segment_docs: tuple[int, ...]
    description: str

    @property
    def documents(self):
        return sum(self.segment_docs)

    @property
    def segment_count(self):
        return len(self.segment_docs)

    def after_ranges(self, count):
        """Declared serving shape after the first ``count`` feed ranges."""
        if count < 1 or count > self.segment_count:
            raise ValueError(
                f"topology {self.name!r} has no {count}-range construction prefix")
        return Topology(
            name=f"{self.name}@range-{count}",
            segment_docs=self.segment_docs[:count],
            description=f"{self.description}, after {count} feed ranges",
        )


# Deliberately uneven: equal chunks would exercise fan-out, but not the skew a
# tiered merge policy leaves behind. The vector is also the feed boundary and
# the post-feed assertion, so the name cannot silently drift to a new shape.
ONE_SEGMENT = Topology(
    name="one-segment",
    segment_docs=(10_000_000,),
    description="force-merged standard 10M baseline",
)

TIERED_5 = Topology(
    name="tiered-5",
    segment_docs=(4_000_000, 3_000_000, 1_500_000, 1_000_000, 500_000),
    description="five descending tiers over the standard 10M corpus",
)

TIERED_45 = Topology(
    name="tiered-45",
    segment_docs=(
        1_000_100, *(1_000_000,) * 8,
        *(100_000,) * 9,
        *(10_000,) * 9,
        *(1_000,) * 9,
        *(100,) * 9,
    ),
    description=(
        "TieredMergePolicy steady state: nine segments at each of five 10x "
        "tiers; the largest segment absorbs the 100-document 10M remainder"),
)

TOPOLOGIES = {
    topology.name: topology
    for topology in (ONE_SEGMENT, TIERED_5, TIERED_45)
}


def resolve_topology(name):
    try:
        return TOPOLOGIES[name]
    except KeyError as error:
        raise ValueError(
            f"unknown topology {name!r}; expected one of {sorted(TOPOLOGIES)}") from error


def assert_topology(topology, observed):
    """Require the complete declared shape, not merely its segment count."""
    errors = []
    if observed.get("shard_count") != 1:
        errors.append(f"expected one shard, found {observed.get('shard_count')}")
    if observed.get("segment_count") != topology.segment_count:
        errors.append(
            f"expected {topology.segment_count} segments, found "
            f"{observed.get('segment_count')}")
    if observed.get("max_docs") != topology.documents:
        errors.append(
            f"expected {topology.documents} max docs, found {observed.get('max_docs')}")
    segments = observed.get("segments", ())
    actual_docs = sorted(segment.get("max_docs") for segment in segments)
    if actual_docs != sorted(topology.segment_docs):
        errors.append(
            f"expected segment documents {list(topology.segment_docs)}, found {actual_docs}")
    deleted = [segment.get("deleted_docs") for segment in segments]
    if any(value != 0 for value in deleted):
        errors.append(f"append-only topology contains deleted documents: {deleted}")
    if observed.get("deleted_docs") != 0:
        errors.append(
            f"expected zero deleted docs, found {observed.get('deleted_docs')}")
    if observed.get("live_docs") != topology.documents:
        errors.append(
            f"expected {topology.documents} live docs, found {observed.get('live_docs')}")
    if "active_merges" not in observed:
        errors.append("topology observation omitted active merge count")
    elif observed["active_merges"]:
        errors.append(f"topology has {observed['active_merges']} active merges")
    merging = [segment.get("id") for segment in segments if segment.get("merging")]
    if merging:
        errors.append(f"segments are actively merging: {merging}")
    if errors:
        raise RuntimeError(f"topology {topology.name!r} mismatch: " + "; ".join(errors))
