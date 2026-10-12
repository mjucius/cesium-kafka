# ADR-0022: A tracker partition with no committed cursor replays from its beginning

- **Status:** Accepted
- **Date:** 2026-10-12
- **Design reference:** [`../design.md`](../design.md) §3.6 step 2, D18, R6, R11, R-7 (post-approval
  revision 6); amends [ADR-0012](0012-locked-isolation-and-offset-reset.md) and
  [ADR-0020](0020-runtime-revalidation.md)

## Context

[ADR-0012](0012-locked-isolation-and-offset-reset.md) let the dispatch group (group B) seek a
tracker partition with no committed offset to its beginning only on a "provable first run", meaning
the group had no committed offset on any partition. The dispatch loop read any other missing offset
as expired or removed, and exited.

That check never caught expiry. KIP-211 starts the retention clock when a group becomes empty, and
an empty group's offsets all expire at the same time. The per-partition expiry it proposed for
groups with members was never implemented. The dispatch loop subscribes to the tracker, so
expiry cannot leave some tracker partitions committed and others not. The check fired in three
other cases:

- **A rebalance before the first commits.** A crash-loop soak started three fresh instances under
  live delayed traffic. The first to join took all six tracker partitions and started each from
  offset 0. A cooperative rebalance moved four of them to the other two instances before any
  cursor commit on them, while the first instance committed on the two it still owned. The other
  two instances each found a partition with no commit in a group that had commits, and exited
  within about 3 seconds. Every later owner of those partitions failed the same check, so the
  deployment could stay down until an operator seeded the dispatch group.
- **Tracker growth.** A new tracker partition has no commit while the old ones do, so the loop that
  was assigned it exited. The growth runbook had to stop the engine and seed the dispatch group.
- **Operator-deleted offsets.** An operator deleted the dispatch group's offsets for specific
  tracker partitions.

Replaying a tracker partition from its beginning is correct in all three cases (design §3.6): a
completed entry in the compacted tracker is either paired with its tombstone or fully compacted, so
the replay rebuilds exactly the pending set. The replay has to finish before the cleaner deletes
tombstones it still needs (D14), the condition every recovery runs under.

## Decision

- The dispatch loop seeks any tracker partition with no committed cursor to the partition's
  beginning and replays it, whatever the rest of the group has committed. It logs a WARN that names
  the partition and the three causes (a first run, a newly added partition, offsets deleted for
  that partition) and says the replay is correct but may be slow.
- The group-wide probe, `DispatchAdmin.groupHasCommittedOffsets`, is removed.
- The tracker integrity checks (R11) are unchanged. A committed cursor below the partition's
  beginning or above its high watermark, an out-of-range fetch, and a tracker topic id change
  still fail fast.
- The ingest group (group A) is unchanged. A source partition with no committed offset still fails
  fast under `auto.offset.reset=none`, because there the reset point is a real choice between loss
  and duplication.

## Consequences

- A rebalance that lands before the first cursor commits no longer stops dispatch instances in a
  fresh multi-instance deployment.
- Tracker growth needs no dispatch seeding and is safe under running dispatch instances. Source
  growth still stops ingest and seeds the new source partitions
  ([operations.md §9](../operations.md#9-growing-partitions-runbook)).
- A new tracker partition now reaches an owner only through a rebalance. Under the classic
  protocol, a static leader that rejoins a dispatch group that is still Stable keeps its old
  assignment (KIP-814) and never assigns the new partition, so dispatch instances stopped for
  growth must leave the group before they restart. The runbook ends by checking that every tracker
  partition has an owner.
- An operator who deletes the dispatch group's offsets for specific tracker partitions now gets a
  WARN and a replay of those partitions from the beginning. Before, dispatch stopped until the
  offsets were seeded again.
- A replay from the beginning of a large tracker partition is slow, as it already was on a first
  run. Re-validation's cursor-age check ([ADR-0020](0020-runtime-revalidation.md)) does not cover
  it, because the partition has no committed cursor to age. Watch
  `cesium_replay_remaining_records` for its progress instead.
