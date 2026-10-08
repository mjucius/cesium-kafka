# ADR-0020: Runtime re-validation

- **Status:** Accepted
- **Date:** 2026-10-07
- **Design reference:** [`../design.md`](../design.md) D23, D14, §3.7, §6, §7.6, R-7/R-8/R-11, R17;
  amends [ADR-0002](0002-pointer-only-payloads-and-retention-validation.md),
  [ADR-0005](0005-tracker-format-compaction-only-and-tombstone-retention-floor.md) and
  [ADR-0018](0018-bounded-wait-for-proven-topic-metadata.md)

## Context

The design promised periodic re-validation: partition parity (R-7), the observed terms of the D14
tombstone-retention floor (R-8, R20), topic identity (R17), an earliest-available-age probe feeding
`cesium_retention_margin_seconds` (R-11), and a tracker-size monitor. 1.x built none of it. Every
check ran once at startup, so an operator who lowered the tracker's `delete.retention.ms`, grew a
topic, or recreated the source under a running engine got no signal until the next restart.

The design also said a D14 violation found at runtime should refuse rather than alert, and that
runtime partition drift should fail fast. Stopping does not help in either case. The partitions move
to surviving instances, which replay from the same committed cursor and meet the same finding. A
recovery hold freezes the cursor, so a cursor-age breach could never clear. Forward dispatch is what
shrinks the exposure, and the remedies (raise `delete.retention.ms`, finish growing the tracker)
apply to a running cluster.

## Decision

- A dedicated `cesium-admin` daemon thread runs one re-validation round every
  `startup-checks.revalidation-interval` (default `PT5M`, must be positive):
  `StartupValidator.revalidate`. It re-runs the source, tracker, DLQ and destination checks against
  the live cluster and never creates a topic or applies an ACL.
- A source or tracker topic id change (or a different cluster id) is the only fatal finding. The
  engine takes its single fatal path (`fatal engine failure (re-validation)`) and exits non-zero
  within one interval, because continuing would relay payloads from a different topic.
- Every other ERROR finding degrades the instance and dispatch continues. It is logged at ERROR on
  `cesium-admin`, counted in `cesium_revalidation_errors`, and sets the `/health/ready` degraded flag
  with a short cause. Readiness status is unchanged (D21). A clean round clears all three. This
  reverses D14's "refuse rather than alert" and R-7's runtime fail-fast. Ingest's fail-fast on a
  source partition with no tracker partition (I-7) stays, and startup still fails on every check as
  before.
- D14's observed term becomes the dispatch group's committed tracker cursor age, compared against
  `delete.retention.ms / 2 + 1h`. Under the `2 × delay.max` floor a breach is still safe until the
  cursor's age reaches the full `delete.retention.ms`, so this alerts with half the retention left.
  It runs only on instances whose `roles` include `dispatch`, since an ingest-only instance cannot
  fix a stalled dispatch fleet. The oldest-pending term is dropped: a replay starts at the committed
  cursor, so the cursor term covers it.
- R-11 gets a boolean check in place of the probe. When the source evicts by size or tier under the
  `ACKNOWLEDGED` gate, a partition that has evicted records but retains nothing older than
  `now − delay.max − 1h` means eviction reached payloads still inside the delay window, and the round
  degrades. A partition idle for longer than `delay.max` reads the same way; the message says so.
- Warnings are logged once, when they first appear. A round that throws is reported as an ERROR
  finding ("round could not complete"), so the schedule keeps running. Once shutdown begins, results
  are dropped, and shutdown waits briefly for an interrupted round before closing the admin client.
- Dropped from the spec, never built: `cesium_retention_margin_seconds` and its probe, the periodic
  tracker-size monitor, and `cesium_tracker_cursor_lag` / `cesium_tracker_cursor_age_seconds`.

## Consequences

- An environment broken under a running engine now surfaces within one interval, without a restart.
- A running engine can exit non-zero on its own when a topic is recreated. Operators who recreate the
  source as part of a reset must follow the R-10 runbook either way.
- The admin plane makes a few more calls per interval: describes, config reads, the ACL describe, the
  dispatch group's offsets, and `listOffsets` by timestamp. They need no new ACL grants:
  `listOffsets` by timestamp needs only Topic DESCRIBE, which startup already requires.
- Tracker size and source earliest-record age are monitored with broker-side metrics.
