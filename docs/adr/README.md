# Architecture Decision Records

This directory records the main decisions behind cesium-kafka, one decision per ADR. Each ADR
takes a decision from the design, [`../design.md`](../design.md), which remains the deep reference
(invariants I1–I9, the full failure matrix, the proofs). When an ADR and `design.md` disagree, the
ADR wins: it reflects a post-approval revision or what the implementation shipped, and the ADR says
so.

ADRs follow the standard format: **Status**, **Context**, **Decision**, **Consequences**. They are
immutable once Accepted. A reversal is a new ADR that supersedes the old one. See
[`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) for the change process: changes to invariants or the
failure matrix need a corresponding test and an ADR.

## Index

| ADR | Title | Status | Design source |
|---|---|---|---|
| [0001](0001-two-consumer-group-architecture.md) | Two-consumer-group architecture | Accepted | §1.2–1.3 |
| [0002](0002-pointer-only-payloads-and-retention-validation.md) | Pointer-only payloads + source-retention validation | Accepted | §1.1, §7.6, R5/R13 |
| [0003](0003-sealed-two-archetype-store-spi.md) | Sealed two-archetype store SPI | Accepted | D7, §4 |
| [0004](0004-one-route-per-process.md) | One route per process | Accepted | §10, §1.1 |
| [0005](0005-tracker-format-compaction-only-and-tombstone-retention-floor.md) | Tracker format, compaction-only, tombstone-retention floor | Accepted | D4, D14, D15, §2.1–2.2, §3.7 |
| [0006](0006-classic-protocol-default-and-kip-848-promotion.md) | Classic protocol default + KIP-848 promotion criteria | Accepted | D12, §3.4.5 |
| [0007](0007-ascii-decimal-control-headers.md) | ASCII-decimal control headers | Accepted | D1, D2, §2.3 |
| [0008](0008-relay-timestamp-dispatch-default.md) | Relay timestamp = DISPATCH default | Accepted | §2.4 |
| [0009](0009-high-watermark-replay-barrier-and-snapshot-ordering.md) | High-watermark replay barrier + snapshot ordering | Accepted | D5, D19, I8, §3.6 |
| [0010](0010-arrival-ring-binary-search-no-hash-map.md) | Arrival-ring binary search (no hash map) | Accepted | D6, §5.2 |
| [0011](0011-committed-cursor-v2-position-plus-sidecar.md) | Committed cursor v2 (position + pinned-entry sidecar) | Accepted | D16, R1, §3.5 |
| [0012](0012-locked-isolation-and-offset-reset.md) | Locked isolation + auto.offset.reset + offsets-retention posture | Accepted | D17, D18, R5/R6 |
| [0013](0013-readiness-decoupled-from-recovery-and-static-membership.md) | Readiness decoupled from recovery + static membership | Accepted | D21, D10, R14, §9 |
| [0014](0014-in-doubt-commit-taxonomy.md) | In-doubt commit taxonomy | Accepted | D20, I9, R4, §3.8 |
| [0015](0015-penalty-box-and-enforced-fetch-budgets.md) | Penalty box + enforced fetch budgets | Accepted | D8, D22, R8/R9, §7 |
| [0016](0016-fastutil-backed-primitive-index.md) | fastutil-backed primitive index | Accepted | Post-approval rev 1, §5 |
| [0017](0017-kafka-4-floor-and-repo-only-publishing.md) | Kafka 4.0 floor + repo-only publishing + app as product surface | Accepted | Post-approval rev 2, §13 |
| [0018](0018-bounded-wait-for-proven-topic-metadata.md) | Bounded wait for proven-existing topic metadata | Accepted | §2.1, §7.6 |
| [0019](0019-2-0-stability-baseline.md) | 2.0 stability baseline | Accepted | Post-approval rev 4 |
| [0020](0020-runtime-revalidation.md) | Runtime re-validation | Accepted | D23, D14, R-7, R-11, R17 |

ADRs 0001–0015 are the set planned in design §12. 0016 and 0017 record three later project-owner
decisions: the fastutil index backing, and the bundle of Kafka 4.0 broker floor, repo-only
publishing, and app as product surface. The design records them in the revision block at the top of
[`../design.md`](../design.md), and they supersede the affected design text. 0018 records a later
fix: a bounded wait for metadata of topics whose existence startup has already proven. 0019 records
the move to 2.0.0 and the stability promise that starts there. 0020 records runtime re-validation
and the degrade-not-stop policy for its findings.
