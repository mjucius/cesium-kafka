# Failure-matrix coverage map (post-M6)

Audit of design §3.9 (the ingest / dispatch / recovery failure matrix) and §11.3 (integration
scenarios) against the tests that actually exist after M6. Each row names the concrete test(s) that
cover it and the lane they run in. **No coverage is claimed that is not backed by a named test.**
Genuine gaps are listed in [Gaps and concerns](#gaps-and-concerns) and tracked for later milestones.

Legend for the **Lane** column:

- **PR** — default `:cesium-kafka-it:integrationTest` lane (representative classic-protocol suite) or
  a module unit/contract test that runs in `./gradlew build`.
- **nightly** — `@Tag("nightly")` integration scenario (heavy Toxiproxy / compaction / multi-instance);
  run by `nightly.yml` via `-PincludeNightly`.
- **kip848** — additionally has a `@Tag("kip848")` `group.protocol=consumer` variant verified passing
  (non-blocking nightly lane, D12).
- **unit / contract** — broker-free coverage in `cesium-kafka-core` tests or the published
  `TrackerBackedStoreContract` (the latter run as `KafkaTrackerStoreContractTest`).

---

## Ingest loop (§3.9)

| Row | Fault | Covering test(s) | Lane |
|-----|-------|------------------|------|
| I-1 | Crash before `beginTransaction` | `IngestCrashPointsIT` (CrashPoints I-1); `IngestLoopTest` (begin/sendOffsets/commit sequencing) | PR |
| I-2 | After some `send()`s, before `sendOffsets` (abort) | `IngestCrashPointsIT` (I-2); `IngestLoopTest` abort-and-restore | PR |
| I-3 | After `sendOffsets`, before `commit` | `IngestCrashPointsIT` (I-3); `IngestLoopTest` (offsets for every touched partition) | PR |
| I-4 | During `commitTransaction` (ambiguous) | `IngestCrashPointsIT` (I-4); `IngestLoopTest` in-doubt commit fault injection (never restores, I9) | PR |
| I-5 | Zombie ingest resumes and commits ⇒ `TxnOffsetCommit` fenced | `ZombieStaticIdFencingIT.duplicateIngestInstanceIdFences…` (group-A duplicate `group.instance.id`) | nightly |
| I-6 | Rebalance mid-batch (impossible mid-transaction) | `IngestLoopTest` (no transaction spans a poll; callbacks at txn boundaries) | unit |
| I-7 | Tracker partition missing (source grew) | `StartupValidatorTest`; `StartupChecksIT` (partition-count mismatch ⇒ `EX_CONFIG`) | PR |
| I-8 | Destination produce error ⇒ transient: abort/retry; permanent (record too large / invalid): poison ⇒ DLQ atomically with the offset advance, partition not wedged (M2) | `IngestPolicyEngineTest` (decision tables); `UnrelayableRejectionsTest` (taxonomy); `IngestLoopTest` / `DispatchLoopTest` (permanent ⇒ DLQ/DROP/FAIL, transient ⇒ retry); `RelayRecordFactoryTest` (unrelayable DLQ shape); `HeaderPolicyIT` (malformed/over-max ⇒ DLQ); `UnrelayableDlqIT` (oversized relay ⇒ DLQ once + following record delivered) | PR |
| I-9 | Group-A committed offsets expired ⇒ `auto.offset.reset=none` fail-fast | `StartupChecksIT` (group-A offsets removed ⇒ fail-fast runbook exit) | PR |
| I-10 | Broker degradation outlasting retries ⇒ pause-all + degrade | `IngestLoopTest` park-and-degrade (membership-preserving pause, no exit) | unit — see [gap 5](#gaps-and-concerns) |

## Dispatch loop (§3.9)

| Row | Fault | Covering test(s) | Lane |
|-----|-------|------------------|------|
| D-1 | Crash before `begin` (incl. after payload fetch) | `DispatchCrashPointsIT` (D-1); `DispatchRestartRecoveryIT` | PR |
| D-2 | After dest+COMPLETE sends, before commit (abort) | `DispatchCrashPointsIT` (D-2); `DispatchLoopTest` abort path | PR |
| D-3 | Crash during `commitTransaction` | `DispatchCrashPointsIT` (D-3); `DispatchLoopTest` + `TrackerBackedStoreContract` in-doubt | PR |
| D-4 | Zombie dispatcher commits after losing *p* ⇒ fenced at `TxnOffsetCommit` | `ZombieFencingIT` (Toxiproxy partition); `ZombieStaticIdFencingIT.duplicateDispatchInstanceIdFences…` | nightly |
| D-5 | Cooperative revocation of *p* between txns | `CooperativeRevocationIT`; `DispatchLoopTest` (callback ordering, `onPartitionsRevoked` drops shard) | nightly + unit |
| D-6 | Crash during replay (pure state rebuild) | `KafkaTrackerStoreRecoveryTest`; `TrackerBackedStoreContract` replay idempotence | contract |
| D-7 | Idle-cursor txn aborts ⇒ cursor stays behind | `CursorReplayIT` (idle-cursor advancement); `DispatchLoopTest` | PR |
| D-8 | Payload fetch fails transiently ⇒ penalty box | `DegradationIT` (penalty-box isolation); `KafkaSeekFetcherTest`, `FetchCandidatesTest`, `DispatchLoopTest` penalty-box skip | nightly + unit |
| D-9 | Payload gone (retention/compaction/size) ⇒ DLQ policy txn | `PayloadExpiryDlqIT` | PR |
| D-10 | **Open foreign txn holds LSO below a committed COMPLETE ⇒ HW barrier** | **`LsoBarrierHazardIT`** (non-negotiable, §11.3-4) | PR + **kip848** |
| D-11 | Dispatch txn exceeds `transaction.timeout` ⇒ server abort | `DispatchLoopTest` (abort == retry); batch bounds prevent | unit — see [gap 4](#gaps-and-concerns) |
| D-12 | Huge due backlog ⇒ time-sliced drain, bounded poll gap | `DispatchLoopTest` (due-storm poll-gap) and `RebalanceScaleIT` (`cesium.dispatch.poll.gap.seconds` bounded under backlog); `BackpressureIT` (§5.3 bounded-memory pause/resume — `cesium.shard.paused` / `cesium.pending.entries` under backlog) | nightly + unit |
| D-13 | `commitTransaction` ambiguous (live) ⇒ in-doubt, never restore | `DispatchLoopTest` in-doubt fault injection; `TrackerBackedStoreContract` drop+re-recover converges | unit + contract |
| D-14 | **Predecessor stalled between accepted `sendOffsets` and `commit`; commits after takeover** | **`BarrierOrderingI8IT`** (non-negotiable, §11.3-5) | PR + **kip848** |
| D-15 | Abortable-retry exhaustion ⇒ park + degrade | `DegradationIT` (oversized relay ⇒ park-and-degrade, membership stable) | nightly |

## Recovery / environment (§3.9)

| Row | Fault | Covering test(s) | Lane |
|-----|-------|------------------|------|
| R-1 | Full restart | `DispatchRestartRecoveryIT`; `CursorReplayIT`; `KafkaTrackerStoreRecoveryTest` | PR + **kip848** |
| R-2 | Scale-out | `RebalanceScaleIT` (scale 1→2→1); `CooperativeRevocationIT` | nightly |
| R-3 | `onPartitionsLost` (fenced member) | `PartitionsLostIT` | nightly |
| R-4 | Tracker ADD ages (compaction-only, never expires) | `CompactionIT` (lone pending ADD survives cleaning); `TrackerBackedStoreContract` pre-compacted-log scripts | nightly + contract |
| R-5 | Source payload expires while pending | `StartupValidatorTest`/`StartupChecksIT` (retention validation); `PayloadExpiryDlqIT` (residual D-9) | PR |
| R-6 | Clock skew between dispatch workers | No EOS test **by design** (no EOS impact); surfaced via `dispatch_lag` histogram | n/a — see [gap 3](#gaps-and-concerns) |
| R-7 | Tracker/source partition-count drift | `StartupValidatorTest`; `StartupChecksIT` (mismatch ⇒ fail-fast) | PR |
| R-8 | `delete.retention.ms` below the D14 floor ⇒ startup FAIL | `CompactionIT` (startup rejection); `StartupValidatorTest` | nightly + PR |
| R-9 | Tracker topic deleted/recreated/truncated ⇒ fail-fast | `StartupChecksIT` (committed-offset > end; topic-ID mismatch) | PR |
| R-10 | Source topic recreated (same name) ⇒ topic-ID mismatch | `StartupChecksIT` (source/tracker recreation ⇒ identity-blob mismatch) | PR |
| R-11 | Source evicting by `retention.bytes`/tiered ⇒ explicit ack | `StartupChecksIT` (size-based-retention acknowledgment gate); `StartupValidatorTest` | PR |

---

## §11.3 integration scenarios

| # | Scenario | Covering test(s) | Lane |
|---|----------|------------------|------|
| 1 | Happy path (immediate / delay / deliver-at / precedence) | `HappyPathImmediateIT`, `HappyPathDelayedIT`, `DelayedDeliveryIT`, `HeaderPolicyIT` | PR |
| 2 | Restart-recovery with crash-point injection | `IngestCrashPointsIT`, `DispatchCrashPointsIT`, `DispatchRestartRecoveryIT` | PR + **kip848** |
| 3 | Zombie fencing (deterministic) | `ZombieFencingIT`, `ZombieStaticIdFencingIT` | nightly (classic) — KIP-848 [finding 2](#gaps-and-concerns) |
| 4 | LSO / barrier hazard (non-negotiable) | `LsoBarrierHazardIT` | PR + **kip848** |
| 5 | Barrier-ordering hazard I8 (non-negotiable) | `BarrierOrderingI8IT` | PR + **kip848** |
| 6 | Rebalance (scale, cooperative, static zero-movement) | `RebalanceScaleIT`, `CooperativeRevocationIT`, `PartitionsLostIT` | nightly (classic) — KIP-848 [finding 2](#gaps-and-concerns) |
| 7 | Cursor-bounded replay incl. heterogeneous delays | `CursorReplayIT`, `DispatchRestartRecoveryIT` (pins survive partial dispatch) | PR |
| 8 | Compaction | `CompactionIT` + broker-free backstop `TrackerBackedStoreContract` pre-compacted-log | nightly + contract |
| 9 | Integrity & environment checks | `StartupChecksIT`, `PayloadExpiryDlqIT`, `HeaderPolicyIT` | PR |
| 10 | Degradation (park + penalty box) | `DegradationIT` | nightly |
| 11 | **KIP-848 lane** | LSO + barrier-ordering kip848 variants (pass); zombie, rebalance **and kill-and-restart** are [findings](#gaps-and-concerns) (gap 2) | nightly (continue-on-error) |
| 12 | Crash-loop soak (true SIGKILL, separate JVMs) | **Not built** | [gap 1](#gaps-and-concerns) |

---

## KIP-848 (`group.protocol=consumer`) lane — verified at M6

Run only nightly and non-blocking (D12). Each EOS-critical scenario was made protocol-parameterizable
(`EngineHarness.Builder.groupProtocol`, `MultiInstanceSupport.groupProtocol`); the consumer-protocol
variant is `@Tag("kip848")`. Verified on `apache/kafka:4.3.0` (KRaft):

| Scenario | Consumer-protocol result |
|----------|--------------------------|
| Restart-recovery (`DispatchRestartRecoveryIT`) | **PASS** |
| LSO / HW barrier (`LsoBarrierHazardIT`) | **PASS** |
| Barrier-ordering I8 (`BarrierOrderingI8IT`) | **PASS** — also the verified takeover/fence under consumer (fast client-side `max.poll.interval.ms` eviction) |
| Zombie fencing — duplicate `group.instance.id` | **Finding** (genuine semantic change — see finding 2) |
| Zombie fencing — Toxiproxy network partition | **Finding** (eviction timing — see finding 2) |
| Rebalance scale 1→2→1 (`RebalanceScaleIT`) | **Finding** (scale-in eviction timing — see finding 2) |

A real production gap surfaced and was **fixed** while building the lane: `KafkaClientFactory` now
strips the classic-only consumer keys (`partition.assignment.strategy`, `session.timeout.ms`,
`heartbeat.interval.ms`) under `group.protocol=consumer`, because kafka-clients 4.3 rejects them
(`"… cannot be set when group.protocol=CONSUMER"`). Without this the engine could not start under the
consumer protocol when the §8 tuning or the K8s static-membership recipe sets those keys. Covered by
`KafkaClientFactoryTest.groupProtocolConsumerStripsClassicOnlyConsumerKeys`.

---

## Gaps and concerns

1. **§11.3-12 crash-loop soak (true SIGKILL) — not built.** The M6 multi-instance harness kills
   instances in-JVM (`EngineHarness.kill()` = interrupt loop threads + drop the in-memory store), a
   faithful stand-in for "process died without a graceful commit" but **not** an OS `SIGKILL`. Every
   fencing guarantee asserted here rests on broker-side, JVM-agnostic mechanisms (`initTransactions()`
   fencing a stale `transactional.id`, `TxnOffsetCommit` epoch rejection, session eviction), so the
   in-JVM model is sound for §11.3-3/-4/-5/-6. The randomized crash-loop soak under sustained load
   with an invariant checker still needs forked JVMs (`ProcessBuilder` + a tiny `main`) and is
   **deferred to M8** (nightly soak), per the design's own phasing.

2. **KIP-848 fencing / rebalance scenarios — documented incompatibilities, classic-only for now.**
   On `apache/kafka:4.3.0` two behaviours differ under `group.protocol=consumer`:
   - *Duplicate `group.instance.id` does not fence the incumbent.* In the classic protocol a second
     member joining with an in-use static id fences the prior holder (`FencedInstanceIdException`).
     Under the consumer protocol the broker instead rejects the newcomer (`UnreleasedInstanceIdException`)
     and the incumbent keeps its assignment — so `ZombieStaticIdFencingIT`'s fence primitive does not
     apply. This is a genuine KIP-848 semantic change, not a bug in cesium.
   - *Session-eviction-driven takeover is too slow for the deterministic budgets.* Because the engine
     now (correctly) strips the client `session.timeout.ms` under the consumer protocol, eviction
     falls to the broker default `group.consumer.session.timeout.ms` (~45 s on the shared substrate)
     instead of the 6–20 s the classic lane sets. `ZombieFencingIT` (network partition) and
     `RebalanceScaleIT` scale-in both wait out that eviction and exceed their drain windows
     (observed: exactly one partition's worth delivered before timeout). The protocol-agnostic
     takeover/replay-to-barrier path **is** verified under consumer by `BarrierOrderingI8IT` (which
     uses fast client-side `max.poll.interval.ms` eviction).
   - *Kill-and-restart on the same `group.instance.id` stalls for a full session timeout.* The first
     two bullets compounding, and the most operationally relevant of the three because it is what a
     **restarted pod** does. `DispatchRestartRecoveryIT`'s kill-and-restart body reuses the static id
     the killed incumbent still holds. Classic fences the incumbent immediately and the successor takes
     over at once; under the consumer protocol the successor is rejected with
     `UnreleasedInstanceIdException` and cannot join until the incumbent is evicted — **measured at
     ~42.5 s** against `apache/kafka:4.3.0` defaults (successor rejected at `T+0.2 s`, incumbent fenced
     at `T+42.7 s`). Entries therefore dispatch ~30 s past their due time, past the test's lateness
     bound. The recovery logic itself is protocol-agnostic and unaffected — only the takeover latency
     differs — so the variant was removed rather than given a larger budget, which would have hidden
     the finding instead of recording it. **Operator consequence:** under `group.protocol=consumer`
     with static membership, a hard pod restart pauses that partition's dispatch for up to
     `group.consumer.session.timeout.ms`.

     Promotion of the consumer protocol to a gating lane is **ADR-0006** tracked; closing these three
     scenarios would need either a broker with a lowered `group.consumer.min.session.timeout.ms` or a
     fence primitive that does not depend on session eviction.

3. **R-6 clock skew — no EOS test by design.** Skew shifts firing time by the skew amount with no
   exactly-once impact (NTP assumed); it is surfaced through the `dispatch_lag` histogram, not gated
   by a correctness test. Listed for completeness; not a gap to close.

4. **D-11 (dispatch txn exceeds `transaction.timeout`) — unit-only.** Covered by `DispatchLoopTest`
   (server abort ≡ retry) and prevented in practice by the batch bounds (D8); there is no dedicated
   IT that drives a real server-side transaction timeout. Low risk (the abort path is the same as
   D-2, which has an IT); a forced-timeout IT could be added in a later hardening pass.

5. **I-10 ingest-side broker-degradation park-and-degrade — thin IT coverage.** The park-and-degrade
   state machine is unit-tested (`IngestLoopTest`), and the **dispatch-side** analog is exercised
   end-to-end by `DegradationIT`. There is no ingest-specific degradation IT; the loops share the
   park-and-degrade implementation, so the risk is low, but an ingest-side IT would close the row at
   the same fidelity as D-15.

6. **§11.4 performance (JMH macro / >10% regression gate) — M8.** The `nightly.yml` soak job runs the
   `@Tag("soak")` memory-ceiling / JOL-footprint tests (`ShardFootprintTest`,
   `TrackerBackedStoreContract` 1M-entry soak). The JMH benchmarks module and the macro perf smoke
   (§11.4) are M8 deliverables and are noted as a follow-on lane in `nightly.yml`.
