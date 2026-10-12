# Failure-matrix coverage map

This maps design §3.9 (the ingest / dispatch / recovery failure matrix) and §11.3 (integration
scenarios) to the tests that exist. Each row names the test(s) that cover it and the lane they run
in. Gaps are listed in [Gaps and concerns](#gaps-and-concerns).

Legend for the Lane column:

- **PR**: default `:cesium-kafka-it:integrationTest` lane (representative classic-protocol suite) or
  a module unit/contract test that runs in `./gradlew build`.
- **nightly**: `@Tag("nightly")` integration scenario (heavy Toxiproxy / compaction / multi-instance);
  run by `nightly.yml` via `-PincludeNightly`.
- **kip848**: additionally has a `@Tag("kip848")` `group.protocol=consumer` variant verified passing
  (non-blocking nightly lane, D12).
- **unit / contract**: broker-free coverage in `cesium-kafka-core` tests or the published
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
| I-10 | Broker degradation outlasting retries ⇒ pause-all + degrade | `IngestLoopTest` park-and-degrade (membership-preserving pause, no exit) | unit; see [gap 5](#gaps-and-concerns) |

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
| D-10 | Open foreign txn holds LSO below a committed COMPLETE ⇒ HW barrier | `LsoBarrierHazardIT` (non-negotiable, §11.3-4) | PR + kip848 |
| D-11 | Dispatch txn exceeds `transaction.timeout` ⇒ server abort | `DispatchLoopTest` (abort == retry); batch bounds prevent | unit; see [gap 4](#gaps-and-concerns) |
| D-12 | Huge due backlog ⇒ time-sliced drain, bounded poll gap | `DispatchLoopTest` (due-storm poll-gap) and `RebalanceScaleIT` (`cesium.dispatch.poll.gap.seconds` bounded under backlog); `BackpressureIT` (§5.3 bounded-memory pause/resume: `cesium.shard.paused` / `cesium.pending.entries` under backlog) | nightly + unit |
| D-13 | `commitTransaction` ambiguous (live) ⇒ in-doubt, never restore | `DispatchLoopTest` in-doubt fault injection; `TrackerBackedStoreContract` drop+re-recover converges | unit + contract |
| D-14 | Predecessor stalled between accepted `sendOffsets` and `commit`; commits after takeover | `BarrierOrderingI8IT` (non-negotiable, §11.3-5) | PR + kip848 |
| D-15 | Abortable-retry exhaustion ⇒ park + degrade | `DispatchLoopTest.parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess` (gauge raise/clear, membership preserved); no real-broker trigger exists (an oversized relay now goes to the DLQ, `UnrelayableDlqIT`) | unit; see [gap 5](#gaps-and-concerns) |

## Recovery / environment (§3.9)

| Row | Fault | Covering test(s) | Lane |
|-----|-------|------------------|------|
| R-1 | Full restart | `DispatchRestartRecoveryIT`; `CursorReplayIT`; `KafkaTrackerStoreRecoveryTest` | PR (kip848: [finding 2](#gaps-and-concerns)) |
| R-2 | Scale-out | `RebalanceScaleIT` (scale 1→2→1); `CooperativeRevocationIT`; `TakeoverBeforeFirstCommitIT` (a fresh deployment's rebalance moves partitions before their first cursor commit; the new owner replays them from the beginning) | nightly + PR |
| R-3 | `onPartitionsLost` (fenced member) | `PartitionsLostIT` | nightly |
| R-4 | Tracker ADD ages (compaction-only, never expires) | `CompactionIT` (lone pending ADD survives cleaning); `TrackerBackedStoreContract` pre-compacted-log scripts | nightly + contract |
| R-5 | Source payload expires while pending | `StartupValidatorTest`/`StartupChecksIT` (retention validation); `PayloadExpiryDlqIT` (residual D-9) | PR |
| R-6 | Clock skew between dispatch workers | No EOS test (skew has no EOS impact); surfaced via `dispatch_lag` histogram | n/a; see [gap 3](#gaps-and-concerns) |
| R-7 | Tracker/source partition-count drift | `StartupValidatorTest`; `StartupChecksIT` (mismatch ⇒ startup fail-fast); `StartupValidatorTest$Revalidate` (runtime drift degrades); `PartitionGrowthIT` (growth runbook: ingest stopped and seeded, the new tracker partition unseeded); `DispatchLoopTest` (`missingCommittedOffsetWithCommitsElsewhereSeeksToBeginning`) | PR |
| R-8 | `delete.retention.ms` below the D14 floor ⇒ startup FAIL; runtime floor and committed-cursor age degrade | `StartupChecksIT` (cursor-age breach, cleared by raising `delete.retention.ms`); `AppQuickstartIT` (degrade-then-clear through the app); `StartupValidatorTest$Revalidate`; `CompactionIT` (startup rejection); `StartupValidatorTest` | nightly + PR |
| R-9 | Tracker topic deleted/recreated/truncated ⇒ fail-fast | `DispatchLoopTest` (`committedCursorBelowBeginningIsFailFast`, `committedCursorAboveBarrierIsFailFast`); the out-of-range poll path has no dedicated test | unit |
| R-10 | Source topic recreated (same name) ⇒ topic-ID mismatch; under a running engine ⇒ re-validation fatal | `StartupChecksIT` (recreation under a running engine ⇒ identity change); `CesiumEngineWiringTest` (identity change ⇒ fatal path); `AppQuickstartIT` (engine exits non-zero); `StartupChecksIT` (source recreation ⇒ identity-blob mismatch); `StartupValidatorTest`, `IdentityBlobTest` | PR |
| R-11 | Source evicting by `retention.bytes`/tiered ⇒ explicit ack; acknowledged eviction inside `delay.max + margin` ⇒ re-validation degrades | `StartupValidatorTest$Revalidate` (earliest retained record vs the window); `StartupChecksIT` (size-based-retention acknowledgment gate); `StartupValidatorTest` | PR |

---

## §11.3 integration scenarios

| # | Scenario | Covering test(s) | Lane |
|---|----------|------------------|------|
| 1 | Happy path (immediate / delay / deliver-at / precedence) | `HappyPathImmediateIT`, `HappyPathDelayedIT`, `DelayedDeliveryIT`, `HeaderPolicyIT` | PR |
| 2 | Restart-recovery with crash-point injection | `IngestCrashPointsIT`, `DispatchCrashPointsIT`, `DispatchRestartRecoveryIT` | PR (kip848: [finding 2](#gaps-and-concerns)) |
| 3 | Zombie fencing (deterministic) | `ZombieFencingIT`, `ZombieStaticIdFencingIT` | nightly (classic); KIP-848 [finding 2](#gaps-and-concerns) |
| 4 | LSO / barrier hazard (non-negotiable) | `LsoBarrierHazardIT` | PR + kip848 |
| 5 | Barrier-ordering hazard I8 (non-negotiable) | `BarrierOrderingI8IT` | PR + kip848 |
| 6 | Rebalance (scale, cooperative, static zero-movement) | `RebalanceScaleIT`, `CooperativeRevocationIT`, `PartitionsLostIT`; `TakeoverBeforeFirstCommitIT` (PR) | nightly (classic); KIP-848 [finding 2](#gaps-and-concerns) |
| 7 | Cursor-bounded replay incl. heterogeneous delays | `CursorReplayIT`, `DispatchRestartRecoveryIT` (pins survive partial dispatch) | PR |
| 8 | Compaction | `CompactionIT` + broker-free backstop `TrackerBackedStoreContract` pre-compacted-log | nightly + contract |
| 9 | Integrity & environment checks | `StartupChecksIT`, `PayloadExpiryDlqIT`, `HeaderPolicyIT` | PR |
| 10 | Degradation (park + penalty box) | `DegradationIT` (penalty box); park-and-degrade in `DispatchLoopTest` / `IngestLoopTest` | nightly + unit |
| 11 | KIP-848 lane | LSO + barrier-ordering kip848 variants (pass); zombie, rebalance and kill-and-restart are [findings](#gaps-and-concerns) (gap 2) | nightly (continue-on-error) |
| 12 | Crash-loop soak (true SIGKILL, separate JVMs) | `CrashLoopSoakIT` | nightly (`soakPerf`); see [gap 1](#gaps-and-concerns) |

---

## KIP-848 (`group.protocol=consumer`) lane

Run only nightly and non-blocking (D12). Each EOS-critical scenario was made protocol-parameterizable
(`EngineHarness.Builder.groupProtocol`, `MultiInstanceSupport.groupProtocol`); the consumer-protocol
variant is `@Tag("kip848")`. Verified on `apache/kafka:4.3.0` (KRaft):

| Scenario | Consumer-protocol result |
|----------|--------------------------|
| Restart-recovery (`DispatchRestartRecoveryIT`) | Finding (same-id kill-and-restart stalls for the session timeout; variant removed, see finding 2) |
| LSO / HW barrier (`LsoBarrierHazardIT`) | PASS |
| Barrier-ordering I8 (`BarrierOrderingI8IT`) | PASS; also verifies takeover/fence under consumer (fast client-side `max.poll.interval.ms` eviction) |
| Zombie fencing: duplicate `group.instance.id` | Finding (KIP-848 semantic change; see finding 2) |
| Zombie fencing: Toxiproxy network partition | Finding (eviction timing; see finding 2) |
| Rebalance scale 1→2→1 (`RebalanceScaleIT`) | Finding (scale-in eviction timing; see finding 2) |

Building the lane exposed a production bug, now fixed: `KafkaClientFactory`
strips the classic-only consumer keys (`partition.assignment.strategy`, `session.timeout.ms`,
`heartbeat.interval.ms`) under `group.protocol=consumer`, because kafka-clients 4.3 rejects them
(`"… cannot be set when group.protocol=CONSUMER"`). Without this the engine could not start under the
consumer protocol when the §8 tuning or the K8s static-membership recipe sets those keys. Covered by
`KafkaClientFactoryTest.groupProtocolConsumerStripsClassicOnlyConsumerKeys`.

---

## Gaps and concerns

1. **§11.3-12 crash-loop soak: built at CI scale; the scenario ITs still kill in-JVM.**
   `CrashLoopSoakIT` runs three app processes, each its own JVM, under 200 records/s of delayed
   load for 6 minutes. Every 5–20 s it SIGKILLs one, and half the restarts arm a random §3.9 crash
   point at which the child halts itself. Afterwards every record must reach the destination exactly
   once and never early, and the DLQ must be empty. A real process death adds what an in-JVM kill
   can't model: no shutdown hook runs, the OS closes the sockets, and the producer's unsent batches
   die with the process. The three processes boot together on a fresh deployment under live load,
   so a rebalance can move a tracker partition before its first cursor commit. The new owner
   replays that partition from its beginning
   ([ADR-0022](adr/0022-tracker-partition-without-cursor-replays-from-beginning.md)). Before
   ADR-0022 that owner exited, and that is how an early run of this soak failed.

   Every other multi-instance scenario still kills in-JVM (`EngineHarness.kill()`: interrupt the
   loop threads, drop the in-memory store). Their fencing guarantees rest on broker-side mechanisms
   (`initTransactions()` fencing a stale `transactional.id`, `TxnOffsetCommit` epoch rejection,
   session eviction), so that model is sound for §11.3-3/-4/-5/-6. Not covered: the full 24 h /
   100 M-entry soak lane, which stays a manual dedicated-host run (`SoakPerfIT` javadoc), and crash
   loops under `group.protocol=consumer`.

2. **KIP-848 fencing / rebalance scenarios: documented incompatibilities, classic-only for now.**
   On `apache/kafka:4.3.0` two behaviours differ under `group.protocol=consumer`:
   - *Duplicate `group.instance.id` does not fence the incumbent.* In the classic protocol a second
     member joining with an in-use static id fences the prior holder (`FencedInstanceIdException`).
     Under the consumer protocol the broker instead rejects the newcomer (`UnreleasedInstanceIdException`)
     and the incumbent keeps its assignment, so `ZombieStaticIdFencingIT`'s fence primitive does not
     apply. The cause is the KIP-848 semantic change, not a cesium bug.
   - *Session-eviction-driven takeover is too slow for the deterministic budgets.* Because the engine
     strips the client `session.timeout.ms` under the consumer protocol, eviction
     falls to the broker default `group.consumer.session.timeout.ms` (~45 s on the shared substrate)
     instead of the 6–20 s the classic lane sets. `ZombieFencingIT` (network partition) and
     `RebalanceScaleIT` scale-in both wait out that eviction and exceed their drain windows
     (observed: exactly one partition's worth delivered before timeout). The protocol-agnostic
     takeover/replay-to-barrier path is verified under consumer by `BarrierOrderingI8IT` (which
     uses fast client-side `max.poll.interval.ms` eviction).
   - *Kill-and-restart on the same `group.instance.id` stalls for a full session timeout.* This combines
     the first two bullets, and it matters most operationally because it is what a restarted pod
     does. `DispatchRestartRecoveryIT`'s kill-and-restart body reuses the static id
     the killed incumbent still holds. Classic fences the incumbent immediately and the successor takes
     over at once; under the consumer protocol the successor is rejected with
     `UnreleasedInstanceIdException` and cannot join until the incumbent is evicted, measured at
     ~42.5 s against `apache/kafka:4.3.0` defaults (successor rejected at `T+0.2 s`, incumbent fenced
     at `T+42.7 s`). Entries therefore dispatch ~30 s past their due time, past the test's lateness
     bound. The recovery logic is protocol-agnostic and unaffected; only the takeover latency
     differs. The variant was removed instead of given a larger budget, because a larger budget would
     hide the finding. **Operator consequence:** under `group.protocol=consumer`
     with static membership, a hard pod restart pauses that partition's dispatch for up to
     `group.consumer.session.timeout.ms`.

     ADR-0006 tracks promotion of the consumer protocol to a gating lane. Closing these three
     scenarios would need either a broker with a lowered `group.consumer.min.session.timeout.ms` or a
     fence primitive that does not depend on session eviction.

3. **R-6 clock skew: no EOS test.** Skew shifts firing time by the skew amount with no
   exactly-once impact (NTP assumed). The `dispatch_lag` histogram surfaces it; no correctness test
   gates it. It is listed for completeness and needs no fix.

4. **D-11 (dispatch txn exceeds `transaction.timeout`): unit-only.** Covered by `DispatchLoopTest`
   (server abort ≡ retry) and prevented in practice by the batch bounds (D8); there is no dedicated
   IT that drives a real server-side transaction timeout. Low risk (the abort path is the same as
   D-2, which has an IT); a forced-timeout IT could be added in a later hardening pass.

5. **I-10 / D-15 park-and-degrade: unit-only.** The park-and-degrade state machine is unit-tested
   on both sides (`IngestLoopTest`, `DispatchLoopTest`
   `parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess`). There is no IT on either side: the one
   clean real-broker trigger (an oversized relay) is now an unrelayable record routed to the DLQ
   (`UnrelayableDlqIT`), and transient relay failures surface as delivery timeouts, which the loops
   classify as in-doubt rather than abortable-retry exhaustion. `DegradationIT` covers only the
   penalty box.

6. **§11.4 performance: built, non-gating.** The `nightly.yml` `soak` job runs the `@Tag("soak")`
   memory-ceiling / JOL-footprint tests (`ShardFootprintTest`, `TrackerBackedStoreContract` 1M-entry
   soak), the scaled `SoakPerfIT` and the `CrashLoopSoakIT` crash loop (`soakPerf`); the `macro-perf`
   job runs the `*PerfIT` suite; the `benchmarks` job runs the JMH module (`continue-on-error`).
   The >10% JMH regression gate is report-only (`results/compare.py`). See
   [performance.md](performance.md).
