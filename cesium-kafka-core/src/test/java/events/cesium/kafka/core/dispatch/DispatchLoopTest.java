package events.cesium.kafka.core.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.core.fetch.FetchOutcome;
import events.cesium.kafka.core.fetch.FetchResult;
import events.cesium.kafka.core.fetch.SeekFetcher;
import events.cesium.kafka.core.headers.RelayPartitioning;
import events.cesium.kafka.core.headers.RelayRecordFactory;
import events.cesium.kafka.core.headers.RelayTimestampPolicy;
import events.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import events.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Dispatch-loop unit suite (design §11.1 "engine sequencing"): the I8 recovery ordering with
 * epoch-tagged barrier discard, the §3.2 transaction shape (per-reason tombstone grouping,
 * cursors for every touched partition, carry-over), the §3.8 taxonomy — abort-and-restore, fatal,
 * the three in-doubt resolutions (I9) — park-and-degrade, the penalty box, backpressure, idle
 * cursor advancement, the due-storm poll-gap bound, and the D-1…D-3 crash points, all against
 * {@link MockConsumer}/{@link MockProducer} with a scripted store and fetcher.
 */
class DispatchLoopTest {

    static {
        // The crash-point seam refuses to arm without the explicit test-only guard property.
        System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
    }

    private static final long NOW = 1_700_000_000_000L;
    private static final String SOURCE = "orders";
    private static final String TRACKER = "orders-tracker";
    private static final String DEST = "orders-out";
    private static final String DLQ = "orders-dlq";

    @AfterEach
    void resetCrashPoints() {
        CrashPoints.reset();
    }

    private static TopicPartition tracker(int partition) {
        return new TopicPartition(TRACKER, partition);
    }

    // ------------------------------------------------------------------ recovery protocol (I8)

    @Test
    void recoveryResolvesCommittedCursorBeforeRequestingBarrier() {
        Harness h = new Harness();
        h.seedCommitted(0, 4L, "sidecar");
        h.admin.barrierValues.put(0, 9L);
        h.start(0);
        h.loop.runOnce();

        assertOrder(h.events, "committed:0", "barrier:0", "beginRecovery:0");
        assertEquals(
                List.of(new ScriptedTrackerStore.RecoveryCall(0, 4L, "sidecar", 9L)),
                h.store.recoveries,
                "beginRecovery receives the committed cursor (offset + opaque sidecar) and the HW barrier");
        assertEquals(4L, h.consumer.position(tracker(0)), "consumer seeked to the cursor offset");
        assertTrue(h.store.isRecovering(0), "barrier above the cursor gates the shard (I4)");
        assertFalse(h.consumer.paused().contains(tracker(0)), "intake resumes once recovery begins");
    }

    @Test
    void barrierFutureIsEpochTaggedAndDiscardedOnRevoke() {
        Harness h = new Harness();
        h.seedCommitted(0, 0L, "");
        CompletableFuture<Long> stale = h.admin.pend(0);
        h.start(0);
        h.loop.runOnce();
        assertOrder(h.events, "committed:0", "barrier:0");
        assertTrue(h.store.recoveries.isEmpty(), "recovery waits for the barrier future");

        h.consumer.rebalance(List.of()); // cooperative revoke of partition 0
        stale.complete(7L); // the snapshot resolves AFTER the revoke: must be discarded (I8)
        h.loop.runOnce();
        assertTrue(h.store.recoveries.isEmpty(), "a stale barrier future never starts recovery");

        h.consumer.rebalance(List.of(tracker(0))); // re-assignment re-resolves and re-snapshots
        h.loop.runOnce();
        assertEquals(1, h.store.recoveries.size(), "re-assignment runs the full protocol again");
        assertEquals(0L, h.store.recoveries.get(0).barrier(), "the FRESH snapshot is used, not the stale 7");
        assertEquals(2L, h.events.stream().filter("barrier:0"::equals).count());
    }

    @Test
    void replayPromotesViaPositionReportsAcrossEveryPoll() {
        Harness h = new Harness();
        h.seedCommitted(0, 0L, "");
        h.admin.barrierValues.put(0, 3L);
        h.start(0);
        h.loop.runOnce();
        assertTrue(h.store.isRecovering(0));

        for (int offset = 0; offset < 3; offset++) {
            h.consumer.addRecord(trackerRecord(0, offset));
        }
        h.loop.runOnce();
        assertEquals(3, h.store.appliedRecords.size(), "replayed records flow through onTrackerRecord");
        assertFalse(h.store.isRecovering(0), "position reaching the barrier promotes the shard ACTIVE");
    }

    @Test
    void onTrackerPositionIsReportedAfterEveryPollEvenWithoutRecords() {
        Harness h = new Harness();
        h.startActive(0);
        int reported = h.store.positions.size();
        h.loop.runOnce(); // empty poll
        h.loop.runOnce(); // empty poll
        assertTrue(
                h.store.positions.size() >= reported + 2,
                "the position is reported after EVERY poll — control-marker promotion depends on it");
    }

    @Test
    void firstRunSeeksToBeginningOnlyWhenProvable() {
        Harness h = new Harness();
        h.admin.groupHasOffsets = false; // no committed offsets anywhere: provable first run
        h.start(0); // note: no committed offset seeded
        h.loop.runOnce();

        assertEquals(
                List.of(new ScriptedTrackerStore.RecoveryCall(0, 0L, "", 0L)),
                h.store.recoveries,
                "explicit first-run recovery from the partition beginning with an empty sidecar");
    }

    @Test
    void missingCommittedOffsetWithGroupHistoryIsFailFast() {
        Harness h = new Harness();
        h.admin.groupHasOffsets = true; // the group committed before: this is expiry, not first run
        h.start(0);

        DispatchLoopFatalException failure = assertThrows(DispatchLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("offset-reset runbook"), failure.getMessage());
        assertTrue(h.store.recoveries.isEmpty(), "never auto-reset (D18)");
    }

    @Test
    void committedCursorBelowBeginningIsFailFast() {
        Harness h = new Harness();
        h.seedCommitted(0, 5L, "");
        h.start(0);
        h.consumer.updateBeginningOffsets(Map.of(tracker(0), 10L)); // tracker truncated

        DispatchLoopFatalException failure = assertThrows(DispatchLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("below the partition beginning"), failure.getMessage());
    }

    @Test
    void committedCursorAboveBarrierIsFailFast() {
        Harness h = new Harness();
        h.seedCommitted(0, 50L, "");
        h.admin.barrierValues.put(0, 10L); // live end below the committed cursor: recreated
        h.start(0);

        DispatchLoopFatalException failure = assertThrows(DispatchLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("exceeds the live end offset"), failure.getMessage());
    }

    // ------------------------------------------------------------------ transaction shape (§3.2)

    @Test
    void transactionSequencingHoldsI1I2I3() {
        Harness h = new Harness();
        h.startActive(0);
        h.store.cursorAt(0, 21L);
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.events.clear();
        h.loop.runOnce();

        assertEquals(
                List.of(
                        "poll",
                        "begin",
                        "send:" + DEST,
                        "send:" + DEST,
                        "encode:DISPATCHED:2",
                        "send:" + TRACKER,
                        "send:" + TRACKER,
                        "cursor:0:2",
                        "sendOffsets",
                        "commit",
                        "batchCommitted:2",
                        "poll"),
                h.events,
                "begin → relays → per-reason tombstones → cursors → offsets → commit → onBatchCommitted,"
                        + " with a liveness poll after the transaction (I3) and never before the commit");
        assertNotNull(h.producer().offsetsGroupMetadata, "I2: groupMetadata accompanied the offsets");

        Map<TopicPartition, OffsetAndMetadata> committed = h.lastCommittedOffsets();
        assertEquals(21L, committed.get(tracker(0)).offset(), "the committed offset is the store's cursor proposal");
        assertEquals("m0", committed.get(tracker(0)).metadata(), "the sidecar rides the offset metadata");
        assertEquals(2.0, h.counterValue("cesium.dispatch.records", "outcome", "dispatched"));
        assertEquals(
                1.0, h.counterValue("cesium.transactions", "loop", "dispatch", "result", "committed", "cause", "none"));
    }

    @Test
    void cursorsCoverEveryTouchedPartition() {
        Harness h = new Harness();
        h.startActive(0, 1);
        h.store.cursorAt(0, 11L);
        h.store.cursorAt(1, 22L);
        h.store.dueQueue.add(TestBatch.builder()
                .add(0, 1, NOW, 10, false)
                .add(1, 7, NOW, 70, false)
                .build());
        h.loop.runOnce();

        Map<TopicPartition, OffsetAndMetadata> committed = h.lastCommittedOffsets();
        assertEquals(11L, committed.get(tracker(0)).offset());
        assertEquals(22L, committed.get(tracker(1)).offset(), "EVERY touched partition's cursor rides the commit (I2)");
    }

    @Test
    void completionsAreGroupedPerReason() {
        Harness h = new Harness();
        h.startActive(0);
        h.fetcher.classify(0, 2, FetchOutcome.GONE);
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2, 3));
        h.loop.runOnce();

        assertEquals(2, h.store.encodeCalls.size(), "one encodeCompletions call per CompletionReason");
        ScriptedTrackerStore.EncodeCall dispatched = h.store.encodeCalls.get(0);
        assertEquals(CompletionReason.DISPATCHED, dispatched.reason());
        assertRows(dispatched.entries(), new long[][] {{0, 1}, {0, 3}});
        ScriptedTrackerStore.EncodeCall expired = h.store.encodeCalls.get(1);
        assertEquals(CompletionReason.PAYLOAD_MISSING_DLQ, expired.reason());
        assertRows(expired.entries(), new long[][] {{0, 2}});

        assertEquals(1L, h.events.stream().filter(("send:" + DLQ)::equals).count(), "GONE ⇒ one DLQ loss notice (D-9)");
        assertEquals(2.0, h.counterValue("cesium.dispatch.records", "outcome", "dispatched"));
        assertEquals(1.0, h.counterValue("cesium.dispatch.records", "outcome", "payload_expired"));
        assertRows(
                h.store.committedBatches.get(0), new long[][] {{0, 1}, {0, 2}, {0, 3}}, "full settled set finalized");
    }

    @Test
    void clampedFlagRidesFromTheBatchOntoTheRelay() {
        Harness h = new Harness();
        h.startActive(0);
        h.store.dueQueue.add(
                TestBatch.builder().add(0, 5, NOW, 50, true).build()); // CLAMP survived the durable round trip
        h.loop.runOnce();

        ProducerRecord<byte[], byte[]> relay = h.producer().history().get(0);
        assertEquals(DEST, relay.topic());
        assertNotNull(relay.headers().lastHeader("cesium-clamped"), "DueBatch.clamped(i) stamps cesium-clamped (§2.3)");
    }

    @Test
    void dropPolicyWritesCompleteOnlyAndFailPolicyStopsTheLoop() {
        Harness drop = new Harness(config(c -> c.policy = UnfetchablePayloadPolicy.DROP));
        drop.startActive(0);
        drop.fetcher.classify(0, 1, FetchOutcome.GONE);
        drop.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        drop.loop.runOnce();
        assertFalse(drop.events.contains("send:" + DLQ), "DROP writes no loss notice");
        assertEquals(CompletionReason.DROPPED, drop.store.encodeCalls.get(0).reason());
        assertEquals(1.0, drop.counterValue("cesium.dispatch.records", "outcome", "dropped"));
        assertEquals(1, drop.producer().commitCount(), "the COMPLETE is still written — never a poison loop (§7.4)");

        Harness fail = new Harness(config(c -> c.policy = UnfetchablePayloadPolicy.FAIL));
        fail.startActive(0);
        fail.fetcher.classify(0, 1, FetchOutcome.GONE);
        fail.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        DispatchLoopFatalException failure = assertThrows(DispatchLoopFatalException.class, fail.loop::runOnce);
        assertTrue(failure.getMessage().contains("FAIL"), failure.getMessage());
        assertFalse(fail.events.contains("begin"), "nothing is produced before the FAIL stop");
        assertRows(fail.store.abortedBatches.get(0), new long[][] {{0, 1}}, "restore is definitive: no txn existed");
    }

    // ------------------------------------------------------------------ carry-over (§3.2/§7)

    @Test
    void carryOverCommitsSettledViewThenRestoresRemainderWithCursorSeeingCarryOverInFlight() {
        Harness h = new Harness();
        h.startActive(0);
        h.fetcher.classify(0, 3, FetchOutcome.CARRY_OVER); // budget-truncated
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2, 3));
        h.events.clear();
        h.loop.runOnce();

        // The cursor is computed for the SETTLED view while the carry-over is still in flight,
        // the transaction commits, and only then is the carry-over restored (testkit shape).
        assertOrder(h.events, "cursor:0:2", "sendOffsets", "commit", "batchCommitted:2", "batchAborted:1");
        assertRows(h.store.cursorCalls.get(0).entries(), new long[][] {{0, 1}, {0, 2}});
        assertRows(h.store.committedBatches.get(0), new long[][] {{0, 1}, {0, 2}});
        assertRows(h.store.abortedBatches.get(0), new long[][] {{0, 3}});
        assertTrue(h.store.penalties.isEmpty(), "CARRY_OVER is not a failure: no penalty");
    }

    @Test
    void transientEntriesCarryOverAndPenalizeTheirPartitionExponentially() {
        Harness h = new Harness();
        h.startActive(0, 1);
        h.fetcher.classify(1, 7, FetchOutcome.TRANSIENT);
        h.store.dueQueue.add(TestBatch.builder()
                .add(0, 1, NOW, 10, false)
                .add(1, 7, NOW, 70, false)
                .build());
        h.loop.runOnce();

        assertRows(h.store.committedBatches.get(0), new long[][] {{0, 1}}, "the fetched entry settles");
        assertRows(h.store.abortedBatches.get(0), new long[][] {{1, 7}}, "the TRANSIENT entry returns to pending");
        assertEquals(1, h.store.penalties.size());
        assertEquals(1L, h.store.penalties.get(0)[0]);
        assertEquals(NOW + 50, h.store.penalties.get(0)[1], "initial penalty backoff (PT0.05S)");

        // A second consecutive TRANSIENT pass doubles the partition's penalty.
        h.fetcher.classify(1, 8, FetchOutcome.TRANSIENT);
        h.store.dueQueue.add(TestBatch.builder()
                .add(0, 2, NOW, 20, false)
                .add(1, 8, NOW, 80, false)
                .build());
        h.loop.runOnce();
        assertEquals(NOW + 100, h.store.penalties.get(1)[1], "exponential per-partition backoff");

        // A clean pass for the partition clears the penalty (stamp in the past).
        h.store.dueQueue.add(TestBatch.onPartition(1, NOW, 9));
        h.loop.runOnce();
        long[] last = h.store.penalties.get(h.store.penalties.size() - 1);
        assertEquals(1L, last[0]);
        assertEquals(0L, last[1], "penalty cleared on the next successful fetch");
    }

    @Test
    void allTransientBatchRestoresEverythingWithoutOpeningATransaction() {
        Harness h = new Harness();
        h.startActive(0);
        h.fetcher.classify(0, 1, FetchOutcome.TRANSIENT);
        h.fetcher.classify(0, 2, FetchOutcome.TRANSIENT);
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();

        assertFalse(h.events.contains("begin"), "never open an empty transaction");
        assertRows(h.store.abortedBatches.get(0), new long[][] {{0, 1}, {0, 2}});
        assertEquals(1, h.store.penalties.size(), "the partition is penalty-boxed, pacing the retry");
    }

    @Test
    void fetcherThrowRestoresBatchAndBacksOff() {
        Harness h = new Harness();
        h.startActive(0);
        h.fetcher.failure = new KafkaException("seek consumer transport failure");
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();

        assertFalse(h.events.contains("begin"), "pre-transaction failure: nothing produced");
        assertRows(h.store.abortedBatches.get(0), new long[][] {{0, 1}, {0, 2}});
        assertFalse(h.store.penalties.isEmpty(), "park machinery stamps a not-before");

        // The drain is gated until the backoff elapses.
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount());
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount(), "exactly one committed copy after the retry");
    }

    // ------------------------------------------------------------------ §3.8 taxonomy

    @Test
    void abortableErrorAbortsRestoresFullBatchAndRetries() {
        Harness h = new Harness();
        h.startActive(0);
        h.producer().failSendOffsetsTimes = 1; // e.g. CommitFailedException: rebalance won the race
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();

        assertTrue(h.events.contains("abort"));
        assertRows(h.store.abortedBatches.get(0), new long[][] {{0, 1}, {0, 2}}, "definitive abort restores ALL");
        assertTrue(h.store.committedBatches.isEmpty());
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "dispatch", "result", "aborted", "cause", "KafkaException"));
        assertEquals(
                0.0,
                h.counterValue("cesium.dispatch.records", "outcome", "dispatched"),
                "per-record counters reflect committed dispositions only (§9)");

        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(2.0, h.counterValue("cesium.dispatch.records", "outcome", "dispatched"));
    }

    @Test
    void fatalErrorFailsTheLoopWithoutRestoring() {
        Harness h = new Harness();
        h.startActive(0);
        h.producer().fenceProducer();
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));

        assertThrows(DispatchLoopFatalException.class, h.loop::runOnce);
        assertFalse(h.events.contains("abort"), "fatal path never tries to abort a fenced producer");
        assertTrue(h.store.abortedBatches.isEmpty(), "no restore on fatal — the durable log is authoritative");
        assertTrue(h.store.committedBatches.isEmpty());
    }

    @Test
    void inDoubtCommitRetriesToDefinitiveCommit() {
        Harness h = new Harness(); // commitRetryLimit = 3
        h.startActive(0);
        h.producer().commitExceptionQueue.add(new TimeoutException("ambiguous"));
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();

        assertEquals(2L, h.events.stream().filter("commit"::equals).count(), "the timed-out commit is retried");
        assertEquals(1, h.store.committedBatches.size(), "a definitive success finalizes normally");
        assertTrue(h.store.abortedBatches.isEmpty());
        assertEquals(1, h.producers.size(), "no producer replacement on a resolved retry");
    }

    @Test
    void inDoubtCommitResolvedAbortableRestoresTheBatch() {
        Harness h = new Harness();
        h.startActive(0);
        h.producer().commitExceptionQueue.add(new TimeoutException("ambiguous"));
        h.producer().commitExceptionQueue.add(new TransactionAbortableException("broker: not committed"));
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.loop.runOnce();

        // KIP-890 TRANSACTION_ABORTABLE is the broker-affirmed "did not commit": definitive.
        assertTrue(h.events.contains("abort"));
        assertRows(h.store.abortedBatches.get(0), new long[][] {{0, 1}, {0, 2}});
        assertTrue(h.store.committedBatches.isEmpty());
        assertEquals(1, h.producers.size(), "definitively-aborted: the producer stays");
        assertFalse(h.events.contains("lost:[0]"), "no shard drop on a definitive outcome");
    }

    @Test
    void stillAmbiguousCommitDropsShardsReplacesProducerAndRecoversWithNeitherCallback() {
        Harness h = new Harness(); // commitRetryLimit = 3
        h.startActive(0);
        h.store.cursorAt(0, 2L);
        h.producer().commitTransactionException = new TimeoutException("ambiguous forever");
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1, 2));
        h.events.clear();
        h.loop.runOnce();

        assertEquals(3L, h.events.stream().filter("commit"::equals).count(), "retried to the commit-retry budget");
        // I9: NEITHER resolution callback — restoring is the duplicate vector.
        assertTrue(h.store.committedBatches.isEmpty());
        assertTrue(h.store.abortedBatches.isEmpty());
        assertOrder(h.events, "lost:[0]", "assigned:[0]", "initTransactions");
        assertEquals(2, h.producers.size(), "the unusable producer is replaced");
        assertTrue(h.producers.get(0).closed());
        assertTrue(h.producer().transactionInitialized(), "replacement fenced via initTransactions");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "dispatch", "result", "in_doubt", "cause", "commit_timeout"));

        // The partitions re-enter the §3.6 protocol: cursor re-fetched (now stable), barrier
        // re-snapshotted, beginRecovery again — the durable log decides the truth.
        h.seedCommitted(0, 2L, "post-resolution"); // e.g. the broker had committed
        h.admin.barrierValues.put(0, 2L);
        h.clock.advanceMillis(101);
        h.events.clear();
        h.loop.runOnce();
        assertOrder(h.events, "committed:0", "barrier:0", "beginRecovery:0");
        assertEquals(
                new ScriptedTrackerStore.RecoveryCall(0, 2L, "post-resolution", 2L),
                h.store.recoveries.get(h.store.recoveries.size() - 1));
    }

    @Test
    void transientProducerReplacementFailureRetriesInsteadOfDying() {
        Harness h = new Harness();
        h.startActive(0);
        h.failProducerInits = 1; // the same broker degradation that caused the ambiguity
        h.producer().commitTransactionException = new TimeoutException("ambiguous forever");
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        h.loop.runOnce(); // must NOT throw (R15)

        assertEquals(2, h.producers.size(), "first replacement attempt failed at initTransactions");
        h.clock.advanceMillis(101);
        h.loop.runOnce(); // recovery retry replaces the half-initialized producer
        assertEquals(3, h.producers.size());
        assertTrue(h.producer().transactionInitialized());
    }

    @Test
    void parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess() {
        Harness h = new Harness(config(c -> c.parkThreshold = 2));
        h.startActive(0);
        h.producer().failSendOffsetsTimes = 2;

        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        h.loop.runOnce(); // failure 1: bounded retry, not yet parked
        assertFalse(h.loop.isDegraded());

        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        h.clock.advanceMillis(101);
        h.loop.runOnce(); // failure 2 = parkThreshold: parked-and-degraded
        assertTrue(h.loop.isDegraded(), "retry exhaustion parks the loop instead of crash-looping (R15)");
        assertFalse(h.store.penalties.isEmpty(), "parked entries sit behind a penalty not-before");

        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));
        h.clock.advanceMillis(201);
        h.loop.runOnce(); // success: clear degraded
        assertFalse(h.loop.isDegraded(), "degraded clears on the first successful commit");
        assertEquals(1, h.producer().commitCount());
    }

    // ------------------------------------------------------------------ due-storm liveness (D-12)

    @Test
    void dueStormInterleavesPollsAndBoundsThePollGap() {
        long sliceMs = 20;
        Harness h = new Harness(config(c -> c.drainSlice = Duration.ofMillis(sliceMs)));
        h.startActive(0);
        h.fetcher.advanceClockMs = 2; // each fetch pass costs 2 ms of simulated work
        int batches = 100;
        int perBatch = 1_000; // 100k-due storm
        long offset = 1;
        for (int b = 0; b < batches; b++) {
            TestBatch.Builder builder = TestBatch.builder();
            for (int i = 0; i < perBatch; i++) {
                builder.add(0, offset, NOW, offset * 10, false);
                offset++;
            }
            h.store.dueQueue.add(builder.build());
        }

        int guard = 0;
        while (!h.store.dueQueue.isEmpty() && guard++ < 1_000) {
            h.loop.runOnce();
        }
        assertTrue(h.store.dueQueue.isEmpty(), "the storm drains");
        assertEquals(batches, h.store.committedBatches.size(), "every batch committed exactly once");

        // I3/D-12: a poll between every pair of transactions, so the max poll gap is one batch.
        int lastBegin = -1;
        for (int i = 0; i < h.events.size(); i++) {
            if (h.events.get(i).equals("begin")) {
                if (lastBegin >= 0) {
                    assertTrue(
                            h.events.subList(lastBegin, i).contains("poll"),
                            "no poll between transactions at event " + i);
                }
                lastBegin = i;
            }
        }
        Gauge gauge = h.registry.find("cesium.dispatch.poll.gap.seconds").gauge();
        assertNotNull(gauge);
        assertTrue(
                gauge.value() * 1000 <= 5,
                "max poll gap is one transaction's work (2 ms), far below the slice bound " + sliceMs + " ms: was "
                        + gauge.value() * 1000 + " ms");
    }

    // ------------------------------------------------------------------ idle cursor advancement (§3.5)

    @Test
    void idleCursorAdvancesInARecordsFreeTransactionRateLimitedAndOnlyWhenMaterial() {
        Harness h = new Harness(config(c -> c.idleCursorInterval = Duration.ofSeconds(2)));
        h.startActive(0);
        h.store.cursorAt(0, 5L);
        h.clock.advanceMillis(2_001);
        h.events.clear();
        h.loop.runOnce();

        assertEquals(
                List.of("poll", "cursor:0:0", "begin", "sendOffsets", "commit", "batchCommitted:0"),
                h.events,
                "records-free transaction: begin → sendOffsets(cursor) → commit, computed on the EMPTY batch");
        assertEquals(5L, h.lastCommittedOffsets().get(tracker(0)).offset());

        h.loop.runOnce(); // immediately again: rate-limited by the interval
        assertEquals(1, h.producer().commitCount(), "idle advancement is rate-limited");

        h.clock.advanceMillis(2_001); // interval elapsed but the cursor did not move
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount(), "no transaction when the cursor did not advance materially");

        h.store.cursorAt(0, 9L); // material advancement
        h.clock.advanceMillis(2_001);
        h.loop.runOnce();
        assertEquals(2, h.producer().commitCount());
        assertEquals(9L, h.lastCommittedOffsets().get(tracker(0)).offset());
    }

    @Test
    void idleCursorAdvancementIsSuppressedWhileRecovering() {
        Harness h = new Harness(config(c -> c.idleCursorInterval = Duration.ofSeconds(2)));
        h.startActive(0);
        h.store.cursorAt(0, 5L);
        h.store.forceRecovering.add(0);
        h.clock.advanceMillis(2_001);
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount(), "suppressed while RECOVERING (§3.5)");
    }

    // ------------------------------------------------------------------ backpressure (§5.3)

    @Test
    void backpressurePausesAboveHighWaterAndResumesBelowLowWater() {
        Harness h = new Harness(config(c -> c.maxPendingPerPartition = 10));
        h.startActive(0);
        h.store.pendingCounts.put(0, 11L);
        h.loop.runOnce();
        assertTrue(h.consumer.paused().contains(tracker(0)), "pending above high-water pauses intake");
        assertEquals(1.0, h.gaugeValue("cesium.shard.paused", "partition", "0"));

        h.store.pendingCounts.put(0, 6L); // between low (5) and high (10): hysteresis holds
        h.loop.runOnce();
        assertTrue(h.consumer.paused().contains(tracker(0)));

        h.store.pendingCounts.put(0, 5L); // at/below low-water: resume
        h.loop.runOnce();
        assertFalse(h.consumer.paused().contains(tracker(0)));
        assertEquals(0.0, h.gaugeValue("cesium.shard.paused", "partition", "0"));
    }

    @Test
    void recoveringShardIsNeverPausedForBackpressure() {
        Harness h = new Harness(config(c -> c.maxPendingPerPartition = 10));
        h.startActive(0);
        h.store.forceRecovering.add(0);
        h.store.pendingCounts.put(0, 1_000L);
        h.loop.runOnce();
        assertFalse(
                h.consumer.paused().contains(tracker(0)),
                "a RECOVERING shard is never paused — replay must reach the barrier (R7)");
    }

    // ------------------------------------------------------------------ crash points (§3.9)

    @Test
    void crashAtD1LeavesNothingProduced() {
        Harness h = new Harness();
        h.startActive(0);
        CrashPoints.install(crashAt(CrashPoints.DISPATCH_BEFORE_BEGIN));
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));

        assertThrows(CrashPoints.SimulatedCrash.class, h.loop::runOnce);
        assertFalse(h.events.contains("begin"), "D-1: before beginTransaction — nothing produced");
        assertTrue(h.store.committedBatches.isEmpty());
        assertTrue(h.store.abortedBatches.isEmpty(), "a crash is not classified as a failure");
    }

    @Test
    void crashAtD2LeavesTransactionUncommitted() {
        Harness h = new Harness();
        h.startActive(0);
        CrashPoints.install(crashAt(CrashPoints.DISPATCH_AFTER_SENDS));
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));

        assertThrows(CrashPoints.SimulatedCrash.class, h.loop::runOnce);
        assertTrue(h.events.contains("sendOffsets"), "D-2 fires after the sends and offsets");
        assertFalse(h.events.contains("commit"), "the transaction never commits — it aborts broker-side (D-2)");
        assertTrue(h.store.committedBatches.isEmpty());
        assertTrue(h.store.abortedBatches.isEmpty());
    }

    @Test
    void crashAtD3LeavesCommitDurableButNoLocalBookkeeping() {
        Harness h = new Harness();
        h.startActive(0);
        CrashPoints.install(crashAt(CrashPoints.DISPATCH_DURING_COMMIT));
        h.store.dueQueue.add(TestBatch.onPartition(0, NOW, 1));

        assertThrows(CrashPoints.SimulatedCrash.class, h.loop::runOnce);
        assertEquals(1, h.producer().commitCount(), "the broker resolved the commit");
        assertTrue(h.store.committedBatches.isEmpty(), "D-3: no local bookkeeping — replay reconciles on restart");
        assertTrue(h.store.abortedBatches.isEmpty());
        assertEquals(
                0.0,
                h.counterValue("cesium.dispatch.records", "outcome", "dispatched"),
                "metrics flush only after the commit bookkeeping");
    }

    private static Consumer<String> crashAt(String point) {
        return id -> {
            if (point.equals(id)) {
                throw new CrashPoints.SimulatedCrash(id);
            }
        };
    }

    // ------------------------------------------------------------------ helpers & harness

    /** Asserts that {@code expected} appear in {@code events} in order (subsequence match). */
    private static void assertOrder(List<String> events, String... expected) {
        int from = 0;
        for (String step : expected) {
            int index = events.subList(from, events.size()).indexOf(step);
            assertTrue(index >= 0, "expected '" + step + "' (in order) in " + events);
            from += index + 1;
        }
    }

    private static void assertRows(List<long[]> actual, long[][] expected) {
        assertRows(actual, expected, "batch view rows");
    }

    private static void assertRows(List<long[]> actual, long[][] expected, String message) {
        assertEquals(expected.length, actual.size(), message + ": size");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i][0], actual.get(i)[0], message + ": partition at row " + i);
            assertEquals(expected[i][1], actual.get(i)[1], message + ": sourceOffset at row " + i);
        }
    }

    private static ConsumerRecord<byte[], byte[]> trackerRecord(int partition, long offset) {
        return new ConsumerRecord<>(TRACKER, partition, offset, new byte[] {1}, new byte[] {2});
    }

    private static DispatchLoopConfig config() {
        return config(c -> {});
    }

    private static DispatchLoopConfig config(Consumer<CfgBuilder> tweak) {
        CfgBuilder b = new CfgBuilder();
        tweak.accept(b);
        return new DispatchLoopConfig(
                TRACKER,
                SOURCE,
                10_000,
                32L * 1024 * 1024,
                Duration.ofSeconds(30),
                b.drainSlice,
                b.idleCursorInterval,
                Duration.ofSeconds(5),
                Duration.ofMillis(50),
                Duration.ofSeconds(10),
                b.policy,
                b.maxPendingPerPartition,
                0L,
                3,
                b.parkThreshold,
                Duration.ofMillis(100),
                Duration.ofSeconds(10));
    }

    private static final class CfgBuilder {
        Duration drainSlice = Duration.ofMinutes(1);
        Duration idleCursorInterval = Duration.ofHours(1); // effectively off unless a test opts in
        UnfetchablePayloadPolicy policy = UnfetchablePayloadPolicy.DLQ;
        long maxPendingPerPartition = 1_000_000;
        int parkThreshold = 5;
    }

    /** One loop with recording mocks; every collaborator wired exactly as production would. */
    private static final class Harness {
        final List<String> events = new ArrayList<>();
        final RecordingConsumer consumer = new RecordingConsumer(events);
        final List<RecordingProducer> producers = new ArrayList<>();
        final ScriptedTrackerStore store = new ScriptedTrackerStore(events);
        final FakeDispatchAdmin admin = new FakeDispatchAdmin(events);
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final MutableClock clock = new MutableClock(NOW);
        final FakeFetcher fetcher = new FakeFetcher(clock);
        final DispatchLoop loop;

        /** While positive, each factory-built producer's initTransactions fails once (transient). */
        int failProducerInits;

        Harness() {
            this(config());
        }

        Harness(DispatchLoopConfig config) {
            loop = new DispatchLoop(
                    config,
                    consumer,
                    () -> {
                        RecordingProducer producer = new RecordingProducer(events);
                        if (failProducerInits > 0) {
                            failProducerInits--;
                            producer.initTransactionsException = new KafkaException("injected init failure");
                        }
                        producers.add(producer);
                        return producer;
                    },
                    store,
                    fetcher,
                    new RelayRecordFactory(DEST, DLQ, true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY),
                    admin,
                    registry,
                    clock);
        }

        private final Map<TopicPartition, OffsetAndMetadata> seededOffsets = new HashMap<>();
        private boolean started;

        /**
         * Seeds the group-B committed offset (and its metadata sidecar) for a tracker partition.
         * MockConsumer clears its committed map on {@code subscribe}, so pre-start seeds are
         * deferred and applied by {@link #start}.
         */
        void seedCommitted(int partition, long offset, String metadata) {
            OffsetAndMetadata seed = new OffsetAndMetadata(offset, metadata);
            if (started) {
                consumer.commitSync(Map.of(tracker(partition), seed));
            } else {
                seededOffsets.put(tracker(partition), seed);
            }
        }

        /** Subscribes (start) and assigns the partitions; recovery has NOT run yet. */
        void start(int... partitions) {
            loop.start();
            started = true;
            if (!seededOffsets.isEmpty()) {
                consumer.commitSync(seededOffsets);
            }
            List<TopicPartition> tps = new ArrayList<>();
            Map<TopicPartition, Long> beginnings = new HashMap<>();
            for (int partition : partitions) {
                tps.add(tracker(partition));
                beginnings.put(tracker(partition), 0L);
            }
            consumer.updateBeginningOffsets(beginnings);
            consumer.rebalance(tps);
        }

        /** Starts with committed cursors at 0 and immediate barriers: ACTIVE after one iteration. */
        void startActive(int... partitions) {
            for (int partition : partitions) {
                seedCommitted(partition, 0L, "");
            }
            start(partitions);
            loop.runOnce();
            for (int partition : partitions) {
                assertFalse(store.isRecovering(partition), "expected partition " + partition + " ACTIVE");
            }
            events.clear();
            store.positions.clear();
        }

        RecordingProducer producer() {
            return producers.get(producers.size() - 1);
        }

        Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets() {
            var history = producer().consumerGroupOffsetsHistory();
            assertFalse(history.isEmpty(), "expected a committed offsets entry");
            return history.get(history.size() - 1).get(consumer.groupMetadata().groupId());
        }

        double counterValue(String name, String... tags) {
            Counter counter = registry.find(name).tags(tags).counter();
            return counter == null ? 0.0 : counter.count();
        }

        double gaugeValue(String name, String... tags) {
            Gauge gauge = registry.find(name).tags(tags).gauge();
            return gauge == null ? -1.0 : gauge.value();
        }
    }

    /** MockConsumer that logs polls and committed-offset fetches for ordering assertions (I8). */
    private static final class RecordingConsumer extends MockConsumer<byte[], byte[]> {
        private final List<String> events;

        RecordingConsumer(List<String> events) {
            super("none");
            this.events = events;
        }

        @Override
        public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            events.add("poll");
            return super.poll(timeout);
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
            for (TopicPartition tp : partitions) {
                events.add("committed:" + tp.partition());
            }
            return super.committed(partitions);
        }
    }

    /** MockProducer that logs transaction events and supports per-call fault injection. */
    private static final class RecordingProducer extends MockProducer<byte[], byte[]> {
        private final List<String> events;
        int failSendOffsetsTimes;
        ConsumerGroupMetadata offsetsGroupMetadata;

        /** FIFO of exceptions thrown by successive {@code commitTransaction} calls (then success). */
        final ArrayDeque<RuntimeException> commitExceptionQueue = new ArrayDeque<>();

        RuntimeException initTransactionsException;

        RecordingProducer(List<String> events) {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
            this.events = events;
        }

        @Override
        public void initTransactions() {
            RuntimeException injected = initTransactionsException;
            if (injected != null) {
                initTransactionsException = null;
                throw injected;
            }
            events.add("initTransactions");
            super.initTransactions();
        }

        @Override
        public void beginTransaction() {
            events.add("begin");
            super.beginTransaction();
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            events.add("send:" + record.topic());
            return super.send(record, callback);
        }

        @Override
        public void sendOffsetsToTransaction(
                Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
            events.add("sendOffsets");
            if (failSendOffsetsTimes > 0) {
                failSendOffsetsTimes--;
                throw new KafkaException("injected sendOffsets failure");
            }
            offsetsGroupMetadata = groupMetadata;
            super.sendOffsetsToTransaction(offsets, groupMetadata);
        }

        @Override
        public void commitTransaction() {
            events.add("commit");
            RuntimeException injected = commitExceptionQueue.poll();
            if (injected != null) {
                throw injected;
            }
            if (commitTransactionException != null) {
                // MockProducer's own field, honored on every call (sticky ambiguity).
                throw commitTransactionException;
            }
            super.commitTransaction();
        }

        @Override
        public void abortTransaction() {
            events.add("abort");
            super.abortTransaction();
        }
    }

    /** Scripted {@link DispatchAdmin}: immediate barriers by default, pendable per partition. */
    private static final class FakeDispatchAdmin implements DispatchAdmin {
        private final List<String> events;
        final Map<Integer, Long> barrierValues = new HashMap<>();
        private final Map<Integer, CompletableFuture<Long>> pendingBarriers = new HashMap<>();
        boolean groupHasOffsets;

        FakeDispatchAdmin(List<String> events) {
            this.events = events;
        }

        /** The next barrier request for {@code partition} returns this incomplete future. */
        CompletableFuture<Long> pend(int partition) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            pendingBarriers.put(partition, future);
            return future;
        }

        @Override
        public CompletableFuture<Long> trackerHighWatermark(int partition) {
            events.add("barrier:" + partition);
            CompletableFuture<Long> pending = pendingBarriers.remove(partition);
            if (pending != null) {
                return pending;
            }
            return CompletableFuture.completedFuture(barrierValues.getOrDefault(partition, 0L));
        }

        @Override
        public boolean groupHasCommittedOffsets() {
            return groupHasOffsets;
        }
    }

    /** Scripted {@link SeekFetcher}: per-(partition, offset) outcomes, default {@code FOUND}. */
    private static final class FakeFetcher implements SeekFetcher {
        private final MutableClock clock;
        private final Map<String, FetchOutcome> outcomes = new HashMap<>();
        RuntimeException failure;
        long advanceClockMs;

        FakeFetcher(MutableClock clock) {
            this.clock = clock;
        }

        void classify(int partition, long sourceOffset, FetchOutcome outcome) {
            outcomes.put(partition + ":" + sourceOffset, outcome);
        }

        @Override
        public FetchResult fetch(DueBatch candidates, long maxBytes, long deadlineMs) {
            RuntimeException injected = failure;
            if (injected != null) {
                failure = null;
                throw injected;
            }
            if (advanceClockMs > 0) {
                clock.advanceMillis(advanceClockMs); // simulated fetch work for the storm test
            }
            int n = candidates.size();
            FetchOutcome[] outs = new FetchOutcome[n];
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                outs[i] = outcomes.getOrDefault(
                        candidates.sourcePartition(i) + ":" + candidates.sourceOffset(i), FetchOutcome.FOUND);
                records.add(
                        outs[i] == FetchOutcome.FOUND
                                ? new ConsumerRecord<>(
                                        SOURCE,
                                        candidates.sourcePartition(i),
                                        candidates.sourceOffset(i),
                                        new byte[] {1},
                                        new byte[] {2})
                                : null);
            }
            return new FetchResult() {
                @Override
                public int size() {
                    return n;
                }

                @Override
                public FetchOutcome outcome(int i) {
                    return outs[i];
                }

                @Override
                public ConsumerRecord<byte[], byte[]> record(int i) {
                    ConsumerRecord<byte[], byte[]> record = records.get(i);
                    if (record == null) {
                        throw new IllegalStateException("record(" + i + ") on outcome " + outs[i]);
                    }
                    return record;
                }

                @Override
                public List<PartitionSummary> partitionSummaries() {
                    return List.of();
                }
            };
        }

        @Override
        public void close() {}
    }

    /** Manually advanced clock: backoff/idle gating is tested without real waiting. */
    static final class MutableClock extends Clock {
        private long nowMs;

        MutableClock(long nowMs) {
            this.nowMs = nowMs;
        }

        void advanceMillis(long ms) {
            nowMs += ms;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(nowMs);
        }
    }
}
