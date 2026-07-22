package com.jucius.cesium.kafka.core.ingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import com.jucius.cesium.kafka.core.headers.RelayPartitioning;
import com.jucius.cesium.kafka.core.headers.RelayRecordFactory;
import com.jucius.cesium.kafka.core.headers.RelayTimestampPolicy;
import com.jucius.cesium.kafka.core.policy.IngestPolicyEngine;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Ingest-loop unit suite (design §11.1 "engine sequencing"): transaction sequencing (I1–I3),
 * the policy decision table's wiring, the §3.8 taxonomy — abort-and-rewind, fatal, in-doubt —
 * park-and-degrade transitions, and shutdown at a transaction boundary, all against
 * {@link MockConsumer}/{@link MockProducer}.
 */
class IngestLoopTest {

    static {
        // The crash-point seam refuses to arm without the explicit test-only guard property.
        System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
    }

    private static final long NOW = 1_700_000_000_000L;
    private static final long SOURCE_TS = NOW - 1_000;
    private static final long DELAY_MAX_MS = Duration.ofDays(1).toMillis();
    private static final String SOURCE = "orders";
    private static final String TRACKER = "orders-tracker";
    private static final String DEST = "orders-out";
    private static final String DLQ = "orders-dlq";
    private static final TopicPartition P0 = new TopicPartition(SOURCE, 0);
    private static final TopicPartition P1 = new TopicPartition(SOURCE, 1);
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    /** The identity blob the loop stamps on its own committed offsets (§3.1); a real resume carries it. */
    private static final String IDENTITY_METADATA = new IdentityBlob("test-cluster", TOPIC_ID).encode();

    private static final byte[] KEY = {1, 2, 3};
    private static final byte[] VALUE = {9, 8, 7, 6};

    @AfterEach
    void resetCrashPoints() {
        CrashPoints.reset();
    }

    // ------------------------------------------------------------------ sequencing (I1/I2/I3)

    @Test
    void emptyPollOpensNoTransaction() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.loop.runOnce();
        assertEquals(List.of("poll"), h.events, "no begin/commit on an empty batch (§3.1)");
        assertFalse(h.producer().transactionInFlight());
        assertEquals(0, h.producer().commitCount());
    }

    @Test
    void transactionSequencingHoldsI1I2I3() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.consumer.addRecord(record(P0, 1, noHeaders()));
        h.loop.runOnce();

        // I1: every send between begin and commit. I2: sendOffsets before commit.
        // I3: no poll between begin and commit — the whole txn fits one iteration.
        assertEquals(List.of("poll", "begin", "send:" + DEST, "send:" + DEST, "sendOffsets", "commit"), h.events);
        assertNotNull(h.producer().offsetsGroupMetadata, "I2: groupMetadata accompanied the offsets");
        assertEquals(
                h.consumer.groupMetadata().groupId(),
                h.producer().offsetsGroupMetadata.groupId(),
                "group metadata is the consumer's own, fetched immediately before the call");

        // Offsets = next offsets, carrying the identity blob (§3.1).
        Map<TopicPartition, OffsetAndMetadata> committed = h.lastCommittedOffsets();
        assertEquals(2L, committed.get(P0).offset());
        IdentityBlob blob = IdentityBlob.decode(committed.get(P0).metadata());
        assertEquals(new IdentityBlob("test-cluster", TOPIC_ID), blob);

        assertEquals(2.0, h.counterValue("cesium.ingest.records", "outcome", "relayed_immediate"));
        assertEquals(
                1.0, h.counterValue("cesium.transactions", "loop", "ingest", "result", "committed", "cause", "none"));
    }

    @Test
    void offsetsCoverEveryPartitionTheTransactionTouches() {
        Harness h = new Harness();
        h.startAssigned(P0, P1);
        h.consumer.addRecord(record(P0, 4, noHeaders()));
        h.consumer.addRecord(record(P1, 9, delayMs("60000")));
        h.loop.runOnce();

        Map<TopicPartition, OffsetAndMetadata> committed = h.lastCommittedOffsets();
        assertEquals(5L, committed.get(P0).offset(), "I2: relays advance their partition");
        assertEquals(10L, committed.get(P1).offset(), "I2: schedules advance their partition too");
    }

    // ------------------------------------------------------------------ decision-table wiring

    @Test
    void schedulesDelayedRecordToTrackerOnSamePartitionNumber() {
        Harness h = new Harness();
        h.startAssigned(P0, P1);
        h.consumer.addRecord(record(P1, 7, delayMs("60000")));
        h.loop.runOnce();

        assertEquals(List.of(new ScheduledRef(1, 7, SOURCE_TS + 60_000, false)), h.store.encoded);
        ProducerRecord<byte[], byte[]> add = h.producer().history().get(0);
        assertEquals(TRACKER, add.topic());
        assertEquals(1, add.partition(), "tracker record lands on the SAME partition number (§2.1)");
        assertArrayEquals(FakeTrackerStore.key(7), add.key(), "the store owns the wire format");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "scheduled"));
    }

    @Test
    void clampPolicyCarriesClampedFlagThroughScheduledRef() {
        Harness h = new Harness(defaultConfig(), MalformedHeaderPolicy.DLQ, OverMaxPolicy.CLAMP);
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, delayMs(String.valueOf(2 * DELAY_MAX_MS))));
        h.loop.runOnce();

        // CLAMP pins to now + delay.max and the flag rides the ScheduledRef into the store (§2.3).
        assertEquals(List.of(new ScheduledRef(0, 0, NOW + DELAY_MAX_MS, true)), h.store.encoded);
        assertEquals(TRACKER, h.producer().history().get(0).topic());
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "clamped"));
        assertEquals(1.0, h.counterValue("cesium.header.errors", "type", "over_max"));
    }

    @Test
    void malformedHeaderRoutesToDlqByDefault() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.loop.runOnce();

        ProducerRecord<byte[], byte[]> dlq = h.producer().history().get(0);
        assertEquals(DLQ, dlq.topic());
        assertEquals(DlqReasons.MALFORMED_HEADER, headerValue(dlq, CesiumHeaders.ERROR_REASON));
        assertArrayEquals(KEY, dlq.key(), "DLQ carries the original key/value (§2.4)");
        assertArrayEquals(VALUE, dlq.value());
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "dlq"));
        assertEquals(1.0, h.counterValue("cesium.header.errors", "type", "malformed"));
        assertEquals(1.0, h.counterValue("cesium.dlq.records", "reason", DlqReasons.MALFORMED_HEADER));
    }

    @Test
    void overMaxRoutesToDlqByDefault() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, delayMs(String.valueOf(2 * DELAY_MAX_MS))));
        h.loop.runOnce();

        ProducerRecord<byte[], byte[]> dlq = h.producer().history().get(0);
        assertEquals(DLQ, dlq.topic());
        assertEquals(DlqReasons.OVER_MAX_DELAY, headerValue(dlq, CesiumHeaders.ERROR_REASON));
        assertEquals(1.0, h.counterValue("cesium.header.errors", "type", "over_max"));
    }

    @Test
    void pastDeliverAtRelaysImmediately() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, deliverAt(String.valueOf(NOW - 5_000))));
        h.loop.runOnce();

        assertEquals(DEST, h.producer().history().get(0).topic());
        assertTrue(h.store.encoded.isEmpty(), "past-due never schedules");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "relayed_immediate"));
        assertEquals(0.0, h.counterValue("cesium.header.errors", "type", "malformed"));
    }

    @Test
    void conflictingHeadersCountConflictAndDeliverAtWins() {
        Harness h = new Harness();
        h.startAssigned(P0);
        Headers headers = new RecordHeaders();
        headers.add(CesiumHeaders.DELAY_MS, ascii("60000"));
        headers.add(CesiumHeaders.DELIVER_AT, ascii(String.valueOf(NOW + 120_000)));
        h.consumer.addRecord(record(P0, 0, headers));
        h.loop.runOnce();

        assertEquals(1.0, h.counterValue("cesium.header.errors", "type", "conflict"));
        assertEquals(
                List.of(new ScheduledRef(0, 0, NOW + 120_000, false)),
                h.store.encoded,
                "precedence: deliver-at wins; conflict is observability-only");
    }

    @Test
    void failPolicyAbortsAndFailsTheLoop() {
        Harness h = new Harness(defaultConfig(), MalformedHeaderPolicy.FAIL, OverMaxPolicy.DLQ);
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));

        assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertTrue(h.events.contains("abort"), "FAIL aborts: the record is not consumed");
        assertEquals(0, h.producer().commitCount());
        assertEquals(
                1.0,
                h.counterValue("cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "policy_fail"));
    }

    // ------------------------------------------------------------------ §3.8 error taxonomy

    @Test
    void abortableErrorAbortsRewindsAndReprocessesToASingleCommittedCopy() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.producer().failSendOffsetsTimes = 1;
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce();

        assertTrue(h.events.contains("abort"));
        assertEquals(0L, h.consumer.position(P0), "consumer rewound to the failed batch's start");
        assertEquals(java.util.Set.of(P0), h.consumer.paused(), "backoff pauses intake; polls stay heartbeats");
        assertEquals(0, h.producer().history().size(), "aborted sends are invisible");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "KafkaException"));

        // While the backoff deadline is in the future the loop heartbeat-polls without opening txns.
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount());

        // MockConsumer dequeues delivered records, so redelivery after the rewind is re-added here.
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(101);
        h.loop.runOnce();

        assertTrue(h.consumer.paused().isEmpty(), "resume on success (§3.8)");
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size(), "exactly one committed copy after the retry");
        assertEquals(1L, h.lastCommittedOffsets().get(P0).offset());
    }

    @Test
    void parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess() {
        IngestLoopConfig config = new IngestLoopConfig(
                SOURCE,
                TRACKER,
                2,
                Duration.ofSeconds(1),
                3,
                2,
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                UnrelayablePolicy.DLQ);
        Harness h = new Harness(config, MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        h.startAssigned(P0);
        h.producer().failSendOffsetsTimes = 2;

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce(); // failure 1: bounded retry, not yet parked
        assertFalse(h.loop.isDegraded());

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(101); // past backoff(1) = 100 ms
        h.loop.runOnce(); // failure 2 = parkThreshold: parked-and-degraded
        assertTrue(h.loop.isDegraded(), "retry exhaustion parks the loop instead of crash-looping (R15)");
        assertEquals(java.util.Set.of(P0), h.consumer.paused(), "membership held with paused heartbeat polls");

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(201); // past backoff(2) = 200 ms
        h.loop.runOnce(); // success: resume + clear degraded
        assertFalse(h.loop.isDegraded(), "degraded clears on the first successful commit");
        assertTrue(h.consumer.paused().isEmpty());
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size(), "still exactly one committed copy");
    }

    @Test
    void fencedProducerFailsTheLoop() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.producer().fenceProducer();

        assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertFalse(h.events.contains("abort"), "fatal path never tries to abort a fenced producer");
        assertEquals(0, h.producer().commitCount());
    }

    @Test
    void inDoubtCommitRetriesThenReplacesProducerAndRepollsFromCommitted_abortOutcome() {
        Harness h = new Harness(); // commitRetryLimit = 3
        h.startAssigned(P0);
        // Simulated broker outcome: the transaction ABORTED — committed offsets stay pre-batch.
        h.consumer.commitSync(Map.of(P0, new OffsetAndMetadata(0L, IDENTITY_METADATA)));
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.producer().commitTransactionException = new TimeoutException("ambiguous");
        h.loop.runOnce();

        assertEquals(3, h.events.stream().filter("commit"::equals).count(), "commit retried to the budget");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "in_doubt", "cause", "commit_timeout"));
        assertEquals(2, h.producers.size(), "in-doubt replaces the producer");
        assertTrue(h.producers.get(0).closed());
        assertTrue(h.producer().transactionInitialized(), "replacement is fenced via initTransactions");
        assertEquals(0L, h.consumer.position(P0), "re-poll position = committed offset (abort outcome)");
        assertTrue(h.producer().history().isEmpty(), "I9 analog: the batch is NEVER restored from memory");

        // The records re-poll and process on the replacement producer — one committed copy total.
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(10_000);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size());
    }

    @Test
    void inDoubtCommitRepollsFromCommitted_commitLandedOutcome() {
        Harness h = new Harness();
        h.startAssigned(P0);
        // Simulated broker outcome: the transaction COMMITTED — committed offsets are post-batch.
        h.consumer.commitSync(Map.of(P0, new OffsetAndMetadata(1L, IDENTITY_METADATA)));
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.producer().commitTransactionException = new TimeoutException("ambiguous");
        h.loop.runOnce();

        assertEquals(1L, h.consumer.position(P0), "re-poll position = committed offset (commit landed)");

        // Nothing left to re-poll: no duplicate processing on the replacement producer.
        h.clock.advanceMillis(10_000);
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount());
        assertTrue(h.producer().history().isEmpty(), "offsets materialize only at commit ⇒ no reprocess");
    }

    @Test
    void resumeOnBlankCommittedMetadataFailsClosed() {
        // VULN-006: at the offset-fetch/resume boundary a committed offset that carries no identity
        // blob used to be silently tolerated, letting anyone able to commit for the ingest group seed
        // an attacker-chosen resume position. A genuine first run has no committed offset, so a
        // committed-but-blank one is unverifiable and the loop must refuse to resume on it.
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.commitSync(Map.of(P0, new OffsetAndMetadata(1L))); // committed, but no identity blob
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.producer().commitTransactionException = new TimeoutException("ambiguous");

        IngestLoopFatalException fatal = assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertTrue(fatal.getMessage().contains("blank/absent"), fatal.getMessage());
    }

    @Test
    void postAmbiguityCommitFailureEscalatesToInDoubtNeverRewind() {
        Harness h = new Harness(); // commitRetryLimit = 3
        h.startAssigned(P0);
        // Simulated broker outcome: the transaction COMMITTED (a timed-out EndTxn can be
        // processed broker-side) — committed offsets are post-batch.
        h.consumer.commitSync(Map.of(P0, new OffsetAndMetadata(1L, IDENTITY_METADATA)));
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        // Attempt 1 times out (ambiguous); the retry fails with a non-timeout, non-fatal
        // exception. After the ambiguity NOTHING can prove "aborted" (D20) — the abort/rewind
        // path would re-send the batch and commit a duplicate copy (the I9 vector).
        h.producer().commitExceptionQueue.add(new TimeoutException("ambiguous"));
        h.producer().commitExceptionQueue.add(new KafkaException("retry failed non-fatally"));
        h.loop.runOnce();

        assertFalse(h.events.contains("abort"), "post-ambiguity failures must NEVER take the abort/rewind path");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "in_doubt", "cause", "KafkaException"));
        assertEquals(
                0.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "KafkaException"));
        assertEquals(2, h.producers.size(), "the in-doubt procedure replaces the producer");
        assertTrue(h.producers.get(0).closed());
        assertEquals(1L, h.consumer.position(P0), "re-poll position = committed offset (commit landed)");
        assertTrue(h.producer().history().isEmpty(), "I9: the batch is NEVER restored from memory");

        // Nothing left to re-poll: the batch is never re-sent when the broker outcome was commit.
        h.clock.advanceMillis(10_000);
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount());
        assertTrue(h.producer().history().isEmpty(), "no second committed copy of the batch exists");
    }

    @Test
    void firstAttemptNonTimeoutCommitFailureIsDefinitivelyAbortable() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        // No ambiguity exists before the first timeout: a non-timeout first-attempt failure is
        // the §3.8 row-1 non-ambiguous commit failure — abort, rewind, retry.
        h.producer().commitExceptionQueue.add(new KafkaException("definitive commit failure"));
        h.loop.runOnce();

        assertTrue(h.events.contains("abort"));
        assertEquals(0L, h.consumer.position(P0), "rewound to the batch start");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "KafkaException"));
        assertEquals(1, h.producers.size(), "no producer replacement for a definitive failure");

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size(), "exactly one committed copy after the retry");
    }

    @Test
    void inDoubtRecoveryRetriesTransientFailuresInsteadOfDyingFatally() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.commitSync(Map.of(P0, new OffsetAndMetadata(0L, IDENTITY_METADATA)));
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.producer().commitTransactionException = new TimeoutException("ambiguous");
        // The replacement producer's initTransactions fails once — the same broker degradation
        // that caused the in-doubt commit. §3.8/R15: retry under capped backoff, never crash-loop.
        h.failProducerInits = 1;
        h.loop.runOnce(); // must NOT throw

        assertEquals(2, h.producers.size(), "first recovery attempt built (and lost) a replacement");
        assertTrue(h.producers.get(0).closed());

        h.clock.advanceMillis(101); // past backoff(1)
        h.loop.runOnce(); // recovery retry: fresh producer, init OK, seek to committed

        assertEquals(3, h.producers.size(), "the retry replaced the half-initialized producer");
        assertTrue(h.producers.get(1).closed());
        assertTrue(h.producer().transactionInitialized(), "replacement fenced via initTransactions");
        assertEquals(0L, h.consumer.position(P0), "re-poll position = committed offset");

        // The loop is fully operational again: the re-polled batch commits exactly one copy.
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size());
        assertFalse(h.loop.isDegraded(), "success clears the failure streak");
    }

    @Test
    void asyncSendFailureSurfacesBeforeSendOffsetsAndAborts() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.producer().failSendCallbackAt = 1; // the second send's callback completes exceptionally
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.consumer.addRecord(record(P0, 1, noHeaders()));
        h.loop.runOnce();

        assertTrue(h.events.contains("abort"));
        assertFalse(
                h.events.contains("sendOffsets"),
                "an async send failure must surface BEFORE the offsets are sent, not at commit time");
        assertEquals(0L, h.consumer.position(P0), "rewound to the batch start");
        assertTrue(h.producer().history().isEmpty(), "aborted sends are invisible");
        assertEquals(
                1.0,
                h.counterValue(
                        "cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "KafkaException"));

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.consumer.addRecord(record(P0, 1, noHeaders()));
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(2, h.producer().history().size(), "exactly one committed copy of each record");
    }

    @Test
    void abortFailureReplacesProducerAndTheNextBatchCommitsOnce() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.producer().failSendOffsetsTimes = 1;
        h.producer().abortTransactionException = new KafkaException("abort failed");
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce(); // abortable failure; the abort itself fails => producer replacement

        assertEquals(2, h.producers.size(), "a failed abort forces a producer replacement");
        assertTrue(h.producers.get(0).closed());
        assertTrue(h.producer().transactionInitialized(), "replacement fenced via initTransactions");
        assertEquals(0L, h.consumer.position(P0), "rewound for retry");

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(1, h.producer().history().size(), "exactly one committed copy on the replacement");
    }

    // ------------------------------------------------------------------ unrelayable poison records (M2/§3.8 I-8)

    @Test
    void permanentRelayRejectionRoutesToDlqAtomicallyAndAdvancesPastIt() {
        Harness h = new Harness();
        h.startAssigned(P0);
        // The destination PERMANENTLY rejects the first record's relay (too large). The second is a
        // normal record at a higher offset on the same partition — it must still be delivered (the
        // partition is not wedged behind the poison record).
        h.producer().sendCallbackError = new RecordTooLargeException("relay exceeds max.message.bytes");
        h.producer().failSendCallbackAt = 0;
        h.consumer.addRecord(record(P0, 0, noHeaders())); // poison
        h.consumer.addRecord(record(P0, 1, noHeaders())); // normal, behind the poison

        h.loop.runOnce(); // attempt 1: relay@0 permanently rejected ⇒ abort, remember, rewind
        assertTrue(h.events.contains("abort"), "a permanent rejection aborts the batch (nothing committed)");
        assertEquals(0, h.producer().commitCount(), "nothing committed on the rejecting attempt");
        assertEquals(0L, h.consumer.position(P0), "rewound to reprocess and route the poison record");
        assertTrue(h.consumer.paused().isEmpty(), "a poison record is NOT a transient failure: no backoff/park gate");
        assertFalse(h.loop.isDegraded(), "a deterministic record rejection must never trip park-and-degrade");
        assertEquals(
                0.0, h.counterValue("cesium.dlq.records", "reason", DlqReasons.UNRELAYABLE), "counts flush on commit");

        // The §3.8 I-8 retry: poison ⇒ DLQ, normal ⇒ destination, source offset advanced past both,
        // all in ONE committed transaction. No clock advance: there is no backoff to wait out.
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.consumer.addRecord(record(P0, 1, noHeaders()));
        h.loop.runOnce();

        assertEquals(1, h.producer().commitCount(), "exactly one committed copy — never an infinite retry loop");
        assertEquals(2, h.producer().history().size(), "the poison DLQ record and the normal relay, atomic");
        ProducerRecord<byte[], byte[]> dlq = h.producer().history().get(0);
        assertEquals(DLQ, dlq.topic(), "poison record routed to the DLQ");
        assertEquals(DlqReasons.UNRELAYABLE, headerValue(dlq, CesiumHeaders.ERROR_REASON));
        assertArrayEquals(KEY, dlq.key(), "DLQ reuses the header-error shape: original key/value (§2.4)");
        assertArrayEquals(VALUE, dlq.value());
        assertNotNull(
                dlq.headers().lastHeader(CesiumHeaders.ERROR_DETAIL), "the destination rejection detail is carried");
        assertEquals(DEST, h.producer().history().get(1).topic(), "the following normal record IS delivered");
        assertEquals(
                2L, h.lastCommittedOffsets().get(P0).offset(), "the source offset advanced past the poison record");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "dlq"));
        assertEquals(1.0, h.counterValue("cesium.dlq.records", "reason", DlqReasons.UNRELAYABLE));
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "relayed_immediate"));
        assertEquals(
                1.0,
                h.counterValue("cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "unrelayable"));
    }

    @Test
    void transientRelayFailureStaysOnTheRetryPathAndNeverDeadLetters() {
        Harness h = new Harness();
        h.startAssigned(P0);
        // A TRANSIENT produce failure (broker availability, not record-level) must NOT be mistaken
        // for a permanent rejection — it stays on the abort/retry path so a blip never drops a message.
        h.producer().sendCallbackError = new KafkaException("transient destination unavailability");
        h.producer().failSendCallbackAt = 0;
        h.consumer.addRecord(record(P0, 0, noHeaders()));

        h.loop.runOnce();
        assertTrue(h.events.contains("abort"));
        assertEquals(java.util.Set.of(P0), h.consumer.paused(), "transient failure arms the backoff gate (retry path)");
        assertEquals(
                0.0,
                h.counterValue("cesium.dlq.records", "reason", DlqReasons.UNRELAYABLE),
                "a transient failure is never routed to the unrelayable DLQ");
        assertEquals(
                0.0,
                h.counterValue("cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "unrelayable"));

        // It retries and succeeds once the destination recovers — exactly the existing behavior.
        h.producer().failSendCallbackAt = -1;
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.clock.advanceMillis(101);
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount());
        assertEquals(DEST, h.producer().history().get(0).topic(), "the record relays normally after the blip");
    }

    @Test
    void unrelayableFailPolicyStopsTheLoop() {
        Harness h = new Harness(defaultConfig(UnrelayablePolicy.FAIL), MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        h.startAssigned(P0);
        h.producer().sendCallbackError = new RecordTooLargeException("too large");
        h.producer().failSendCallbackAt = 0;
        h.consumer.addRecord(record(P0, 0, noHeaders()));

        IngestLoopFatalException failure = assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("on-unrelayable=FAIL"), failure.getMessage());
        assertTrue(h.events.contains("abort"), "FAIL aborts: the record is not consumed");
        assertEquals(0, h.producer().commitCount());
    }

    @Test
    void unrelayableDropPolicyAdvancesWithoutADlqRecord() {
        Harness h = new Harness(defaultConfig(UnrelayablePolicy.DROP), MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        h.startAssigned(P0);
        h.producer().sendCallbackError = new RecordTooLargeException("too large");
        h.producer().failSendCallbackAt = 0;
        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce(); // attempt 1: rejected ⇒ remember as DROP, rewind

        h.consumer.addRecord(record(P0, 0, noHeaders()));
        h.loop.runOnce(); // attempt 2: dropped — nothing produced, but the offset advances past it

        assertEquals(1, h.producer().commitCount());
        assertTrue(h.producer().history().isEmpty(), "DROP produces nothing — not even a DLQ record");
        assertEquals(
                1L, h.lastCommittedOffsets().get(P0).offset(), "the offset still advances past the dropped record");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "unrelayable_dropped"));
    }

    @Test
    void rejectedHeaderErrorDlqWriteEscalatesToDropAndAdvancesPastThePoisonRecord() {
        // §3.8 I-8 sibling path: a malformed-header record routed to the DLQ whose DLQ copy is ITSELF
        // permanently rejected (the original payload + all headers + provenance exceeds the producer
        // max.request.size / DLQ max.message.bytes). The header-error DLQ send is attributed, so the
        // rejection routes through the unrelayable path (escalating to DROP) instead of falling to the
        // abortable path and wedging the partition on an infinite abort/retry loop.
        Harness h = new Harness(); // onMalformed=DLQ, onUnrelayable=DLQ (defaults)
        h.startAssigned(P0);
        h.producer().sendCallbackError = new RecordTooLargeException("DLQ copy exceeds max.request.size");
        h.producer().failSendCallbackAt = 0; // reject the header-error DLQ write itself
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number"))); // malformed ⇒ header-error DLQ

        h.loop.runOnce(); // attempt 1: header-error DLQ write rejected ⇒ abort, remember as DLQ, rewind
        assertTrue(h.events.contains("abort"), "a rejected DLQ write aborts the batch (nothing committed)");
        assertEquals(0, h.producer().commitCount(), "nothing committed on the rejecting attempt");
        assertEquals(0L, h.consumer.position(P0), "rewound to reprocess the poison record, not advanced past it");
        assertTrue(
                h.consumer.paused().isEmpty(),
                "a rejected DLQ write is NOT a transient failure: no backoff/park gate (would wedge the partition)");
        assertFalse(h.loop.isDegraded(), "a deterministic DLQ-write rejection must never trip park-and-degrade");
        assertEquals(
                1.0,
                h.counterValue("cesium.transactions", "loop", "ingest", "result", "aborted", "cause", "unrelayable"),
                "attributed: routed through the unrelayable abort path, not the generic abortable path");

        // attempt 2: the unrelayable DLQ copy is rejected AGAIN ⇒ promoteUnrelayable escalates to DROP.
        h.producer().failSendCallbackAt = 1; // the reprocess's DLQ send is the producer's 2nd send
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.loop.runOnce();
        assertEquals(0, h.producer().commitCount(), "still nothing committed while the DLQ copy keeps being rejected");
        assertEquals(
                1.0,
                h.counterValue("cesium.unrelayable.dlq.rejected"),
                "the escalate-to-DROP path is taken when the DLQ copy is itself rejected");

        // attempt 3: now resolved as DROP ⇒ nothing produced, but the source offset advances past it.
        h.producer().failSendCallbackAt = -1;
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.loop.runOnce();
        assertEquals(1, h.producer().commitCount(), "exactly one committed copy — never an infinite retry/wedge");
        assertTrue(h.producer().history().isEmpty(), "DROP produces nothing — not even a DLQ record");
        assertEquals(
                1L,
                h.lastCommittedOffsets().get(P0).offset(),
                "the source offset advanced past the poison record (partition not wedged)");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "unrelayable_dropped"));
    }

    // ------------------------------------------------------------------ I-7 tracker partition drift

    @Test
    void missingTrackerPartitionFailsFastWithTheI7Runbook() {
        IngestLoopConfig config = new IngestLoopConfig(
                SOURCE,
                TRACKER,
                1, // the tracker has 1 partition; the source grew to 2
                Duration.ofSeconds(1),
                3,
                IngestLoopConfig.DEFAULT_PARK_THRESHOLD,
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                UnrelayablePolicy.DLQ);
        Harness h = new Harness(config, MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        h.startAssigned(P0, P1);
        h.consumer.addRecord(record(P1, 0, delayMs("60000")));

        IngestLoopFatalException failure = assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("I-7"), failure.getMessage());
        assertTrue(failure.getMessage().contains("grow the tracker topic first"), failure.getMessage());
        assertTrue(h.events.contains("abort"), "the batch aborts before the loop fails (§3.9 I-7)");
        assertEquals(0, h.producer().commitCount());
        assertTrue(h.store.encoded.isEmpty(), "nothing is encoded for a partition that cannot exist");
    }

    @Test
    void operatorGrownTrackerPassesTheLiveMetadataRecheck() {
        IngestLoopConfig config = new IngestLoopConfig(
                SOURCE,
                TRACKER,
                1, // stale startup count; the live tracker has since been grown to 2
                Duration.ofSeconds(1),
                3,
                IngestLoopConfig.DEFAULT_PARK_THRESHOLD,
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                UnrelayablePolicy.DLQ);
        Harness h = new Harness(config, MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        h.producer().partitionsForResult = List.of(
                new PartitionInfo(TRACKER, 0, null, null, null), new PartitionInfo(TRACKER, 1, null, null, null));
        h.startAssigned(P0, P1);
        h.consumer.addRecord(record(P1, 7, delayMs("60000")));
        h.loop.runOnce();

        assertEquals(1, h.producer().commitCount(), "the live re-check accepts the grown tracker");
        ProducerRecord<byte[], byte[]> add = h.producer().history().get(0);
        assertEquals(TRACKER, add.topic());
        assertEquals(1, add.partition());
    }

    // ------------------------------------------------------------------ committed-metric semantics

    @Test
    void abortRetryCyclesNeverInflateThePerRecordCounters() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.producer().failSendOffsetsTimes = 2;

        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.loop.runOnce(); // attempt 1: aborted
        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.clock.advanceMillis(101);
        h.loop.runOnce(); // attempt 2: aborted
        assertEquals(
                0.0,
                h.counterValue("cesium.ingest.records", "outcome", "dlq"),
                "per-record counters reflect committed dispositions only (§9): aborted attempts count nothing");
        assertEquals(0.0, h.counterValue("cesium.header.errors", "type", "malformed"));
        assertEquals(0.0, h.counterValue("cesium.dlq.records", "reason", DlqReasons.MALFORMED_HEADER));

        h.consumer.addRecord(record(P0, 0, delayMs("not-a-number")));
        h.clock.advanceMillis(201);
        h.loop.runOnce(); // attempt 3: commits
        assertEquals(1, h.producer().history().size(), "exactly one committed copy");
        assertEquals(1.0, h.counterValue("cesium.ingest.records", "outcome", "dlq"), "exactly one count for it");
        assertEquals(1.0, h.counterValue("cesium.header.errors", "type", "malformed"));
        assertEquals(1.0, h.counterValue("cesium.dlq.records", "reason", DlqReasons.MALFORMED_HEADER));
    }

    @Test
    void closingTheLoopUnregistersItsGauges() {
        Harness h = new Harness();
        assertNotNull(h.registry.find("cesium.degraded").gauge());
        assertNotNull(
                h.registry.find("cesium.loop.last.iteration.timestamp.seconds").gauge());
        h.loop.stop();
        h.loop.run(); // exits at the first stop-flag check; closes clients in the finally

        // Micrometer returns the EXISTING meter on name+tag collision — a restarted loop against
        // the same registry must bind fresh gauges, not sample this dead loop's atomics forever.
        assertNull(h.registry.find("cesium.degraded").gauge());
        assertNull(
                h.registry.find("cesium.loop.last.iteration.timestamp.seconds").gauge());
    }

    @Test
    void transientPollFailureBacksOffWithoutFailingTheLoop() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.setPollException(new KafkaException("transient transport"));
        h.loop.runOnce();

        assertEquals(java.util.Set.of(P0), h.consumer.paused());
        assertEquals(0, h.producer().commitCount());
    }

    @Test
    void noOffsetForPartitionIsFailFastWithRunbook() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.setPollException(new NoOffsetForPartitionException(P0));

        IngestLoopFatalException failure = assertThrows(IngestLoopFatalException.class, h.loop::runOnce);
        assertTrue(failure.getMessage().contains("auto.offset.reset=none"), failure.getMessage());
    }

    @Test
    void rebalanceWhileParkedPausesNewlyAssignedPartitions() {
        Harness h = new Harness();
        h.startAssigned(P0);
        h.consumer.setPollException(new KafkaException("transient"));
        h.loop.runOnce(); // arms the backoff gate

        h.consumer.rebalance(List.of(P0, P1)); // listener runs; P1 is newly assigned mid-backoff
        assertTrue(h.consumer.paused().contains(P1), "listener keeps the park coherent for new partitions");
    }

    // ------------------------------------------------------------------ crash points & shutdown

    @Test
    void simulatedCrashBeforeSendOffsetsLeavesNothingCommitted() {
        Harness h = new Harness();
        CrashPoints.install(id -> {
            if (CrashPoints.INGEST_AFTER_SENDS.equals(id)) {
                throw new CrashPoints.SimulatedCrash(id);
            }
        });
        h.startAssigned(P0);
        h.consumer.addRecord(record(P0, 0, noHeaders()));

        assertThrows(CrashPoints.SimulatedCrash.class, h.loop::runOnce);
        assertFalse(h.events.contains("sendOffsets"), "I-2 crashed before the offsets were sent");
        assertFalse(h.events.contains("abort"), "a crash is not classified as an abortable failure");
        assertEquals(0, h.producer().commitCount());
        assertTrue(h.producer().history().isEmpty(), "uncommitted sends are invisible (§3.9 I-2)");
    }

    @Test
    void stopRequestedMidTransactionIsHonoredAtTheBoundary() {
        Harness h = new Harness();
        // Stop is requested between sendOffsets and commit (I-3): the commit must still complete —
        // shutdown is honored at transaction boundaries, never by abandoning an open transaction.
        CrashPoints.install(id -> {
            if (CrashPoints.INGEST_AFTER_SEND_OFFSETS.equals(id)) {
                h.loop.stop();
            }
        });
        h.consumer.schedulePollTask(() -> {
            h.consumer.rebalance(List.of(P0));
            h.consumer.seek(P0, 0);
            h.consumer.addRecord(record(P0, 0, noHeaders()));
        });

        h.loop.run(); // returns: the stop flag is observed at the loop's next boundary check

        assertEquals(1, h.producer().commitCount(), "the in-flight transaction committed before exit");
        assertEquals(1, h.producer().history().size());
        assertTrue(h.producer().closed(), "clean close on the way out");
        assertTrue(h.consumer.closed());
    }

    // ------------------------------------------------------------------ harness

    private static IngestLoopConfig defaultConfig() {
        return defaultConfig(UnrelayablePolicy.DLQ);
    }

    private static IngestLoopConfig defaultConfig(UnrelayablePolicy onUnrelayable) {
        return new IngestLoopConfig(
                SOURCE,
                TRACKER,
                2,
                Duration.ofSeconds(1),
                3,
                IngestLoopConfig.DEFAULT_PARK_THRESHOLD,
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                onUnrelayable);
    }

    private static ConsumerRecord<byte[], byte[]> record(TopicPartition tp, long offset, Headers headers) {
        return new ConsumerRecord<>(
                tp.topic(),
                tp.partition(),
                offset,
                SOURCE_TS,
                TimestampType.CREATE_TIME,
                KEY.length,
                VALUE.length,
                KEY,
                VALUE,
                headers,
                Optional.empty());
    }

    private static Headers noHeaders() {
        return new RecordHeaders();
    }

    private static Headers delayMs(String value) {
        Headers headers = new RecordHeaders();
        headers.add(CesiumHeaders.DELAY_MS, ascii(value));
        return headers;
    }

    private static Headers deliverAt(String value) {
        Headers headers = new RecordHeaders();
        headers.add(CesiumHeaders.DELIVER_AT, ascii(value));
        return headers;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static String headerValue(ProducerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertNotNull(header, "expected header " + key);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /** One loop with recording mocks; every collaborator wired exactly as production would. */
    private static final class Harness {
        final List<String> events = new ArrayList<>();
        final RecordingConsumer consumer = new RecordingConsumer(events);
        final List<RecordingProducer> producers = new ArrayList<>();
        final FakeTrackerStore store = new FakeTrackerStore();
        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        final MutableClock clock = new MutableClock(NOW);
        final IngestLoop loop;

        /** While positive, each factory-built producer's initTransactions fails once (transient). */
        int failProducerInits;

        Harness() {
            this(defaultConfig(), MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        }

        Harness(IngestLoopConfig config, MalformedHeaderPolicy onMalformed, OverMaxPolicy onOverMax) {
            Duration delayMax = Duration.ofMillis(DELAY_MAX_MS);
            loop = new IngestLoop(
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
                    new DelayHeaderCodec(delayMax, false),
                    new IngestPolicyEngine(delayMax, onMalformed, onOverMax),
                    new RelayRecordFactory(DEST, DLQ, true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY),
                    new IdentityBlob("test-cluster", TOPIC_ID),
                    registry,
                    clock);
        }

        void startAssigned(TopicPartition... partitions) {
            loop.start();
            consumer.rebalance(List.of(partitions));
            for (TopicPartition tp : partitions) {
                consumer.seek(tp, 0);
            }
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
    }

    /** MockConsumer that logs poll events so I3 (no poll inside a transaction) is assertable. */
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
    }

    /** MockProducer that logs transaction events and supports per-call fault injection. */
    private static final class RecordingProducer extends MockProducer<byte[], byte[]> {
        private final List<String> events;
        int failSendOffsetsTimes;
        ConsumerGroupMetadata offsetsGroupMetadata;

        /** FIFO of exceptions thrown by successive {@code commitTransaction} calls (then success). */
        final ArrayDeque<RuntimeException> commitExceptionQueue = new ArrayDeque<>();

        /** Zero-based send index whose callback completes exceptionally (the async-failure path). */
        int failSendCallbackAt = -1;

        RuntimeException sendCallbackError = new KafkaException("injected async send failure");
        RuntimeException initTransactionsException;
        List<PartitionInfo> partitionsForResult = List.of();
        private int sendIndex;

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
            if (sendIndex++ == failSendCallbackAt) {
                // The async-failure path: the I/O thread completes this send's callback with an
                // exception; the record never reaches the (mock) transaction buffer.
                callback.onCompletion(null, sendCallbackError);
                return java.util.concurrent.CompletableFuture.failedFuture(sendCallbackError);
            }
            return super.send(record, callback);
        }

        @Override
        public synchronized List<PartitionInfo> partitionsFor(String topic) {
            return partitionsForResult;
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
            super.commitTransaction();
        }

        @Override
        public void abortTransaction() {
            // MockProducer's own public abortTransactionException field injects the failure.
            events.add("abort");
            super.abortTransaction();
        }
    }

    /** Manually advanced clock: backoff gating is tested without real waiting. */
    private static final class MutableClock extends Clock {
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
