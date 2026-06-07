package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.testing.CrashPoints;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §3.9 ingest crash points I-1…I-4 (§11.3-2): kill the loop at the named point, restart the
 * harness (same transactional.id, same static-membership id), and assert a read_committed
 * verifier sees each input EXACTLY once. For I-2/I-3 the read_uncommitted counter additionally
 * observes the aborted duplicates in the log — proving the crash aborted a real in-flight
 * transaction rather than nothing having happened. I-4 (commit returned, outcome locally unknown)
 * restarts into committed offsets and reprocesses nothing.
 */
class IngestCrashPointsIT extends KafkaIT {

    private static final int INPUTS = 5;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        CrashPoints.reset();
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void i1CrashBeforeBeginLeavesNothingInTheLog() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.INGEST_BEFORE_BEGIN);
        assertEquals(
                INPUTS,
                uncommitted,
                "I-1 fires before beginTransaction: no aborted duplicates can exist in the destination log");
    }

    @Test
    void i2CrashAfterSendsAbortsTheInFlightTransaction() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.INGEST_AFTER_SENDS);
        assertTrue(
                uncommitted > INPUTS,
                () -> "I-2 left an in-flight transaction whose aborted relays must be visible to read_uncommitted;"
                        + " saw " + uncommitted + " total records for " + INPUTS + " committed");
    }

    @Test
    void i3CrashAfterSendOffsetsBeforeCommitAbortsTheInFlightTransaction() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.INGEST_AFTER_SEND_OFFSETS);
        assertTrue(
                uncommitted > INPUTS,
                () -> "I-3 left an accepted-but-uncommitted transaction whose aborted relays must be visible to"
                        + " read_uncommitted; saw " + uncommitted + " total records for " + INPUTS + " committed");
    }

    @Test
    void i4CrashAfterCommitReturnedReprocessesNothing() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.INGEST_DURING_COMMIT);
        assertEquals(
                INPUTS,
                uncommitted,
                "I-4's transaction committed before the crash: the only destination records are the committed"
                        + " copies — the restart resumed from committed offsets and reprocessed nothing");
    }

    /**
     * The shared scenario: produce {@value #INPUTS} keyed records, crash the loop at
     * {@code crashPoint} on its first batch, restart, and verify exactly-once delivery under
     * read_committed. Returns the read_uncommitted total for the caller's abort-evidence check.
     */
    private int crashAndVerifyExactlyOnce(String crashPoint) {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long timestamp = System.currentTimeMillis();
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < INPUTS; i++) {
                produce(producer, source, 0, timestamp, "k" + i, "v" + i);
            }
            producer.flush();
        }

        EngineHarness.installCrashOnce(crashPoint);
        harness.start();
        Throwable death = harness.awaitDeath(WAIT);
        assertInstanceOf(
                CrashPoints.SimulatedCrash.class,
                death,
                () -> "the loop must die at " + crashPoint + " but died of: " + death);

        // Restart the "process": same application id, same instanceId => same transactional.id
        // (initTransactions fences/resolves the dangling transaction) and same group.instance.id.
        CrashPoints.reset();
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> committed = readExactlyCommitted(destination, INPUTS, WAIT);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(committed);
        for (int i = 0; i < INPUTS; i++) {
            String key = "k" + i;
            assertTrue(byKey.containsKey(key), () -> "missing input " + key);
        }

        // Offsets land exactly past the batch — committed atomically with the relays.
        awaitCommittedOffset(harness.ingestGroupId(), new TopicPartition(source, 0), INPUTS);

        harness.stop();
        return countReadUncommitted(destination);
    }
}
