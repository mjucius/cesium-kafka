package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §3.9 dispatch crash points D-1…D-3 (§11.3-2): kill the dispatch loop at the named point
 * of its first dispatch transaction, restart the harness (same transactional.id, same
 * static-membership id), and assert a read_committed verifier sees each delayed input EXACTLY
 * once, at-or-after its requested instant — never early, never lost, never duplicated. For D-2
 * the read_uncommitted counter additionally observes the aborted relays in the destination log,
 * proving the crash interrupted a real in-flight transaction. D-3 (commit returned, outcome
 * locally unrecorded) restarts into committed tombstones and re-dispatches nothing.
 */
class DispatchCrashPointsIT extends KafkaIT {

    private static final int INPUTS = 4;
    private static final long DELAY_MS = 3_000;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        CrashPoints.reset();
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void d1CrashBeforeBeginLeavesNothingInTheDestinationLog() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.DISPATCH_BEFORE_BEGIN);
        assertEquals(
                INPUTS,
                uncommitted,
                "D-1 fires before beginTransaction: no aborted relays can exist in the destination log");
    }

    @Test
    void d2CrashAfterSendsAbortsTheInFlightDispatchTransaction() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.DISPATCH_AFTER_SENDS);
        assertTrue(
                uncommitted > INPUTS,
                () -> "D-2 left an in-flight transaction whose aborted relays must be visible to read_uncommitted;"
                        + " saw " + uncommitted + " total records for " + INPUTS + " committed");
    }

    @Test
    void d3CrashAfterCommitReturnedRedispatchesNothing() {
        int uncommitted = crashAndVerifyExactlyOnce(CrashPoints.DISPATCH_DURING_COMMIT);
        assertEquals(
                INPUTS,
                uncommitted,
                "D-3's transaction committed before the crash: the restart replays the committed tombstones,"
                        + " finds nothing pending, and re-dispatches nothing — the only destination records are"
                        + " the committed copies");
    }

    /**
     * The shared scenario: schedule {@value #INPUTS} delayed records, crash the dispatch loop at
     * {@code crashPoint} during its first dispatch transaction, restart, and verify exactly-once
     * on-time-or-late delivery under read_committed plus the durable ADD + COMPLETE lifecycle.
     * Returns the destination's read_uncommitted total for the caller's abort-evidence check.
     */
    private int crashAndVerifyExactlyOnce(String crashPoint) {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // Default (30 s) idle cadence: the crash point must fire on the DISPATCH
                // transaction's commit, not on an earlier records-free idle advancement.
                .build();

        long ts = System.currentTimeMillis();
        long scheduledFor = ts + DELAY_MS;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < INPUTS; i++) {
                produce(producer, source, 0, ts, "k" + i, "v" + i, h(CesiumHeaders.DELAY_MS, Long.toString(DELAY_MS)));
            }
            producer.flush();
        }

        EngineHarness.installCrashOnce(crashPoint);
        harness.start();
        Throwable death = harness.awaitDispatchDeath(WAIT);
        assertInstanceOf(
                CrashPoints.SimulatedCrash.class,
                death,
                () -> "the dispatch loop must die at " + crashPoint + " but died of: " + death);

        // Restart the "process": stop the surviving ingest loop, drop the in-memory store, and
        // start fresh — same transactional.ids (initTransactions resolves the dangling dispatch
        // transaction) and same static-membership ids.
        CrashPoints.reset();
        harness.stop();
        harness.start();

        // RETRY_GRACE: after the crash+restart the loop re-dispatches; a wider quiet window than
        // the default 1 s ensures a duplicate from a delayed penalty/retry re-dispatch is observed
        // rather than slipping past the verifier once INPUTS committed records first appear.
        List<ConsumerRecord<byte[], byte[]>> committed = readExactlyCommitted(destination, INPUTS, WAIT, RETRY_GRACE);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(committed); // exactly once per key
        for (int i = 0; i < INPUTS; i++) {
            ConsumerRecord<byte[], byte[]> record = byKey.get("k" + i);
            assertNotNull(record, "missing input k" + i);
            assertTrue(
                    record.timestamp() >= scheduledFor,
                    () -> "dispatched before the requested instant by " + (scheduledFor - record.timestamp()) + " ms");
        }

        // The durable lifecycle settled exactly once: one ADD + one tombstone per input.
        readExactlyCommitted(harness.trackerTopic(), INPUTS * 2, WAIT);

        harness.stop();
        return countReadUncommitted(destination);
    }
}
