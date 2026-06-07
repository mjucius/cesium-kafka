package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import com.jucius.cesium.kafka.store.tracker.TrackerDecodeResult;
import com.jucius.cesium.kafka.store.tracker.TrackerWireFormat;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §3.1 atomicity (§11.3-1/-2 composite): one poll batch mixing an immediate relay, a
 * delayed schedule, and a DLQ outcome commits as ONE transaction across three topics. Killed at
 * I-2, NONE of the three outputs is visible to read_committed (the dangling transaction gates the
 * LSO); after restart each output appears exactly once, and the read_uncommitted view shows the
 * aborted first attempt in the logs.
 */
class TransactionalityIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        CrashPoints.reset();
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void mixedBatchCommitsAtomicallyAcrossAllThreeOutputTopics() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        String tracker = harness.trackerTopic();

        long timestamp = System.currentTimeMillis();
        long delayMs = 300_000;
        RecordMetadata delayed;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            // One partition, produced before the loop starts: one poll batch, one transaction.
            produce(producer, source, 0, timestamp, "k-immediate", "v-immediate");
            delayed = produce(
                    producer,
                    source,
                    0,
                    timestamp,
                    "k-delayed",
                    "v-delayed",
                    h(CesiumHeaders.DELAY_MS, Long.toString(delayMs)));
            produce(producer, source, 0, timestamp, "k-bad", "v-bad", h(CesiumHeaders.DELAY_MS, "garbage"));
            producer.flush();
        }

        EngineHarness.installCrashOnce(CrashPoints.INGEST_AFTER_SENDS);
        harness.start();
        Throwable death = harness.awaitDeath(WAIT);
        assertInstanceOf(CrashPoints.SimulatedCrash.class, death);

        // The transaction is dangling: NONE of the three outputs is visible to read_committed —
        // not the relay, not the ADD, not the DLQ record. (Bounded negative observation window.)
        assertNoCommittedRecords(destination, Duration.ofSeconds(2));
        assertNoCommittedRecords(tracker, Duration.ofSeconds(2));
        assertNoCommittedRecords(dlq, Duration.ofSeconds(2));

        // Restart: initTransactions aborts the dangling transaction, the batch reprocesses, and
        // all three outputs commit atomically — each exactly once.
        CrashPoints.reset();
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> relayed = readExactlyCommitted(destination, 1, WAIT);
        assertArrayEquals(bytes("k-immediate"), relayed.get(0).key());
        assertArrayEquals(bytes("v-immediate"), relayed.get(0).value());

        List<ConsumerRecord<byte[], byte[]>> adds = readExactlyCommitted(tracker, 1, WAIT);
        assertArrayEquals(
                TrackerWireFormat.encodeKey(delayed.offset()), adds.get(0).key());
        TrackerDecodeResult.Add add = assertInstanceOf(
                TrackerDecodeResult.Add.class,
                TrackerWireFormat.decode(
                        adds.get(0).key(), adds.get(0).value(), adds.get(0).headers()));
        assertEquals(timestamp + delayMs, add.dispatchAtMs());

        List<ConsumerRecord<byte[], byte[]>> dead = readExactlyCommitted(dlq, 1, WAIT);
        assertArrayEquals(bytes("k-bad"), dead.get(0).key());
        assertEquals(DlqReasons.MALFORMED_HEADER, headerValue(dead.get(0).headers(), CesiumHeaders.ERROR_REASON));

        harness.stop();

        // The aborted first attempt is real: the logs contain more records than the committed
        // copies (>= one output of the crashed transaction plus the three committed outputs).
        int uncommittedTotal =
                countReadUncommitted(destination) + countReadUncommitted(tracker) + countReadUncommitted(dlq);
        assertTrue(
                uncommittedTotal > 3,
                () -> "expected aborted-output evidence beyond the 3 committed records, saw " + uncommittedTotal);
    }
}
