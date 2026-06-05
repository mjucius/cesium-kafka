package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import events.cesium.kafka.core.admin.IdentityBlob;
import events.cesium.kafka.store.tracker.TrackerDecodeResult;
import events.cesium.kafka.store.tracker.TrackerWireFormat;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-1 (delayed half): records with {@code cesium-delay-ms} / {@code cesium-deliver-at}
 * produce NO destination record but a tracker ADD on the SAME partition — key = big-endian source
 * offset, value = the exact 12-byte v1 wire format with the requested {@code dispatchAtMs} —
 * and the group-A source offsets advance transactionally, carrying the §3.1 identity blob.
 */
class HappyPathDelayedIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void delayedRecordsBecomeTrackerAddsOnTheSamePartition() {
        String source = createTopic("src", 3);
        String destination = createTopic("dst", 3);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long timestamp = System.currentTimeMillis();
        long delayMs = 300_000; // 5 minutes — safely in the future for the whole test
        long deliverAtMs = timestamp + 600_000;
        RecordMetadata delayed;
        RecordMetadata absolute;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            // cesium-delay-ms is relative to the source record timestamp (D2) — explicit timestamp
            // makes the expected dispatch instant exact: timestamp + delayMs.
            delayed = produce(
                    producer,
                    source,
                    2,
                    timestamp,
                    "k-delayed",
                    "v-delayed",
                    h(CesiumHeaders.DELAY_MS, Long.toString(delayMs)));
            absolute = produce(
                    producer,
                    source,
                    1,
                    timestamp,
                    "k-absolute",
                    "v-absolute",
                    h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAtMs)));
            producer.flush();
        }

        harness.start();
        String tracker = harness.trackerTopic();

        List<ConsumerRecord<byte[], byte[]>> adds = readExactlyCommitted(tracker, 2, WAIT);

        ConsumerRecord<byte[], byte[]> delayedAdd = onPartition(adds, 2);
        assertArrayEquals(
                TrackerWireFormat.encodeKey(delayed.offset()),
                delayedAdd.key(),
                "tracker key is the big-endian source offset");
        assertArrayEquals(
                TrackerWireFormat.encodeAddValue(timestamp + delayMs, false),
                delayedAdd.value(),
                "tracker ADD value is the exact 12-byte v1 encoding of timestamp + delay");
        TrackerDecodeResult decoded =
                TrackerWireFormat.decode(delayedAdd.key(), delayedAdd.value(), delayedAdd.headers());
        TrackerDecodeResult.Add add = assertInstanceOf(TrackerDecodeResult.Add.class, decoded);
        assertEquals(timestamp + delayMs, add.dispatchAtMs());
        assertFalse(add.clamped(), "an in-range delay is not clamped");

        ConsumerRecord<byte[], byte[]> absoluteAdd = onPartition(adds, 1);
        assertArrayEquals(TrackerWireFormat.encodeKey(absolute.offset()), absoluteAdd.key());
        assertArrayEquals(
                TrackerWireFormat.encodeAddValue(deliverAtMs, false),
                absoluteAdd.value(),
                "deliver-at ADD carries the requested absolute instant");

        // No destination record was produced for either scheduled input.
        assertEquals(0, logEndOffsetTotal(destination), "scheduled records must not relay at ingest");
        assertEquals(0, logEndOffsetTotal(dlq));

        // Source offsets advanced transactionally (visible only because the txn committed) and
        // carry the identity blob binding them to the cluster + source topic id (§3.1).
        OffsetAndMetadata committedP2 =
                awaitCommittedOffset(harness.ingestGroupId(), new TopicPartition(source, 2), delayed.offset() + 1);
        OffsetAndMetadata committedP1 =
                awaitCommittedOffset(harness.ingestGroupId(), new TopicPartition(source, 1), absolute.offset() + 1);
        for (OffsetAndMetadata committed : List.of(committedP2, committedP1)) {
            assertNotNull(committed.metadata());
            IdentityBlob blob = IdentityBlob.decode(committed.metadata());
            assertTrue(
                    blob.matches(harness.identity()),
                    () -> "offset metadata identity mismatch: " + blob.describeMismatch(harness.identity()));
        }

        harness.stop();
    }

    private static ConsumerRecord<byte[], byte[]> onPartition(List<ConsumerRecord<byte[], byte[]>> records, int p) {
        List<ConsumerRecord<byte[], byte[]>> matches =
                records.stream().filter(r -> r.partition() == p).toList();
        assertEquals(1, matches.size(), () -> "expected exactly one tracker record on partition " + p);
        return matches.get(0);
    }
}
