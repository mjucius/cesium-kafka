package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.store.tracker.TrackerWireFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The first full-system proof (design §11.3-1, delayed delivery end-to-end): ingest loop +
 * KafkaTrackerStore + dispatch loop + seek fetcher in one harness. {@code cesium-delay-ms} and
 * {@code cesium-deliver-at} records arrive on the destination AT-OR-AFTER their requested instant
 * (never before; bounded lateness), exactly once under read_committed, payload/key/headers
 * byte-for-byte (control headers stripped), provenance + {@code cesium-scheduled-for} stamped, the
 * CLAMP case arrives with {@code cesium-clamped: true}, the tracker shows ADD then COMPLETE
 * tombstone per scheduled entry, and immediate traffic interleaves with the delayed entries.
 */
class DelayedDeliveryIT extends KafkaIT {

    /** CI-tolerant lateness bound: dispatch must land at-or-after, and within this much after. */
    private static final long LATENESS_BOUND_MS = 15_000;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void delayedRecordsArriveOnTimeExactlyOnceWithFullFidelity() {
        String source = createTopic("src", 2);
        String destination = createTopic("dst", 2);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(Duration.ofSeconds(8))
                .onOverMax(OverMaxPolicy.CLAMP)
                .build();

        // Start FIRST: the immediate-before-delayed interleaving assertion must not race a slow
        // harness startup — with the pipeline hot, ingest relays within ~a second of the produce.
        harness.start();
        String tracker = harness.trackerTopic();

        long ts = System.currentTimeMillis();
        long delayedFor = ts + 5_000;
        long deliverAt = ts + 7_000;
        RecordMetadata delayed;
        RecordMetadata absolute;
        RecordMetadata clamped;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, source, 0, ts, "k-now", "v-now", h("x-app", "now-42"));
            delayed = produce(
                    producer,
                    source,
                    0,
                    ts,
                    "k-delay",
                    "v-delay",
                    h("x-app", "delay-42"),
                    h(CesiumHeaders.DELAY_MS, "5000"));
            absolute = produce(
                    producer, source, 1, ts, "k-at", "v-at", h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
            // One hour >> delay.max=8s and on-over-max: CLAMP => scheduled at ingestNow + 8s.
            clamped = produce(producer, source, 1, ts, "k-clamp", "v-clamp", h(CesiumHeaders.DELAY_MS, "3600000"));
            producer.flush();
        }

        List<ConsumerRecord<byte[], byte[]>> out = readExactlyCommitted(destination, 4, WAIT);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(out);

        // Immediate traffic interleaves: relayed at ingest, long before any delayed entry.
        ConsumerRecord<byte[], byte[]> now = byKey.get("k-now");
        assertNotNull(now, "immediate record missing");
        assertArrayEquals(bytes("v-now"), now.value());
        assertEquals("now-42", headerValue(now.headers(), "x-app"));
        assertNull(headerValue(now.headers(), CesiumHeaders.SCHEDULED_FOR), "immediate relays carry no schedule");
        assertTrue(now.timestamp() < delayedFor, "the immediate relay must land before any delayed dispatch");

        ConsumerRecord<byte[], byte[]> delay = byKey.get("k-delay");
        assertNotNull(delay, "delay-ms record missing");
        assertDelayedFidelity(delay, "v-delay", source, 0, delayed.offset(), ts, delayedFor);
        assertEquals("delay-42", headerValue(delay.headers(), "x-app"), "producer headers preserved byte-for-byte");

        ConsumerRecord<byte[], byte[]> at = byKey.get("k-at");
        assertNotNull(at, "deliver-at record missing");
        assertDelayedFidelity(at, "v-at", source, 1, absolute.offset(), ts, deliverAt);

        ConsumerRecord<byte[], byte[]> clamp = byKey.get("k-clamp");
        assertNotNull(clamp, "clamped record missing");
        assertEquals("true", headerValue(clamp.headers(), CesiumHeaders.CLAMPED), "CLAMP contract stamp (§2.3)");
        long clampScheduledFor = Long.parseLong(headerValue(clamp.headers(), CesiumHeaders.SCHEDULED_FOR));
        assertTrue(
                clampScheduledFor >= ts + 8_000 && clampScheduledFor <= ts + 8_000 + WAIT.toMillis(),
                () -> "CLAMP pins to ingestNow + delay.max, got " + clampScheduledFor + " for produce time " + ts);
        assertOnTime(clamp, clampScheduledFor);
        assertTrue(
                clamp.timestamp() < ts + 3_600_000,
                "the clamped record must arrive at the clamped instant, not the requested hour");
        assertArrayEquals(bytes("v-clamp"), clamp.value());
        assertNull(headerValue(clamp.headers(), CesiumHeaders.DELAY_MS), "control header stripped");

        // Tracker lifecycle: ADD then COMPLETE tombstone for each of the three scheduled entries.
        List<ConsumerRecord<byte[], byte[]>> trackerLog = readExactlyCommitted(tracker, 6, WAIT);
        assertAddThenComplete(trackerLog, 0, delayed.offset());
        assertAddThenComplete(trackerLog, 1, absolute.offset());
        assertAddThenComplete(trackerLog, 1, clamped.offset());

        assertEquals(0, logEndOffsetTotal(dlq), "nothing dead-lettered on the happy path");
        harness.stop();
    }

    /** Relay fidelity + provenance + timing for one scheduled entry (§2.4 contracts). */
    private static void assertDelayedFidelity(
            ConsumerRecord<byte[], byte[]> record,
            String expectedValue,
            String source,
            int sourcePartition,
            long sourceOffset,
            long sourceTimestamp,
            long scheduledFor) {
        assertArrayEquals(bytes(expectedValue), record.value(), "payload bytes preserved");
        assertNull(headerValue(record.headers(), CesiumHeaders.DELAY_MS), "cesium-delay-ms stripped");
        assertNull(headerValue(record.headers(), CesiumHeaders.DELIVER_AT), "cesium-deliver-at stripped");
        assertNull(headerValue(record.headers(), CesiumHeaders.CLAMPED), "in-range schedules are not clamped");
        assertEquals(
                Long.toString(scheduledFor),
                headerValue(record.headers(), CesiumHeaders.SCHEDULED_FOR),
                "cesium-scheduled-for carries the requested instant");
        assertEquals(source, headerValue(record.headers(), CesiumHeaders.SOURCE_TOPIC));
        assertEquals(Integer.toString(sourcePartition), headerValue(record.headers(), CesiumHeaders.SOURCE_PARTITION));
        assertEquals(Long.toString(sourceOffset), headerValue(record.headers(), CesiumHeaders.SOURCE_OFFSET));
        assertEquals(Long.toString(sourceTimestamp), headerValue(record.headers(), CesiumHeaders.SOURCE_TIMESTAMP));
        assertNotNull(headerValue(record.headers(), CesiumHeaders.RELAYED_AT), "provenance stamped");
        assertOnTime(record, scheduledFor);
    }

    /** The headline precision SLO: at-or-after the schedule, never before; bounded lateness. */
    private static void assertOnTime(ConsumerRecord<byte[], byte[]> record, long scheduledFor) {
        // relay.timestamp policy DISPATCH (default): record timestamp == the dispatch instant.
        assertTrue(
                record.timestamp() >= scheduledFor,
                () -> "dispatched " + (scheduledFor - record.timestamp()) + " ms BEFORE the requested instant");
        assertTrue(
                record.timestamp() <= scheduledFor + LATENESS_BOUND_MS,
                () -> "dispatched " + (record.timestamp() - scheduledFor) + " ms late (bound " + LATENESS_BOUND_MS
                        + " ms)");
    }

    /**
     * Asserts the tracker carries exactly one ADD and one later DISPATCHED tombstone for the
     * {@code (partition, sourceOffset)} entry — the durable schedule lifecycle (§2.2).
     */
    private static void assertAddThenComplete(
            List<ConsumerRecord<byte[], byte[]>> trackerLog, int partition, long sourceOffset) {
        byte[] key = TrackerWireFormat.encodeKey(sourceOffset);
        List<ConsumerRecord<byte[], byte[]>> entry = trackerLog.stream()
                .filter(r -> r.partition() == partition && Arrays.equals(key, r.key()))
                .toList();
        assertEquals(
                2,
                entry.size(),
                () -> "expected ADD + COMPLETE for partition " + partition + " sourceOffset " + sourceOffset + ", saw "
                        + entry.size());
        ConsumerRecord<byte[], byte[]> add = entry.get(0);
        ConsumerRecord<byte[], byte[]> complete = entry.get(1);
        assertNotNull(add.value(), "first record is the ADD");
        assertNull(complete.value(), "second record is the tombstone");
        assertTrue(add.offset() < complete.offset(), "ADD precedes its tombstone");
        assertEquals(
                CompletionReason.DISPATCHED.name(),
                headerValue(complete.headers(), TrackerWireFormat.COMPLETION_REASON_HEADER),
                "the tombstone carries the DISPATCHED reason header (D15)");
    }
}
