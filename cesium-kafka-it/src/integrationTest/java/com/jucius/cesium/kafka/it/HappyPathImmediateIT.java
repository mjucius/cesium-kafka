package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-1 (immediate half): records without control headers, with a past
 * {@code cesium-deliver-at}, and with {@code cesium-delay-ms: 0} all relay immediately;
 * key/value/headers are preserved byte-for-byte (control headers stripped, provenance stamped),
 * and a read_committed verifier sees each input exactly once.
 */
class HappyPathImmediateIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void immediateRecordsRelayExactlyOnceWithFullFidelity() {
        String source = createTopic("src", 2);
        String destination = createTopic("dst", 2);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long timestamp = System.currentTimeMillis();
        RecordMetadata plain;
        RecordMetadata pastDeliverAt;
        RecordMetadata zeroDelay;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            // No control headers at all, plus a custom header and a non-control cesium-* header
            // (the prefix names a namespace, not the strip set — both must survive).
            plain = produce(
                    producer,
                    source,
                    0,
                    timestamp,
                    "k-plain",
                    "v-plain",
                    h("x-custom", "alpha"),
                    h("cesium-passthrough", "keep-me"));
            // Past deliver-at: relays immediately, NOT an error (§2.3).
            pastDeliverAt = produce(
                    producer,
                    source,
                    1,
                    timestamp,
                    "k-past",
                    "v-past",
                    h(CesiumHeaders.DELIVER_AT, Long.toString(timestamp - 60_000)));
            // Zero delay: dispatchAt == record timestamp <= now, relays immediately.
            zeroDelay = produce(producer, source, 0, timestamp, "k-zero", "v-zero", h(CesiumHeaders.DELAY_MS, "0"));
            producer.flush();
        }

        harness.start();

        List<ConsumerRecord<byte[], byte[]>> relayed = readExactlyCommitted(destination, 3, WAIT);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(relayed);
        assertEquals(3, byKey.size(), "each input key exactly once");

        ConsumerRecord<byte[], byte[]> plainOut = byKey.get("k-plain");
        assertNotNull(plainOut);
        assertArrayEquals(bytes("v-plain"), plainOut.value(), "value preserved byte-for-byte");
        assertEquals("alpha", headerValue(plainOut.headers(), "x-custom"), "user header preserved");
        assertEquals(
                "keep-me",
                headerValue(plainOut.headers(), "cesium-passthrough"),
                "non-control cesium-* header is NOT stripped (namespace != strip set)");
        assertProvenance(plainOut, source, 0, plain.offset(), timestamp);

        ConsumerRecord<byte[], byte[]> pastOut = byKey.get("k-past");
        assertNotNull(pastOut);
        assertArrayEquals(bytes("v-past"), pastOut.value());
        assertNull(
                headerValue(pastOut.headers(), CesiumHeaders.DELIVER_AT),
                "control header cesium-deliver-at stripped on relay");
        assertProvenance(pastOut, source, 1, pastDeliverAt.offset(), timestamp);

        ConsumerRecord<byte[], byte[]> zeroOut = byKey.get("k-zero");
        assertNotNull(zeroOut);
        assertArrayEquals(bytes("v-zero"), zeroOut.value());
        assertNull(
                headerValue(zeroOut.headers(), CesiumHeaders.DELAY_MS),
                "control header cesium-delay-ms stripped on relay");
        assertProvenance(zeroOut, source, 0, zeroDelay.offset(), timestamp);

        // Nothing was scheduled or dead-lettered.
        assertEquals(0, logEndOffsetTotal(harness.trackerTopic()), "no tracker ADDs for immediate relays");
        assertEquals(0, logEndOffsetTotal(dlq), "no DLQ records on the happy path");

        harness.stop();
    }

    private static void assertProvenance(
            ConsumerRecord<byte[], byte[]> relayed,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            long sourceTimestamp) {
        assertEquals(sourceTopic, headerValue(relayed.headers(), CesiumHeaders.SOURCE_TOPIC));
        assertEquals(Integer.toString(sourcePartition), headerValue(relayed.headers(), CesiumHeaders.SOURCE_PARTITION));
        assertEquals(Long.toString(sourceOffset), headerValue(relayed.headers(), CesiumHeaders.SOURCE_OFFSET));
        assertEquals(Long.toString(sourceTimestamp), headerValue(relayed.headers(), CesiumHeaders.SOURCE_TIMESTAMP));
        String relayedAt = headerValue(relayed.headers(), CesiumHeaders.RELAYED_AT);
        assertNotNull(relayedAt, "cesium-relayed-at stamped");
        assertTrue(Long.parseLong(relayedAt) >= sourceTimestamp, "relayed-at is at or after the source timestamp");
        assertNull(
                headerValue(relayed.headers(), CesiumHeaders.SCHEDULED_FOR),
                "immediate relays carry no cesium-scheduled-for");
    }
}
