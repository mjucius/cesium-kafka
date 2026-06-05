package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import events.cesium.kafka.core.headers.DlqReasons;
import events.cesium.kafka.core.policy.OverMaxPolicy;
import events.cesium.kafka.store.tracker.TrackerDecodeResult;
import events.cesium.kafka.store.tracker.TrackerWireFormat;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-9 (header-policy subset): a malformed control header dead-letters per the §2.4 DLQ
 * contract; an over-max delay under {@code CLAMP} becomes a tracker ADD with the clamped wire flag
 * at {@code now + delay.max}; a header conflict resolves by precedence ({@code deliver-at} wins)
 * and increments {@code cesium_header_errors_total{type="conflict"}}.
 */
class HeaderPolicyIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void malformedHeaderProducesDlqRecordMatchingTheContract() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long timestamp = System.currentTimeMillis();
        RecordMetadata bad;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            bad = produce(
                    producer,
                    source,
                    0,
                    timestamp,
                    "k-bad",
                    "v-bad",
                    h(CesiumHeaders.DELAY_MS, "not-a-number"),
                    h("x-custom", "alpha"));
            producer.flush();
        }
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readExactlyCommitted(dlq, 1, WAIT);
        ConsumerRecord<byte[], byte[]> record = dlqRecords.get(0);

        // §2.4 contract: original key/value and ALL headers, including the offending control header.
        assertArrayEquals(bytes("k-bad"), record.key());
        assertArrayEquals(bytes("v-bad"), record.value());
        assertEquals(
                "not-a-number",
                headerValue(record.headers(), CesiumHeaders.DELAY_MS),
                "the offending control header is preserved on the DLQ record — that is the point");
        assertEquals("alpha", headerValue(record.headers(), "x-custom"));

        // Error reason/detail + provenance (always stamped on DLQ records).
        assertEquals(DlqReasons.MALFORMED_HEADER, headerValue(record.headers(), CesiumHeaders.ERROR_REASON));
        String detail = headerValue(record.headers(), CesiumHeaders.ERROR_DETAIL);
        assertNotNull(detail);
        assertTrue(detail.contains(CesiumHeaders.DELAY_MS), "detail names the offending header: " + detail);
        assertEquals(source, headerValue(record.headers(), CesiumHeaders.SOURCE_TOPIC));
        assertEquals("0", headerValue(record.headers(), CesiumHeaders.SOURCE_PARTITION));
        assertEquals(Long.toString(bad.offset()), headerValue(record.headers(), CesiumHeaders.SOURCE_OFFSET));
        assertEquals(Long.toString(timestamp), headerValue(record.headers(), CesiumHeaders.SOURCE_TIMESTAMP));

        // The record went to the DLQ only: no relay, no schedule.
        assertEquals(0, logEndOffsetTotal(destination));
        assertEquals(0, logEndOffsetTotal(harness.trackerTopic()));
        assertEquals(
                1.0,
                harness.meterRegistry()
                        .get("cesium.header.errors")
                        .tag("type", "malformed")
                        .counter()
                        .count());

        harness.stop();
    }

    @Test
    void overMaxWithClampPolicySchedulesClampedAddAtNowPlusDelayMax() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        Duration delayMax = Duration.ofHours(1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(delayMax)
                .onOverMax(OverMaxPolicy.CLAMP)
                .build();

        long beforeProduceMs = System.currentTimeMillis();
        RecordMetadata over;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            // Twice the maximum: an over-max violation whatever the ingest clock reads.
            over = produce(
                    producer,
                    source,
                    0,
                    beforeProduceMs,
                    "k-over",
                    "v-over",
                    h(CesiumHeaders.DELAY_MS, Long.toString(delayMax.toMillis() * 2)));
            producer.flush();
        }
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> adds = readExactlyCommitted(harness.trackerTopic(), 1, WAIT);
        long afterObservedMs = System.currentTimeMillis();
        ConsumerRecord<byte[], byte[]> add = adds.get(0);

        assertArrayEquals(TrackerWireFormat.encodeKey(over.offset()), add.key());
        assertEquals(
                TrackerWireFormat.FLAG_CLAMPED,
                add.value()[3] & TrackerWireFormat.FLAG_CLAMPED,
                "wire flags bit 0 carries the CLAMP marker");
        TrackerDecodeResult.Add decoded = assertInstanceOf(
                TrackerDecodeResult.Add.class, TrackerWireFormat.decode(add.key(), add.value(), add.headers()));
        assertTrue(decoded.clamped(), "decoded ADD is clamped");
        long min = beforeProduceMs + delayMax.toMillis();
        long max = afterObservedMs + delayMax.toMillis();
        assertTrue(
                decoded.dispatchAtMs() >= min && decoded.dispatchAtMs() <= max,
                () -> "clamp target " + decoded.dispatchAtMs() + " outside [ingest-now + delay.max] window [" + min
                        + ", " + max + "]");

        // CLAMP schedules — it never relays early and never dead-letters.
        assertEquals(0, logEndOffsetTotal(destination));
        assertEquals(0, logEndOffsetTotal(dlq));
        assertEquals(
                1.0,
                harness.meterRegistry()
                        .get("cesium.header.errors")
                        .tag("type", "over_max")
                        .counter()
                        .count());
        assertEquals(
                1.0,
                harness.meterRegistry()
                        .get("cesium.ingest.records")
                        .tag("outcome", "clamped")
                        .counter()
                        .count());

        harness.stop();
    }

    @Test
    void conflictingHeadersResolveByPrecedenceAndIncrementTheConflictMetric() {
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
            // Both control headers: deliver-at (past => immediate relay) MUST win over the
            // 10-minute delay-ms — a record someone pinned to an instant relays at that instant.
            produce(
                    producer,
                    source,
                    0,
                    timestamp,
                    "k-conflict",
                    "v-conflict",
                    h(CesiumHeaders.DELIVER_AT, Long.toString(timestamp - 1_000)),
                    h(CesiumHeaders.DELAY_MS, "600000"));
            producer.flush();
        }
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> relayed = readExactlyCommitted(destination, 1, WAIT);
        ConsumerRecord<byte[], byte[]> record = relayed.get(0);
        assertArrayEquals(bytes("v-conflict"), record.value());
        assertNull(headerValue(record.headers(), CesiumHeaders.DELIVER_AT), "control headers stripped");
        assertNull(headerValue(record.headers(), CesiumHeaders.DELAY_MS), "control headers stripped");

        // deliver-at won: nothing was scheduled from the losing delay-ms.
        assertEquals(0, logEndOffsetTotal(harness.trackerTopic()));
        assertEquals(
                1.0,
                harness.meterRegistry()
                        .get("cesium.header.errors")
                        .tag("type", "conflict")
                        .counter()
                        .count(),
                "cesium_header_errors_total{type=conflict} incremented exactly once");

        harness.stop();
    }
}
