package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/** Proves {@link DeliveryInvariants} can fail: synthetic drains, no broker. */
class DeliveryInvariantsTest {

    private static final Map<String, Long> REQUESTED = Map.of("k-0", 1_000L, "k-1", 2_000L);

    @Test
    void cleanDrainPassesAndReportsWorstLateness() {
        long maxLate =
                DeliveryInvariants.check(List.of(delivered("k-0", 1_000), delivered("k-1", 2_500)), 2, REQUESTED::get);
        assertEquals(500, maxLate);
    }

    @Test
    void earlinessWithinTheSkewToleranceIsAllowed() {
        DeliveryInvariants.check(
                List.of(delivered("k-0", 1_000 - DeliveryInvariants.SKEW_TOLERANCE_MS), delivered("k-1", 2_000)),
                2,
                REQUESTED::get);
    }

    @Test
    void duplicateKeyFails() {
        assertFails(
                "delivered more than once",
                List.of(delivered("k-0", 1_000), delivered("k-1", 2_000), delivered("k-0", 1_000)));
    }

    @Test
    void missingKeyFails() {
        assertFails("expected 2 distinct keys, saw 1", List.of(delivered("k-0", 1_000)));
    }

    @Test
    void unknownKeyFails() {
        assertFails("never produced", List.of(delivered("k-0", 1_000), delivered("k-9", 2_000)));
    }

    @Test
    void earlyRecordFails() {
        assertFails(
                "worst early by 151ms",
                List.of(delivered("k-0", 1_000), delivered("k-1", 2_000 - DeliveryInvariants.SKEW_TOLERANCE_MS - 1)));
    }

    private static void assertFails(String expectedMessage, List<ConsumerRecord<byte[], byte[]>> delivered) {
        AssertionError failure =
                assertThrows(AssertionError.class, () -> DeliveryInvariants.check(delivered, 2, REQUESTED::get));
        assertTrue(failure.getMessage().contains(expectedMessage), failure::getMessage);
    }

    private static ConsumerRecord<byte[], byte[]> delivered(String key, long timestamp) {
        return new ConsumerRecord<>(
                "dst",
                0,
                0L,
                timestamp,
                TimestampType.CREATE_TIME,
                0,
                0,
                key.getBytes(StandardCharsets.UTF_8),
                new byte[0],
                new RecordHeaders(),
                Optional.empty());
    }
}
