package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Cursor-v2 behavior end-to-end (design §3.5, §11.3-7): replay after a restart is bounded by the
 * committed cursor — traffic-since-last-commit plus the sidecar pins, never completion history
 * (the R1 scaling fix); a quiet partition's cursor advances through records-free idle
 * transactions; and a sidecar overflow (tiny {@code cursor.sidecar-max-bytes}) falls back to the
 * min-pending cut, with the post-restart replay applying already-settled tombstones as R2 no-ops
 * and still recovering exactly once.
 */
class CursorReplayIT extends KafkaIT {

    private static final int TRAFFIC = 150;
    private static final int PINS = 3;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void replayIsBoundedByTheCommittedCursorNotByHistory() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .idleCursorInterval(Duration.ofSeconds(1))
                .build();

        harness.start();
        long ts = System.currentTimeMillis();
        // Far enough out that the pre-restart observations cannot race the pins becoming due,
        // even on a slow CI box; the post-restart verifier waits past it.
        long pinsFor = ts + 45_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < TRAFFIC; i++) {
                produce(producer, source, 0, ts, "k-" + i, "v-" + i, h(CesiumHeaders.DELAY_MS, "1500"));
            }
            for (int i = 0; i < PINS; i++) {
                produce(
                        producer,
                        source,
                        0,
                        ts,
                        "k-pin-" + i,
                        "v-pin",
                        h(CesiumHeaders.DELIVER_AT, Long.toString(pinsFor)));
            }
            producer.flush();
        }

        // The high traffic completes...
        readExactlyCommitted(destination, TRAFFIC, WAIT);

        // ...and the committed cursor catches up to the tracker high watermark (the pins ride the
        // sidecar), so a successor's replay is ~zero records — NOT the ~450-record history of
        // ADDs + tombstones this partition accumulated. That catch-up IS the boundedness proof:
        // replay always starts at the committed cursor.
        String tracker = harness.trackerTopic();
        TopicPartition trackerTp = new TopicPartition(tracker, 0);
        await("dispatch cursor catches up to the tracker high watermark")
                .atMost(WAIT)
                .untilAsserted(() -> {
                    long highWatermark = logEndOffsetTotal(tracker);
                    OffsetAndMetadata committed =
                            committedOffsets(harness.dispatchGroupId()).get(trackerTp);
                    assertNotNull(committed, "no committed dispatch cursor yet");
                    assertEquals(
                            highWatermark,
                            committed.offset(),
                            "the all-encoded cursor tracks the live position (§3.5)");
                    SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(committed.metadata());
                    assertEquals(PINS, sidecar.entries().size(), "exactly the pins are pinned");
                });
        long historyLength = logEndOffsetTotal(tracker);
        assertTrue(
                historyLength > TRAFFIC * 2,
                () -> "test invariant: the tracker history (" + historyLength + ") dwarfs the replay window");

        // Restart: recovery seeds the pins from the sidecar and replays ~nothing.
        harness.stop();
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> all =
                readExactlyCommitted(destination, TRAFFIC + PINS, Duration.ofSeconds(90));
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(all); // every key exactly once
        for (int i = 0; i < PINS; i++) {
            ConsumerRecord<byte[], byte[]> pin = byKey.get("k-pin-" + i);
            assertNotNull(pin, "missing pinned entry k-pin-" + i);
            assertTrue(
                    pin.timestamp() >= pinsFor,
                    () -> "pin dispatched " + (pinsFor - pin.timestamp()) + " ms before its requested instant");
        }
        harness.stop();
    }

    @Test
    void idleCursorAdvancesOnAQuietPartitionWithoutDispatches() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .idleCursorInterval(Duration.ofSeconds(1))
                .build();

        harness.start();
        long ts = System.currentTimeMillis();
        long pinFor = ts + 300_000; // five minutes: nothing ever dispatches during this test
        RecordMetadata pin;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            pin = produce(
                    producer, source, 0, ts, "k-pin", "v-pin", h(CesiumHeaders.DELIVER_AT, Long.toString(pinFor)));
            producer.flush();
        }

        String tracker = harness.trackerTopic();
        readExactlyCommitted(tracker, 1, WAIT); // the ADD is durable; the entry is pending

        // The records-free idle transaction commits a cursor past the ADD, pinning the pending
        // entry in the sidecar — no dispatch, no destination record, no tracker append.
        TopicPartition trackerTp = new TopicPartition(tracker, 0);
        await("idle cursor advancement on a quiet partition").atMost(WAIT).untilAsserted(() -> {
            OffsetAndMetadata committed =
                    committedOffsets(harness.dispatchGroupId()).get(trackerTp);
            assertNotNull(committed, "no committed dispatch cursor yet");
            assertTrue(
                    committed.offset() > pin.offset(),
                    () -> "cursor " + committed.offset() + " must advance past the pending ADD at " + pin.offset());
            SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(committed.metadata());
            assertEquals(1, sidecar.entries().size(), "the pending entry rides the sidecar");
            assertEquals(pin.offset(), sidecar.entries().get(0).sourceOffset());
            assertEquals(pinFor, sidecar.entries().get(0).dispatchAtMs());
        });
        assertEquals(0, logEndOffsetTotal(destination), "nothing dispatched from the quiet partition");
        harness.stop();
    }

    @Test
    void sidecarOverflowFallsBackToMinPendingAndStillRecoversExactlyOnce() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .idleCursorInterval(Duration.ofSeconds(1))
                // The smallest legal budget: the identity header fits, only a handful of the 24
                // pins do — forcing the §3.5 min-pending overflow fallback.
                .storeProperty(KafkaTrackerStore.SIDECAR_MAX_BYTES_KEY, "128")
                .build();

        harness.start();
        long ts = System.currentTimeMillis();
        long pinsFor = ts + 30_000;
        int pins = 24;
        int traffic = 60;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            // Pins FIRST: their ADDs occupy the lowest tracker offsets, so the overflow cut (the
            // first non-encoded pin's ADD offset) provably pins the cursor below the later
            // traffic ADDs and tombstones.
            for (int i = 0; i < pins; i++) {
                produce(
                        producer,
                        source,
                        0,
                        ts,
                        "k-pin-" + i,
                        "v",
                        h(CesiumHeaders.DELIVER_AT, Long.toString(pinsFor)));
            }
            for (int i = 0; i < traffic; i++) {
                produce(producer, source, 0, ts, "k-" + i, "v", h(CesiumHeaders.DELAY_MS, "2000"));
            }
            producer.flush();
        }

        readExactlyCommitted(destination, traffic, WAIT);

        // Overflow: the committed cursor falls back to a pin's ADD offset near the log start —
        // far below the high watermark (which sits past the traffic tombstones).
        String tracker = harness.trackerTopic();
        TopicPartition trackerTp = new TopicPartition(tracker, 0);
        await("overflowed cursor pinned at the min-pending cut").atMost(WAIT).untilAsserted(() -> {
            OffsetAndMetadata committed =
                    committedOffsets(harness.dispatchGroupId()).get(trackerTp);
            assertNotNull(committed, "no committed dispatch cursor yet");
            assertTrue(
                    committed.offset() <= pins + 4,
                    () -> "cursor " + committed.offset() + " must sit at a pin ADD offset (min-pending fallback),"
                            + " not at the high watermark " + logEndOffsetTotal(tracker));
        });

        // Restart: replay covers the non-encoded pins, the traffic ADDs AND their tombstones —
        // the tombstones apply as R2 no-ops — converging to exactly the 24 pending pins.
        harness.stop();
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> all =
                readExactlyCommitted(destination, traffic + pins, Duration.ofSeconds(90));
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(all); // exactly once, no replays
        for (int i = 0; i < pins; i++) {
            ConsumerRecord<byte[], byte[]> record = byKey.get("k-pin-" + i);
            assertNotNull(record, "missing pinned entry k-pin-" + i);
            assertTrue(
                    record.timestamp() >= pinsFor,
                    () -> "pin dispatched before its requested instant: " + record.timestamp() + " < " + pinsFor);
        }
        harness.stop();
    }
}
