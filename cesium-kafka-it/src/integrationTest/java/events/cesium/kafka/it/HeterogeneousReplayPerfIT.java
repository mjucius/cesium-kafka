package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.store.tracker.SidecarCodec;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Macro perf — <strong>cursor-v2 bounded-replay scaling proof</strong> (design §11.4 / §3.5 / §5.5 /
 * R1): a large completed volume plus a handful of long-delay pins, then a dispatcher restart whose
 * replay consumes only <em>traffic-since-the-last-commit</em> (the published formula) — NOT the full
 * ADD+tombstone history. The actual replay record count is measured from the broker (tracker high
 * watermark at restart minus the committed cursor) and reported.
 *
 * <p>Topology is the production split that makes "traffic while the dispatcher is down" real: an
 * always-up ingest-only instance and a restartable dispatch-only instance under one application id
 * (one tracker, one cursor). Phase 1 drains a volume {@code V} and lets the cursor catch up to the
 * tracker head (the few pins ride the sidecar, all-encoded mode). The dispatcher stops; phase 2
 * publishes a further {@code DELTA} while it is down; the restart replays ≈ {@code DELTA}, a small
 * fraction of the ≈ {@code 2V} ADD+tombstone history.
 *
 * <p>{@code @Tag("nightly")}.
 */
@Tag("nightly")
class HeterogeneousReplayPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(HeterogeneousReplayPerfIT.class);

    private static final int VOLUME = Integer.getInteger("perf.replay.volume", 8_000);
    private static final int DELTA = Integer.getInteger("perf.replay.delta", 1_500);
    private static final int PINS = 5;

    private EngineHarness ingest;
    private EngineHarness dispatch;

    @AfterEach
    void tearDown() {
        if (dispatch != null) {
            dispatch.close();
        }
        if (ingest != null) {
            ingest.close();
        }
    }

    @Test
    void restartReplayIsBoundedByTrafficSinceLastCommitNotByHistory() {
        String applicationId = unique("replay-app");
        String source = createTopic("rp-src", 1);
        String destination = createTopic("rp-dst", 1);
        String dlq = createTopic("rp-dlq", 1);

        ingest = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("ingest-1")
                .roles(Role.INGEST)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(Duration.ofHours(2))
                .build();
        dispatch = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("dispatch-1")
                .roles(Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(Duration.ofHours(2))
                .idleCursorInterval(Duration.ofSeconds(1))
                .build();

        ingest.start();
        dispatch.start();
        String tracker = ingest.trackerTopic();
        TopicPartition trackerTp = new TopicPartition(tracker, 0);

        // Phase 1: a large short-delay volume that completes, plus PINS far-future pins that stay pending.
        long pinsFor = System.currentTimeMillis() + Duration.ofHours(1).toMillis();
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            PerfSupport.produce(producer, source, 1, VOLUME, "v-", 64, PerfSupport.delayMs(2_000));
            PerfSupport.produce(producer, source, 1, PINS, "pin-", 64, PerfSupport.deliverAt(pinsFor));
        }

        // The whole volume drains (the pins stay pending) ...
        readExactlyCommitted(destination, VOLUME, Duration.ofMinutes(4), RETRY_GRACE);

        // ... and the committed cursor catches up to the tracker head, the pins riding the sidecar
        // (all-encoded mode): a restart from HERE would replay ≈ nothing.
        await("cursor catches up to the tracker head with the pins in the sidecar")
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    long head = logEndOffsetTotal(tracker);
                    OffsetAndMetadata committed =
                            committedOffsets(dispatch.dispatchGroupId()).get(trackerTp);
                    assertNotNull(committed, "no committed dispatch cursor yet");
                    assertTrue(
                            committed.offset() >= head - 2,
                            () -> "cursor " + committed.offset() + " must track the tracker head " + head);
                    SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(committed.metadata());
                    assertTrue(sidecar.entries().size() == PINS, "exactly the pins ride the sidecar");
                });

        // Stop the dispatcher: its cursor is committed at the head.
        dispatch.stop();
        long cursorAtStop =
                committedOffsets(dispatch.dispatchGroupId()).get(trackerTp).offset();

        // Phase 2 (dispatcher DOWN): publish DELTA more entries — pure tracker growth, no tombstones.
        long deltaDueAt = System.currentTimeMillis() + 4_000;
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            PerfSupport.produce(producer, source, 1, DELTA, "delta-", 64, PerfSupport.deliverAt(deltaDueAt));
        }
        await("the DELTA ADDs are durable in the tracker (ingest still up while dispatch is down)")
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> logEndOffsetTotal(tracker) >= cursorAtStop + DELTA);

        long headAtRestart = logEndOffsetTotal(tracker);
        long replayRecords = headAtRestart - cursorAtStop; // the bounded replay range (§3.5)
        long fullHistory = headAtRestart; // every ADD + tombstone this partition ever wrote

        log.info(
                "MEASURED cursor-v2 replay: replay={} records (≈ DELTA {}), full history={} records (≈2V+Δ+pins); "
                        + "replay is {}% of history | published formula ≈ traffic-since-last-commit [§3.5/R1]",
                replayRecords, DELTA, fullHistory, String.format("%.1f", 100.0 * replayRecords / fullHistory));
        System.out.printf(
                "%n[PERF] cursor-v2 replay: %d records replayed (DELTA=%d, ±tol) vs %d full history "
                        + "(=%.1f%%); the cursor bounded replay to traffic-since-last-commit, not history%n",
                replayRecords, DELTA, fullHistory, 100.0 * replayRecords / fullHistory);

        // The published formula: replay ≈ traffic-since-last-commit (DELTA), ±30%.
        assertTrue(
                replayRecords >= DELTA * 0.7 && replayRecords <= DELTA * 1.3 + 16,
                () -> "replay " + replayRecords + " not within ±30% of the published traffic-since-last-commit " + DELTA
                        + " (cursor-v2 formula)");
        // And the headline scaling proof: replay is a small fraction of the full ADD+tombstone history.
        assertTrue(
                replayRecords < VOLUME,
                () -> "replay " + replayRecords + " must be far below the full history " + fullHistory + " (≈2×"
                        + VOLUME + "): replay tracks the cursor, not completion history (R1)");

        // Restart: replay the DELTA off the cursor, seed the pins from the sidecar, deliver DELTA once.
        dispatch.start();
        List<ConsumerRecord<byte[], byte[]>> all =
                readExactlyCommitted(destination, VOLUME + DELTA, Duration.ofMinutes(4), RETRY_GRACE);
        byKey(all); // every key exactly once across the restart — no full-history re-dispatch
        dispatch.stop();
    }
}
