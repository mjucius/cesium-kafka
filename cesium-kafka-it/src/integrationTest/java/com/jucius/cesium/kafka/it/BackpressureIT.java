package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Backpressure (design §5.3, R7): the dispatch loop pauses an <strong>ACTIVE</strong> shard's
 * tracker intake when its pending index exceeds {@code dispatch.max-pending-per-partition}, lets the
 * backlog accumulate <em>durably in the tracker topic</em> (never in memory — no OOM), and resumes
 * below the low-water mark; a <strong>RECOVERING</strong> shard is never paused (replay must reach
 * the barrier). Asserted through the {@code cesium.shard.paused} and {@code cesium.pending.entries}
 * gauges (§9), with deterministic small-batch intake rather than timing.
 *
 * <p>Class-{@code @Tag("nightly")}: an M6 fault-injection scenario pushed off the PR lane to nightly
 * (design §13); the §5.3 pause/resume logic also has unit coverage in cesium-kafka-core.
 */
@Tag("nightly")
class BackpressureIT extends KafkaIT {

    /** Per-ACTIVE-shard high-water mark; intake pauses above it, resumes below half (low-water 4). */
    private static final long HIGH_WATER = 8;

    /**
     * Tracker (and source) intake granularity. With small polls the index crosses the high-water
     * mark a few records past it — not by the whole 10 000-record tracker batch — so the pause point
     * is observable and the index stays demonstrably bounded while the topic backlog grows.
     */
    private static final String SMALL_POLL = "5";

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void activeShardPausesAboveHighWaterAccumulatesDurablyThenResumesBelowLowWater() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // A handful of entries already trips the pause; no need to produce millions.
                .maxPendingPerPartition(HIGH_WATER)
                // Small polls so the index settles just past the high-water mark instead of slurping
                // the whole backlog in one tracker poll (which would defeat the bounded-memory point).
                .kafkaProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, SMALL_POLL)
                .build();
        harness.start();

        // A burst that far exceeds the high-water mark, due well in the future so it stays pending
        // long enough to observe the pause before anything drains.
        int total = 40;
        long ts = System.currentTimeMillis();
        long deliverAt = ts + 30_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < total; i++) {
                produce(
                        producer,
                        source,
                        0,
                        ts,
                        "k" + i,
                        "v" + i,
                        h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
            }
            producer.flush();
        }

        // The whole backlog is durable in the tracker topic (40 ADDs, no tombstones yet).
        String tracker = harness.trackerTopic();
        readExactlyCommitted(tracker, total, WAIT);

        // The ACTIVE shard pauses its tracker intake above the high-water mark.
        await("shard 0 paused above the high-water mark")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() ->
                        EngineMetrics.gaugeIs(harness.meterRegistry(), "cesium.shard.paused", 1.0, "partition", "0"));

        // Bounded memory: the in-memory index settled just past the high-water mark, NOT at 40 — the
        // 30-odd unconsumed ADDs are parked durably in the topic, not the heap (the §5.3 OOM guard).
        double pending = EngineMetrics.gauge(harness.meterRegistry(), "cesium.pending.entries", "partition", "0");
        assertTrue(
                pending <= HIGH_WATER + Long.parseLong(SMALL_POLL),
                () -> "paused index should be bounded near the high-water mark (" + HIGH_WATER + "), was " + pending
                        + " while the tracker holds " + total + " durable ADDs");
        assertTrue(
                pending > HIGH_WATER, () -> "the pause must follow crossing the high-water mark, pending=" + pending);

        // Resume: the entries come due (deliverAt), the ACTIVE shard drains below the low-water mark,
        // intake resumes, and the whole backlog eventually dispatches exactly once.
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, total, Duration.ofMinutes(2), RETRY_GRACE);
        byKey(delivered); // exactly-once: each of the 40 keys once, no duplicate from the pause churn

        await("shard 0 resumed below the low-water mark after draining")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() ->
                        EngineMetrics.gaugeIs(harness.meterRegistry(), "cesium.shard.paused", 0.0, "partition", "0"));
        harness.stop();
    }

    @Test
    void recoveringShardIsNeverPausedAndReachesActiveDespiteHugePendingBacklog() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        String applicationId = unique("bp-recover-app");

        // Phase 1 — build a durable pending backlog FAR above the high-water mark with an ingest-only
        // instance: far-future entries that never come due, so the tracker accumulates lone pending
        // ADDs (never completed, never compacted — D4).
        int backlog = 30;
        long ts = System.currentTimeMillis();
        long farFuture = ts + Duration.ofMinutes(30).toMillis();
        EngineHarness ingest = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("ingest-1")
                .roles(Role.INGEST)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // delay.max above the far-future schedule so the entries are accepted (not clamped/DLQ).
                .delayMax(Duration.ofHours(1))
                .build();
        try {
            ingest.start();
            try (Producer<byte[], byte[]> producer = newProducer()) {
                for (int i = 0; i < backlog; i++) {
                    produce(
                            producer,
                            source,
                            0,
                            ts,
                            "k" + i,
                            "v" + i,
                            h(CesiumHeaders.DELIVER_AT, Long.toString(farFuture)));
                }
                producer.flush();
            }
            readExactlyCommitted(ingest.trackerTopic(), backlog, WAIT);
        } finally {
            ingest.stop();
        }

        // Phase 2 — a FRESH dispatch-only instance with a tiny high-water mark recovers the backlog.
        // If RECOVERING shards were subject to backpressure, replay would pause at the high-water mark
        // and never reach the barrier (a wedge). Reaching ACTIVE — provable because an ACTIVE shard
        // with pending >> high-water DOES pause — proves recovery streamed past it unpaused (R7).
        harness = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("dispatch-1")
                .roles(Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(Duration.ofHours(1))
                .maxPendingPerPartition(HIGH_WATER)
                .build();
        harness.start();

        await("the whole backlog replayed into the fresh index (recovery never paused)")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(250))
                .until(() -> EngineMetrics.gauge(harness.meterRegistry(), "cesium.pending.entries", "partition", "0")
                        == (double) backlog);

        await("the recovered shard reached ACTIVE and only THEN paused on the oversized backlog")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() ->
                        EngineMetrics.gaugeIs(harness.meterRegistry(), "cesium.shard.paused", 1.0, "partition", "0"));

        double pending = EngineMetrics.gauge(harness.meterRegistry(), "cesium.pending.entries", "partition", "0");
        assertTrue(
                pending == backlog,
                () -> "recovery must replay every pending ADD past the high-water mark, saw " + pending + " of "
                        + backlog);
        harness.stop();
    }
}
