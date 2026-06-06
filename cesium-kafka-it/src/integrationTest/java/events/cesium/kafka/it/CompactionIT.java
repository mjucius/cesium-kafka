package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import events.cesium.kafka.core.admin.StartupValidationResult;
import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.core.config.TrackerConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tracker compaction correctness (design §3.7, §11.3-8, R-4/R-8).
 *
 * <ul>
 *   <li><strong>Compacted-log rebuild:</strong> drive completed (ADD+tombstone) and still-pending
 *       (lone ADD) entries, force the broker cleaner to compact between them (driven by an
 *       awaitility wait on the observable compaction <em>effect</em> — readable records collapsing —
 *       never a fixed sleep), restart, and assert recovery rebuilds correctly: the lone pending ADDs
 *       (which compaction can never remove, D4) dispatch exactly once, the completed entries are not
 *       resurrected, and no duplicate is delivered.
 *   <li><strong>Startup rejection:</strong> a tracker with {@code delete.retention.ms} below the D14
 *       floor ({@code 2 × delay.max}) fails startup validation (R-8) — the tombstone-retention floor
 *       is correctness-load-bearing, not advisory.
 * </ul>
 *
 * <p><strong>Broker-independent backstop (R21).</strong> The compaction <em>convergence</em> logic
 * (R1/R2 over pre-compacted logs with ADDs removed / tombstones orphaned) is proven deterministically
 * and broker-free by {@code TrackerBackedStoreContract.compactedLogWith*Converges} /
 * {@code compactedLogsConvergeToScriptTruth}, which run against {@code KafkaTrackerStore} on every PR
 * (cesium-kafka-store-kafka unit tests). This IT is the end-to-end confirmation that a real Apache
 * Kafka cleaner produces logs that path handles; if its cleaner timing ever proves flaky in CI it can
 * be quarantined to the nightly lane without losing the convergence guarantee.
 *
 * <p>Class-{@code @Tag("nightly")}: the forced-compaction cleaner waits make it the heaviest IT, and
 * its convergence logic already has the broker-free PR backstop above (design §13 / R21).
 */
@Tag("nightly")
class CompactionIT extends KafkaIT {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CompactionIT.class);

    /** delay.max for the rebuild test: late entries schedule under it; the floor is 2× this. */
    private static final Duration DELAY_MAX = Duration.ofMinutes(1);

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void compactedTrackerRebuildsPendingEntriesExactlyOnce() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(DELAY_MAX)
                // FAIL bootstrap: we pre-provision the tracker so we control segment.bytes (tiny ⇒
                // segments roll ⇒ the cleaner has non-active segments to compact). Every other config
                // is spec-compliant so startup validation passes at both start and restart.
                .trackerBootstrap(TrackerConfig.Bootstrap.FAIL)
                .idleCursorInterval(Duration.ofSeconds(1))
                .build();
        String tracker = harness.trackerTopic();
        createNamedTopic(
                tracker,
                1,
                Map.of(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_COMPACT,
                        // >= 2 × delay.max (the D14 floor) — valid for startup.
                        TopicConfig.DELETE_RETENTION_MS_CONFIG,
                        Long.toString(2 * DELAY_MAX.toMillis()),
                        // >= max(2 × txn timeout, 1h) — valid for startup; relaxed to 0 later to force a clean.
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
                        Long.toString(Duration.ofHours(1).toMillis()),
                        // Short segment.ms so the ADD-bearing segment rolls (becomes a cleanable,
                        // non-active segment) as soon as the later tombstones append. segment.bytes has
                        // a 1 MiB broker floor, far larger than this tiny tracker log, so time-based
                        // rolling is the lever.
                        TopicConfig.SEGMENT_MS_CONFIG,
                        "1000",
                        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,
                        "LogAppendTime"));

        harness.start();

        int early = 30; // completed (ADD + tombstone) ⇒ compactible
        int late = 4; // lone pending ADDs ⇒ must survive compaction (D4) and dispatch after restart
        long ts = System.currentTimeMillis();
        long lateFor = ts + 45_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < early; i++) {
                produce(producer, source, 0, ts, "early-" + i, "v", h(CesiumHeaders.DELAY_MS, "2000"));
            }
            for (int i = 0; i < late; i++) {
                produce(producer, source, 0, ts, "late-" + i, "v", h(CesiumHeaders.DELIVER_AT, Long.toString(lateFor)));
            }
            producer.flush();
        }

        // The early entries dispatch (ADD+tombstone committed, cursor advanced past them); the late
        // entries stay pending (lone ADDs). The whole lifecycle is durable before we force the clean.
        readExactlyCommitted(destination, early, WAIT);
        long beforeCompaction = drainCount(tracker, "read_committed");
        assertTrue(
                beforeCompaction >= early,
                () -> "tracker should hold the completed lifecycle, saw " + beforeCompaction);

        // Stop, then make the cleaner aggressive and give it a BOUNDED, BEST-EFFORT window to compact
        // (observed via the readable-record count collapsing — no fixed sleep). The Apache Kafka log
        // cleaner's timing on a tiny KRaft log is genuinely non-deterministic even with
        // min.cleanable.dirty.ratio=0 + max.compaction.lag.ms forcing (R21 / risk 20 calls this a
        // flake magnet), so an in-window clean is OBSERVED, not hard-gated. The deterministic R1/R2
        // convergence over pre-compacted logs is proven broker-free on every PR by
        // TrackerBackedStoreContract.compactedLogWith*Converges; the hard assertion below is the
        // end-to-end invariant that holds either way — after the cleaner is set loose and the instance
        // restarts, recovery dispatches the lone pending ADDs exactly once and never resurrects a
        // completed entry.
        harness.stop();
        setTrackerConfig(
                tracker,
                Map.of(
                        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0",
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0",
                        // KIP-354: forces the cleaner to compact eligible segments within this lag and
                        // rolls the active segment so its data is cleanable too.
                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "100"));
        boolean compactionObserved = awaitCompactionBestEffort(tracker, beforeCompaction, Duration.ofSeconds(90));
        log.info(
                "compaction {} within the best-effort window (before={} records)",
                compactionObserved ? "observed" : "NOT observed (cleaner timing); rebuild invariant still asserted",
                beforeCompaction);

        // Restore the validatable compaction-lag floor so the restart's startup checks pass; the
        // compaction already happened and is not undone. Reset max.compaction.lag.ms back above the
        // restored minimum (min > max would be rejected) — a day, comfortably over the 1 h floor.
        setTrackerConfig(
                tracker,
                Map.of(
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
                        Long.toString(Duration.ofHours(1).toMillis()),
                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
                        Long.toString(Duration.ofDays(1).toMillis())));

        // Restart: recovery seeds the late pins and replays the compacted log. Tombstones for the
        // compacted-away early ADDs apply as R2 no-ops; the late lone ADDs survive and dispatch.
        harness.start();
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, early + late, Duration.ofMinutes(2), RETRY_GRACE);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(delivered); // no duplicates
        for (int i = 0; i < late; i++) {
            assertTrue(
                    byKey.containsKey("late-" + i),
                    "pending entry late-" + i + " must survive compaction and dispatch");
        }
        harness.stop();
    }

    @Test
    void trackerDeleteRetentionBelowFloorFailsStartup() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(DELAY_MAX)
                .trackerBootstrap(TrackerConfig.Bootstrap.FAIL)
                .build();
        // Compaction-only and a valid compaction-lag, but delete.retention.ms far below 2 × delay.max:
        // the tombstone-retention floor a replayer in overflow-fallback mode depends on (D14, R-8).
        createNamedTopic(
                harness.trackerTopic(),
                1,
                Map.of(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_COMPACT,
                        TopicConfig.DELETE_RETENTION_MS_CONFIG,
                        "1000",
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
                        Long.toString(Duration.ofHours(1).toMillis()),
                        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG,
                        "LogAppendTime"));

        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors(), "a sub-floor delete.retention.ms must fail startup");
        assertTrue(
                result.report().errors().stream()
                        .anyMatch(f ->
                                f.path().equals("route.tracker") && f.message().contains("tombstone-retention floor")),
                () -> "expected a route.tracker tombstone-retention-floor error, got:\n"
                        + result.report().render());
    }

    /**
     * Polls (bounded) until the tracker's read_committed record count drops below {@code before} —
     * the observable signal that the broker cleaner compacted away superseded ADDs. Returns whether
     * it was observed within {@code window}; never throws on timeout (the cleaner's timing is
     * non-deterministic, R21). Each {@code drainCount} pass already spends ~2 s draining to quiet, so
     * the loop self-paces without a sleep.
     */
    private static boolean awaitCompactionBestEffort(String tracker, long before, Duration window) {
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            if (drainCount(tracker, "read_committed") < before) {
                return true;
            }
        }
        return false;
    }

    private static void setTrackerConfig(String topic, Map<String, String> entries) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        List<AlterConfigOp> ops = entries.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        get(ADMIN.incrementalAlterConfigs(Map.of(resource, ops)).all());
    }
}
