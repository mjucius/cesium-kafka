package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.admin.StartupValidationResult;
import com.jucius.cesium.kafka.core.admin.StartupValidator;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.core.loop.LoopFatalException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-9 (startup-check subset) against a real broker: compacted source fail-fast,
 * tracker partition-parity fail-fast naming the grow-tracker-first runbook, the {@code CREATE}
 * bootstrap writing the §2.1 tracker configs, DLQ existence when a policy routes to it, the §7.6
 * retention floor in FAIL mode, the R13 size-based-retention acknowledgment gate, the R-10
 * source-recreation topic-ID fail-fast, and the I-9 {@code auto.offset.reset=none} fail-fast
 * after the group's committed offsets disappear. Three runtime re-validation cases (D23) close it:
 * the R-11 earliest retained record under acknowledged eviction, the D14 committed-cursor age, and
 * a source recreated under a running engine.
 */
class StartupChecksIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void compactedSourceFailsFast() {
        String source = createTopic("src", 1, Map.of("cleanup.policy", "compact"));
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors());
        assertHasError(result, "route.source.topic", "must not be compacted");
    }

    @Test
    void trackerPartitionMismatchFailsFastNamingTheRunbook() {
        String source = createTopic("src", 2);
        String destination = createTopic("dst", 2);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        // Pre-provision the tracker with otherwise-compliant configs but the WRONG partition count.
        createNamedTopic(
                harness.trackerTopic(),
                3,
                Map.of(
                        "cleanup.policy", "compact",
                        "delete.retention.ms", Long.toString(Duration.ofDays(2).toMillis()),
                        "min.compaction.lag.ms",
                                Long.toString(Duration.ofHours(1).toMillis()),
                        "segment.ms", Long.toString(Duration.ofHours(1).toMillis()),
                        "message.timestamp.type", "LogAppendTime"));

        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors());
        assertHasError(result, "route.tracker", "grow the tracker topic first");
    }

    @Test
    void createBootstrapAppliesTheSpecTrackerConfigs() {
        String source = createTopic("src", 3);
        String destination = createTopic("dst", 3);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        StartupValidationResult result = harness.validate();
        assertFalse(result.report().hasErrors(), () -> result.report().render());

        String tracker = harness.trackerTopic();
        var description =
                get(ADMIN.describeTopics(List.of(tracker)).allTopicNames()).get(tracker);
        assertEquals(3, description.partitions().size(), "partitions mirrored from the source");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, tracker);
        Config topicConfig = get(ADMIN.describeConfigs(List.of(resource)).all()).get(resource);
        assertEquals("compact", topicConfig.get("cleanup.policy").value(), "compaction-only cleanup (D4)");
        long delayMaxMs = harness.config().delay().max().toMillis();
        assertEquals(
                Long.toString(2 * delayMaxMs),
                topicConfig.get("delete.retention.ms").value(),
                "tombstone-retention floor 2 x delay.max (D14)");
        assertEquals(
                Long.toString(StartupValidator.minCompactionLagFloorMs(
                        harness.config().kafka().transactions().timeout())),
                topicConfig.get("min.compaction.lag.ms").value(),
                "compaction lag floor max(2 x txn timeout, 1h)");
        assertEquals(
                Long.toString(Duration.ofHours(1).toMillis()),
                topicConfig.get("segment.ms").value());
        assertEquals("LogAppendTime", topicConfig.get("message.timestamp.type").value());
    }

    @Test
    void missingDlqTopicFailsWhenAPolicyRoutesToIt() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        // DLQ is configured (the default policies route to it) but never created on the cluster.
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(unique("dlq-missing"))
                .build();

        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors());
        assertHasError(result, "route.dlq.topic", "does not exist");
    }

    @Test
    void sourceRetentionBelowDelayMaxPlusMarginFailsInFailMode() {
        // 60 s retention vs the default delay.max P1D + 1h margin: a payload could expire long
        // before its dispatch time.
        String source = createTopic("src", 1, Map.of("retention.ms", "60000"));
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors());
        assertHasError(result, "route.source.topic", "below delay.max + margin");
    }

    @Test
    void sizeBasedRetentionRequiresExplicitAcknowledgment() {
        String source = createTopic("src", 1, Map.of("retention.bytes", "1073741824"));
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);

        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        StartupValidationResult rejected = harness.validate();
        assertTrue(rejected.report().hasErrors(), "retention.bytes without ACKNOWLEDGED must reject");
        assertHasError(rejected, "startup-checks.size-based-retention", "ACKNOWLEDGED");
        harness.close();

        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .acknowledgeSizeBasedRetention()
                .build();
        StartupValidationResult acknowledged = harness.validate();
        assertFalse(
                acknowledged.report().hasErrors(), () -> acknowledged.report().render());
        assertTrue(
                acknowledged.report().warnings().stream()
                        .anyMatch(f -> f.path().equals("startup-checks.size-based-retention")),
                "the named acceptance is surfaced as a warning, never silent (risk 19)");
    }

    @Test
    void revalidationFlagsAcknowledgedEvictionInsideTheDelayWindow() {
        String source = createTopic("src", 1, Map.of("retention.bytes", "1073741824"));
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .acknowledgeSizeBasedRetention()
                .build();
        StartupValidationResult startup = harness.validate();
        assertFalse(startup.report().hasErrors(), () -> startup.report().render());
        IdentityBlob identity = startup.identity().orElseThrow();
        Uuid trackerId = topicId(harness.trackerTopic());

        // Offsets 0 and 1 are older than delay.max + margin (P1D + 1h); offset 2 is fresh.
        long now = System.currentTimeMillis();
        long old = now - Duration.ofDays(2).toMillis();
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, source, 0, old, "k", "v0");
            produce(producer, source, 0, old, "k", "v1");
            produce(producer, source, 0, now, "k", "v2");
        }
        TopicPartition partition = new TopicPartition(source, 0);

        // Eviction (deleteRecords stands in for size eviction) that leaves an old record retained:
        // the margin is still positive.
        get(ADMIN.deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(1)))
                .all());
        StartupValidator.Revalidation clean = harness.revalidate(identity, trackerId, Clock.systemUTC());
        assertFalse(clean.report().hasErrors(), () -> clean.report().render());

        // Eviction past every record older than the window: the margin is negative.
        get(ADMIN.deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(2)))
                .all());
        StartupValidator.Revalidation flagged = harness.revalidate(identity, trackerId, Clock.systemUTC());
        assertTrue(flagged.identityChange().isEmpty());
        assertTrue(
                flagged.report().errors().stream()
                        .anyMatch(f -> f.path().equals("startup-checks.size-based-retention")
                                && f.message().contains("partition(s) [0]")
                                && f.message().contains("R-11")),
                () -> flagged.report().render());
    }

    @Test
    void sourceRecreationFailsFastViaIdentityBlobMismatch() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        // Run once for real: the loop commits offsets carrying the §3.1 identity blob.
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, source, 0, System.currentTimeMillis(), "k", "v");
        }
        harness.start();
        String groupId = harness.ingestGroupId();
        TopicPartition partition = new TopicPartition(source, 0);
        OffsetAndMetadata committedByLoop = awaitCommittedOffset(groupId, partition, 1);
        assertNotNull(committedByLoop.metadata());
        assertFalse(committedByLoop.metadata().isBlank(), "the loop commits the identity blob with every offset");
        harness.stop();
        Uuid originalTopicId = harness.identity().sourceTopicId();

        // Delete and recreate the source under the SAME name: Kafka assigns a fresh topic id.
        get(ADMIN.deleteTopics(List.of(source)).all());
        await("source topic deletion").atMost(WAIT).until(() -> !topicExists(source));
        await("source topic recreation").atMost(WAIT).ignoreExceptions().until(() -> {
            if (!topicExists(source)) {
                createNamedTopic(source, 1, Map.of());
            }
            return topicExists(source) && !topicId(source).equals(originalTopicId);
        });

        // The KRaft group coordinator scrubs committed offsets for deleted partitions, so re-seed
        // the loop's OWN captured offset+blob: this models the R-10 premise — committed offsets
        // that survive a recreation (cluster restore from backup, recreation racing coordinator
        // cleanup) — deterministically. The lingering static member must be evicted first.
        await("stale offsets re-seeded").atMost(WAIT).ignoreExceptions().until(() -> {
            get(ADMIN.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions())
                    .all());
            get(ADMIN.alterConsumerGroupOffsets(groupId, Map.of(partition, committedByLoop))
                    .all());
            OffsetAndMetadata reseeded = committedOffsets(groupId).get(partition);
            return reseeded != null && committedByLoop.metadata().equals(reseeded.metadata());
        });

        // Without the read-back check this would pass cleanly and the engine would resume stale
        // committed offsets against a different topic incarnation — the R-10 wrong-payload window.
        StartupValidationResult result = harness.validate();
        assertTrue(result.report().hasErrors(), () -> result.report().render());
        assertHasError(result, "route.source.topic", "recreated under the same name");
        assertHasError(result, "route.source.topic", "R-10");
    }

    @Test
    void deletedGroupOffsetsFailFastUnderResetNoneWithTheRunbook() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, source, 0, System.currentTimeMillis(), "k", "v");
        }
        harness.start();
        String groupId = harness.ingestGroupId();
        awaitCommittedOffset(groupId, new TopicPartition(source, 0), 1);
        harness.stop();

        // Simulate KIP-211 committed-offset expiry: evict the lingering static member, then
        // delete the group — its committed offsets disappear with it.
        await("group offsets deletion").atMost(WAIT).ignoreExceptions().until(() -> {
            get(ADMIN.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions())
                    .all());
            get(ADMIN.deleteConsumerGroups(List.of(groupId)).all());
            return committedOffsets(groupId).isEmpty();
        });

        // The restart fail-fasts (I-9): auto.offset.reset=none is locked, and neither a silent
        // skip nor a mass duplicate is acceptable — the operator chooses the reset point.
        harness.start();
        Throwable death = harness.awaitDeath(WAIT);
        LoopFatalException fatal = assertInstanceOf(LoopFatalException.class, death);
        assertTrue(fatal.getMessage().contains("auto.offset.reset=none"), fatal.getMessage());
        assertTrue(fatal.getMessage().contains("offset-reset runbook"), fatal.getMessage());
    }

    @Test
    void revalidationFlagsAnOldCursorUntilDeleteRetentionIsRaised() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        StartupValidationResult startup = harness.validate(); // CREATE bootstrap: delete.retention.ms = 2 x P1D
        assertFalse(startup.report().hasErrors(), () -> startup.report().render());
        String tracker = harness.trackerTopic();
        TopicPartition trackerPartition = new TopicPartition(tracker, 0);

        // One appended tracker record (LogAppendTime: now) and a dispatch cursor still in front of it.
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, tracker, 0, System.currentTimeMillis(), "k", "v");
        }
        get(ADMIN.alterConsumerGroupOffsets(
                        harness.dispatchGroupId(), Map.of(trackerPartition, new OffsetAndMetadata(0L)))
                .all());

        // The only way to age a fresh record past delete.retention.ms / 2 (= 24 h here) is to
        // read the round 30 h in the future.
        Clock future = Clock.offset(Clock.systemUTC(), Duration.ofHours(30));
        IdentityBlob identity = startup.identity().orElseThrow();
        Uuid trackerId = topicId(tracker);

        StartupValidator.Revalidation breached = harness.revalidate(identity, trackerId, future);
        assertTrue(breached.identityChange().isEmpty());
        assertTrue(
                breached.report().errors().stream()
                        .anyMatch(f -> f.path().equals("route.tracker")
                                && f.message().contains("committed cursor older than delete.retention.ms / 2")),
                () -> breached.report().render());

        // Raising delete.retention.ms to 4 days moves the cutoff to 18 h before now: clean again.
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, tracker);
        get(ADMIN.incrementalAlterConfigs(Map.of(
                        resource,
                        List.of(new AlterConfigOp(
                                new ConfigEntry(
                                        "delete.retention.ms",
                                        Long.toString(Duration.ofDays(4).toMillis())),
                                AlterConfigOp.OpType.SET))))
                .all());
        await("revalidation clean after raising delete.retention.ms")
                .atMost(WAIT)
                .until(() -> !harness.revalidate(identity, trackerId, future)
                        .report()
                        .hasErrors());
    }

    @Test
    void revalidationReportsASourceRecreatedUnderARunningEngineAsAnIdentityChange() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        StartupValidationResult startup = harness.validate();
        assertFalse(startup.report().hasErrors(), () -> startup.report().render());
        IdentityBlob identity = startup.identity().orElseThrow();
        Uuid trackerId = topicId(harness.trackerTopic());

        get(ADMIN.deleteTopics(List.of(source)).all());
        await("source topic deletion").atMost(WAIT).until(() -> !topicExists(source));
        await("source topic recreation").atMost(WAIT).ignoreExceptions().until(() -> {
            if (!topicExists(source)) {
                createNamedTopic(source, 1, Map.of());
            }
            return topicExists(source) && !topicId(source).equals(identity.sourceTopicId());
        });

        String change = harness.revalidate(identity, trackerId, Clock.systemUTC())
                .identityChange()
                .orElseThrow(() -> new AssertionError("a recreated source must be an identity change"));
        assertTrue(change.contains("recreated under the same name"), change);
        assertTrue(change.contains("R17"), change);
    }

    private static Uuid topicId(String topic) {
        return get(ADMIN.describeTopics(List.of(topic)).allTopicNames())
                .get(topic)
                .topicId();
    }

    private static void assertHasError(StartupValidationResult result, String path, String messageFragment) {
        List<ValidationReport.Finding> errors = result.report().errors();
        assertTrue(
                errors.stream()
                        .anyMatch(f -> f.path().equals(path) && f.message().contains(messageFragment)),
                () -> "expected an ERROR at '" + path + "' containing '" + messageFragment + "' but got:\n"
                        + result.report().render());
    }
}
