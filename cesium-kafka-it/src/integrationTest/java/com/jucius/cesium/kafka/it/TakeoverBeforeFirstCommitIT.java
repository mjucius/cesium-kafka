package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A fresh multi-instance deployment under live traffic (design §3.6 step 2, ADR-0022): a cooperative
 * rebalance moves tracker partitions to a joining dispatch member before anyone committed a cursor
 * on them, while the group already committed on another partition. The new owner must start such a
 * partition from its beginning instead of exiting. Deterministic shape: four tracker partitions and
 * a one-hour idle-cursor interval, so only a partition that dispatched has a cursor; instance 1
 * alone dispatches on partition 0, then instance 2 joins and takes two partitions, at least one
 * never committed. Both dispatch loops must stay up, and entries on every partition must then be
 * delivered exactly once.
 */
class TakeoverBeforeFirstCommitIT extends KafkaIT {

    private static final int PARTITIONS = 4;

    private EngineHarness first;
    private EngineHarness second;

    @AfterEach
    void tearDown() {
        if (second != null) {
            second.close();
        }
        if (first != null) {
            first.close();
        }
    }

    @Test
    void partitionsMovedBeforeTheirFirstCursorCommitRecoverFromTheBeginning() {
        String applicationId = unique("takeover-app");
        String source = createTopic("src", PARTITIONS);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        first = harness(applicationId, "it-1", source, destination, dlq, Role.INGEST, Role.DISPATCH);
        second = harness(applicationId, "it-2", source, destination, dlq, Role.DISPATCH);
        String dispatchGroup = first.dispatchGroupId();

        // Instance 1 owns all four tracker partitions and dispatches on partition 0 only, so the
        // group's one committed cursor is on partition 0.
        first.start();
        TopicPartition committedTp = new TopicPartition(first.trackerTopic(), 0);
        produceDue(source, 0, "early", 3);
        readExactlyCommitted(destination, 3, WAIT);
        await("only tracker partition 0 has a committed dispatch cursor")
                .atMost(WAIT)
                .until(() -> committedPartitions(dispatchGroup).equals(Set.of(committedTp)));

        // Instance 2 joins: the cooperative split moves two partitions to it, and at most one of them
        // has a cursor. Each must recover (ACTIVE); before ADR-0022 the loop exited instead.
        second.start();
        await("the cooperative split gives each instance two tracker partitions")
                .atMost(WAIT)
                .until(() -> ownedBy(dispatchGroup, "it-1").size() == 2
                        && ownedBy(dispatchGroup, "it-2").size() == 2);
        Set<TopicPartition> moved = ownedBy(dispatchGroup, "it-2");
        await("instance 2 recovers " + moved).atMost(WAIT).until(() -> {
            second.assertDispatchAlive();
            return moved.stream()
                    .allMatch(tp -> EngineMetrics.gaugeIs(
                            second.meterRegistry(),
                            KafkaTrackerStore.SHARD_STATE_METRIC,
                            2.0,
                            "partition",
                            Integer.toString(tp.partition())));
        });

        // Entries on every partition, under both owners: each delivered exactly once.
        for (int partition = 0; partition < PARTITIONS; partition++) {
            produceDue(source, partition, "late", 2);
        }
        byKey(readExactlyCommitted(destination, 3 + 2 * PARTITIONS, WAIT));
        assertEquals(
                0.0,
                EngineMetrics.counter(first.meterRegistry(), "cesium.ingest.records", "outcome", "relayed_immediate"),
                "every entry went through the tracker, none past due at ingest");
        await("instance 2 dispatched the entries on " + moved)
                .atMost(WAIT)
                .until(() -> EngineMetrics.counter(
                                second.meterRegistry(), "cesium.dispatch.records", "outcome", "dispatched")
                        >= 2 * moved.size());
        first.assertDispatchAlive();
        second.assertDispatchAlive();
    }

    private static EngineHarness harness(
            String applicationId, String instanceId, String source, String destination, String dlq, Role... roles) {
        return EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId(instanceId)
                .roles(roles)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // Idle partitions never commit a cursor within the test.
                .idleCursorInterval(Duration.ofHours(1))
                .build();
    }

    /**
     * Entries due three seconds after they are produced to source partition {@code partition}: long
     * enough that a just-started ingest loop still schedules them instead of relaying them past due.
     */
    private static void produceDue(String source, int partition, String keyPrefix, int count) {
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < count; i++) {
                produce(
                        producer,
                        source,
                        partition,
                        System.currentTimeMillis(),
                        keyPrefix + "-" + partition + "-" + i,
                        "v",
                        h(CesiumHeaders.DELAY_MS, "3000"));
            }
        }
    }

    private static Set<TopicPartition> committedPartitions(String groupId) {
        return committedOffsets(groupId).entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /** The tracker partitions the static dispatch member {@code instanceId} owns right now. */
    private static Set<TopicPartition> ownedBy(String dispatchGroup, String instanceId) {
        String member = dispatchGroup + "." + instanceId;
        return get(ADMIN.describeConsumerGroups(List.of(dispatchGroup)).all()).get(dispatchGroup).members().stream()
                .filter(m -> m.groupInstanceId().filter(member::equals).isPresent())
                .map(m -> m.assignment().topicPartitions())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(Set.of());
    }
}
