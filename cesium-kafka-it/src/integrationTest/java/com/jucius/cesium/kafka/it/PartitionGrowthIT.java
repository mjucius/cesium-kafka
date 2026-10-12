package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The operations.md §9 growing-partitions runbook, end to end (R-7). A new source partition has no
 * committed ingest offset and {@code auto.offset.reset=none} is locked (D18), so source growth is
 * offline: stop the engine, wait for both groups to empty, grow the tracker then the source, seed
 * only the new source partitions to earliest, restart. The dispatch group is not seeded: its loop
 * starts the new tracker partition from the beginning (ADR-0022). It still has to empty, because a
 * static leader rejoining a group that is still Stable keeps its old assignment (KIP-814) and the
 * new tracker partition would stay unassigned. A delayed record on the new source partition must
 * then be delivered after its delay.
 */
class PartitionGrowthIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void offlineGrowthSeedingOnlyNewSourcePartitionsDeliversFromANewSourcePartition() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // The stopped static members stay in their groups until the session times out;
                // keep that to seconds rather than the 45 s default.
                .kafkaProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000")
                .kafkaProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000")
                .build();
        harness.start();
        String tracker = harness.trackerTopic();

        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(
                    producer,
                    source,
                    0,
                    System.currentTimeMillis(),
                    "k-before",
                    "v",
                    h(CesiumHeaders.DELAY_MS, "2000"));
        }
        readExactlyCommitted(destination, 1, WAIT);
        harness.stop();

        // Runbook: ingest offsets can only be altered once the group is empty; the dispatch group
        // must empty so the restart is a fresh join that assigns the new tracker partition.
        awaitEmpty(harness.ingestGroupId());
        awaitEmpty(harness.dispatchGroupId());
        get(ADMIN.createPartitions(Map.of(tracker, NewPartitions.increaseTo(2))).all());
        get(ADMIN.createPartitions(Map.of(source, NewPartitions.increaseTo(2))).all());
        get(ADMIN.alterConsumerGroupOffsets(
                        harness.ingestGroupId(), Map.of(new TopicPartition(source, 1), new OffsetAndMetadata(0L)))
                .all());

        harness.start();
        long ts = System.currentTimeMillis();
        long scheduledFor = ts + 3_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(producer, source, 1, ts, "k-after", "v", h(CesiumHeaders.DELAY_MS, "3000"));
        }

        List<ConsumerRecord<byte[], byte[]>> out = readExactlyCommitted(destination, 2, WAIT);
        ConsumerRecord<byte[], byte[]> after = byKey(out).get("k-after");
        assertNotNull(after, "the record on the new source partition was not delivered");
        assertTrue(
                after.timestamp() >= scheduledFor,
                () -> "dispatched before the requested instant by " + (scheduledFor - after.timestamp()) + " ms");
        // Both lifecycles completed: an ADD and a tombstone on each tracker partition.
        assertEquals(4, readExactlyCommitted(tracker, 4, WAIT).size());
        harness.stop();
    }

    private static void awaitEmpty(String groupId) {
        await("group " + groupId + " empty").atMost(WAIT).until(() -> {
            var groups = get(ADMIN.describeConsumerGroups(List.of(groupId)).all());
            return groups.get(groupId).members().isEmpty();
        });
    }
}
