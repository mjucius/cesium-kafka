package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import events.cesium.kafka.core.config.Role;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Rebalance / scaling correctness (design §11.3-6, R-2, D21/R14) over a real two-member consumer
 * group ({@link MultiInstanceSupport}).
 *
 * <ul>
 *   <li><strong>Scale 1&rarr;2&rarr;1 mid-backlog:</strong> with a backlog of delayed (pending)
 *       entries, a member joins (cooperative split — the surviving member keeps one of its partitions
 *       rather than revoking everything) and then leaves; every entry is delivered exactly once with
 *       no loss and no duplicate (the only proof that takeover dispatch waited for the §3.6 barrier),
 *       no member is fenced (no {@code lost} rebalance), and the loops stay live (bounded poll gap —
 *       replay is off-callback, §6).
 *   <li><strong>Static-membership zero movement (D21/R14):</strong> a member stops and restarts
 *       inside the session timeout; its partitions do <em>not</em> migrate to the other member (the
 *       survivor revokes nothing), and the returning member replays its own partition's pending state.
 * </ul>
 *
 * <p>Class-{@code @Tag("nightly")} (heavy: a real two-member group over the dedicated Toxiproxy
 * broker, with 50–150 s drain windows). <strong>Classic-protocol only:</strong> scale-in waits out
 * the static member's <em>session-timeout eviction</em>, which under {@code group.protocol=consumer}
 * falls to the ~45 s broker default (the client {@code session.timeout.ms} is broker-side under
 * KIP-848), too slow for this test's drain budget; and the cooperative-stickiness assertion is itself
 * classic-only. The KIP-848 rebalance-under-consumer finding is recorded in
 * {@code docs/failure-matrix-coverage.md}; the protocol-agnostic takeover/replay-to-barrier path is
 * covered under {@code consumer} by {@code BarrierOrderingI8IT}.
 */
@Tag("nightly")
class RebalanceScaleIT {

    private static final Duration STABLE = Duration.ofSeconds(60);

    private MultiInstanceSupport group;

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    @Test
    void scaleOneToTwoToOneMidBacklogPreservesExactlyOnceAndLiveness() {
        ToxiproxySupport cluster = ToxiproxySupport.shared();
        // A 20 s session timeout (vs the 6 s fencing default): this scale test asserts the group stays
        // HEALTHY (no member fenced), so the survivor must tolerate CI scheduling/GC stalls without a
        // spurious session-timeout eviction; scale-in eviction of the stopped member still happens
        // within the window. (The fencing scenarios that WANT fast eviction keep the 6 s default.)
        group = MultiInstanceSupport.newGroup(cluster, 2, Duration.ofSeconds(20), Role.INGEST, Role.DISPATCH);
        group.addInstance("slot-0", false);
        group.addInstance("slot-1", false);
        String dispatchGroup = group.dispatchGroupId();

        // One member owns both tracker partitions.
        group.start("slot-0");
        group.awaitMemberPartitionCount(dispatchGroup, dispatchInstanceId("slot-0"), 2);

        // A backlog of delayed entries, far enough out to stay pending across both scale operations.
        int total = 60;
        long ts = System.currentTimeMillis();
        long deliverAt = ts + 50_000;
        produceDelayed(cluster.directBootstrap(), group.sourceTopic(), total, deliverAt);

        // All ADDs durable in the tracker (no tombstones yet — nothing is due).
        String tracker = group.trackerTopic();
        new FencingProbe(cluster.directBootstrap()).readCommitted(tracker, total, STABLE, Duration.ofSeconds(2));
        Set<TopicPartition> slot0Before =
                group.describeAssignments(dispatchGroup).get(dispatchInstanceId("slot-0"));

        // Scale out: cooperative split, one tracker partition each.
        group.start("slot-1");
        Map<String, Set<TopicPartition>> split = awaitDispatchSplit();
        assertEquals(2, split.size(), "expected two static dispatch members");
        Set<TopicPartition> slot0After = split.get(dispatchInstanceId("slot-0"));
        assertEquals(1, slot0After.size(), "the surviving member keeps exactly one partition (cooperative)");
        assertTrue(
                slot0Before.containsAll(slot0After),
                "cooperative rebalance keeps one of the survivor's own partitions, not a fresh one");

        // The new owner replayed its partition off-callback and keeps polling (bounded gap, §6).
        assertPollGapBounded("slot-1");

        // Scale back in: a graceful stop of a static member triggers reassignment after the session
        // timeout (no LeaveGroup); the survivor takes both partitions back.
        group.stop("slot-1");
        group.awaitMemberPartitionCount(dispatchGroup, dispatchInstanceId("slot-0"), 2);

        // Every entry delivered exactly once across the whole 1→2→1 churn — no loss, no duplicate.
        List<ConsumerRecord<byte[], byte[]>> delivered = new FencingProbe(cluster.directBootstrap())
                .readCommitted(group.destinationTopic(), total, Duration.ofSeconds(150), KafkaIT.RETRY_GRACE);
        FencingProbe.byKey(delivered);

        // No member was ever fenced/lost during the healthy scale (group stayed healthy, R-2).
        assertEquals(
                0.0,
                EngineMetrics.counter(
                        group.harness("slot-0").meterRegistry(), "cesium.dispatch.rebalances", "event", "lost"),
                "no dispatch member should be fenced during a healthy scale");
        assertPollGapBounded("slot-0");
    }

    @Test
    void rollingRestartWithStaticMembershipMovesZeroPartitions() {
        ToxiproxySupport cluster = ToxiproxySupport.shared();
        // A session timeout comfortably longer than a stop+start cycle: the whole point of static
        // membership is that the slot is NOT evicted while the member reboots (D21).
        group = MultiInstanceSupport.newGroup(cluster, 2, Duration.ofSeconds(20), Role.INGEST, Role.DISPATCH);
        group.addInstance("slot-0", false);
        group.addInstance("slot-1", false);

        group.start("slot-0");
        group.start("slot-1");
        Map<String, Set<TopicPartition>> before = awaitDispatchSplit();
        Set<TopicPartition> slot0Owned = before.get(dispatchInstanceId("slot-0"));
        Set<TopicPartition> slot1Owned = before.get(dispatchInstanceId("slot-1"));

        // Pending backlog (far-future) so the returning member has real state to replay.
        int total = 40;
        long ts = System.currentTimeMillis();
        long deliverAt = ts + Duration.ofMinutes(5).toMillis();
        produceDelayed(cluster.directBootstrap(), group.sourceTopic(), total, deliverAt);
        new FencingProbe(cluster.directBootstrap())
                .readCommitted(group.trackerTopic(), total, STABLE, Duration.ofSeconds(2));

        double slot0RevokedBefore = EngineMetrics.counter(
                group.harness("slot-0").meterRegistry(), "cesium.dispatch.rebalances", "event", "revoked");

        // Rolling restart of slot-1 inside the session window.
        group.stop("slot-1");
        group.start("slot-1");
        Map<String, Set<TopicPartition>> after = awaitDispatchSplit();

        // Zero movement: the survivor's assignment is identical and it revoked nothing.
        assertEquals(
                slot0Owned,
                after.get(dispatchInstanceId("slot-0")),
                "static membership: the survivor's partitions must not move during a rolling restart");
        assertEquals(
                slot1Owned,
                after.get(dispatchInstanceId("slot-1")),
                "the returning static member reclaims exactly its own partitions");
        assertEquals(
                slot0RevokedBefore,
                EngineMetrics.counter(
                        group.harness("slot-0").meterRegistry(), "cesium.dispatch.rebalances", "event", "revoked"),
                "the survivor revoked nothing during the rolling restart (D21 zero movement)");

        // Replay happens on the RETURNING member: it re-seeded its own partition's pending entries.
        int slot1Partition = slot1Owned.iterator().next().partition();
        await("the returning member replayed its own partition's pending backlog")
                .atMost(STABLE)
                .pollInterval(Duration.ofMillis(250))
                .until(() -> EngineMetrics.gauge(
                                group.harness("slot-1").meterRegistry(),
                                "cesium.pending.entries",
                                "partition",
                                Integer.toString(slot1Partition))
                        > 0.0);
    }

    /** The static {@code group.instance.id} group B assigns to {@code slot} (mirrors the factory). */
    private String dispatchInstanceId(String slot) {
        return group.dispatchGroupId() + "." + slot;
    }

    /**
     * Awaits group B's two static members ({@code slot-0}, {@code slot-1}) each owning exactly one
     * tracker partition, then returns the split. {@code awaitAssignmentStable} cannot be used for
     * group B: it verifies coverage of the <em>source</em> topic, but dispatch members own
     * <em>tracker</em> partitions; the per-member count is topic-agnostic and so works for either group.
     */
    private Map<String, Set<TopicPartition>> awaitDispatchSplit() {
        String dispatchGroup = group.dispatchGroupId();
        group.awaitMemberPartitionCount(dispatchGroup, dispatchInstanceId("slot-0"), 1);
        group.awaitMemberPartitionCount(dispatchGroup, dispatchInstanceId("slot-1"), 1);
        return group.describeAssignments(dispatchGroup);
    }

    /**
     * The recent inter-poll gap stays well under {@code max.poll.interval.ms} (5 min): replay runs
     * off the callback and never stalls the group-B poll loop (§6, R-2). The IT poll ceiling is 2 s,
     * so a healthy idle gap is a couple of seconds; a wedge would show minutes.
     */
    private void assertPollGapBounded(String slot) {
        double gapSeconds =
                EngineMetrics.gauge(group.harness(slot).meterRegistry(), "cesium.dispatch.poll.gap.seconds");
        assertTrue(
                Double.isNaN(gapSeconds) || gapSeconds < 15.0,
                () -> slot + " group-B poll gap should stay bounded (no replay stall), was " + gapSeconds + " s");
    }

    private static void produceDelayed(String bootstrap, String sourceTopic, int total, long deliverAtMs) {
        byte[] deliverAt = Long.toString(deliverAtMs).getBytes(StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = ToxiproxySupport.newProducer(bootstrap)) {
            for (int i = 0; i < total; i++) {
                int partition = i % 2; // spread across both source partitions
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        sourceTopic,
                        partition,
                        ("k" + i).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8),
                        List.of(new RecordHeader(CesiumHeaders.DELIVER_AT, deliverAt)));
                var unused = producer.send(record);
            }
            producer.flush();
        }
    }
}
