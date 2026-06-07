package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §3.4 point 4 / §3.9 row D-5: <strong>cooperative (incremental) revocation</strong> of a
 * dispatch shard. Group B runs the {@code CooperativeStickyAssignor} (the v1 classic-protocol
 * default), so when a third instance joins a 2-member group over a 3-partition tracker, exactly ONE
 * tracker partition is revoked from its owner and handed to the newcomer — the other two shards are
 * never revoked and keep dispatching.
 *
 * <p>The race the design excludes <em>structurally</em> (not by luck) is "a transaction touching
 * partition {@code p} commits after {@code p} was revoked": by I3 rebalance callbacks run only
 * between transactions, and {@code onPartitionsRevoked} drops the shard before it returns, so no
 * later dispatch transaction can reference {@code p}. The observable consequence — and the assertion
 * here — is that every scheduled entry is delivered exactly once across the rebalance: a transaction
 * that touched the moved partition after revocation would surface as a duplicate.
 *
 * <p>We additionally pin the <em>cooperative</em> shape (vs an eager stop-the-world reshuffle): of
 * the three pre-join shards exactly one moves (the original owners together retain two), and the
 * prior owner of the moved partition fires a {@code onPartitionsRevoked} that its registry counts —
 * the D-5 drop-the-shard path — while its other shard is never revoked.
 *
 * <p>Class-{@code @Tag("nightly")} (heavy: a three-member group over the dedicated Toxiproxy broker).
 * Inherently classic-protocol (it asserts the {@code CooperativeStickyAssignor} incremental-revocation
 * shape), so it has no KIP-848 variant; design §13.
 */
@Tag("nightly")
class CooperativeRevocationIT {

    private static final int SOURCE_PARTITIONS = 3;
    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final Duration RETRY_GRACE = Duration.ofSeconds(6);
    private static final Duration GRACE = Duration.ofSeconds(1);

    private ToxiproxySupport cluster;
    private MultiInstanceSupport group;

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    @Test
    void thirdInstanceJoinIncrementallyRevokesOneShardWithoutDuplicatesOrLosses() {
        cluster = ToxiproxySupport.shared();
        group = MultiInstanceSupport.newGroup(cluster, SOURCE_PARTITIONS, Role.INGEST, Role.DISPATCH);
        group.addInstance("slot-0", false);
        group.addInstance("slot-1", false);
        group.start("slot-0");
        group.start("slot-1");
        group.awaitAssignmentStable(group.ingestGroupId(), 2);

        int n = 12; // 4 per source partition
        long deliverAt = System.currentTimeMillis() + 25_000; // dispatch after the rebalance settles
        ScenarioSupport.produceDeliverAt(
                cluster.directBootstrap(), group.sourceTopic(), SOURCE_PARTITIONS, n, "k", deliverAt);
        FencingProbe probe = new FencingProbe(cluster.directBootstrap());
        probe.readCommitted(group.trackerTopic(), n, WAIT, GRACE); // ADDs committed; entries pending in group B

        // Two members covering all three tracker shards ({2,1}); snapshot ownership and revoke counts.
        Map<String, Set<TopicPartition>> before = awaitDispatchCoverage(2);
        double revoked0Before = ScenarioSupport.rebalanceEvents(group.harness("slot-0"), "dispatch", "revoked");
        double revoked1Before = ScenarioSupport.rebalanceEvents(group.harness("slot-1"), "dispatch", "revoked");

        // A third instance joins -> cooperative-sticky moves exactly one shard to it ({1,1,1}).
        group.addInstance("slot-2", false);
        group.start("slot-2");
        Map<String, Set<TopicPartition>> after = awaitDispatchCoverage(3);

        // Cooperative shape: each original member lost at most one shard, and the two of them
        // together kept two of the original three (only one moved — sticky, not a full reshuffle).
        int retained = 0;
        String revokedOwner = null;
        for (String instanceId : List.of("slot-0", "slot-1")) {
            String memberKey = ScenarioSupport.dispatchGroupInstanceId(group, instanceId);
            Set<TopicPartition> ownedBefore = before.getOrDefault(memberKey, Set.of());
            Set<TopicPartition> ownedAfter = after.getOrDefault(memberKey, Set.of());
            Set<TopicPartition> kept = new HashSet<>(ownedBefore);
            kept.retainAll(ownedAfter);
            assertTrue(
                    ownedBefore.size() - kept.size() <= 1,
                    () -> instanceId + " lost more than one shard — not an incremental cooperative revocation");
            retained += kept.size();
            if (kept.size() < ownedBefore.size()) {
                revokedOwner = instanceId;
            }
        }
        assertEquals(2, retained, "exactly one of the three tracker shards moved to the new member (sticky)");
        assertEquals(
                1,
                after.get(ScenarioSupport.dispatchGroupInstanceId(group, "slot-2"))
                        .size(),
                "the new member owns exactly the single moved shard");
        assertNotNull(revokedOwner, "one original member must have had a shard revoked");

        // D-5: the prior owner of the moved shard fired onPartitionsRevoked (dropping the shard before
        // any later transaction could reference it). Attributed to THIS join by the delta.
        double revokedBefore = revokedOwner.equals("slot-0") ? revoked0Before : revoked1Before;
        double revokedAfter = ScenarioSupport.rebalanceEvents(group.harness(revokedOwner), "dispatch", "revoked");
        assertTrue(
                revokedAfter > revokedBefore,
                "the prior owner of the moved shard must fire a cooperative onPartitionsRevoked (D-5)");

        // The real D-5 proof: every entry delivered exactly once — on the moved shard and the two
        // untouched ones alike — so no transaction touched a partition after its revocation.
        probe.assertExactlyOnceByKey(group.destinationTopic(), n, WAIT, RETRY_GRACE);
    }

    /**
     * Awaits group B settling to {@code members} members whose disjoint assignments cover all
     * {@link #SOURCE_PARTITIONS} tracker partitions (the dispatch analog of
     * {@link MultiInstanceSupport#awaitAssignmentStable}, which is source-topic specific), then
     * returns each member's assignment keyed by {@code group.instance.id}.
     */
    private Map<String, Set<TopicPartition>> awaitDispatchCoverage(int members) {
        String dispatchGroup = group.dispatchGroupId();
        await("group B: " + members + " member(s) covering all " + SOURCE_PARTITIONS + " tracker shard(s)")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    Map<String, Set<TopicPartition>> assignments = group.describeAssignments(dispatchGroup);
                    if (assignments.size() != members) {
                        return false;
                    }
                    Set<TopicPartition> union = new HashSet<>();
                    int total = 0;
                    for (Set<TopicPartition> owned : assignments.values()) {
                        if (owned.isEmpty()) {
                            return false;
                        }
                        union.addAll(owned);
                        total += owned.size();
                    }
                    return total == union.size() && union.size() == SOURCE_PARTITIONS;
                });
        return group.describeAssignments(dispatchGroup);
    }
}
