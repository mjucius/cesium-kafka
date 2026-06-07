package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §3.9 row R-3: <strong>{@code onPartitionsLost} on a fenced member</strong>. When a member
 * is evicted (session timeout) it cannot cleanly revoke — on rejoin the cooperative consumer invokes
 * {@code onPartitionsLost} for the partitions it can no longer claim, and the dispatch loop responds
 * by dropping those shards with no flush, aborting any open transaction, then rejoining and replaying
 * from the committed cursor (§6 callbacks; {@code SchedulerStore.onPartitionsLost}).
 *
 * <p>This is induced deterministically with Toxiproxy (R21): the proxied member is
 * {@link ToxiproxySupport#cut cut} past its session timeout — so the broker evicts it and reassigns
 * its tracker shard to the survivor — and then {@link ToxiproxySupport#restore healed}, so it rejoins
 * to a generation that has moved on and fires {@code onPartitionsLost}. We assert the loop counted a
 * {@code lost} rebalance event (R-3) and that every scheduled entry is delivered exactly once across
 * the loss, the rejoin, and the replay — no duplicate from the survivor's takeover, no loss from the
 * dropped-and-rebuilt shard.
 *
 * <p>Class-{@code @Tag("nightly")} (heavy: a partitioned two-member group over the dedicated
 * Toxiproxy broker, session-timeout eviction windows); design §13.
 */
@Tag("nightly")
class PartitionsLostIT {

    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final Duration RETRY_GRACE = Duration.ofSeconds(6);
    private static final Duration GRACE = Duration.ofSeconds(1);

    private ToxiproxySupport cluster;
    private MultiInstanceSupport group;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            ToxiproxySupport.restore(cluster.brokerProxy());
        }
        if (group != null) {
            group.close();
        }
    }

    @Test
    void fencedMemberFiresOnPartitionsLostThenRejoinsAndReplaysExactlyOnce() {
        cluster = ToxiproxySupport.shared();
        group = MultiInstanceSupport.newGroup(cluster, 2, Role.INGEST, Role.DISPATCH);
        group.addInstance("slot-0", false); // survivor (direct, fault-immune)
        group.addInstance("slot-1", true); // the member to fence (proxied)
        group.start("slot-0");
        group.start("slot-1");
        group.awaitAssignmentStable(group.ingestGroupId(), 2);
        group.awaitMemberCount(group.dispatchGroupId(), 2);

        int n = 8;
        long deliverAt = System.currentTimeMillis() + 22_000;
        ScenarioSupport.produceDeliverAt(cluster.directBootstrap(), group.sourceTopic(), 2, n, "k", deliverAt);
        FencingProbe probe = new FencingProbe(cluster.directBootstrap());
        probe.readCommitted(group.trackerTopic(), n, WAIT, GRACE);

        // Cut the proxied member and hold it cut until the broker has evicted it and moved its shard
        // to the survivor (this await is the deterministic "it was truly evicted" gate).
        ToxiproxySupport.cut(cluster.brokerProxy());
        group.awaitMemberPartitionCount(
                group.dispatchGroupId(), ScenarioSupport.dispatchGroupInstanceId(group, "slot-0"), 2);

        // Heal: the evicted member rejoins to a moved-on generation and fires onPartitionsLost (R-3),
        // dropping the shard (no flush) and re-recovering.
        ToxiproxySupport.restore(cluster.brokerProxy());
        await("fenced member observed a dispatch onPartitionsLost (R-3)")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertTrue(
                        ScenarioSupport.rebalanceEvents(group.harness("slot-1"), "dispatch", "lost") > 0,
                        "expected an onPartitionsLost on the fenced dispatch member"));

        // No duplicate (survivor's takeover, then the returning member's replay) and no loss.
        probe.assertExactlyOnceByKey(group.destinationTopic(), n, WAIT, RETRY_GRACE);
    }
}
