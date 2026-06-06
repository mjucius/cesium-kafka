package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import events.cesium.kafka.core.config.Role;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for the {@link MultiInstanceSupport} capability: two {@link EngineHarness} instances
 * with distinct {@code instanceId}s but one {@code applicationId} form a single real consumer group
 * and split the source partitions (static membership, cooperative assignor), and killing one member
 * reassigns its partitions to the survivor once its session expires. Proves the multi-instance group
 * substrate the fencing scenarios rely on.
 */
class MultiInstanceHarnessSmokeIT {

    private MultiInstanceSupport group;

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    @Test
    void twoInstancesFormOneGroupAndSplitPartitions() {
        ToxiproxySupport cluster = ToxiproxySupport.shared();
        group = MultiInstanceSupport.newGroup(cluster, 2, Role.INGEST);
        group.addInstance("slot-0", false);
        group.addInstance("slot-1", false);
        String groupId = group.ingestGroupId();

        // One member alone owns both partitions.
        group.start("slot-0");
        group.awaitMemberPartitionCount(groupId, group.ingestGroupInstanceId("slot-0"), 2);

        // Second member joins; cooperative rebalance splits the two partitions one each.
        group.start("slot-1");
        Map<String, Set<TopicPartition>> stable = group.awaitAssignmentStable(groupId, 2);
        assertEquals(2, stable.size(), "expected two static members");
        stable.forEach(
                (member, owned) -> assertEquals(1, owned.size(), () -> member + " should own exactly one partition"));
    }

    @Test
    void killingAMemberReassignsItsPartitions() {
        ToxiproxySupport cluster = ToxiproxySupport.shared();
        group = MultiInstanceSupport.newGroup(cluster, 2, Role.INGEST);
        group.addInstance("slot-0", false);
        group.addInstance("slot-1", false);
        String groupId = group.ingestGroupId();

        group.start("slot-0");
        group.start("slot-1");
        group.awaitAssignmentStable(groupId, 2);

        // SIGKILL stand-in on slot-0: after its (short) session expires, slot-1 takes both partitions.
        group.kill("slot-0");
        Set<TopicPartition> survivorOwns =
                group.awaitMemberPartitionCount(groupId, group.ingestGroupInstanceId("slot-1"), 2);
        assertEquals(2, survivorOwns.size(), "survivor should own both partitions after reassignment");
    }
}
