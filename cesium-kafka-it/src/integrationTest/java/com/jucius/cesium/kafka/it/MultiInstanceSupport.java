package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

/**
 * Runs N {@link EngineHarness} instances as one real consumer group: same {@code applicationId} and
 * same source/destination/tracker topics, but distinct {@code instanceId}s &mdash; so each member
 * gets its own {@code transactional.id}s (immediate {@code initTransactions()} fencing, D10) and its
 * own static {@code group.instance.id} (two static members of group A / group B, D21). A chosen
 * member can be routed through {@link ToxiproxySupport#proxiedBootstrap()} so its client&#8596;broker
 * link is cut while the other member keeps heartbeating on the direct path &mdash; the deterministic
 * substrate for the zombie-fencing (§11.3-3), LSO/barrier (§11.3-4/5) and penalty-box (§11.3-10)
 * scenarios.
 *
 * <p><strong>Process-model fidelity gap (documented per the task).</strong> The design specifies
 * <em>separate JVMs</em> so SIGKILL is real (§11.3). These v1 instances are in-JVM:
 * {@link #kill(String)} is {@link EngineHarness#kill()} (interrupt the loop threads, drop the
 * in-memory store) &mdash; a faithful stand-in for "process died without a graceful commit", but not
 * an OS {@code SIGKILL}. This is sound for fencing because the guarantees rest on
 * <em>broker-side</em> mechanisms that are JVM-agnostic: {@code initTransactions()} fencing a stale
 * {@code transactional.id}, {@code TxnOffsetCommit} rejection on a stale member/generation epoch, a
 * duplicate {@code group.instance.id} join fencing the prior static member, and session-timeout
 * eviction. None depend on OS process semantics. The crash-loop soak (§11.3-12), which needs true
 * SIGKILL, forks JVMs separately; the fencing scenarios built on this harness do not.
 */
final class MultiInstanceSupport implements AutoCloseable {

    /**
     * Short-but-robust default session timeout so a killed/partitioned static member is evicted in
     * seconds (vs the 45 s production default) without spuriously evicting a healthy member under CI
     * load; the cluster lowers {@code group.min.session.timeout.ms} to 1000 so scenario suites can go
     * lower still. The {@link #newGroup(ToxiproxySupport, int, Duration, Role...)} overload raises it
     * for scenarios that must keep a static member alive across a stop+start (the D21 zero-movement
     * rolling restart, whose whole point is that the slot is NOT evicted while the member reboots).
     */
    static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(6);

    private static final Duration STABILIZE = Duration.ofSeconds(60);

    private final ToxiproxySupport cluster;
    private final String applicationId;
    private final String source;
    private final String destination;
    private final String dlq;
    private final int sourcePartitions;
    private final Role[] roles;
    private final String sessionTimeoutMs;
    private final String heartbeatIntervalMs;
    private final Map<String, Instance> instances = new LinkedHashMap<>();
    private KafkaConfig.GroupProtocol groupProtocol = KafkaConfig.GroupProtocol.CLASSIC;

    private MultiInstanceSupport(
            ToxiproxySupport cluster,
            String applicationId,
            String source,
            String destination,
            String dlq,
            int sourcePartitions,
            Duration sessionTimeout,
            Role[] roles) {
        this.cluster = cluster;
        this.applicationId = applicationId;
        this.source = source;
        this.destination = destination;
        this.dlq = dlq;
        this.sourcePartitions = sourcePartitions;
        this.sessionTimeoutMs = Long.toString(sessionTimeout.toMillis());
        // Heartbeat well under a third of the session timeout (the broker rejects >= session/3).
        this.heartbeatIntervalMs = Long.toString(Math.max(500, Math.min(1500, sessionTimeout.toMillis() / 4)));
        this.roles = roles.clone();
    }

    /**
     * Provisions a fresh source/destination/DLQ topic set on the cluster and returns a group runner
     * for it with the {@linkplain #DEFAULT_SESSION_TIMEOUT default session timeout}.
     * {@code sourcePartitions} &ge; 2 is what makes a 2-member group actually split work.
     */
    static MultiInstanceSupport newGroup(ToxiproxySupport cluster, int sourcePartitions, Role... roles) {
        return newGroup(cluster, sourcePartitions, DEFAULT_SESSION_TIMEOUT, roles);
    }

    /**
     * As {@link #newGroup(ToxiproxySupport, int, Role...)} but with an explicit static-member session
     * timeout — raised by the zero-movement rolling-restart scenario (D21/R14) so a member can stop
     * and restart inside the window without the broker evicting its static slot and triggering the
     * very rebalance the test asserts does not happen.
     */
    static MultiInstanceSupport newGroup(
            ToxiproxySupport cluster, int sourcePartitions, Duration sessionTimeout, Role... roles) {
        String applicationId = ToxiproxySupport.unique("m6-app");
        String source = cluster.createTopic("m6-src", sourcePartitions);
        String destination = cluster.createTopic("m6-dst", sourcePartitions);
        String dlq = cluster.createTopic("m6-dlq", 1);
        return new MultiInstanceSupport(
                cluster, applicationId, source, destination, dlq, sourcePartitions, sessionTimeout, roles);
    }

    /**
     * Runs every member this group provisions under the given consumer group protocol (default
     * {@link KafkaConfig.GroupProtocol#CLASSIC}). The KIP-848 lane (§11.3-11, D12) calls this with
     * {@link KafkaConfig.GroupProtocol#CONSUMER} before {@link #addInstance} so the same
     * multi-instance scenario body re-forms one real consumer group under
     * {@code group.protocol=consumer}. Must be set before instances are added.
     */
    MultiInstanceSupport groupProtocol(KafkaConfig.GroupProtocol groupProtocol) {
        if (!instances.isEmpty()) {
            throw new IllegalStateException("set the group protocol before adding instances");
        }
        this.groupProtocol = Objects.requireNonNull(groupProtocol, "groupProtocol");
        return this;
    }

    // ------------------------------------------------------------------ instance lifecycle

    /**
     * Registers (but does not start) a member with the given stable {@code instanceId}. When
     * {@code proxied} is true the member is wired to {@link ToxiproxySupport#proxiedBootstrap()} so
     * cutting {@link ToxiproxySupport#brokerProxy()} partitions exactly this member; otherwise it
     * uses the always-healthy direct path.
     */
    EngineHarness addInstance(String instanceId, boolean proxied) {
        if (instances.containsKey(instanceId)) {
            throw new IllegalArgumentException("instance already registered: " + instanceId);
        }
        String bootstrap = proxied ? cluster.proxiedBootstrap() : cluster.directBootstrap();
        EngineHarness harness = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId(instanceId)
                .groupProtocol(groupProtocol)
                .bootstrapServers(bootstrap)
                .roles(roles)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .kafkaProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs)
                .kafkaProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs)
                .build();
        instances.put(instanceId, new Instance(instanceId, harness, proxied));
        return harness;
    }

    /** Starts the member (validates, builds clients/loops, spawns daemon threads). */
    void start(String instanceId) {
        instance(instanceId).harness.start();
    }

    /** Graceful stop (raise stop flags, join, close store). Static membership holds the slot. */
    void stop(String instanceId) {
        instance(instanceId).harness.stop();
    }

    /** SIGKILL stand-in (interrupt loops mid-flight, drop the in-memory store). */
    void kill(String instanceId) {
        instance(instanceId).harness.kill();
    }

    EngineHarness harness(String instanceId) {
        return instance(instanceId).harness;
    }

    String applicationId() {
        return applicationId;
    }

    String sourceTopic() {
        return source;
    }

    String destinationTopic() {
        return destination;
    }

    String trackerTopic() {
        // Any started instance resolves the same tracker name (applicationId-derived).
        return instances.values().iterator().next().harness.trackerTopic();
    }

    String ingestGroupId() {
        return KafkaClientFactory.ingestGroupId(applicationId);
    }

    String dispatchGroupId() {
        return KafkaClientFactory.dispatchGroupId(applicationId);
    }

    /** The static {@code group.instance.id} group A assigns to {@code instanceId}. */
    String ingestGroupInstanceId(String instanceId) {
        return ingestGroupId() + "." + instanceId;
    }

    // ------------------------------------------------------------------ group observation

    /**
     * Awaits the group reaching {@code expectedMembers} members whose assignments together cover all
     * {@code sourcePartitions} with no partition owned twice, then returns each member's assignment
     * keyed by its {@code group.instance.id}. Observed over the cluster's direct admin, so it keeps
     * working while a member is partitioned.
     */
    Map<String, Set<TopicPartition>> awaitAssignmentStable(String groupId, int expectedMembers) {
        await("group " + groupId + " stable with " + expectedMembers + " member(s) covering " + sourcePartitions
                        + " partition(s)")
                .atMost(STABILIZE)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    Map<String, Set<TopicPartition>> assignments = describeAssignments(groupId);
                    if (assignments.size() != expectedMembers) {
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
                    // Full coverage of the source topic and no double-ownership.
                    long covered = union.stream()
                            .filter(tp -> tp.topic().equals(source))
                            .count();
                    return total == union.size() && covered == sourcePartitions;
                });
        return describeAssignments(groupId);
    }

    /** Awaits the member with the given {@code group.instance.id} owning exactly {@code count} partitions. */
    Set<TopicPartition> awaitMemberPartitionCount(String groupId, String groupInstanceId, int count) {
        await("member " + groupInstanceId + " owning " + count + " partition(s)")
                .atMost(STABILIZE)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    Set<TopicPartition> owned = describeAssignments(groupId).get(groupInstanceId);
                    return owned != null && owned.size() == count;
                });
        Set<TopicPartition> owned = describeAssignments(groupId).get(groupInstanceId);
        assertNotNull(owned, "no assignment for " + groupInstanceId);
        return owned;
    }

    /** Awaits the group having exactly {@code expected} members (by static id), any assignment. */
    void awaitMemberCount(String groupId, int expected) {
        await("group " + groupId + " having " + expected + " member(s)")
                .atMost(STABILIZE)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> describeAssignments(groupId).size() == expected);
    }

    /** Each member's assigned partitions keyed by {@code group.instance.id} (static membership). */
    Map<String, Set<TopicPartition>> describeAssignments(String groupId) {
        Admin admin = cluster.admin();
        ConsumerGroupDescription description;
        try {
            description = admin.describeConsumerGroups(List.of(groupId))
                    .all()
                    .get(30, TimeUnit.SECONDS)
                    .get(groupId);
        } catch (Exception e) {
            throw new AssertionError("describeConsumerGroups(" + groupId + ") failed", e);
        }
        Map<String, Set<TopicPartition>> out = new LinkedHashMap<>();
        for (MemberDescription member : description.members()) {
            String key = member.groupInstanceId().orElse(member.consumerId());
            out.put(key, Set.copyOf(member.assignment().topicPartitions()));
        }
        return out;
    }

    @Override
    public void close() {
        for (Instance instance : instances.values()) {
            instance.harness.close();
        }
    }

    private Instance instance(String instanceId) {
        return Objects.requireNonNull(instances.get(instanceId), () -> "no such instance: " + instanceId);
    }

    private record Instance(String instanceId, EngineHarness harness, boolean proxied) {}
}
