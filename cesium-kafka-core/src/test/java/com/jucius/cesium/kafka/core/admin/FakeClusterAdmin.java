package com.jucius.cesium.kafka.core.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Deterministic in-memory {@link ClusterAdmin} for unit tests: topics, configs, and broker
 * configs are plain maps; create/ACL calls are recorded; failures are injectable.
 */
final class FakeClusterAdmin implements ClusterAdmin {

    record CreatedTopic(String name, int partitions, Map<String, String> configs) {}

    record AclGrant(String topic, String principal) {}

    String clusterId = Uuid.randomUuid().toString();
    final Map<String, TopicFacts> topics = new HashMap<>();
    final Map<String, Map<String, String>> topicConfigs = new HashMap<>();
    final Map<String, String> brokerConfigs = new HashMap<>();
    final Map<String, Map<TopicPartition, OffsetAndMetadata>> groupOffsets = new HashMap<>();
    final List<CreatedTopic> created = new ArrayList<>();
    final List<AclGrant> aclGrants = new ArrayList<>();
    final Map<String, Set<String>> trackerWriteAcls = new HashMap<>();
    ClusterAdminException aclFailure;
    ClusterAdminException describeFailure;
    ClusterAdminException describeAclsFailure;
    ClusterAdminException groupOffsetsFailure;
    ClusterAdminException topicConfigsFailure;

    // ---- metadata-propagation simulation (all default-off; see TopicVisibility) ----
    // A real broker answers UNKNOWN_TOPIC_OR_PARTITION for a topic whose metadata record it has not
    // published yet, even though the controller already acknowledged the create. These knobs make
    // that window deterministic: each maps a topic to how many of its next calls behave as lagging.
    // Integer.MAX_VALUE means "never becomes visible".
    final Map<String, Integer> invisibleDescribes = new HashMap<>();
    final Map<String, Integer> shortDescribes = new HashMap<>();
    final Map<String, Integer> invisibleConfigs = new HashMap<>();
    final Map<String, Integer> describeCalls = new HashMap<>();
    final Map<String, Integer> topicConfigCalls = new HashMap<>();

    /**
     * The topic's next {@code count} describes report it unknown, as a lagging broker would.
     *
     * <p>{@link #createTopic} restarts the count, so for a bootstrap scenario this reads as "the
     * broker publishes the created topic only on describe {@code count + 1}" — the existence
     * pre-check that ran before the create (and correctly saw nothing) does not consume the window.
     */
    void invisibleForDescribes(String topic, int count) {
        invisibleDescribes.put(topic, count);
    }

    /**
     * The topic's next {@code count} describes report ONE partition regardless of its real count —
     * a partially propagated, cursor-paginated {@code DescribeTopicPartitions} reassembly.
     */
    void shortForDescribes(String topic, int count) {
        shortDescribes.put(topic, count);
    }

    /** The topic's next {@code count} {@code topicConfigs} calls throw UnknownTopicOrPartition. */
    void configsUnknownForCalls(String topic, int count) {
        invisibleConfigs.put(topic, count);
    }

    /** True when this topic's {@code callNumber}-th call still falls inside its lagging window. */
    private static boolean lagging(Map<String, Integer> window, String topic, int callNumber) {
        return callNumber <= window.getOrDefault(topic, 0);
    }

    /** Registers one committed offset (with metadata) for a consumer group. */
    void putGroupOffset(String groupId, TopicPartition partition, OffsetAndMetadata offset) {
        groupOffsets.computeIfAbsent(groupId, ignored -> new LinkedHashMap<>()).put(partition, offset);
    }

    /** Pre-registers an existing ALLOW WRITE ACL on a topic (e.g. for FAIL-mode startups). */
    void putTrackerWriteAcl(String topic, String principal) {
        trackerWriteAcls
                .computeIfAbsent(topic, ignored -> new LinkedHashSet<>())
                .add(principal);
    }

    /** Registers a topic with the given partition count and effective configs. */
    TopicFacts addTopic(String name, int partitions, Map<String, String> configs) {
        TopicFacts facts = new TopicFacts(name, Uuid.randomUuid(), partitions);
        topics.put(name, facts);
        topicConfigs.put(name, new LinkedHashMap<>(configs));
        return facts;
    }

    @Override
    public String clusterId() {
        if (describeFailure != null) {
            throw describeFailure;
        }
        return clusterId;
    }

    @Override
    public Optional<TopicFacts> describeTopic(String topic) {
        if (describeFailure != null) {
            throw describeFailure;
        }
        int call = describeCalls.merge(topic, 1, Integer::sum);
        if (lagging(invisibleDescribes, topic, call)) {
            return Optional.empty();
        }
        TopicFacts facts = topics.get(topic);
        if (facts != null && lagging(shortDescribes, topic, call)) {
            return Optional.of(new TopicFacts(facts.name(), facts.topicId(), 1));
        }
        return Optional.ofNullable(facts);
    }

    @Override
    public Map<String, String> topicConfigs(String topic) {
        if (topicConfigsFailure != null) {
            throw topicConfigsFailure;
        }
        int call = topicConfigCalls.merge(topic, 1, Integer::sum);
        if (lagging(invisibleConfigs, topic, call)) {
            throw new ClusterAdminException(
                    "describeConfigs(" + topic + ") failed",
                    new UnknownTopicOrPartitionException("This server does not host this topic-partition."));
        }
        return topicConfigs.getOrDefault(topic, Map.of());
    }

    @Override
    public Optional<String> brokerConfig(String key) {
        return Optional.ofNullable(brokerConfigs.get(key));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> groupOffsets(String groupId) {
        if (groupOffsetsFailure != null) {
            throw groupOffsetsFailure;
        }
        return groupOffsets.getOrDefault(groupId, Map.of());
    }

    @Override
    public void createTopic(String topic, int partitions, Map<String, String> configs) {
        created.add(new CreatedTopic(topic, partitions, new LinkedHashMap<>(configs)));
        addTopic(topic, partitions, configs);
        // The propagation window opens when the controller commits the create, so the describes that
        // matter are the ones after this point — not the existence pre-check that preceded it.
        describeCalls.remove(topic);
        topicConfigCalls.remove(topic);
    }

    @Override
    public void grantTrackerWriteAcl(String topic, String principal) {
        if (aclFailure != null) {
            throw aclFailure;
        }
        aclGrants.add(new AclGrant(topic, principal));
        putTrackerWriteAcl(topic, principal);
    }

    @Override
    public Set<String> describeTrackerWritePrincipals(String topic) {
        if (describeAclsFailure != null) {
            throw describeAclsFailure;
        }
        return Set.copyOf(trackerWriteAcls.getOrDefault(topic, Set.of()));
    }
}
