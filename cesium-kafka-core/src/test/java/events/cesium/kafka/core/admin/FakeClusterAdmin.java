package events.cesium.kafka.core.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

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
    ClusterAdminException aclFailure;
    ClusterAdminException describeFailure;
    ClusterAdminException groupOffsetsFailure;

    /** Registers one committed offset (with metadata) for a consumer group. */
    void putGroupOffset(String groupId, TopicPartition partition, OffsetAndMetadata offset) {
        groupOffsets.computeIfAbsent(groupId, ignored -> new LinkedHashMap<>()).put(partition, offset);
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
        return Optional.ofNullable(topics.get(topic));
    }

    @Override
    public Map<String, String> topicConfigs(String topic) {
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
    }

    @Override
    public void grantTrackerWriteAcl(String topic, String principal) {
        if (aclFailure != null) {
            throw aclFailure;
        }
        aclGrants.add(new AclGrant(topic, principal));
    }
}
