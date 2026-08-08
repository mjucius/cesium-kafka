package com.jucius.cesium.kafka.app.lifecycle;

import com.jucius.cesium.kafka.core.admin.ClusterAdmin;
import com.jucius.cesium.kafka.core.admin.TopicFacts;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

/**
 * A {@link ClusterAdmin} whose topics are briefly invisible, modelling a broker that has not yet
 * published a metadata record the controller already committed.
 *
 * <p>Only {@code describeTopic} is meaningful — {@code CesiumEngine.buildRouteDescriptor} calls
 * nothing else — so every other method throws rather than quietly return a lie.
 */
final class LaggingClusterAdmin implements ClusterAdmin {

    private final Map<String, TopicFacts> topics = new HashMap<>();
    private final Map<String, Integer> invisibleDescribes = new HashMap<>();
    final Map<String, Integer> describeCalls = new HashMap<>();

    /** Registers a topic and returns its facts. */
    TopicFacts addTopic(String name, int partitions) {
        TopicFacts facts = new TopicFacts(name, Uuid.randomUuid(), partitions);
        topics.put(name, facts);
        return facts;
    }

    /** The topic's next {@code count} describes report it unknown, as a lagging broker would. */
    void invisibleForDescribes(String topic, int count) {
        invisibleDescribes.put(topic, count);
    }

    @Override
    public Optional<TopicFacts> describeTopic(String topic) {
        int call = describeCalls.merge(topic, 1, Integer::sum);
        if (call <= invisibleDescribes.getOrDefault(topic, 0)) {
            return Optional.empty();
        }
        return Optional.ofNullable(topics.get(topic));
    }

    @Override
    public String clusterId() {
        throw new UnsupportedOperationException("buildRouteDescriptor must not reach clusterId()");
    }

    @Override
    public Map<String, String> topicConfigs(String topic) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not reach topicConfigs()");
    }

    @Override
    public Optional<String> brokerConfig(String key) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not reach brokerConfig()");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> groupOffsets(String groupId) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not reach groupOffsets()");
    }

    @Override
    public void createTopic(String topic, int partitions, Map<String, String> configs) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not create topics");
    }

    @Override
    public void grantTrackerWriteAcl(String topic, String principal) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not grant ACLs");
    }

    @Override
    public Set<String> describeTrackerWritePrincipals(String topic) {
        throw new UnsupportedOperationException("buildRouteDescriptor must not read ACLs");
    }
}
