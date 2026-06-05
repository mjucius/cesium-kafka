package events.cesium.kafka.core.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * The production {@link ClusterAdmin}: a thin synchronous adapter over a real
 * {@link Admin} client with a per-call timeout. Owns no client lifecycle — the caller (the
 * {@code cesium-admin} thread, §6) creates and closes the {@code Admin}.
 */
public final class KafkaClusterAdmin implements ClusterAdmin {

    /** Default per-call timeout. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final Admin admin;
    private final Duration timeout;

    /** Wraps {@code admin} with the {@link #DEFAULT_TIMEOUT}. */
    public KafkaClusterAdmin(Admin admin) {
        this(admin, DEFAULT_TIMEOUT);
    }

    /** Wraps {@code admin} with an explicit per-call timeout. */
    public KafkaClusterAdmin(Admin admin, Duration timeout) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public String clusterId() {
        String id = await(admin.describeCluster().clusterId(), "describeCluster.clusterId");
        if (id == null) {
            throw new ClusterAdminException(
                    "the cluster did not report a cluster id (required for the §3.1 identity blob)");
        }
        return id;
    }

    @Override
    public Optional<TopicFacts> describeTopic(String topic) {
        KafkaFuture<TopicDescription> future =
                admin.describeTopics(List.of(topic)).topicNameValues().get(topic);
        if (future == null) {
            throw new ClusterAdminException("describeTopics did not return a future for '" + topic + "'");
        }
        try {
            TopicDescription description = await(future, "describeTopics(" + topic + ")");
            return Optional.of(new TopicFacts(
                    description.name(),
                    description.topicId(),
                    description.partitions().size()));
        } catch (ClusterAdminException e) {
            if (e.hasCause(UnknownTopicOrPartitionException.class)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public Map<String, String> topicConfigs(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Map<ConfigResource, Config> all =
                await(admin.describeConfigs(List.of(resource)).all(), "describeConfigs(" + topic + ")");
        Config config = all.get(resource);
        if (config == null) {
            throw new ClusterAdminException("describeConfigs returned no config for topic '" + topic + "'");
        }
        Map<String, String> values = new LinkedHashMap<>();
        for (ConfigEntry entry : config.entries()) {
            String value = entry.value();
            if (value != null) {
                values.put(entry.name(), value);
            }
        }
        return Map.copyOf(values);
    }

    @Override
    public Optional<String> brokerConfig(String key) {
        Collection<Node> nodes = await(admin.describeCluster().nodes(), "describeCluster.nodes");
        if (nodes.isEmpty()) {
            throw new ClusterAdminException("describeCluster returned no live brokers");
        }
        Node node = nodes.iterator().next();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id()));
        Map<ConfigResource, Config> all =
                await(admin.describeConfigs(List.of(resource)).all(), "describeConfigs(broker " + node.id() + ")");
        Config config = all.get(resource);
        if (config == null) {
            return Optional.empty();
        }
        ConfigEntry entry = config.get(key);
        return entry == null ? Optional.empty() : Optional.ofNullable(entry.value());
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> groupOffsets(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> raw = await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "listConsumerGroupOffsets(" + groupId + ")");
        // The client can report partitions without a committed offset as null entries.
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        raw.forEach((partition, offset) -> {
            if (offset != null) {
                offsets.put(partition, offset);
            }
        });
        return Map.copyOf(offsets);
    }

    @Override
    public void createTopic(String topic, int partitions, Map<String, String> configs) {
        NewTopic newTopic = new NewTopic(topic, Optional.of(partitions), Optional.empty()).configs(configs);
        try {
            await(admin.createTopics(List.of(newTopic)).all(), "createTopics(" + topic + ")");
        } catch (ClusterAdminException e) {
            // Lost a creation race with another instance: the caller re-describes and validates
            // the winner's topic either way (idempotent bootstrap).
            if (!e.hasCause(TopicExistsException.class)) {
                throw e;
            }
        }
    }

    @Override
    public void grantTrackerWriteAcl(String topic, String principal) {
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
        List<AclBinding> bindings = List.of(
                new AclBinding(
                        pattern, new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        pattern, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(
                        pattern,
                        new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
        await(admin.createAcls(bindings).all(), "createAcls(" + topic + ")");
    }

    private <T> T await(KafkaFuture<T> future, String operation) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterAdminException(operation + " interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new ClusterAdminException(operation + " failed: " + cause, cause);
        } catch (TimeoutException e) {
            throw new ClusterAdminException(operation + " timed out after " + timeout, e);
        }
    }
}
