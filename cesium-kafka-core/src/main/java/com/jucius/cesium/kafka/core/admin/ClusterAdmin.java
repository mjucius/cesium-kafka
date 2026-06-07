package com.jucius.cesium.kafka.core.admin;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * The narrow admin-plane boundary {@link StartupValidator} talks through (design §2.1, §6's
 * {@code cesium-admin} thread). An interface rather than a direct {@code Admin} dependency so unit
 * tests inject a deterministic fake; production wires {@link KafkaClusterAdmin} around the real
 * client.
 *
 * <p>Every method is synchronous and throws {@link ClusterAdminException} on transport or
 * broker-side failure.
 */
public interface ClusterAdmin {

    /** The cluster id from {@code describeCluster()} — half of the §3.1 identity blob. */
    String clusterId();

    /** Describes one topic; empty when the topic does not exist. */
    Optional<TopicFacts> describeTopic(String topic);

    /**
     * The topic's <em>effective</em> (resolved, defaults included) config entries, name to value.
     * Callers fall back to the documented Kafka defaults for keys a fake omits.
     */
    Map<String, String> topicConfigs(String topic);

    /**
     * One broker-level config value (e.g. {@code offsets.retention.minutes}), read from an
     * arbitrary live broker; empty when the broker does not report the key.
     */
    Optional<String> brokerConfig(String key);

    /**
     * The consumer group's committed offsets and metadata per partition, as the broker reports
     * them ({@code Admin.listConsumerGroupOffsets}) — empty for a group that does not exist or
     * has no committed offsets. The metadata strings carry the §3.1 identity blobs the engine
     * commits with every offset; {@link StartupValidator} compares them against the live cluster
     * and source-topic identity to detect source recreation (R-10/R17).
     */
    Map<TopicPartition, OffsetAndMetadata> groupOffsets(String groupId);

    /**
     * Creates a topic with the given partition count, the cluster-default replication factor, and
     * the given topic configs. Losing a creation race ({@code TopicExistsException}) is not an
     * error — the caller re-describes and validates the winner's topic either way.
     */
    void createTopic(String topic, int partitions, Map<String, String> configs);

    /**
     * Grants the cesium principal write (plus read/describe) on the tracker topic — the R12
     * normative deployment requirement the {@code CREATE} bootstrap applies when
     * {@code route.tracker.acl-principal} is configured. Throws {@link ClusterAdminException}
     * whose cause chain carries {@code SecurityDisabledException} when the cluster has no
     * authorizer.
     */
    void grantTrackerWriteAcl(String topic, String principal);

    /**
     * The principals that currently hold an {@code ALLOW WRITE} (or {@code ALLOW ALL}) ACL
     * reaching the tracker topic ({@code Admin.describeAcls}, LITERAL/wildcard/prefixed patterns
     * that match the name) — the live state of the R12 write restriction, read on
     * <em>every</em> startup so {@link StartupValidator} can verify it rather than assume the
     * {@code CREATE}-time grant is still intact, detect a foreign writer, or warn when no
     * principal is configured. Throws {@link ClusterAdminException} whose cause chain carries
     * {@code SecurityDisabledException} (or {@code UnsupportedVersionException}) when the cluster
     * has no authorizer — the verification is then downgraded to a WARN, mirroring the
     * ACL-apply path.
     */
    Set<String> describeTrackerWritePrincipals(String topic);
}
