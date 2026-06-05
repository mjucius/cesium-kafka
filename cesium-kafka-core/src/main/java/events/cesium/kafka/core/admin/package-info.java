/**
 * Admin-plane startup validation and tracker bootstrap (design §2.1, §3.1, §3.6, §7.6, §8; failure
 * rows R-7..R-11).
 *
 * <p>{@link events.cesium.kafka.core.admin.StartupValidator} runs every environment check the
 * design requires before either loop starts — source/destination/DLQ existence, source
 * non-compaction and retention (time-, size-, and tier-aware with the explicit
 * size-based-retention acknowledgment gate), tracker bootstrap ({@code CREATE}) or validation
 * ({@code FAIL}) including the D14 tombstone-retention floor and partition parity (R-7), and the
 * broker offsets-retention vs max-tolerated-outage check (D18) — aggregating every finding into
 * one {@link events.cesium.kafka.core.config.ValidationReport} so an operator fixes a broken
 * environment in one round trip. The caller fail-fasts on errors.
 *
 * <p>{@link events.cesium.kafka.core.admin.IdentityBlob} is the versioned
 * {@code {v, clusterId, sourceTopicId}} blob both loops carry in their committed
 * {@code OffsetAndMetadata} metadata (§3.1, R-10/R-17): Kafka topic IDs survive nothing, so a
 * recreated topic is detected as an identity mismatch and the engine fail-fasts instead of
 * delivering wrong payloads.
 *
 * <p>The cluster is reached through the {@link events.cesium.kafka.core.admin.ClusterAdmin}
 * boundary so unit tests inject a fake; production wires
 * {@link events.cesium.kafka.core.admin.KafkaClusterAdmin} around a real
 * {@code org.apache.kafka.clients.admin.Admin}.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.core.admin;
