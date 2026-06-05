package events.cesium.kafka.api.store;

import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * Immutable description of the route a store instance serves: the resolved topic names, their
 * Kafka topic ids, and the shared partition count.
 *
 * <p>Topic <em>ids</em> are included because names survive recreation but ids do not — a recreated
 * topic gets a new {@link Uuid}. Stores that persist identity material (e.g. in the cursor sidecar)
 * should bind these ids so a recreated source or tracker topic is detected as a fail-fast rather
 * than silently delivering wrong payloads or replaying into an empty log (design §3.6, R11/R17).
 *
 * @param applicationId the route's application id; namespaces consumer groups, transactional ids,
 *     and default internal topic names
 * @param sourceTopic name of the user-owned source topic records are consumed from
 * @param sourceTopicId Kafka topic id of the source topic at configure time
 * @param destinationTopic name of the user-owned destination topic records are relayed to
 * @param destinationTopicId Kafka topic id of the destination topic at configure time
 * @param trackerTopic name of the cesium-owned tracker topic (default
 *     {@code cesium.<applicationId>.tracker}); meaningful only for stores whose
 *     {@link StoreCapabilities#requiresTrackerTopic()} is {@code true}
 * @param trackerTopicId Kafka topic id of the tracker topic, or {@code null} when the route has no
 *     tracker topic (external stores with {@code requiresTrackerTopic() == false})
 * @param dlqTopic name of the dead-letter topic receiving policy and loss-notice records, or
 *     {@code null} when no DLQ is configured
 * @param partitionCount partition count of the source topic — and therefore of the tracker topic,
 *     whose partition count must equal the source's (validated at startup, design §2.1)
 */
public record RouteDescriptor(
        String applicationId,
        String sourceTopic,
        Uuid sourceTopicId,
        String destinationTopic,
        Uuid destinationTopicId,
        String trackerTopic,
        @Nullable Uuid trackerTopicId,
        @Nullable String dlqTopic,
        int partitionCount) {

    /** Validates that the partition count is positive. */
    public RouteDescriptor {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive, got " + partitionCount);
        }
    }
}
