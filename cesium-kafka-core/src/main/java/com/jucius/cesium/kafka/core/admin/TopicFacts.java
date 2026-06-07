package com.jucius.cesium.kafka.core.admin;

import java.util.Objects;
import org.apache.kafka.common.Uuid;

/**
 * The identity-bearing facts of one existing topic (design §2.1, §3.1): name, topic ID (survives
 * nothing — a recreated topic gets a new {@link Uuid}), and partition count.
 *
 * @param name the topic name
 * @param topicId the broker-assigned topic id
 * @param partitionCount the live partition count
 */
public record TopicFacts(String name, Uuid topicId, int partitionCount) {

    /** Validates that components are present and the partition count is positive. */
    public TopicFacts {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(topicId, "topicId");
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1, got " + partitionCount);
        }
    }
}
