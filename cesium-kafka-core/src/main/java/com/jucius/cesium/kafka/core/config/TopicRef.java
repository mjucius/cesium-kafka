package com.jucius.cesium.kafka.core.config;

import java.util.Objects;

/**
 * A reference to a Kafka topic by name (design §8 sketch). Modeled as a record rather than a bare
 * string so future per-topic settings (e.g. an explicit partition expectation) are a compatible
 * schema evolution.
 *
 * @param topic the topic name; empty when the operator omitted the required key
 */
public record TopicRef(String topic) {

    /** Materializes an absent name as empty so validation can aggregate the missing-field error. */
    public TopicRef {
        topic = Objects.requireNonNullElse(topic, "");
    }

    /** True when a non-blank topic name is configured. */
    public boolean isConfigured() {
        return !topic.isBlank();
    }
}
