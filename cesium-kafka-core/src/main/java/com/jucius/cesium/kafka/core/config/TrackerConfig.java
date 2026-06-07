package com.jucius.cesium.kafka.core.config;

import java.util.Objects;
import java.util.Optional;

/**
 * Tracker-topic settings (design §2.1, §8).
 *
 * @param topic explicit tracker topic name; empty means the default
 *     {@code cesium.<applicationId>.tracker} is derived via {@link #resolvedTopic(String)}
 * @param bootstrap default {@link Bootstrap#CREATE}: bootstrap creates the topic with the §2.1
 *     configs (compaction-only cleanup, tombstone-retention floor, compaction-lag floor) and
 *     applies the ACL when {@code aclPrincipal} is set; {@link Bootstrap#FAIL} validates an
 *     existing topic instead
 * @param aclPrincipal principal granted exclusive write access by {@code CREATE} bootstrap.
 *     Tracker write access restricted to the cesium principal is a normative deployment
 *     requirement (R12): a forged ADD is a duplicate-injection primitive and a forged tombstone is
 *     a data-loss primitive
 */
public record TrackerConfig(String topic, Bootstrap bootstrap, Optional<String> aclPrincipal) {

    /** Tracker-topic bootstrap mode (§8 defaults table: {@code CREATE | FAIL}). */
    public enum Bootstrap {
        /** Create the tracker topic (partitions mirrored from source) applying the §2.1 configs. */
        CREATE,
        /** Require a pre-provisioned topic; validate its configs and fail fast on drift. */
        FAIL
    }

    /** Materializes the §8 defaults for absent components. */
    public TrackerConfig {
        topic = Objects.requireNonNullElse(topic, "");
        bootstrap = Objects.requireNonNullElse(bootstrap, Bootstrap.CREATE);
        aclPrincipal = Objects.requireNonNullElse(aclPrincipal, Optional.empty());
    }

    /** Returns a {@code TrackerConfig} populated entirely from the §8 defaults table. */
    public static TrackerConfig defaults() {
        return new TrackerConfig("", Bootstrap.CREATE, Optional.empty());
    }

    /**
     * The effective tracker topic name: the explicit override when set, otherwise the default
     * {@code cesium.<applicationId>.tracker} (§2.1).
     */
    public String resolvedTopic(String applicationId) {
        return topic.isBlank() ? "cesium." + applicationId + ".tracker" : topic;
    }
}
