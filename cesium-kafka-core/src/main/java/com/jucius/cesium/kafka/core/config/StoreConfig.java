package com.jucius.cesium.kafka.core.config;

import java.util.Map;
import java.util.Objects;

/**
 * Scheduler-store selection and its opaque property subtree (design §8 sketch, D7).
 *
 * <p>The {@code properties} map is the store's private namespace, exposed to implementations
 * through {@code com.jucius.cesium.kafka.api.store.ConfigView} — the engine never interprets it.
 *
 * @param type the ServiceLoader-registered store type; default {@code kafka-tracker} (the
 *     flagship in-memory index durably backed by the Kafka tracker topic)
 * @param properties store-specific keys relative to {@code store.properties}; immutable
 */
public record StoreConfig(String type, Map<String, String> properties) {

    /** Default store type ({@code kafka-tracker}, §8 defaults table). */
    public static final String DEFAULT_TYPE = "kafka-tracker";

    /**
     * Materializes the §8 defaults for absent components; the {@code properties} map is wrapped in
     * a value-redacting {@link SecretMap} so a store secret (e.g. the reserved
     * {@code store.kafka.hmac.*}) never leaks through {@code toString} (L6).
     */
    public StoreConfig {
        type = Objects.requireNonNullElse(type, DEFAULT_TYPE);
        properties = SecretMap.copyOf(properties == null ? Map.of() : properties);
    }

    /** Returns a {@code StoreConfig} populated entirely from the §8 defaults table. */
    public static StoreConfig defaults() {
        return new StoreConfig(DEFAULT_TYPE, Map.of());
    }
}
