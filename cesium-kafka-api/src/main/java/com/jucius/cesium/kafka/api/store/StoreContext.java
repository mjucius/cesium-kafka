package com.jucius.cesium.kafka.api.store;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Engine-provided dependencies handed to a store in {@link SchedulerStore#configure}.
 *
 * <p>The context is the store's only window into the engine: route identity, typed configuration
 * and metrics. Stores must not construct their own Kafka clients to write scheduler state — durable
 * writes flow through the engine's transactional producer ({@link TrackerBackedStore}).
 */
public interface StoreContext {

    /**
     * The route this store instance serves: application id, resolved topic names and topic ids,
     * and the shared source/tracker partition count.
     */
    RouteDescriptor route();

    /**
     * Typed read-only view of the {@code store.properties} configuration subtree. Stores should
     * validate every key they read — and reject keys they do not recognize — inside
     * {@link SchedulerStore#validate()}.
     */
    ConfigView config();

    /** Registry for the store's metrics; the engine pre-configures common tags. */
    MeterRegistry meterRegistry();
}
