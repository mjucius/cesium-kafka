package com.jucius.cesium.kafka.api.store;

/**
 * Discovery hook for {@link SchedulerStore} implementations (design §4.5).
 *
 * <p>Providers are registered via {@code META-INF/services} and loaded with
 * {@link java.util.ServiceLoader}; the engine picks the provider whose {@link #typeId()} matches.
 * <strong>Selection is always explicit</strong> — {@code store.type: <typeId>}; the engine never
 * auto-selects a store, and an unknown type id, or one registered more than once on the classpath,
 * fails startup naming the type id.
 */
public interface SchedulerStoreProvider {

    /**
     * Stable, unique identifier operators put in {@code store.type} (the v1 production store is
     * {@code kafka-tracker}). Lowercase-kebab by convention; must never change once released.
     */
    String typeId();

    /**
     * Creates a new, unconfigured store instance. The engine drives the lifecycle from here:
     * {@code configure → capabilities → validate → start}.
     */
    SchedulerStore create();
}
