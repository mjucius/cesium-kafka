package com.jucius.cesium.kafka.api.store;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;

/**
 * Engine-provided dependencies handed to a store in {@link SchedulerStore#configure}.
 *
 * <p>The context is the store's only window into the engine: route identity, typed configuration,
 * an injectable clock, metrics, and the per-partition ownership epoch. Stores must not construct
 * their own Kafka clients to write scheduler state — durable writes flow either through the
 * engine's transactional producer ({@link TrackerBackedStore}) or through the store's external
 * system ({@link ExternalSchedulerStore}).
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

    /**
     * The clock the store must use for all time decisions (due comparisons, maintenance pacing).
     * Injectable so contract tests can drive virtual time deterministically.
     */
    Clock clock();

    /** Registry for the store's metrics; the engine pre-configures common tags. */
    MeterRegistry meterRegistry();

    /**
     * Group generation / member epoch of the engine's current ownership of {@code partition} —
     * mirroring the Kafka {@code ConsumerGroupMetadata} identity. Lets an external store implement
     * store-side fencing via conditional writes on the epoch, rejecting zombie writers that lost
     * the partition in an earlier generation (design §4.4 item 4).
     *
     * @param partition the partition whose current ownership epoch is requested; must be currently
     *     assigned to this store instance
     */
    OwnershipEpoch epoch(int partition);
}
