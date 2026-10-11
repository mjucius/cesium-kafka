/**
 * The loop-to-store boundary (design §4–§5): {@link com.jucius.cesium.kafka.api.store.TrackerBackedStore}
 * and the value types that cross it. Engine-internal, with no compatibility promise (ADR-0021); the
 * only implementation is {@code KafkaTrackerStore}.
 *
 * <p>The <em>engine</em> owns consumer groups and partition ownership, Kafka transactions and
 * fencing (invariants I1–I9), dispatch timing, header policies, and the recovery barrier gate. The
 * <em>store</em> owns durable recording of scheduler-state mutations (as tracker-record bytes the
 * engine produces), recovery enumeration, the in-memory time index, and the per-partition recovery
 * cursor the engine commits as the group-B offset+metadata. Hot-path types are primitive-accessor
 * views ({@link com.jucius.cesium.kafka.api.store.DueBatch}) so the steady state allocates nothing per
 * entry, and the thread-confinement contract is explicit so the store needs no locking.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.api.store;
