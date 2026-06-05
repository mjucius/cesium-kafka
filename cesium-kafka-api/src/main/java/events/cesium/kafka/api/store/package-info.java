/**
 * The scheduler-store SPI (design §4): the stable boundary between the cesium engine and pluggable
 * scheduler-state stores.
 *
 * <p>The <em>engine</em> owns consumer groups and partition ownership, Kafka transactions and
 * fencing (invariants I1–I9), dispatch timing, header policies, and the recovery barrier gate. The
 * <em>store</em> owns durable recording of scheduler-state mutations, recovery enumeration, the
 * in-memory time index, and — for the tracker archetype — the per-partition recovery cursor the
 * engine commits as the group-B offset+metadata.
 *
 * <p>The root {@link events.cesium.kafka.api.store.SchedulerStore} is sealed with exactly two
 * {@code non-sealed} archetypes, because the engine must know a store's transaction-participation
 * model to orchestrate correctly (exhaustive Java 21 {@code switch} wiring, no instanceof chains):
 * {@link events.cesium.kafka.api.store.TrackerBackedStore} enlists its writes in the engine's
 * Kafka transactions (exactly-once), while {@link
 * events.cesium.kafka.api.store.ExternalSchedulerStore} performs ordered out-of-band writes under
 * explicit ordering contracts. Hot-path types are primitive-accessor views ({@link
 * events.cesium.kafka.api.store.DueBatch}) so the steady state allocates nothing per entry, and
 * the thread-confinement contract is explicit so the v1 store needs no locking.
 *
 * <p>Discovery is via {@link java.util.ServiceLoader} registration of {@link
 * events.cesium.kafka.api.store.SchedulerStoreProvider}; selection is always explicit
 * ({@code store.type}), never automatic. This package is semver-stable from 1.0; evolution is
 * additive via default methods.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.api.store;
