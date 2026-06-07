package com.jucius.cesium.kafka.api.store;

/**
 * Self-declared capabilities of a {@link SchedulerStore} implementation.
 *
 * <p>The engine reads these at startup to wire the matching orchestration (design §4.3) and to
 * surface degraded guarantees explicitly: a store that cannot participate in the engine's Kafka
 * transactions must say so, and its weaker dispatch guarantee is reported on the {@code /info}
 * endpoint rather than discovered in production.
 *
 * @param affinity how the store's durable writes relate to the engine's Kafka transactions; must be
 *     consistent with the implemented archetype ({@link TrackerBackedStore} ⇒
 *     {@link TransactionAffinity#KAFKA_TRANSACTIONAL}, {@link ExternalSchedulerStore} ⇒
 *     {@link TransactionAffinity#EXTERNAL})
 * @param dispatchGuarantee the strongest delivery guarantee the store's settle path supports, as
 *     observed by {@code read_committed} consumers of the destination topic
 * @param requiresTrackerTopic whether the store needs the cesium-owned tracker topic to exist
 *     (creation/validation is then part of startup bootstrap, design §2.1)
 * @param supportsCancellation whether the store can honor a per-record cancellation API; v1 ships
 *     no cancellation API, but the tracker wire format reserves the record type ({@code 0x02
 *     CANCEL}, design D15)
 */
public record StoreCapabilities(
        TransactionAffinity affinity,
        DispatchGuarantee dispatchGuarantee,
        boolean requiresTrackerTopic,
        boolean supportsCancellation) {

    /** How a store's durable writes participate in the engine's Kafka transactions. */
    public enum TransactionAffinity {
        /**
         * The store encodes its durable mutations as Kafka records that the engine produces with
         * its own transactional producer, inside the ingest/dispatch transactions. Scheduler
         * state, destination writes, and consumer offsets commit or abort atomically — the
         * exactly-once archetype ({@link TrackerBackedStore}).
         */
        KAFKA_TRANSACTIONAL,
        /**
         * The store writes to an external system (e.g. a database) outside Kafka's transactions,
         * under the explicit ordering contracts of {@link ExternalSchedulerStore}: idempotent
         * upserts <em>before</em> ingest commit, dispatch marking <em>after</em> dispatch commit.
         */
        EXTERNAL
    }

    /** Strongest dispatch guarantee the store's settle path supports. */
    public enum DispatchGuarantee {
        /**
         * Completion facts share Kafka's transactional atomicity (tracker tombstones or an
         * offset-metadata cursor committed inside the dispatch transaction): a crash at any point
         * re-dispatches cleanly or is fully settled — never both.
         */
        EXACTLY_ONCE,
        /**
         * A crash window exists between the dispatch transaction commit and the store's
         * out-of-band completion write ({@link ExternalSchedulerStore#markDispatched}); recovery
         * may re-dispatch entries settled in that window. Eliminated by implementing
         * {@link ExternalSchedulerStore#cursorToCommit cursor reconciliation}.
         */
        AT_LEAST_ONCE
    }
}
