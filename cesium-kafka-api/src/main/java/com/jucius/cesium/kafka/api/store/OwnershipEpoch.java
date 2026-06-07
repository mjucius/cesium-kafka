package com.jucius.cesium.kafka.api.store;

/**
 * The engine's current ownership identity for a partition, mirroring the Kafka
 * {@code ConsumerGroupMetadata} identity (group generation / member epoch plus member id) of the
 * consumer-group membership that granted the partition.
 *
 * <p>Exposed through {@link StoreContext#epoch(int)} so an {@link ExternalSchedulerStore} can
 * implement <em>store-side fencing</em>: conditional writes that compare-and-swap on the epoch so
 * a zombie writer — a previous owner that lost the partition but has not yet noticed — cannot
 * clobber the new owner's state (design §4.4 item 4). Tracker-backed stores do not need it: their
 * writes ride the engine's Kafka transactions, which KIP-447 group-metadata fencing already
 * protects.
 *
 * <p>The generation id is monotonically increasing per rebalance; a writer holding a smaller
 * generation than the one recorded against a row/lease must be rejected.
 *
 * @param groupGenerationId the consumer-group generation (classic protocol) or member epoch
 *     (KIP-848) under which the engine currently owns the partition
 * @param memberId the group member id of the owning consumer, disambiguating writers within a
 *     generation
 */
public record OwnershipEpoch(int groupGenerationId, String memberId) {}
