package com.jucius.cesium.kafka.api.store;

/**
 * Identity and schedule of one delayed message: 20 bytes of real state plus the CLAMP marker.
 *
 * <p>cesium never copies payloads. The scheduler state for a delayed record is exactly this
 * pointer — the source coordinate the payload will be re-fetched from at dispatch time, plus the
 * instant it becomes due. {@code (sourcePartition, sourceOffset)} is unique forever (Kafka offsets
 * never recycle within a topic incarnation) and serves as the store's compaction/upsert identity.
 *
 * <p><strong>API evolution note.</strong> The {@code clamped} component was added (M4) so the
 * ingest {@code delay.on-over-max: CLAMP} decision survives the durable round trip: the relay is
 * stamped {@code cesium-clamped: true} at <em>dispatch</em> time — possibly on another instance
 * after a rebalance, with the index rebuilt purely from the store's durable record — so the store
 * encoding is the only carrier (design §2.3; the v1 tracker wire format reserves flags bit 0 for
 * it, design §2.2/D15). The {@linkplain #ScheduledRef(int, long, long) three-argument constructor}
 * is retained so the change is additive for existing callers.
 *
 * @param sourcePartition partition of the source topic the record was consumed from; tracker-backed
 *     stores write their durable record to the same partition number
 * @param sourceOffset offset of the record in {@code sourcePartition}; the durable identity of the
 *     entry for completion lookup, compaction keying, and idempotent upserts
 * @param dispatchAtMs requested delivery instant, epoch milliseconds UTC; derived from
 *     {@code cesium-delay-ms} relative to the source record timestamp, or from
 *     {@code cesium-deliver-at} directly (design §2.3)
 * @param clamped whether the ingest {@code delay.on-over-max: CLAMP} policy pinned
 *     {@code dispatchAtMs} to {@code now + delay.max}; stores must preserve this marker so the
 *     dispatch-time relay can stamp {@code cesium-clamped: true} (design §2.3)
 */
public record ScheduledRef(int sourcePartition, long sourceOffset, long dispatchAtMs, boolean clamped) {

    /** An unclamped schedule — the common case, and the pre-{@code clamped} constructor shape. */
    public ScheduledRef(int sourcePartition, long sourceOffset, long dispatchAtMs) {
        this(sourcePartition, sourceOffset, dispatchAtMs, false);
    }
}
