package events.cesium.kafka.api.store;

/**
 * Identity and schedule of one delayed message: 20 bytes of real state.
 *
 * <p>cesium never copies payloads. The scheduler state for a delayed record is exactly this
 * pointer — the source coordinate the payload will be re-fetched from at dispatch time, plus the
 * instant it becomes due. {@code (sourcePartition, sourceOffset)} is unique forever (Kafka offsets
 * never recycle within a topic incarnation) and serves as the store's compaction/upsert identity.
 *
 * @param sourcePartition partition of the source topic the record was consumed from; tracker-backed
 *     stores write their durable record to the same partition number
 * @param sourceOffset offset of the record in {@code sourcePartition}; the durable identity of the
 *     entry for completion lookup, compaction keying, and idempotent upserts
 * @param dispatchAtMs requested delivery instant, epoch milliseconds UTC; derived from
 *     {@code cesium-delay-ms} relative to the source record timestamp, or from
 *     {@code cesium-deliver-at} directly (design §2.3)
 */
public record ScheduledRef(int sourcePartition, long sourceOffset, long dispatchAtMs) {}
