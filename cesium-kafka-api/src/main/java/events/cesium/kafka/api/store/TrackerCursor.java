package events.cesium.kafka.api.store;

/**
 * A committed recovery cursor for one tracker partition: a position-tracking offset plus a
 * versioned, size-bounded metadata blob (design §3.5, "cursor v2").
 *
 * <p>The engine commits this pair as the group-B {@code OffsetAndMetadata} via
 * {@code sendOffsetsToTransaction} — the offset-metadata channel is the one durable
 * completion-fact channel that shares Kafka's transactional atomicity (design §4.4 item 1).
 * Recovery seeds the index from the sidecar, then replays the tracker from {@code offset} to the
 * barrier.
 *
 * @param offset the next tracker offset replay starts from. Monotonic per partition; every pending
 *     entry either has a tracker ADD offset {@code >= offset} or is encoded in {@code metadata}
 *     (invariant I5)
 * @param metadata the pinned-entry <em>sidecar</em>: a versioned, Base64-encoded blob of the
 *     oldest pending entries (with their original tracker ADD offsets) plus self-describing
 *     identity material (cluster id, topic ids). Bounded by the validated sidecar byte budget
 *     ({@code dispatch.cursor.sidecar-max-bytes}, checked against broker
 *     {@code offset.metadata.max.bytes} at startup). Possibly empty, never {@code null}. The
 *     encoding is owned by the store and opaque to the engine
 */
public record TrackerCursor(long offset, String metadata) {}
