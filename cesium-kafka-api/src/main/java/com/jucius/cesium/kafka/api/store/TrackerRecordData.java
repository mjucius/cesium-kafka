package com.jucius.cesium.kafka.api.store;

import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * Opaque bytes destined for the tracker topic, encoded by a {@link TrackerBackedStore} and
 * produced by the <em>engine's</em> transactional producer inside its transactions.
 *
 * <p>The wire format is owned by the store and opaque to the engine — the engine never inspects
 * these bytes, it only routes them to the tracker partition matching the source partition.
 *
 * <p>This is a zero-copy transport envelope, not a value type: the array components are
 * intentionally not defensively copied, and {@code equals}/{@code hashCode} inherit record
 * semantics over array references (identity). Do not use instances as map keys or compare them
 * for equality.
 *
 * @param key the record key (for the v1 wire format, the 8-byte big-endian source offset — the
 *     compaction identity, design §2.2)
 * @param value the record value, or {@code null} for a completion <em>tombstone</em> — compaction
 *     requires tombstones to have null values (design D15)
 * @param headers record headers; tombstones legally carry the completion reason here because
 *     headers survive where values cannot
 */
// ArrayRecordComponent: byte[] components are dictated by the design §4.2 sketch — this is a
// zero-copy hot-path transport envelope (no defensive copies, no value-equality use), see javadoc.
@SuppressWarnings("ArrayRecordComponent")
public record TrackerRecordData(byte[] key, byte @Nullable [] value, Headers headers) {}
