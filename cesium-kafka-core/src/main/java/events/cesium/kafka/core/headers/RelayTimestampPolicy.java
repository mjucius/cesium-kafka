package events.cesium.kafka.core.headers;

/**
 * Destination record timestamp policy, config {@code relay.timestamp} (design §2.4).
 *
 * <p>{@link #DISPATCH} is the default: a delayed record carrying an hours-old CreateTime can
 * violate the destination's {@code message.timestamp.difference.max.ms} or skew time-based
 * retention; the original time remains recoverable from the {@code cesium-source-timestamp}
 * provenance header.
 */
public enum RelayTimestampPolicy {
    /** Stamp the relay instant (now) as the destination record timestamp. Default. */
    DISPATCH,
    /**
     * Carry the source record's timestamp through; records without a timestamp fall back to
     * broker/producer assignment.
     */
    SOURCE
}
