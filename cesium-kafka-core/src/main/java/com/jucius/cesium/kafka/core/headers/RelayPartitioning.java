package com.jucius.cesium.kafka.core.headers;

/**
 * Destination partitioning policy, config {@code relay.partitioning} (design §2.4).
 *
 * <p>{@link #BY_KEY} is the default because the destination's partition count may differ from the
 * source's; {@link #SOURCE_PARTITION} requires equal partition counts (validated at startup, not
 * here).
 */
public enum RelayPartitioning {
    /** Leave the partition unset and let the producer's keyed partitioner choose. Default. */
    BY_KEY,
    /** Pin the destination partition to the source partition number. Opt-in. */
    SOURCE_PARTITION
}
