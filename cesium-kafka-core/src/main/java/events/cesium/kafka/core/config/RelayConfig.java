package events.cesium.kafka.core.config;

import events.cesium.kafka.core.headers.RelayPartitioning;
import events.cesium.kafka.core.headers.RelayTimestampPolicy;
import java.util.Objects;

/**
 * Relay fidelity settings (design §2.4). The components are the same enum types the
 * {@link events.cesium.kafka.core.headers.RelayRecordFactory} consumes — one type per concept, so
 * config wires into the relay path without translation.
 *
 * @param timestamp which timestamp the relayed record carries; default
 *     {@link RelayTimestampPolicy#DISPATCH} because a delayed record carrying an hours-old
 *     CreateTime can violate the destination's {@code message.timestamp.difference.max.ms} or skew
 *     time-based retention — the original time is recoverable from the
 *     {@code cesium-source-timestamp} provenance header
 * @param partitioning destination partitioning; default {@link RelayPartitioning#BY_KEY} because
 *     the destination partition count may differ from the source's
 */
public record RelayConfig(RelayTimestampPolicy timestamp, RelayPartitioning partitioning) {

    /** Materializes the §8 defaults for absent components. */
    public RelayConfig {
        timestamp = Objects.requireNonNullElse(timestamp, RelayTimestampPolicy.DISPATCH);
        partitioning = Objects.requireNonNullElse(partitioning, RelayPartitioning.BY_KEY);
    }

    /** Returns a {@code RelayConfig} populated entirely from the §8 defaults table. */
    public static RelayConfig defaults() {
        return new RelayConfig(RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
    }
}
