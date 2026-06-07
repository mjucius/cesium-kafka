package com.jucius.cesium.kafka.core.config;

import com.jucius.cesium.kafka.core.headers.RelayPartitioning;
import com.jucius.cesium.kafka.core.headers.RelayTimestampPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import java.util.Objects;

/**
 * Relay fidelity and failure settings (design §2.4). The components are the same enum types the
 * {@link com.jucius.cesium.kafka.core.headers.RelayRecordFactory} and the ingest/dispatch loops
 * consume — one type per concept, so config wires into the relay path without translation.
 *
 * @param timestamp which timestamp the relayed record carries; default
 *     {@link RelayTimestampPolicy#DISPATCH} because a delayed record carrying an hours-old
 *     CreateTime can violate the destination's {@code message.timestamp.difference.max.ms} or skew
 *     time-based retention — the original time is recoverable from the
 *     {@code cesium-source-timestamp} provenance header
 * @param partitioning destination partitioning; default {@link RelayPartitioning#BY_KEY} because
 *     the destination partition count may differ from the source's
 * @param onUnrelayable policy for records the destination <em>permanently</em> rejects on produce
 *     (too large, invalid); default {@link UnrelayablePolicy#DLQ} (§3.8 I-8) — routing the poison
 *     record to the DLQ keeps the source partition moving instead of wedging it behind an infinite
 *     abort/rewind/retry loop. Governs both the ingest immediate-relay and dispatch delayed-relay
 *     paths; transient produce failures are unaffected (they stay on the retry path)
 */
public record RelayConfig(
        RelayTimestampPolicy timestamp, RelayPartitioning partitioning, UnrelayablePolicy onUnrelayable) {

    /** Materializes the §8 defaults for absent components. */
    public RelayConfig {
        timestamp = Objects.requireNonNullElse(timestamp, RelayTimestampPolicy.DISPATCH);
        partitioning = Objects.requireNonNullElse(partitioning, RelayPartitioning.BY_KEY);
        onUnrelayable = Objects.requireNonNullElse(onUnrelayable, UnrelayablePolicy.DLQ);
    }

    /** Returns a {@code RelayConfig} populated entirely from the §8 defaults table. */
    public static RelayConfig defaults() {
        return new RelayConfig(RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY, UnrelayablePolicy.DLQ);
    }
}
