package com.jucius.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Header options (design §2.4).
 *
 * <p>The boxed component type is deliberate: binding layers pass {@code null} for an absent key and
 * the compact constructor materializes the documented default — a primitive {@code boolean} could
 * not distinguish "absent" from an explicit {@code false}.
 *
 * @param stampProvenance default {@code true}: stamp {@code cesium-relayed-at},
 *     {@code cesium-source-*}, and {@code cesium-scheduled-for} provenance headers on relay
 */
public record HeadersConfig(Boolean stampProvenance) {

    /** Materializes the §8 defaults for absent components. */
    public HeadersConfig {
        stampProvenance = Objects.requireNonNullElse(stampProvenance, true);
    }

    /** Returns a {@code HeadersConfig} populated entirely from the §8 defaults table. */
    public static HeadersConfig defaults() {
        return new HeadersConfig(true);
    }
}
