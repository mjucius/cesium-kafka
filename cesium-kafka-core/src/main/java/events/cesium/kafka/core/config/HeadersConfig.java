package events.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Control-header protocol options (design §2.3, §2.4).
 *
 * <p>Boxed component types are deliberate: binding layers pass {@code null} for absent keys and
 * the compact constructor materializes the documented default — a primitive {@code boolean} could
 * not distinguish "absent" from an explicit {@code false}.
 *
 * @param stampProvenance default {@code true}: stamp {@code cesium-relayed-at},
 *     {@code cesium-source-*}, and {@code cesium-scheduled-for} provenance headers on relay
 * @param acceptBinaryLongValues default {@code false}: accept 8-byte big-endian long header
 *     values <em>instead of</em> canonical ASCII decimal — the modes are exclusive because
 *     "8 ASCII digits" vs "8-byte long" is ambiguous (D1)
 */
public record HeadersConfig(Boolean stampProvenance, Boolean acceptBinaryLongValues) {

    /** Materializes the §8 defaults for absent components. */
    public HeadersConfig {
        stampProvenance = Objects.requireNonNullElse(stampProvenance, true);
        acceptBinaryLongValues = Objects.requireNonNullElse(acceptBinaryLongValues, false);
    }

    /** Returns a {@code HeadersConfig} populated entirely from the §8 defaults table. */
    public static HeadersConfig defaults() {
        return new HeadersConfig(true, false);
    }
}
