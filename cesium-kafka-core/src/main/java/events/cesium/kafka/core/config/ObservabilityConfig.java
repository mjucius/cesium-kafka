package events.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Observability endpoint settings (design §9): the HTTP server serving {@code /metrics},
 * {@code /health/live}, {@code /health/ready}, and {@code /info}.
 *
 * @param port listen port; default 8081
 */
public record ObservabilityConfig(Integer port) {

    /** Default observability port (§8 defaults table). */
    public static final int DEFAULT_PORT = 8081;

    /** Materializes the §8 defaults for absent components. */
    public ObservabilityConfig {
        port = Objects.requireNonNullElse(port, DEFAULT_PORT);
    }

    /** Returns an {@code ObservabilityConfig} populated entirely from the §8 defaults table. */
    public static ObservabilityConfig defaults() {
        return new ObservabilityConfig(DEFAULT_PORT);
    }
}
