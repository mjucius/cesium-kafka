package com.jucius.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Observability endpoint settings (design §9): the HTTP server serving {@code /metrics},
 * {@code /health/live}, {@code /health/ready}, and {@code /info}.
 *
 * @param port listen port; default 8081
 * @param bindAddress the interface the listener binds; default {@code 0.0.0.0} (all interfaces) so
 *     k8s liveness/readiness probes reach it. Set {@code 127.0.0.1} to restrict the unauthenticated
 *     endpoints to a loopback sidecar scrape; on {@code 0.0.0.0} network-restrict the port (security L1).
 * @param detailedInfo whether {@code /info} discloses the sensitive/operational fields
 *     (applicationId — the fencing-id seed — roles, store capabilities, acknowledgments); default
 *     {@code false} so the unauthenticated endpoint exposes only version/build (security M3). Version
 *     and build provenance are always present.
 */
public record ObservabilityConfig(Integer port, String bindAddress, Boolean detailedInfo) {

    /** Default observability port (§8 defaults table). */
    public static final int DEFAULT_PORT = 8081;

    /** Default bind address: all interfaces, so k8s liveness/readiness probes reach the listener (set 127.0.0.1 to restrict, L1). */
    public static final String DEFAULT_BIND_ADDRESS = "0.0.0.0";

    /** Default {@code /info} detail level: sensitive fields are off on the unauthenticated endpoint. */
    public static final boolean DEFAULT_DETAILED_INFO = false;

    /** Materializes the §8 defaults for absent components. */
    public ObservabilityConfig {
        port = Objects.requireNonNullElse(port, DEFAULT_PORT);
        bindAddress = Objects.requireNonNullElse(bindAddress, DEFAULT_BIND_ADDRESS);
        detailedInfo = Objects.requireNonNullElse(detailedInfo, DEFAULT_DETAILED_INFO);
    }

    /** Returns an {@code ObservabilityConfig} populated entirely from the §8 defaults table. */
    public static ObservabilityConfig defaults() {
        return new ObservabilityConfig(DEFAULT_PORT, DEFAULT_BIND_ADDRESS, DEFAULT_DETAILED_INFO);
    }
}
