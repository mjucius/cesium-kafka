package com.jucius.cesium.kafka.app;

/**
 * The discovery seam for the observability HTTP server (design §9). {@link CesiumApp} owns the
 * lifecycle (engine, config, shutdown) and the parallel observability module
 * ({@code com.jucius.cesium.kafka.app.metrics}) owns the server itself; this {@link java.util.ServiceLoader}
 * hook lets the app start the server <em>before</em> the engine (so probes answer during startup)
 * and stop it after readiness has flipped false on shutdown — without a compile-time dependency on
 * the server class, so the two modules evolve independently.
 *
 * <p><strong>Contract for implementors.</strong> Register a provider in
 * {@code META-INF/services/com.jucius.cesium.kafka.app.ObservabilityServerFactory}; the app loads the
 * single registered factory (more than one is a startup error) and calls {@link #start} once. The
 * returned {@link AutoCloseable} <em>is already serving</em>; the app closes it during graceful
 * shutdown, after {@code EngineHealth.shuttingDown()} is already true so the final probes report
 * NOT_READY. When no provider is registered the app logs a warning and runs without the HTTP
 * surface (the engine is unaffected).
 */
@FunctionalInterface
public interface ObservabilityServerFactory {

    /**
     * Builds and starts the observability HTTP server bound to {@code runtime.port()}.
     *
     * @param runtime the registry, health seam, clock, liveness window, and {@code /info} supplier
     * @return a handle whose {@link AutoCloseable#close()} stops the server; never {@code null}
     * @throws Exception when the server cannot bind or start (the app treats this as fatal)
     */
    AutoCloseable start(ObservabilityRuntime runtime) throws Exception;
}
