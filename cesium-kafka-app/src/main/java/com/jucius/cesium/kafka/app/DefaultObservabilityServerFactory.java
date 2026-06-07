package com.jucius.cesium.kafka.app;

import com.jucius.cesium.kafka.app.health.HealthAssessor;
import com.jucius.cesium.kafka.app.metrics.ObservabilityServer;

/**
 * The production {@link ObservabilityServerFactory}, discovered by {@link CesiumApp} through
 * {@link java.util.ServiceLoader} (registered in
 * {@code META-INF/services/com.jucius.cesium.kafka.app.ObservabilityServerFactory}). It bridges the
 * lifecycle module's {@link ObservabilityRuntime} read-side seam to the observability module's
 * {@link ObservabilityServer} (design §9) without either module depending on the other at compile
 * time: {@code CesiumApp} references only this package's {@link ObservabilityServerFactory} interface
 * and {@code ObservabilityRuntime}, and this factory is the single point that names the concrete
 * server.
 *
 * <p>{@link #start(ObservabilityRuntime)} builds a {@link HealthAssessor} over the engine-written
 * {@code EngineHealth} (with the app's clock and liveness-staleness window), wires {@code /metrics}
 * to the shared registry's scrape and {@code /info} to the per-request {@code ServiceInfo} supplier,
 * binds {@code runtime.port()}, and returns the already-serving {@link ObservabilityServer} as the
 * {@link AutoCloseable} the app closes during graceful shutdown (after readiness has flipped false).
 */
public final class DefaultObservabilityServerFactory implements ObservabilityServerFactory {

    @Override
    public AutoCloseable start(ObservabilityRuntime runtime) throws Exception {
        HealthAssessor assessor = new HealthAssessor(runtime.health(), runtime.clock(), runtime.livenessStaleAfter());
        ObservabilityServer server =
                new ObservabilityServer(runtime.port(), runtime.registry(), assessor, runtime.serviceInfo());
        server.start();
        return server;
    }
}
