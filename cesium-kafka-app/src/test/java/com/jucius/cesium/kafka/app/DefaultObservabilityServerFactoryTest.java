package com.jucius.cesium.kafka.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.app.health.MutableEngineHealth;
import com.jucius.cesium.kafka.app.metrics.BuildInfo;
import com.jucius.cesium.kafka.app.metrics.ObservabilityServer;
import com.jucius.cesium.kafka.app.metrics.ServiceInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/**
 * Proves the {@link ObservabilityServerFactory} ServiceLoader seam {@link CesiumApp} relies on is
 * really wired: exactly one provider is registered in
 * {@code META-INF/services/com.jucius.cesium.kafka.app.ObservabilityServerFactory}, it is the
 * {@link DefaultObservabilityServerFactory}, and {@link DefaultObservabilityServerFactory#start}
 * returns an already-serving HTTP surface backed by the runtime's registry and health seam — the
 * surface that is silently absent (logged warning, no endpoints) when no provider is registered.
 */
class DefaultObservabilityServerFactoryTest {

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void exactlyOneFactoryIsRegisteredViaServiceLoader() {
        List<ObservabilityServerFactory> factories = ServiceLoader.load(ObservabilityServerFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        assertEquals(1, factories.size(), () -> "expected one provider, got " + factories);
        assertInstanceOf(DefaultObservabilityServerFactory.class, factories.get(0));
    }

    @Test
    void startedServerServesHealthAndMetricsThenStopsOnClose() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Counter.builder("cesium.test.events").register(registry).increment();
        MutableEngineHealth health = new MutableEngineHealth(Clock.systemUTC());
        ObservabilityRuntime runtime = new ObservabilityRuntime(
                "0.0.0.0",
                0,
                false,
                registry,
                health,
                Clock.systemUTC(),
                Duration.ofMinutes(5),
                DefaultObservabilityServerFactoryTest::info);

        ObservabilityServerFactory factory =
                ServiceLoader.load(ObservabilityServerFactory.class).findFirst().orElseThrow();

        int port;
        try (AutoCloseable handle = factory.start(runtime)) {
            // The factory hands back the concrete server so the app can stop it; tests read its port.
            ObservabilityServer server = assertInstanceOf(ObservabilityServer.class, handle);
            port = server.port();

            // Liveness is UP with no loops registered; readiness is NOT_READY until startup completes.
            assertEquals(200, get(port, "/health/live").statusCode());
            assertEquals(503, get(port, "/health/ready").statusCode());

            HttpResponse<String> metrics = get(port, "/metrics");
            assertEquals(200, metrics.statusCode());
            assertTrue(metrics.body().contains("cesium_test_events"), metrics.body());

            health.markStartupComplete();
            assertEquals(200, get(port, "/health/ready").statusCode());
        }

        // close() released the port: a follow-up request can no longer connect.
        assertThrows(ConnectException.class, () -> get(port, "/health/live"));
        registry.close();
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static ServiceInfo info() {
        return new ServiceInfo(
                new BuildInfo("test", Optional.empty()),
                "factory-test",
                List.of("ingest", "dispatch"),
                "kafka-tracker",
                Optional.empty(),
                List.of());
    }
}
