package com.jucius.cesium.kafka.app.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jucius.cesium.kafka.api.store.StoreCapabilities;
import com.jucius.cesium.kafka.api.store.StoreCapabilities.DispatchGuarantee;
import com.jucius.cesium.kafka.api.store.StoreCapabilities.TransactionAffinity;
import com.jucius.cesium.kafka.app.health.HealthAssessor;
import com.jucius.cesium.kafka.app.health.ShardRecovery;
import com.jucius.cesium.kafka.core.config.Role;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * HTTP-surface tests for the {@link ObservabilityServer} (design §9), broker-free: a
 * {@link FakeEngineHealth} drives the live/ready/degraded/shutting-down transitions and a real
 * {@link PrometheusMeterRegistry} backs {@code /metrics}. Every assertion is over the actual HTTP
 * status code and JSON/exposition body returned by a started server bound to an ephemeral port.
 */
class ObservabilityServerTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration GENEROUS_STALE = Duration.ofMinutes(5);

    private final HttpClient client = HttpClient.newHttpClient();

    private FakeEngineHealth health;
    private PrometheusMeterRegistry registry;
    private AtomicReference<ServiceInfo> info;
    private ObservabilityServer server;

    @BeforeEach
    void setUp() {
        health = new FakeEngineHealth(Role.INGEST, Role.DISPATCH);
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        info = new AtomicReference<>(sampleInfo());
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        registry.close();
    }

    /** Starts the server against the current fixture state and records the bound ephemeral port. */
    private void startServer() throws Exception {
        startServer("0.0.0.0", false);
    }

    /** Starts the server honoring the given bind address and {@code /info} detail level. */
    private void startServer(String bindAddress, boolean detailedInfo) throws Exception {
        server = new ObservabilityServer(
                bindAddress,
                0,
                detailedInfo,
                registry,
                new HealthAssessor(health, Clock.systemUTC(), GENEROUS_STALE),
                info::get);
        server.start();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send("GET", path);
    }

    private HttpResponse<String> send(String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void liveAndReadyWhenHealthy() throws Exception {
        startServer();

        HttpResponse<String> live = get("/health/live");
        assertEquals(200, live.statusCode());
        assertEquals("UP", JSON.readTree(live.body()).get("status").asText());

        HttpResponse<String> ready = get("/health/ready");
        JsonNode body = JSON.readTree(ready.body());
        assertEquals(200, ready.statusCode());
        assertEquals("READY", body.get("status").asText());
        assertTrue(body.get("startupComplete").asBoolean());
        assertFalse(body.get("shuttingDown").asBoolean());
        assertFalse(body.get("degraded").asBoolean());
        // Both started roles are reported in the per-loop detail.
        assertTrue(body.get("loops").has("ingest"));
        assertTrue(body.get("loops").has("dispatch"));
        assertTrue(body.get("loops").get("dispatch").get("assigned").asBoolean());
    }

    @Test
    void readyIsFalseBeforeStartupCompletes() throws Exception {
        health.startupComplete = false;
        startServer();

        HttpResponse<String> ready = get("/health/ready");
        assertEquals(503, ready.statusCode());
        JsonNode body = JSON.readTree(ready.body());
        assertEquals("NOT_READY", body.get("status").asText());
        assertFalse(body.get("startupComplete").asBoolean());

        // Liveness is independent of readiness: a started-but-not-yet-ready loop is still alive.
        assertEquals(200, get("/health/live").statusCode());
    }

    @Test
    void readyFlipsFalseOnShutdownButLivenessHolds() throws Exception {
        health.shuttingDown = true;
        startServer();

        HttpResponse<String> ready = get("/health/ready");
        assertEquals(503, ready.statusCode());
        JsonNode body = JSON.readTree(ready.body());
        assertEquals("NOT_READY", body.get("status").asText());
        assertTrue(body.get("shuttingDown").asBoolean());

        // preStop drain: readiness is false but the instance keeps serving and stays live.
        assertEquals(200, get("/health/live").statusCode());
    }

    @Test
    void livenessFailsWhenLoopDead() throws Exception {
        health.loopAlive = false;
        startServer();

        HttpResponse<String> live = get("/health/live");
        assertEquals(503, live.statusCode());
        assertEquals("DOWN", JSON.readTree(live.body()).get("status").asText());
    }

    @Test
    void livenessFailsWhenHeartbeatStale() throws Exception {
        health.heartbeatFresh = false;
        startServer();

        HttpResponse<String> live = get("/health/live");
        assertEquals(503, live.statusCode());
        JsonNode ingest = JSON.readTree(live.body()).get("loops").get("ingest");
        assertTrue(ingest.get("alive").asBoolean());
        assertFalse(ingest.get("fresh").asBoolean());
    }

    @Test
    void readyFailsWhenConsumerUnassigned() throws Exception {
        health.consumerAssigned = false;
        startServer();

        assertEquals(503, get("/health/ready").statusCode());
        // Still alive: assignment gates readiness, not liveness.
        assertEquals(200, get("/health/live").statusCode());
    }

    @Test
    void degradedInstanceStaysReadyWithCauseDetail() throws Exception {
        health.degraded = true;
        health.degradedCause = "destination ISR below min.insync (parked, retrying)";
        startServer();

        HttpResponse<String> ready = get("/health/ready");
        // Decoupled (§3.8): degraded is detail, never a probe failure.
        assertEquals(200, ready.statusCode());
        JsonNode body = JSON.readTree(ready.body());
        assertEquals("READY", body.get("status").asText());
        assertTrue(body.get("degraded").asBoolean());
        assertTrue(body.get("degradedCause").asText().contains("min.insync"));
    }

    @Test
    void recoveringShardIsReadyWithDecoupledDetail() throws Exception {
        health.recovering = List.of(new ShardRecovery(7, ShardRecovery.State.RECOVERING, 12_000, 4_500));
        startServer();

        HttpResponse<String> ready = get("/health/ready");
        // Decoupled from recovery (D21): a replaying instance is READY.
        assertEquals(200, ready.statusCode());
        JsonNode recovery = JSON.readTree(ready.body()).get("recovery");
        assertEquals(1, recovery.size());
        JsonNode shard = recovery.get(0);
        assertEquals(7, shard.get("partition").asInt());
        assertEquals("RECOVERING", shard.get("state").asText());
        assertEquals(12_000, shard.get("recordsRemaining").asLong());
        assertEquals(4_500, shard.get("etaMillis").asLong());
    }

    @Test
    void metricsRendersRegisteredMeter() throws Exception {
        Counter.builder("cesium.test.events")
                .tag("outcome", "relayed")
                .register(registry)
                .increment(3);
        startServer();

        HttpResponse<String> metrics = get("/metrics");
        assertEquals(200, metrics.statusCode());
        assertTrue(metrics.headers().firstValue("content-type").orElse("").startsWith("text/plain"));
        assertTrue(metrics.body().contains("cesium_test_events"), metrics.body());
    }

    @Test
    void infoDefaultRedactsSensitiveFields() throws Exception {
        // detailed-info OFF by default (security M3): version/build for ops, nothing reconnaissance-worthy.
        startServer();

        HttpResponse<String> response = get("/info");
        assertEquals(200, response.statusCode());
        JsonNode body = JSON.readTree(response.body());
        assertEquals("1.2.3", body.get("version").asText());
        assertEquals("abc1234", body.get("gitCommit").asText());
        assertEquals("kafka-tracker", body.get("store").get("type").asText());
        // The applicationId is the fencing-id seed — it must not leak on the unauthenticated endpoint.
        assertFalse(body.has("applicationId"), body.toString());
        assertFalse(body.has("roles"), body.toString());
        assertFalse(body.has("acknowledgments"), body.toString());
        assertFalse(body.get("store").has("capabilities"), body.toString());
    }

    @Test
    void infoDetailedExposesBuildStoreRolesAndAcknowledgments() throws Exception {
        // Opt-in detailed mode (observability.detailed-info=true) discloses the operational fields.
        startServer("0.0.0.0", true);

        HttpResponse<String> response = get("/info");
        assertEquals(200, response.statusCode());
        JsonNode body = JSON.readTree(response.body());
        assertEquals("1.2.3", body.get("version").asText());
        assertEquals("abc1234", body.get("gitCommit").asText());
        assertEquals("orders-delay", body.get("applicationId").asText());
        assertEquals("kafka-tracker", body.get("store").get("type").asText());
        assertEquals(
                "KAFKA_TRANSACTIONAL",
                body.get("store").get("capabilities").get("affinity").asText());
        assertEquals(List.of("ingest", "dispatch"), readStrings(body.get("roles")));
        assertEquals(List.of("size-based-retention"), readStrings(body.get("acknowledgments")));
    }

    @Test
    void enablesSlowClientReapingAndBoundsTheDispatchPool() throws Exception {
        // M1: the unauthenticated server must reap slow clients and bound in-flight handling so a
        // slowloris cannot pin every dispatcher thread and starve the health probes.
        startServer();

        String maxReq = System.getProperty(ObservabilityServer.MAX_REQ_TIME_PROPERTY);
        String maxRsp = System.getProperty(ObservabilityServer.MAX_RSP_TIME_PROPERTY);
        assertNotNull(maxReq, "maxReqTime must be set before HttpServer.create");
        assertNotNull(maxRsp, "maxRspTime must be set before HttpServer.create");
        assertTrue(Long.parseLong(maxReq) > 0, maxReq);
        assertTrue(Long.parseLong(maxRsp) > 0, maxRsp);
        // In-flight handling is bounded by a modest fixed pool, larger than the original 2 threads.
        assertEquals(ObservabilityServer.DISPATCH_THREADS, server.dispatchThreadCeiling());
        assertTrue(ObservabilityServer.DISPATCH_THREADS > 2, "the dispatch pool was modestly increased");
    }

    @Test
    void honorsConfiguredLoopbackBindAddress() throws Exception {
        // L1: binding 127.0.0.1 restricts the listener to loopback rather than every interface.
        startServer("127.0.0.1", false);

        assertTrue(server.boundAddress().isLoopbackAddress(), () -> "bound to " + server.boundAddress());
        // And it still answers on loopback.
        assertEquals(200, get("/health/live").statusCode());
    }

    @Test
    void defaultBindAddressIsWildcard() throws Exception {
        // L1: the default keeps the wildcard bind so k8s probes reach the listener.
        startServer();

        assertTrue(server.boundAddress().isAnyLocalAddress(), () -> "bound to " + server.boundAddress());
    }

    @Test
    void methodOtherThanGetIsRejected() throws Exception {
        startServer();
        assertEquals(405, send("POST", "/health/live").statusCode());
    }

    @Test
    void unknownPathUnderAContextIsNotFound() throws Exception {
        startServer();
        // The JDK server matches contexts by prefix; handlers enforce their exact path.
        assertEquals(404, get("/metrics/extra").statusCode());
    }

    private static List<String> readStrings(JsonNode array) {
        List<String> values = new java.util.ArrayList<>();
        for (JsonNode element : array) {
            values.add(element.asText());
        }
        return values;
    }

    private static ServiceInfo sampleInfo() {
        return new ServiceInfo(
                new BuildInfo("1.2.3", Optional.of("abc1234")),
                "orders-delay",
                List.of("ingest", "dispatch"),
                "kafka-tracker",
                Optional.of(new StoreCapabilities(
                        TransactionAffinity.KAFKA_TRANSACTIONAL, DispatchGuarantee.EXACTLY_ONCE, true, false)),
                List.of("size-based-retention"));
    }
}
