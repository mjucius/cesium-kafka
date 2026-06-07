package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.app.health.HealthAssessor;
import com.jucius.cesium.kafka.app.lifecycle.CesiumEngine;
import com.jucius.cesium.kafka.app.metrics.BuildInfo;
import com.jucius.cesium.kafka.app.metrics.ObservabilityServer;
import com.jucius.cesium.kafka.app.metrics.ServiceInfo;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The end-to-end product proof <em>through the app</em> (design §9, §11.3, M7): the production
 * {@link CesiumEngine} (the hardened {@code EngineHarness}) plus the production
 * {@link ObservabilityServer} — exactly the wiring {@code CesiumApp.main} performs — run in-process
 * against the shared {@link KafkaIT} broker with {@code roles=[ingest, dispatch]}. It asserts:
 *
 * <ul>
 *   <li>{@code /health/ready} answers {@code 503} until startup completes, then {@code 200} (the probe
 *       answers throughout startup because the server starts before the engine);
 *   <li>a {@code cesium-delay-ms} record produced to the source arrives on the destination at-or-after
 *       its requested instant, exactly once under {@code read_committed} — delivered by the engine,
 *       not the test;
 *   <li>{@code /metrics} serves the engine's {@code cesium_*} meters from the shared registry;
 *   <li>readiness carries the recovery/degraded decoupling detail (D21, §3.8) while staying ready;
 *   <li>{@code engine.stop()} flips readiness {@code 503} (shutdown signalled before clients close),
 *       joins cleanly with no fatal loop death, and the consumed source offset is committed.
 * </ul>
 *
 * <p>The first-run group-A offset seed is the documented explicit-operator step (D18/§3.6): the
 * production engine does not auto-seed, so the test performs it before {@code start()} exactly as the
 * quickstart's {@code topic-init} container does.
 */
class AppQuickstartIT extends KafkaIT {

    /** CI-tolerant lateness ceiling: dispatch must land at-or-after the schedule, within this much. */
    private static final long LATENESS_BOUND_MS = 20_000;

    private static final Duration SHUTDOWN_BUDGET = Duration.ofSeconds(30);

    /** Comfortably above the dispatch poll ceiling so an idle loop is never a false-negative (§6). */
    private static final Duration LIVENESS_STALE_AFTER = Duration.ofSeconds(90);

    private final HttpClient http = HttpClient.newHttpClient();

    private CesiumConfig config;
    private PrometheusMeterRegistry registry;
    private CesiumEngine engine;
    private ObservabilityServer server;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        if (server != null) {
            server.stop();
        }
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    void appServesProbesAndDelaysExactlyOnceEndToEnd() throws Exception {
        String source = createTopic("src", 3);
        String destination = createTopic("dst", 3);
        String dlq = createTopic("dlq", 1);

        // Build a production-shaped config the same way the harness does, then drop the throwaway
        // harness (we drive the production CesiumEngine directly, not the harness's loops).
        try (EngineHarness configSource = EngineHarness.builder()
                .instanceId("cesium-0")
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build()) {
            config = configSource.config();
        }
        String ingestGroup = KafkaClientFactory.ingestGroupId(config.applicationId());
        seedFirstRunOffsets(source, ingestGroup);

        Clock clock = Clock.systemUTC();
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        engine = new CesiumEngine(config, registry, clock, SHUTDOWN_BUDGET);
        server = new ObservabilityServer(
                0, registry, new HealthAssessor(engine.health(), clock, LIVENESS_STALE_AFTER), this::serviceInfo);
        server.start();

        // Server up, engine not started: liveness is UP (no loops to be wedged), readiness is NOT_READY
        // until the startup checks pass — exactly what a k8s readiness gate needs during rollout.
        assertEquals(200, get("/health/live").statusCode());
        assertEquals(503, get("/health/ready").statusCode());

        engine.start();

        // Produce one delayed record now so the ingest ADD wakes the dispatch poll promptly.
        long sourceTimestamp = System.currentTimeMillis();
        long delayMs = 5_000;
        long scheduledFor = sourceTimestamp + delayMs;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            produce(
                    producer,
                    source,
                    0,
                    sourceTimestamp,
                    "k1",
                    "v1",
                    h("x-app", "keep-me"),
                    h(CesiumHeaders.DELAY_MS, Long.toString(delayMs)));
            producer.flush();
        }

        // Readiness flips to 200 once startup is complete and both loops are iterating + assigned.
        await("readiness 200 after startup completes")
                .atMost(WAIT)
                .pollInSameThread()
                .until(() -> get("/health/ready").statusCode() == 200);

        HttpResponse<String> info = get("/info");
        assertEquals(200, info.statusCode());
        assertTrue(info.body().contains(config.applicationId()), info.body());

        // The headline proof: delivered THROUGH THE APP, at-or-after the requested instant, exactly once.
        List<ConsumerRecord<byte[], byte[]>> delivered = readExactlyCommitted(destination, 1, WAIT);
        ConsumerRecord<byte[], byte[]> record = delivered.get(0);
        assertArrayEquals(bytes("v1"), record.value(), "payload preserved byte-for-byte");
        assertEquals("keep-me", headerValue(record.headers(), "x-app"), "producer header preserved");
        assertEquals(
                Long.toString(scheduledFor),
                headerValue(record.headers(), CesiumHeaders.SCHEDULED_FOR),
                "cesium-scheduled-for carries the requested instant");
        assertTrue(
                record.timestamp() >= scheduledFor,
                () -> "dispatched " + (scheduledFor - record.timestamp()) + " ms BEFORE the requested instant");
        assertTrue(
                record.timestamp() <= scheduledFor + LATENESS_BOUND_MS,
                () -> "dispatched " + (record.timestamp() - scheduledFor) + " ms late (bound " + LATENESS_BOUND_MS
                        + " ms)");

        // /metrics serves the engine's cesium_* meters from the very registry the engine feeds.
        HttpResponse<String> metrics = get("/metrics");
        assertEquals(200, metrics.statusCode());
        assertTrue(
                metrics.body().contains("cesium_loop_last_iteration_timestamp_seconds"),
                "the per-loop heartbeat gauge feeding liveness must be exposed");
        assertTrue(
                metrics.body().contains("cesium_ingest_records_total"), "ingest disposition counter must be exposed");

        // Recovery + degraded are readiness DETAIL, not gates (D21, §3.8): the instance stays ready.
        HttpResponse<String> ready = get("/health/ready");
        assertEquals(200, ready.statusCode());
        assertTrue(ready.body().contains("\"recovery\":"), ready.body());
        assertTrue(ready.body().contains("\"degraded\":false"), ready.body());

        // Graceful stop: readiness flips false (shutdown signalled before any client closes), every
        // loop joins within the budget, no fatal death.
        boolean clean = engine.stop(SHUTDOWN_BUDGET);
        assertTrue(clean, "all loops joined within the shutdown budget");
        assertTrue(engine.fatalCause().isEmpty(), "clean shutdown: no fatal loop death");

        HttpResponse<String> afterStop = get("/health/ready");
        assertEquals(503, afterStop.statusCode());
        assertTrue(afterStop.body().contains("\"shuttingDown\":true"), afterStop.body());

        // Committed cleanly: the consumed source offset advanced (EOS read-process-write committed it).
        OffsetAndMetadata committed = committedOffsets(ingestGroup).get(new TopicPartition(source, 0));
        assertNotNull(committed, "ingest group has a committed offset for the consumed partition");
        assertTrue(committed.offset() >= 1, () -> "expected committed offset >= 1, got " + committed.offset());
    }

    /**
     * The documented first-run operator step (D18/§3.6, I-9 runbook): {@code auto.offset.reset} is
     * locked to {@code none}, so seed committed offset 0 for every source partition before the ingest
     * loop starts — exactly what the quickstart's {@code topic-init} container does via the CLI.
     */
    private static void seedFirstRunOffsets(String source, String ingestGroup) {
        int partitions = get(ADMIN.describeTopics(List.of(source)).allTopicNames())
                .get(source)
                .partitions()
                .size();
        Map<TopicPartition, OffsetAndMetadata> seed = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            seed.put(new TopicPartition(source, p), new OffsetAndMetadata(0L));
        }
        get(ADMIN.alterConsumerGroupOffsets(ingestGroup, seed).all());
    }

    private ServiceInfo serviceInfo() {
        return new ServiceInfo(
                new BuildInfo("integration-test", Optional.empty()),
                config.applicationId(),
                List.of("ingest", "dispatch"),
                config.store().type(),
                engine.storeCapabilities(),
                List.of());
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .GET()
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
