package com.jucius.cesium.kafka.app.metrics;

import com.jucius.cesium.kafka.app.health.HealthAssessor;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The observability HTTP server (design §9): a JDK {@link HttpServer} (zero extra dependencies)
 * serving {@code /metrics} (Prometheus exposition from the engine's {@link PrometheusMeterRegistry}),
 * {@code /health/live}, {@code /health/ready} (decoupled from shard recovery, D21), and {@code
 * /info}. Every handler is cheap and non-blocking — it reads cached health atomics or scrapes the
 * in-memory registry, never calling Kafka synchronously.
 *
 * <p><strong>Lifecycle.</strong> The app {@link #start() starts} this server <em>before</em> the
 * engine, so liveness/readiness probes answer throughout startup (readiness reports {@code
 * NOT_READY} until the startup checks pass), and {@link #stop() stops} it after readiness has
 * already flipped false on the shutdown signal (preStop drain). {@code start()}/{@code stop()} are a
 * single life: {@code stop()} is idempotent and also runs from {@link #close()}.
 *
 * <p>Handlers run on a tiny daemon thread pool so a slower {@code /metrics} scrape never blocks a
 * health probe and the pool never keeps the JVM alive on shutdown.
 */
public final class ObservabilityServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityServer.class);

    /** Seconds the JDK server waits for in-flight exchanges to finish before closing connections. */
    private static final int STOP_GRACE_SECONDS = 1;

    /** Two daemon dispatcher threads: a slow scrape cannot starve a concurrent health probe. */
    private static final int DISPATCH_THREADS = 2;

    private final int requestedPort;
    private final Supplier<String> metricsScrape;
    private final HealthAssessor health;
    private final Supplier<ServiceInfo> info;

    private @Nullable HttpServer server;
    private @Nullable ExecutorService dispatcher;

    /**
     * @param port the listen port ({@code observability.port}, default 8081; {@code 0} binds an
     *     ephemeral port — used by tests, read back via {@link #port()})
     * @param registry the engine's Prometheus registry; {@code /metrics} renders {@code
     *     registry.scrape()}
     * @param health the assessor that derives the liveness/readiness verdicts from the engine's
     *     {@code EngineHealth} signals
     * @param info supplies the current {@code /info} snapshot on each request
     */
    public ObservabilityServer(
            int port, PrometheusMeterRegistry registry, HealthAssessor health, Supplier<ServiceInfo> info) {
        this(port, registry::scrape, health, info);
    }

    /**
     * Test/decoupling seam: the {@code /metrics} body comes from an arbitrary scrape supplier rather
     * than a {@link PrometheusMeterRegistry}, so the HTTP surface can be exercised without a registry.
     */
    ObservabilityServer(int port, Supplier<String> metricsScrape, HealthAssessor health, Supplier<ServiceInfo> info) {
        this.requestedPort = port;
        this.metricsScrape = metricsScrape;
        this.health = health;
        this.info = info;
    }

    /** Binds the port, registers the handlers, and starts serving. */
    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("observability server already started");
        }
        HttpServer http = HttpServer.create(new InetSocketAddress(requestedPort), 0);
        http.createContext(MetricsHttpHandler.PATH, new MetricsHttpHandler(metricsScrape));
        http.createContext("/health/live", new HealthHttpHandler("/health/live", health::liveness));
        http.createContext("/health/ready", new HealthHttpHandler("/health/ready", health::readiness));
        http.createContext(InfoHttpHandler.PATH, new InfoHttpHandler(info));
        ExecutorService exec = Executors.newFixedThreadPool(DISPATCH_THREADS, daemonFactory());
        http.setExecutor(exec);
        http.start();
        this.dispatcher = exec;
        this.server = http;
        log.info("observability endpoints listening on port {}", port());
    }

    /** The actually-bound port (resolves the ephemeral port when started with {@code 0}). */
    public synchronized int port() {
        HttpServer http = server;
        if (http == null) {
            throw new IllegalStateException("observability server not started");
        }
        return http.getAddress().getPort();
    }

    /** Stops serving and releases the port and dispatcher threads. Idempotent. */
    public synchronized void stop() {
        HttpServer http = server;
        if (http == null) {
            return;
        }
        http.stop(STOP_GRACE_SECONDS);
        server = null;
        ExecutorService exec = dispatcher;
        if (exec != null) {
            exec.shutdownNow();
            dispatcher = null;
        }
        log.info("observability endpoints stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private static ThreadFactory daemonFactory() {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "cesium-obs-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
