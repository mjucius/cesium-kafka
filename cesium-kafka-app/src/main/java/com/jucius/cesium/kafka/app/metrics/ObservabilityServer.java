package com.jucius.cesium.kafka.app.metrics;

import com.jucius.cesium.kafka.app.health.HealthAssessor;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
 * <p><strong>Slow-client hardening (security M1).</strong> The endpoints are unauthenticated, so the
 * JDK HttpServer's lack of built-in slow-client protection is a DoS surface: a slowloris that
 * dribbles a request body-or-header can pin a dispatcher thread forever and starve health probes.
 * Two guards close it. (1) {@code start()} sets the JDK reaper properties {@code
 * sun.net.httpserver.maxReqTime}/{@code maxRspTime} ({@value #DEFAULT_SLOW_CLIENT_TIMEOUT_SECONDS}s)
 * <em>before</em> {@link HttpServer#create} — they are read once in a static initializer, and only
 * applied here when unset so an operator can override them with {@code -D}. (2) Handlers run on a
 * small fixed daemon pool ({@value #DISPATCH_THREADS} threads) fronting a bounded queue: a handful
 * of slow clients cannot occupy every thread, and excess connections beyond the queue are dropped
 * (the JDK closes the connection) rather than queued without limit.
 */
public final class ObservabilityServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityServer.class);

    /** Seconds the JDK server waits for in-flight exchanges to finish before closing connections. */
    private static final int STOP_GRACE_SECONDS = 1;

    /**
     * Dispatcher threads (security M1): bounds concurrent in-flight requests. Raised from the original
     * 2 so a few slow clients cannot occupy every thread and starve the cheap health probes; kept
     * small because every handler is non-blocking and completes in microseconds.
     */
    static final int DISPATCH_THREADS = 8;

    /**
     * Bounded dispatch queue (security M1): connections beyond {@link #DISPATCH_THREADS} in-flight
     * wait here; once it fills, the executor rejects and the JDK closes the surplus connection rather
     * than letting an attacker queue work without limit.
     */
    static final int DISPATCH_QUEUE_CAPACITY = 32;

    /** JDK property reaping a client that takes too long to send its request (seconds; -1 = off). */
    static final String MAX_REQ_TIME_PROPERTY = "sun.net.httpserver.maxReqTime";

    /** JDK property reaping a client that takes too long to receive its response (seconds; -1 = off). */
    static final String MAX_RSP_TIME_PROPERTY = "sun.net.httpserver.maxRspTime";

    /** Default request/response reaping window applied when the operator has not set the property. */
    static final String DEFAULT_SLOW_CLIENT_TIMEOUT_SECONDS = "10";

    private final String bindAddress;
    private final int requestedPort;
    private final boolean detailedInfo;
    private final Supplier<String> metricsScrape;
    private final HealthAssessor health;
    private final Supplier<ServiceInfo> info;

    private @Nullable HttpServer server;
    private @Nullable ExecutorService dispatcher;

    /**
     * @param bindAddress the interface to bind ({@code observability.bind-address}, default {@code
     *     0.0.0.0}; {@code 127.0.0.1} restricts the unauthenticated endpoints to loopback, security L1)
     * @param port the listen port ({@code observability.port}, default 8081; {@code 0} binds an
     *     ephemeral port — used by tests, read back via {@link #port()})
     * @param detailedInfo whether {@code /info} discloses the sensitive fields (applicationId, roles,
     *     store capabilities, acknowledgments); default {@code false} (security M3)
     * @param registry the engine's Prometheus registry; {@code /metrics} renders {@code
     *     registry.scrape()}
     * @param health the assessor that derives the liveness/readiness verdicts from the engine's
     *     {@code EngineHealth} signals
     * @param info supplies the current {@code /info} snapshot on each request
     */
    public ObservabilityServer(
            String bindAddress,
            int port,
            boolean detailedInfo,
            PrometheusMeterRegistry registry,
            HealthAssessor health,
            Supplier<ServiceInfo> info) {
        this(bindAddress, port, detailedInfo, registry::scrape, health, info);
    }

    /**
     * Test/decoupling seam: the {@code /metrics} body comes from an arbitrary scrape supplier rather
     * than a {@link PrometheusMeterRegistry}, so the HTTP surface can be exercised without a registry.
     */
    ObservabilityServer(
            String bindAddress,
            int port,
            boolean detailedInfo,
            Supplier<String> metricsScrape,
            HealthAssessor health,
            Supplier<ServiceInfo> info) {
        this.bindAddress = bindAddress;
        this.requestedPort = port;
        this.detailedInfo = detailedInfo;
        this.metricsScrape = metricsScrape;
        this.health = health;
        this.info = info;
    }

    /** Binds the port, registers the handlers, and starts serving. */
    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("observability server already started");
        }
        // Must precede HttpServer.create: the JDK reads these once in a static initializer.
        applySlowClientReaping();
        HttpServer http = HttpServer.create(new InetSocketAddress(bindAddress, requestedPort), 0);
        http.createContext(MetricsHttpHandler.PATH, new MetricsHttpHandler(metricsScrape));
        http.createContext("/health/live", new HealthHttpHandler("/health/live", health::liveness));
        http.createContext("/health/ready", new HealthHttpHandler("/health/ready", health::readiness));
        http.createContext(InfoHttpHandler.PATH, new InfoHttpHandler(info, detailedInfo));
        ExecutorService exec = newDispatcher();
        http.setExecutor(exec);
        http.start();
        this.dispatcher = exec;
        this.server = http;
        log.info("observability endpoints listening on {}:{}", bindAddress, port());
    }

    /** The actually-bound port (resolves the ephemeral port when started with {@code 0}). */
    public synchronized int port() {
        return startedServer().getAddress().getPort();
    }

    /** The interface the listener actually bound (resolves {@code bindAddress}); see security L1. */
    public synchronized InetAddress boundAddress() {
        return Objects.requireNonNull(startedServer().getAddress().getAddress(), "bound address");
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

    /** Test seam (security M1): the live dispatcher's in-flight ceiling, or -1 before {@link #start()}. */
    synchronized int dispatchThreadCeiling() {
        ExecutorService exec = dispatcher;
        return exec instanceof ThreadPoolExecutor pool ? pool.getMaximumPoolSize() : -1;
    }

    private HttpServer startedServer() {
        HttpServer http = server;
        if (http == null) {
            throw new IllegalStateException("observability server not started");
        }
        return http;
    }

    /**
     * Enables the JDK HttpServer's slow-client reaper (security M1). Only sets a property the operator
     * has not already supplied (via {@code -Dsun.net.httpserver.maxReqTime=...}), so the default is a
     * floor an operator can override, never an override of their choice.
     */
    private static void applySlowClientReaping() {
        defaultSystemProperty(MAX_REQ_TIME_PROPERTY, DEFAULT_SLOW_CLIENT_TIMEOUT_SECONDS);
        defaultSystemProperty(MAX_RSP_TIME_PROPERTY, DEFAULT_SLOW_CLIENT_TIMEOUT_SECONDS);
    }

    private static void defaultSystemProperty(String key, String value) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
        }
    }

    /**
     * A fixed daemon pool fronting a bounded queue (security M1). When the queue saturates the
     * executor's {@link ThreadPoolExecutor.AbortPolicy} rejects with a {@code
     * RejectedExecutionException}, which the JDK dispatcher catches and turns into a closed connection
     * — so surplus load is shed instead of queued without limit.
     */
    private static ExecutorService newDispatcher() {
        return new ThreadPoolExecutor(
                DISPATCH_THREADS,
                DISPATCH_THREADS,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(DISPATCH_QUEUE_CAPACITY),
                daemonFactory(),
                new ThreadPoolExecutor.AbortPolicy());
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
