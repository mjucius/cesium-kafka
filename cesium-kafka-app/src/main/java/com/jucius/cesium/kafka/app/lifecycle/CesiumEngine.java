package com.jucius.cesium.kafka.app.lifecycle;

import com.jucius.cesium.kafka.api.store.ConfigView;
import com.jucius.cesium.kafka.api.store.OwnershipEpoch;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.SchedulerStore;
import com.jucius.cesium.kafka.api.store.SchedulerStoreProvider;
import com.jucius.cesium.kafka.api.store.StoreCapabilities;
import com.jucius.cesium.kafka.api.store.StoreContext;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.app.health.EngineHealth;
import com.jucius.cesium.kafka.app.health.MutableEngineHealth;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.admin.KafkaClusterAdmin;
import com.jucius.cesium.kafka.core.admin.StartupValidationResult;
import com.jucius.cesium.kafka.core.admin.StartupValidator;
import com.jucius.cesium.kafka.core.admin.TopicFacts;
import com.jucius.cesium.kafka.core.admin.TopicVisibility;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.CesiumConfigValidator;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.ValidationContext;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.core.dispatch.DispatchLoop;
import com.jucius.cesium.kafka.core.dispatch.DispatchLoopConfig;
import com.jucius.cesium.kafka.core.dispatch.KafkaDispatchAdmin;
import com.jucius.cesium.kafka.core.fetch.KafkaSeekFetcher;
import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.RelayRecordFactory;
import com.jucius.cesium.kafka.core.ingest.IngestLoop;
import com.jucius.cesium.kafka.core.ingest.IngestLoopConfig;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import com.jucius.cesium.kafka.core.policy.IngestPolicyEngine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The production engine: the hardened, process-resident version of the integration-test
 * {@code EngineHarness} (design §6, §10). Given a validated {@link CesiumConfig} and a shared
 * {@link MeterRegistry}, {@link #start()}:
 *
 * <ol>
 *   <li>runs {@link StartupValidator} over a real {@link KafkaClusterAdmin} (CREATE bootstrap,
 *       parity, retention, identity) and fail-fasts on errors;
 *   <li>captures the cluster id + topic ids and builds the {@link RouteDescriptor} +
 *       {@link StoreContext};
 *   <li>re-runs the §5.3 heap-budget check with the <em>real</em> source partition count (M5
 *       obligation #10 — config-load only had an estimate of 1);
 *   <li>resolves the {@link SchedulerStore} through the published {@link SchedulerStoreProvider}
 *       ServiceLoader hook by {@code store.type} and drives {@code configure → validate → start};
 *   <li>per {@linkplain Role role}, spawns {@code ingest.workers} {@link IngestLoop}s and
 *       {@code dispatch.workers} {@link DispatchLoop}s — each owning its own Kafka clients (distinct
 *       transactional-id worker ordinals), the shared store, registry, {@link RelayRecordFactory},
 *       and (for dispatch) a {@link KafkaSeekFetcher} — on named non-daemon threads.
 * </ol>
 *
 * <p><strong>Lifecycle.</strong> {@link #start()} is non-blocking; {@link #awaitDone()} blocks until
 * a loop dies fatally (§3.8) or {@link #stop(Duration)} completes; {@link #stop(Duration)} performs
 * the §6 graceful shutdown — <em>readiness flips false first</em> (so a preStop hook / load balancer
 * drains), then every loop is signalled to stop at a transaction boundary and joined within the
 * budget, then the store and admin client close. A single worker parking-and-degrading (§3.8/R15)
 * never terminates the loop, so it never trips the fatal path; only a thread that actually dies does.
 *
 * <p><strong>Health seam.</strong> The engine writes the {@link EngineHealth} signals the
 * observability layer reads: {@code startupComplete} after the store starts, {@code shuttingDown} at
 * the very top of {@code stop()}, per-loop thread liveness, and — via a 1 s sampler over each loop's
 * {@code cesium_loop_last_iteration_timestamp_seconds} gauge and {@code isDegraded()} — heartbeat
 * freshness, an assignment proxy, and the aggregate degraded flag (§6 "gauge sampling, heartbeat
 * freshness"; §9).
 */
public final class CesiumEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CesiumEngine.class);

    /** The loop heartbeat gauge each loop publishes (§9); the sampler reads it for freshness. */
    static final String HEARTBEAT_GAUGE = "cesium.loop.last.iteration.timestamp.seconds";

    /** How often the health sampler bridges loop gauges into the {@link EngineHealth} seam. */
    static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    /** Bound on the admin client close during shutdown (§6: bounded, then hard). */
    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final CesiumConfig config;
    private final MeterRegistry registry;
    private final Clock clock;
    private final Duration shutdownTimeout;
    private final long maxHeapBytes;
    private final MutableEngineHealth health;

    // Build scratch: appended only on the start() thread (spawnLoops) and read only there
    // (registerForHealth, the startup log). The cross-thread readers — doStop() on the SIGTERM
    // shutdown-hook thread and sample() on the health-sampler thread — read the immutable snapshot
    // published to the volatile {@link #handles} at the end of spawnLoops instead (safe publication).
    private final List<LoopHandle> ingestHandles = new ArrayList<>();
    private final List<LoopHandle> dispatchHandles = new ArrayList<>();
    private final ConcurrentMap<Role, Double> lastHeartbeatSeconds = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> fatalCause = new AtomicReference<>();
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final Object stopLock = new Object();

    private volatile boolean started;
    private volatile boolean stopping;
    private volatile boolean stopDone;
    private volatile boolean stopClean;

    private volatile @Nullable Admin admin;
    private volatile @Nullable TrackerBackedStore store;
    private volatile @Nullable ScheduledExecutorService healthSampler;
    private volatile @Nullable StoreCapabilities storeCapabilities;

    /**
     * The running loop handles, published as an immutable snapshot once {@link #spawnLoops} has
     * finished appending them on the start() thread. Reading this volatile establishes the
     * happens-before edge the SIGTERM shutdown-hook thread ({@link #doStop}) and the health-sampler
     * thread ({@link #allHandles}/{@link #sample}) need — the build-time {@link #ingestHandles}/
     * {@link #dispatchHandles} ArrayLists are not thread-safe and must not be read off-thread.
     */
    private volatile LoopHandles handles = LoopHandles.EMPTY;

    /**
     * @param config a validated configuration (a clean {@link ValidationReport} from the loader)
     * @param registry the shared metrics registry every loop and the store register into
     * @param clock the wall clock the loops and the health seam share (must be one instance)
     * @param shutdownTimeout the §6 graceful-shutdown budget {@link #awaitDone()} and the app's
     *     signal handler pass to {@link #stop(Duration)}
     */
    public CesiumEngine(CesiumConfig config, MeterRegistry registry, Clock clock, Duration shutdownTimeout) {
        this.config = Objects.requireNonNull(config, "config");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        this.maxHeapBytes = Runtime.getRuntime().maxMemory();
        this.health = new MutableEngineHealth(clock);
    }

    // ------------------------------------------------------------------ health/observation surface

    /** The engine-written health signals the observability layer reads (design §9). */
    public EngineHealth health() {
        return health;
    }

    /** The shared wall clock; the observability layer must use the same instance (heartbeat math). */
    public Clock clock() {
        return clock;
    }

    /** The configured {@code store.type} (for the {@code /info} endpoint). */
    public String storeType() {
        return config.store().type();
    }

    /** The resolved store's self-declared capabilities, present once {@link #start()} has resolved it. */
    public Optional<StoreCapabilities> storeCapabilities() {
        return Optional.ofNullable(storeCapabilities);
    }

    /** The fatal cause that terminated a loop, when {@link #awaitDone()} ended on the fatal path. */
    public Optional<Throwable> fatalCause() {
        return Optional.ofNullable(fatalCause.get());
    }

    // ------------------------------------------------------------------ lifecycle

    /**
     * Validates the environment, resolves the store, and spawns the configured loops — fail-fast
     * (throws {@link EngineStartupException}) on any validation error or store rejection. Non-blocking
     * on success; the loops run on their own threads and {@link #awaitDone()} blocks for them.
     */
    public void start() {
        if (started) {
            throw new IllegalStateException("engine already started");
        }
        started = true;
        try {
            Admin adminClient = Admin.create(adminProperties());
            this.admin = adminClient;
            KafkaClusterAdmin clusterAdmin = new KafkaClusterAdmin(adminClient);

            StartupValidationResult validation = new StartupValidator(clusterAdmin).validate(config);
            logFindings(validation.report());
            if (validation.report().hasErrors()) {
                throw new EngineStartupException(
                        "startup validation failed; the environment is not ready (see findings above)",
                        validation.report());
            }
            IdentityBlob identity = validation
                    .identity()
                    .orElseThrow(
                            () -> new EngineStartupException("startup validation passed but captured no identity"));
            int partitionCount = validation
                    .sourcePartitions()
                    .orElseThrow(() ->
                            new EngineStartupException("startup validation passed but captured no partition count"));

            revalidateHeapBudget(partitionCount);

            TrackerBackedStore resolved = resolveStore(config.store().type());
            this.store = resolved;
            RouteDescriptor route = buildRouteDescriptor(clusterAdmin, identity, partitionCount);
            resolved.configure(new EngineStoreContext(
                    route, new MapConfigView(config.store().properties())));
            this.storeCapabilities = resolved.capabilities();
            resolved.validate();
            resolved.start();

            spawnLoops(adminClient, resolved, identity, partitionCount);
            // Startup checks + store are up AND every loop is registered with the health seam: only
            // now satisfy readiness's startup gate. Flipping it before spawnLoops would open a brief
            // premature-READY window where readiness() iterates zero started roles and returns 200;
            // after registration the freshly-registered loops read NOT_READY (stale heartbeat,
            // assigned=false) until the sampler observes them iterating, so READY stays loop-gated (§9).
            health.markStartupComplete();
            startHealthSampler();
            log.info(
                    "cesium engine started: applicationId={} roles={} ingestWorkers={} dispatchWorkers={} store={}",
                    config.applicationId(),
                    config.roles(),
                    ingestHandles.size(),
                    dispatchHandles.size(),
                    config.store().type());
        } catch (EngineStartupException e) {
            stop(shutdownTimeout); // unwind any partially-built clients/store
            throw e;
        } catch (RuntimeException e) {
            stop(shutdownTimeout);
            throw new EngineStartupException("engine failed to start: " + e.getMessage(), e);
        }
    }

    /**
     * Blocks until the engine finishes: a loop dies fatally (§3.8) or an external
     * {@link #stop(Duration)} completes. Ensures a graceful shutdown ran (idempotently) before
     * returning.
     *
     * @return {@code true} when the engine stopped cleanly with no fatal loop death and within the
     *     shutdown budget; {@code false} when a loop died fatally or a loop overran the budget — the
     *     app maps {@code false} to a non-zero exit (§6)
     */
    public boolean awaitDone() {
        if (!started) {
            throw new IllegalStateException("engine not started");
        }
        try {
            terminationLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        boolean clean = stop(shutdownTimeout); // graceful-stop the survivors on the fatal path; idempotent
        return fatalCause.get() == null && clean;
    }

    /**
     * Graceful shutdown (design §6), idempotent and safe from any thread (the signal handler and
     * {@link #awaitDone()} both call it): readiness flips false <em>first</em>, then every loop is
     * signalled to stop at a transaction boundary and joined within {@code budget}, then the health
     * sampler, the store, and the admin client close.
     *
     * @return {@code true} when every loop joined within the budget
     */
    public boolean stop(Duration budget) {
        synchronized (stopLock) {
            if (stopDone) {
                return stopClean;
            }
            stopping = true; // mark before signalling so loop terminations read as expected, not fatal
            health.beginShutdown(); // §9: readiness false before any client closes (preStop drain)
            stopClean = doStop(budget);
            stopDone = true;
            terminationLatch.countDown(); // release an awaitDone() waiting on an external stop
            return stopClean;
        }
    }

    @Override
    public void close() {
        stop(shutdownTimeout);
    }

    // ------------------------------------------------------------------ pure wiring decisions (tested)

    /**
     * The loops this configuration runs: {@code ingest.workers} ingest specs then
     * {@code dispatch.workers} dispatch specs, per the selected {@linkplain CesiumConfig#roles()
     * roles}. Each worker gets a distinct ordinal driving its transactional id (§3.3, D10). Pure —
     * unit-tested without a broker.
     */
    static List<LoopSpec> planLoops(CesiumConfig config) {
        List<LoopSpec> specs = new ArrayList<>();
        if (config.roles().contains(Role.INGEST)) {
            for (int ordinal = 0; ordinal < config.ingest().workers(); ordinal++) {
                specs.add(new LoopSpec(Role.INGEST, ordinal));
            }
        }
        if (config.roles().contains(Role.DISPATCH)) {
            for (int ordinal = 0; ordinal < config.dispatch().workers(); ordinal++) {
                specs.add(new LoopSpec(Role.DISPATCH, ordinal));
            }
        }
        return specs;
    }

    /**
     * Resolves the tracker-backed store for {@code typeId} through the published
     * {@link SchedulerStoreProvider} ServiceLoader hook — the same explicit-selection path the
     * integration harness uses (design §4.5). Duplicate providers for one type, an unknown type, or
     * a non-tracker-backed store all fail fast. Pure — unit-tested without a broker.
     */
    static TrackerBackedStore resolveStore(String typeId) {
        SchedulerStore resolved = null;
        for (SchedulerStoreProvider provider : ServiceLoader.load(SchedulerStoreProvider.class)) {
            if (provider.typeId().equals(typeId)) {
                if (resolved != null) {
                    throw new EngineStartupException("duplicate SchedulerStoreProvider registered for store.type '"
                            + typeId + "' — remove the conflicting provider jar from the classpath");
                }
                resolved = provider.create();
            }
        }
        if (resolved == null) {
            throw new EngineStartupException("no SchedulerStoreProvider registered for store.type '" + typeId
                    + "' — the kafka-tracker store ships in cesium-kafka-store-kafka; check the classpath");
        }
        if (resolved instanceof TrackerBackedStore trackerBacked) {
            return trackerBacked;
        }
        throw new EngineStartupException("store.type '" + typeId + "' resolved to a non-tracker-backed store ("
                + resolved.getClass().getName() + "); this app build orchestrates tracker-backed stores only");
    }

    /**
     * The §5.3 worst-case heap-budget report re-evaluated at a given partition count — the pure core
     * of the M5 obligation-#10 re-validation, extracted so it is unit-testable without a broker.
     */
    static ValidationReport heapBudgetReport(CesiumConfig config, int partitionCount, long maxHeapBytes) {
        return new CesiumConfigValidator().validate(config, new ValidationContext(maxHeapBytes, partitionCount));
    }

    /** One planned loop worker: its role and its transactional-id ordinal. */
    record LoopSpec(Role role, int workerOrdinal) {}

    // ------------------------------------------------------------------ startup helpers

    /**
     * M5 obligation #10: re-run the §5.3 worst-case heap-budget check now that the <em>real</em>
     * source partition count is known (config-load only had an estimate of 1). A breach under the
     * default {@code startup-checks.heap-budget: FAIL} is a fail-fast; the always-present INFO
     * footprint finding is logged either way.
     */
    private void revalidateHeapBudget(int partitionCount) {
        ValidationReport report = heapBudgetReport(config, partitionCount, maxHeapBytes);
        logFindings(report);
        if (report.hasErrors()) {
            throw new EngineStartupException(
                    "worst-case index footprint exceeds the heap budget at the real partition count (" + partitionCount
                            + "); lower dispatch.max-pending-per-partition, assign fewer partitions, or raise -Xmx"
                            + " (design §5.3, M5 obligation #10)",
                    report);
        }
    }

    /** The route identity triple + topic facts from the same admin plane production uses (§3.5). */
    private RouteDescriptor buildRouteDescriptor(
            KafkaClusterAdmin clusterAdmin, IdentityBlob identity, int partitions) {
        // All three topics were described successfully moments ago during startup validation, so an
        // unknown-topic answer here is a laggy broker's metadata image — possibly a different node
        // than validation asked — not a vanished topic. Wait it out before declaring the latter.
        TopicVisibility visibility = new TopicVisibility(clusterAdmin);
        TopicFacts source = describeOrFail(visibility, config.route().source().topic());
        TopicFacts destination =
                describeOrFail(visibility, config.route().destination().topic());
        // The tracker exists by now: the CREATE bootstrap ran inside startup validation.
        TopicFacts tracker = describeOrFail(visibility, trackerTopic());
        String dlq = config.route().hasDlq() ? config.route().dlq().get().topic() : null;
        return new RouteDescriptor(
                config.applicationId(),
                identity.clusterId(),
                source.name(),
                source.topicId(),
                destination.name(),
                destination.topicId(),
                tracker.name(),
                tracker.topicId(),
                dlq,
                partitions);
    }

    private static TopicFacts describeOrFail(TopicVisibility visibility, String topic) {
        return visibility
                .awaitVisible(topic)
                .orElseThrow(() -> new EngineStartupException(
                        "topic '" + topic + "' vanished between startup validation and store build"));
    }

    private void spawnLoops(
            Admin adminClient, TrackerBackedStore sharedStore, IdentityBlob identity, int partitionCount) {
        KafkaClientFactory clients = new KafkaClientFactory(config);
        RelayRecordFactory relay = buildRelayFactory();
        KafkaDispatchAdmin dispatchAdmin = new KafkaDispatchAdmin(
                adminClient, trackerTopic(), KafkaClientFactory.dispatchGroupId(config.applicationId()));
        try {
            spawnLoopBodies(clients, relay, dispatchAdmin, sharedStore, identity, partitionCount);
            registerForHealth(Role.INGEST, ingestHandles);
            registerForHealth(Role.DISPATCH, dispatchHandles);
        } finally {
            // Publish whatever was built — even on a partial-spawn failure — so the unwind stop()
            // and the sampler read a consistent, safely-published snapshot (and any threads that did
            // start are still joined on the unwind path). The build lists are start()-thread-only.
            this.handles = new LoopHandles(List.copyOf(ingestHandles), List.copyOf(dispatchHandles));
        }
    }

    private void spawnLoopBodies(
            KafkaClientFactory clients,
            RelayRecordFactory relay,
            KafkaDispatchAdmin dispatchAdmin,
            TrackerBackedStore sharedStore,
            IdentityBlob identity,
            int partitionCount) {
        for (LoopSpec spec : planLoops(config)) {
            switch (spec.role()) {
                case INGEST -> {
                    IngestLoop loop = new IngestLoop(
                            IngestLoopConfig.from(config, partitionCount),
                            clients.newIngestConsumer(),
                            () -> clients.newIngestProducer(spec.workerOrdinal()),
                            sharedStore,
                            new DelayHeaderCodec(
                                    config.delay().max(), config.headers().acceptBinaryLongValues()),
                            new IngestPolicyEngine(
                                    config.delay().max(),
                                    config.delay().onMalformedHeader(),
                                    config.delay().onOverMax()),
                            relay,
                            identity,
                            registry,
                            clock);
                    ingestHandles.add(spawn(spec, loop, loop::stop, loop::isDegraded));
                }
                case DISPATCH -> {
                    DispatchLoop loop = new DispatchLoop(
                            DispatchLoopConfig.from(config, maxHeapBytes),
                            clients.newTrackerConsumer(),
                            () -> clients.newDispatchProducer(spec.workerOrdinal()),
                            sharedStore,
                            new KafkaSeekFetcher(
                                    config.route().source().topic(),
                                    config.dispatch().fetch().partitionTimeFloor(),
                                    clients.newSeekConsumer(),
                                    registry,
                                    clock),
                            relay,
                            dispatchAdmin,
                            registry,
                            clock);
                    dispatchHandles.add(spawn(spec, loop, loop::stop, loop::isDegraded));
                }
            }
        }
    }

    private RelayRecordFactory buildRelayFactory() {
        String dlq = config.route().hasDlq() ? config.route().dlq().get().topic() : null;
        return new RelayRecordFactory(
                config.route().destination().topic(),
                dlq,
                config.headers().stampProvenance(),
                config.route().relay().timestamp(),
                config.route().relay().partitioning());
    }

    private String trackerTopic() {
        return config.route().tracker().resolvedTopic(config.applicationId());
    }

    private Map<String, Object> adminProperties() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(config.kafka().properties());
        props.putAll(config.kafka().admin().properties());
        // The bootstrap is correctness-load-bearing; surface a clear error if it is absent.
        if (!props.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new EngineStartupException("kafka.properties." + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
                    + " is required to reach the cluster");
        }
        return props;
    }

    // ------------------------------------------------------------------ loop threads & fatal path

    private LoopHandle spawn(LoopSpec spec, Runnable body, Runnable stopper, BooleanSupplier degradedProbe) {
        String name = "cesium-" + spec.role().name().toLowerCase(Locale.ROOT) + "-" + spec.workerOrdinal();
        Thread thread = new Thread(
                () -> {
                    Throwable failure = null;
                    try {
                        body.run();
                    } catch (Throwable t) {
                        failure = t;
                    } finally {
                        onLoopTerminated(spec.role(), failure);
                    }
                },
                name);
        thread.setDaemon(false);
        LoopHandle handle = new LoopHandle(spec.role(), thread, stopper, degradedProbe);
        thread.start();
        return handle;
    }

    /**
     * A loop thread terminated. During shutdown that is expected. Otherwise it is fatal (§3.8): a
     * thrown failure, or a clean exit with no stop request — either way the durable log is
     * authoritative for a successor, so the engine signals the fatal path and lets {@link #awaitDone()}
     * gracefully stop the survivors and exit non-zero (readiness is already gated by liveness/death).
     */
    private void onLoopTerminated(Role role, @Nullable Throwable failure) {
        if (stopping) {
            if (failure != null) {
                log.warn("{} loop threw during shutdown (tolerated)", role, failure);
            }
            return;
        }
        Throwable cause =
                failure != null ? failure : new IllegalStateException(role + " loop exited without a stop request");
        if (fatalCause.compareAndSet(null, cause)) {
            log.error("fatal {} loop failure — initiating engine shutdown", role, cause);
        }
        terminationLatch.countDown();
    }

    private boolean doStop(Duration budget) {
        long deadlineNanos = System.nanoTime() + Math.max(0, budget.toNanos());
        LoopHandles snapshot = handles; // safely-published read (may be EMPTY if start() failed early)
        // Signal all loops up front so they all begin draining; join ingest-before-dispatch (§6).
        for (LoopHandle handle : snapshot.all()) {
            try {
                handle.stopper.run();
            } catch (RuntimeException e) {
                log.debug("stop signal to a {} loop failed (it may already have died)", handle.role, e);
            }
        }
        boolean clean = true;
        for (LoopHandle handle : snapshot.ingest()) {
            clean &= join(handle, deadlineNanos);
        }
        for (LoopHandle handle : snapshot.dispatch()) {
            clean &= join(handle, deadlineNanos);
        }
        if (!clean) {
            log.warn("one or more loops did not stop within the {} shutdown budget", budget);
        }
        shutdownSampler();
        closeStore();
        closeAdmin();
        return clean;
    }

    private boolean join(LoopHandle handle, long deadlineNanos) {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (remainingMs > 0) {
            try {
                handle.thread.join(remainingMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return !handle.thread.isAlive();
    }

    private void shutdownSampler() {
        ScheduledExecutorService sampler = healthSampler;
        if (sampler != null) {
            sampler.shutdownNow();
            healthSampler = null;
        }
    }

    private void closeStore() {
        TrackerBackedStore current = store;
        if (current != null) {
            try {
                current.close();
            } catch (RuntimeException e) {
                log.warn("store close failed during shutdown", e);
            }
            store = null;
        }
    }

    private void closeAdmin() {
        Admin current = admin;
        if (current != null) {
            try {
                current.close(ADMIN_CLOSE_TIMEOUT);
            } catch (RuntimeException e) {
                log.warn("admin client close failed during shutdown", e);
            }
            admin = null;
        }
    }

    // ------------------------------------------------------------------ health sampler (§6/§9)

    private void registerForHealth(Role role, List<LoopHandle> handles) {
        if (handles.isEmpty()) {
            return;
        }
        List<Thread> threads = handles.stream().map(h -> h.thread).toList();
        // A role is "alive" only while every one of its worker threads is alive.
        health.registerLoop(role, () -> threads.stream().allMatch(Thread::isAlive));
    }

    private void startHealthSampler() {
        ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "cesium-health");
            thread.setDaemon(true);
            return thread;
        });
        long periodMs = SAMPLE_INTERVAL.toMillis();
        var unused = sampler.scheduleWithFixedDelay(this::sample, periodMs, periodMs, TimeUnit.MILLISECONDS);
        this.healthSampler = sampler;
    }

    /**
     * Bridges the loops' observable surface into the {@link EngineHealth} seam (design §6
     * "gauge sampling, heartbeat freshness"). For each role: when its
     * {@code cesium_loop_last_iteration_timestamp_seconds} gauge has advanced since the last sample
     * the loop is iterating, so its heartbeat is bumped and its consumer treated as engaged
     * (assignment is not exposed by the loop SPI without reaching into its consumer; loop iteration
     * — subscribe done, polling — is the available proxy). The degraded flag aggregates every
     * worker's {@code isDegraded()}.
     */
    private void sample() {
        try {
            for (Role role : Role.values()) {
                double heartbeat = heartbeatSeconds(role);
                if (heartbeat > 0.0) {
                    Double previous = lastHeartbeatSeconds.get(role);
                    if (previous == null || heartbeat > previous) {
                        lastHeartbeatSeconds.put(role, heartbeat);
                        health.heartbeat(role);
                        health.consumerAssigned(role, true);
                    }
                }
            }
            TreeSet<String> degradedRoles = new TreeSet<>();
            for (LoopHandle handle : allHandles()) {
                if (handle.degradedProbe.getAsBoolean()) {
                    degradedRoles.add(handle.role.name().toLowerCase(Locale.ROOT));
                }
            }
            if (degradedRoles.isEmpty()) {
                health.setDegraded(false, null);
            } else {
                health.setDegraded(true, "parked-and-degraded loop(s): " + String.join(", ", degradedRoles));
            }
        } catch (RuntimeException e) {
            // The sampler must never die: a transient registry/probe hiccup is logged, not fatal.
            log.debug("health sample iteration failed", e);
        }
    }

    private double heartbeatSeconds(Role role) {
        Gauge gauge = registry.find(HEARTBEAT_GAUGE)
                .tag("loop", role.name().toLowerCase(Locale.ROOT))
                .gauge();
        return gauge == null ? Double.NaN : gauge.value();
    }

    private List<LoopHandle> allHandles() {
        return handles.all(); // safely-published snapshot read (sampler/shutdown-hook threads)
    }

    private void logFindings(ValidationReport report) {
        for (ValidationReport.Finding finding : report.findings()) {
            switch (finding.severity()) {
                case ERROR -> log.error("startup finding {}: {}", finding.path(), finding.message());
                case WARNING -> log.warn("startup finding {}: {}", finding.path(), finding.message());
                case INFO -> log.info("startup finding {}: {}", finding.path(), finding.message());
            }
        }
    }

    // ------------------------------------------------------------------ plumbing types

    /**
     * An immutable, safely-published snapshot of the running loop handles (ingest then dispatch),
     * published to the {@link #handles} volatile so the shutdown-hook and health-sampler threads read
     * the handles with a happens-before edge to the start() thread that built them.
     */
    private record LoopHandles(List<LoopHandle> ingest, List<LoopHandle> dispatch) {
        static final LoopHandles EMPTY = new LoopHandles(List.of(), List.of());

        /** Ingest handles before dispatch handles — the §6 graceful-join order. */
        List<LoopHandle> all() {
            List<LoopHandle> all = new ArrayList<>(ingest.size() + dispatch.size());
            all.addAll(ingest);
            all.addAll(dispatch);
            return all;
        }
    }

    /** One running loop worker: its role, thread, graceful stopper, and degraded probe. */
    private static final class LoopHandle {
        final Role role;
        final Thread thread;
        final Runnable stopper;
        final BooleanSupplier degradedProbe;

        LoopHandle(Role role, Thread thread, Runnable stopper, BooleanSupplier degradedProbe) {
            this.role = role;
            this.thread = thread;
            this.stopper = stopper;
            this.degradedProbe = degradedProbe;
        }
    }

    /**
     * The production {@link StoreContext}: real route identity, the {@code store.properties} subtree,
     * the engine's shared clock, the shared registry, and a placeholder epoch (tracker-backed stores
     * ride the engine's Kafka transactions and never consult it — design §4.4 item 4).
     */
    private final class EngineStoreContext implements StoreContext {
        private final RouteDescriptor route;
        private final ConfigView configView;

        EngineStoreContext(RouteDescriptor route, ConfigView configView) {
            this.route = route;
            this.configView = configView;
        }

        @Override
        public RouteDescriptor route() {
            return route;
        }

        @Override
        public ConfigView config() {
            return configView;
        }

        @Override
        public Clock clock() {
            return clock;
        }

        @Override
        public MeterRegistry meterRegistry() {
            return registry;
        }

        @Override
        public OwnershipEpoch epoch(int partition) {
            return new OwnershipEpoch(0, "cesium-engine");
        }
    }
}
