package com.jucius.cesium.kafka.app.lifecycle;

import com.jucius.cesium.kafka.api.store.ConfigView;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.SchedulerStore;
import com.jucius.cesium.kafka.api.store.SchedulerStoreProvider;
import com.jucius.cesium.kafka.api.store.StoreContext;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.app.health.EngineHealth;
import com.jucius.cesium.kafka.app.health.MutableEngineHealth;
import com.jucius.cesium.kafka.app.health.ShardRecovery;
import com.jucius.cesium.kafka.core.admin.ClusterAdmin;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.admin.KafkaClusterAdmin;
import com.jucius.cesium.kafka.core.admin.StartupValidationResult;
import com.jucius.cesium.kafka.core.admin.StartupValidator;
import com.jucius.cesium.kafka.core.admin.StartupValidator.Revalidation;
import com.jucius.cesium.kafka.core.admin.TopicFacts;
import com.jucius.cesium.kafka.core.admin.TopicVisibility;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.Role;
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
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStoreProvider;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
 *   <li>resolves the {@link SchedulerStore} through the published {@link SchedulerStoreProvider}
 *       ServiceLoader hook by {@code store.type} and drives {@code configure → validate → start};
 *   <li>per {@linkplain Role role}, spawns {@code ingest.workers} {@link IngestLoop}s and
 *       {@code dispatch.workers} (validated to 1) {@link DispatchLoop}s — each owning its own Kafka clients (distinct
 *       transactional-id worker ordinals), the shared store, registry, {@link RelayRecordFactory},
 *       and (for dispatch) a {@link KafkaSeekFetcher} — on named non-daemon threads.
 * </ol>
 *
 * <p><strong>Lifecycle.</strong> {@link #start()} is non-blocking; {@link #awaitDone()} blocks until
 * a fatal engine failure (a loop thread dying, §3.8, or a topic identity change found by
 * re-validation, R17) or {@link #stop(Duration)} completes; {@link #stop(Duration)} performs
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
 *
 * <p><strong>Re-validation (D23, ADR-0020).</strong> Every {@code startup-checks.revalidation-interval}
 * a dedicated {@code cesium-admin} thread re-runs the environment checks
 * ({@link StartupValidator#revalidate}). A source or tracker topic identity change takes the fatal
 * path; any other error is logged at ERROR, counted in {@code cesium_revalidation_errors}, and sets
 * the readiness degraded flag with a short cause, while dispatch continues.
 */
public final class CesiumEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CesiumEngine.class);

    /** The loop heartbeat gauge each loop publishes (§9); the sampler reads it for freshness. */
    static final String HEARTBEAT_GAUGE = "cesium.loop.last.iteration.timestamp.seconds";

    /** How often the health sampler bridges loop gauges into the {@link EngineHealth} seam. */
    static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    /** Bound on the admin client close during shutdown (§6: bounded, then hard). */
    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    /** How long shutdown waits for an interrupted re-validation round before closing the admin client. */
    private static final Duration REVALIDATION_STOP_TIMEOUT = Duration.ofSeconds(5);

    /** The count of errors the last re-validation round found (§9: {@code cesium_revalidation_errors}). */
    static final String REVALIDATION_ERRORS_GAUGE = "cesium.revalidation.errors";

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
    // Health-sampler thread only: the previous {remaining, atMs} sample per recovering partition.
    private final Map<Integer, long[]> lastReplaySample = new HashMap<>();
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
    private volatile @Nullable ScheduledExecutorService adminExecutor;
    private final AtomicInteger revalidationErrors = new AtomicInteger();
    /** The degraded cause from the last re-validation round, or null when it was clean. */
    private volatile @Nullable String revalidationCause;
    // cesium-admin thread only: the last round's warnings, so a standing warning is logged once.
    private Set<String> lastRevalidationWarnings = Set.of();

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
        Gauge.builder(REVALIDATION_ERRORS_GAUGE, revalidationErrors, AtomicInteger::get)
                .description("errors found by the last runtime re-validation round (D23); 0 when clean")
                .register(registry);
    }

    // ------------------------------------------------------------------ health/observation surface

    /** The engine-written health signals the observability layer reads (design §9). */
    public EngineHealth health() {
        return health;
    }

    /**
     * The first fatal engine failure (see {@link #failFatally}), when {@link #awaitDone()} ended on
     * the fatal path.
     */
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

            TrackerBackedStore resolved = resolveStore(config.store().type());
            this.store = resolved;
            RouteDescriptor route = buildRouteDescriptor(clusterAdmin, config, identity, partitionCount);
            resolved.configure(new EngineStoreContext(
                    route, ConfigView.of(storeProperties(config, validation.offsetMetadataMaxBytes()))));
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
            startRevalidation(clusterAdmin, identity, Objects.requireNonNull(route.trackerTopicId(), "trackerTopicId"));
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
     * Blocks until the engine finishes: a fatal engine failure is signalled (a loop thread dying,
     * §3.8, or a topic identity change, R17) or an external {@link #stop(Duration)} completes. Ensures a graceful shutdown ran (idempotently) before
     * returning.
     *
     * @return {@code true} when the engine stopped cleanly with no fatal engine failure and within
     *     the shutdown budget; {@code false} on a fatal engine failure or when a loop overran the budget — the
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
     * The {@code store.properties} the store is configured with (§3.5). For the bundled
     * {@code kafka-tracker} store, {@code dispatch.cursor.sidecar-max-bytes} is the default for the
     * store's {@code cursor.sidecar-max-bytes}, clamped to the broker's
     * {@code offset.metadata.max.bytes} (the validator has already warned about the clamp). An
     * explicit store value always wins and is never clamped, only warned about. A dispatch value
     * outside the store's accepted range is left out — with a warning — so it can never turn into a
     * startup failure: the store then keeps its own default. Pure — unit-tested without a broker.
     */
    static Map<String, String> storeProperties(CesiumConfig config, OptionalInt offsetMetadataMaxBytes) {
        Map<String, String> properties = config.store().properties();
        if (!KafkaTrackerStoreProvider.TYPE_ID.equals(config.store().type())) {
            return properties; // another store's keys are its own; never inject one it may reject
        }
        String key = KafkaTrackerStore.SIDECAR_MAX_BYTES_KEY;
        String explicit = properties.get(key);
        if (explicit != null) {
            try {
                int value = Integer.parseInt(explicit.trim());
                if (offsetMetadataMaxBytes.isPresent() && value > offsetMetadataMaxBytes.getAsInt()) {
                    log.warn(
                            "store.properties.{} ({}) exceeds broker offset.metadata.max.bytes ({}); cursor commits"
                                    + " fail once the sidecar outgrows the broker cap — lower it or raise the broker"
                                    + " setting (§3.5)",
                            key,
                            value,
                            offsetMetadataMaxBytes.getAsInt());
                }
            } catch (NumberFormatException e) {
                // Not ours to report: the store's validate() rejects it with a non-disclosing message.
            }
            return properties;
        }
        int budget = config.dispatch().cursor().sidecarMaxBytes();
        int effective =
                offsetMetadataMaxBytes.isPresent() ? Math.min(budget, offsetMetadataMaxBytes.getAsInt()) : budget;
        if (effective < KafkaTrackerStore.MIN_SIDECAR_MAX_BYTES
                || effective > KafkaTrackerStore.MAX_SIDECAR_MAX_BYTES) {
            log.warn(
                    "sidecar budget {} (dispatch.cursor.sidecar-max-bytes, clamped to the broker cap) is outside the"
                            + " store's accepted range [{}, {}]; keeping the store default of {} bytes",
                    effective,
                    KafkaTrackerStore.MIN_SIDECAR_MAX_BYTES,
                    KafkaTrackerStore.MAX_SIDECAR_MAX_BYTES,
                    KafkaTrackerStore.DEFAULT_SIDECAR_MAX_BYTES);
            return properties;
        }
        Map<String, String> withBudget = new HashMap<>(properties);
        withBudget.put(key, Integer.toString(effective));
        return withBudget;
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

    /** One planned loop worker: its role and its transactional-id ordinal. */
    record LoopSpec(Role role, int workerOrdinal) {}

    // ------------------------------------------------------------------ startup helpers

    /**
     * The route identity triple + topic facts from the same admin plane production uses (§3.5).
     *
     * <p>Package-private and static so the wiring test can drive it over a fake {@link ClusterAdmin}
     * — in particular to prove the bounded metadata wait below is really on this path.
     */
    static RouteDescriptor buildRouteDescriptor(
            ClusterAdmin clusterAdmin, CesiumConfig config, IdentityBlob identity, int partitions) {
        // All three topics were described successfully moments ago during startup validation, so an
        // unknown-topic answer here is a laggy broker's metadata image — possibly a different node
        // than validation asked — not a vanished topic. Wait it out before declaring the latter.
        TopicVisibility visibility = new TopicVisibility(clusterAdmin);
        TopicFacts source = describeOrFail(visibility, config.route().source().topic());
        TopicFacts destination =
                describeOrFail(visibility, config.route().destination().topic());
        // The tracker exists by now: the CREATE bootstrap ran inside startup validation.
        TopicFacts tracker = describeOrFail(visibility, trackerTopic(config));
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
                            clients.newIngestConsumer(spec.workerOrdinal()),
                            () -> clients.newIngestProducer(spec.workerOrdinal()),
                            sharedStore,
                            new DelayHeaderCodec(config.delay().max()),
                            new IngestPolicyEngine(
                                    config.delay().max(),
                                    config.delay().onMalformedHeader(),
                                    config.delay().onOverMax()),
                            relay,
                            identity,
                            registry,
                            clock);
                    ingestHandles.add(spawn(
                            spec,
                            clients.ingestTransactionalId(spec.workerOrdinal()),
                            loop,
                            loop::stop,
                            loop::isDegraded));
                }
                case DISPATCH -> {
                    DispatchLoop loop = new DispatchLoop(
                            DispatchLoopConfig.from(config, maxHeapBytes),
                            clients.newTrackerConsumer(spec.workerOrdinal()),
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
                    dispatchHandles.add(spawn(
                            spec,
                            clients.dispatchTransactionalId(spec.workerOrdinal()),
                            loop,
                            loop::stop,
                            loop::isDegraded));
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
        return trackerTopic(config);
    }

    private static String trackerTopic(CesiumConfig config) {
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

    /**
     * Starts one loop on a named non-daemon thread. The thread's MDC carries {@code applicationId},
     * {@code loop} and {@code txn} (design §9), set once here so every log line from the loop, and the
     * fatal log in the {@code finally}, is tagged without any per-record MDC work.
     */
    private LoopHandle spawn(
            LoopSpec spec, String txn, Runnable body, Runnable stopper, BooleanSupplier degradedProbe) {
        String loopName = spec.role().name().toLowerCase(Locale.ROOT);
        String name = "cesium-" + loopName + "-" + spec.workerOrdinal();
        String applicationId = config.applicationId();
        Thread thread = new Thread(
                () -> {
                    MDC.put("applicationId", applicationId);
                    MDC.put("loop", loopName);
                    MDC.put("txn", txn);
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
     * A loop thread terminated. During shutdown that is expected (a throw is logged at WARN).
     * Otherwise it is a fatal engine failure (§3.8): a thrown failure, or a clean exit with no stop
     * request. Either way the durable log is authoritative for a successor, so this delegates to
     * {@link #failFatally} (readiness is already gated by liveness/death).
     */
    private void onLoopTerminated(Role role, @Nullable Throwable failure) {
        if (stopping) {
            if (failure != null) {
                log.warn("{} loop threw during shutdown (tolerated)", role, failure);
            }
            return;
        }
        failFatally(
                role.name().toLowerCase(Locale.ROOT) + " loop",
                failure != null ? failure : new IllegalStateException(role + " loop exited without a stop request"));
    }

    /**
     * The single fatal path: records the first cause, logs it once, and releases {@link #awaitDone()},
     * which gracefully stops the survivors and reports failure so the app exits non-zero. Ignored once
     * shutdown has begun, since anything that fails while stopping is expected.
     *
     * @param source what failed, for the log line (for example {@code "ingest loop"})
     */
    private void failFatally(String source, Throwable cause) {
        if (stopping) {
            return;
        }
        if (fatalCause.compareAndSet(null, cause)) {
            log.error("fatal engine failure ({}) — initiating engine shutdown", source, cause);
        }
        terminationLatch.countDown();
    }

    private boolean doStop(Duration budget) {
        // First, so no round reports on clients that are closing; the wait lets an interrupted round
        // unwind before closeAdmin() pulls the Admin out from under it. Findings are dropped (stopping).
        shutdown(adminExecutor, REVALIDATION_STOP_TIMEOUT);
        adminExecutor = null;
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
        shutdown(healthSampler, Duration.ZERO);
        healthSampler = null;
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

    private static void shutdown(@Nullable ScheduledExecutorService executor, Duration wait) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(wait.toMillis(), TimeUnit.MILLISECONDS) && !wait.isZero()) {
                log.warn("a background task did not stop within {}", wait);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    // ------------------------------------------------------------------ re-validation (D23, ADR-0020)

    private void startRevalidation(ClusterAdmin clusterAdmin, IdentityBlob identity, Uuid trackerTopicId) {
        // A fresh validator per round: TopicVisibility is confined to the thread running it.
        this.adminExecutor = scheduleRevalidation(
                config.startupChecks().revalidationInterval(),
                config.applicationId(),
                () -> new StartupValidator(clusterAdmin, clock).revalidate(config, identity, trackerTopicId),
                this::onRevalidation);
    }

    /**
     * Runs {@code round} every {@code interval} on a dedicated {@code cesium-admin} daemon thread and
     * hands each result to {@code sink}. A round that throws is reported as an error finding ("round
     * could not complete") rather than escaping, because one uncaught throw would cancel every later
     * run of {@code scheduleWithFixedDelay}.
     */
    static ScheduledExecutorService scheduleRevalidation(
            Duration interval, String applicationId, Supplier<Revalidation> round, Consumer<Revalidation> sink) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "cesium-admin");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMs = interval.toMillis();
        var unused = executor.scheduleWithFixedDelay(
                () -> {
                    MDC.put("applicationId", applicationId); // dedicated thread: idempotent, never pooled
                    Revalidation result;
                    try {
                        result = round.get();
                    } catch (Throwable t) {
                        log.debug("re-validation round failed", t);
                        result = new Revalidation(
                                Optional.empty(),
                                new ValidationReport(List.of(ValidationReport.Finding.error(
                                        "startup-checks.revalidation-interval",
                                        "the re-validation round could not complete: " + t))));
                    }
                    try {
                        sink.accept(result);
                    } catch (RuntimeException e) {
                        log.warn("re-validation result handling failed", e);
                    }
                },
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS);
        return executor;
    }

    /**
     * Applies one re-validation round (D1): an identity change is fatal; every other error is logged at
     * ERROR, counted in {@code cesium_revalidation_errors} and becomes the degraded cause; a warning is
     * logged when it first appears. Ignored entirely once shutdown has begun.
     */
    void onRevalidation(Revalidation result) {
        if (stopping) {
            return;
        }
        if (result.identityChange().isPresent()) {
            failFatally(
                    "re-validation",
                    new IllegalStateException(result.identityChange().get()));
            return;
        }
        List<ValidationReport.Finding> errors = result.report().errors();
        for (ValidationReport.Finding error : errors) {
            log.error("re-validation finding {}: {}", error.path(), error.message());
        }
        Set<String> warnings = new LinkedHashSet<>();
        for (ValidationReport.Finding warning : result.report().warnings()) {
            String line = warning.path() + ": " + warning.message();
            warnings.add(line);
            if (!lastRevalidationWarnings.contains(line)) {
                log.warn("re-validation finding {}", line);
            }
        }
        lastRevalidationWarnings = warnings;
        String previous = revalidationCause;
        revalidationErrors.set(errors.size());
        revalidationCause = errors.isEmpty()
                ? null
                : "re-validation: " + errors.size() + " error(s) at "
                        + errors.stream()
                                .map(ValidationReport.Finding::path)
                                .distinct()
                                .collect(Collectors.joining(", "));
        if (previous != null && errors.isEmpty()) {
            log.info("re-validation is clean again; the degraded cause is cleared");
        }
    }

    /** The degraded cause from the last re-validation round, or null when it was clean. */
    @Nullable String revalidationCause() {
        return revalidationCause;
    }

    /**
     * The readiness degraded cause: parked-and-degraded loops and the re-validation cause, either or
     * both; null when neither applies.
     */
    static @Nullable String degradedCause(Set<String> degradedRoles, @Nullable String revalidationCause) {
        List<String> causes = new ArrayList<>(2);
        if (!degradedRoles.isEmpty()) {
            causes.add("parked-and-degraded loop(s): " + String.join(", ", degradedRoles));
        }
        if (revalidationCause != null) {
            causes.add(revalidationCause);
        }
        return causes.isEmpty() ? null : String.join("; ", causes);
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
     * worker's {@code isDegraded()}. The recovery detail is read from the store's per-partition
     * {@code cesium.shard.state}/{@code cesium.replay.remaining.records} gauges.
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
                    }
                }
            }
            TreeSet<String> degradedRoles = new TreeSet<>();
            for (LoopHandle handle : allHandles()) {
                if (handle.degradedProbe.getAsBoolean()) {
                    degradedRoles.add(handle.role.name().toLowerCase(Locale.ROOT));
                }
            }
            String cause = degradedCause(degradedRoles, revalidationCause);
            health.setDegraded(cause != null, cause);
            health.updateRecovery(recoverySnapshot(registry, clock.millis(), lastReplaySample));
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

    /**
     * The non-ACTIVE shards, by partition, from the store's recovery gauges (D21: detail only,
     * never a readiness input). {@code etaMillis} is the slope between this sample and
     * {@code previous} (updated in place): {@code -1} until two samples exist or while the
     * backlog is not shrinking.
     */
    static List<ShardRecovery> recoverySnapshot(MeterRegistry registry, long nowMs, Map<Integer, long[]> previous) {
        List<ShardRecovery> shards = new ArrayList<>();
        Map<Integer, long[]> current = new HashMap<>();
        for (Gauge stateGauge :
                registry.find(KafkaTrackerStore.SHARD_STATE_METRIC).gauges()) {
            String tag = stateGauge.getId().getTag("partition");
            ShardRecovery.State state =
                    switch ((int) stateGauge.value()) {
                        case 0 -> ShardRecovery.State.ASSIGNED;
                        case 1 -> ShardRecovery.State.RECOVERING;
                        default -> ShardRecovery.State.ACTIVE;
                    };
            if (tag == null || state == ShardRecovery.State.ACTIVE) {
                continue; // ACTIVE shards are not reported
            }
            int partition = Integer.parseInt(tag);
            Gauge remainingGauge = registry.find(KafkaTrackerStore.REPLAY_REMAINING_METRIC)
                    .tag("partition", tag)
                    .gauge();
            long remaining = remainingGauge == null ? 0 : (long) remainingGauge.value();
            long etaMillis = -1;
            long[] last = previous.get(partition);
            if (last != null && last[0] > remaining && nowMs > last[1]) {
                etaMillis = remaining * (nowMs - last[1]) / (last[0] - remaining);
            }
            current.put(partition, new long[] {remaining, nowMs});
            shards.add(new ShardRecovery(partition, state, remaining, etaMillis));
        }
        previous.clear();
        previous.putAll(current);
        shards.sort(Comparator.comparingInt(ShardRecovery::partition));
        return shards;
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
     * The production {@link StoreContext}: real route identity, the {@code store.properties} subtree
     * and the shared registry.
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
        public MeterRegistry meterRegistry() {
            return registry;
        }
    }
}
