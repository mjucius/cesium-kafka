package com.jucius.cesium.kafka.it;

import com.jucius.cesium.kafka.api.store.ConfigView;
import com.jucius.cesium.kafka.api.store.OwnershipEpoch;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.SchedulerStore;
import com.jucius.cesium.kafka.api.store.SchedulerStoreProvider;
import com.jucius.cesium.kafka.api.store.StoreContext;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.admin.KafkaClusterAdmin;
import com.jucius.cesium.kafka.core.admin.StartupValidationResult;
import com.jucius.cesium.kafka.core.admin.StartupValidator;
import com.jucius.cesium.kafka.core.admin.TopicFacts;
import com.jucius.cesium.kafka.core.admin.TopicVisibility;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.CesiumConfigValidator;
import com.jucius.cesium.kafka.core.config.CheckMode;
import com.jucius.cesium.kafka.core.config.DelayConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.StartupChecks;
import com.jucius.cesium.kafka.core.config.StoreConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import com.jucius.cesium.kafka.core.config.TrackerConfig;
import com.jucius.cesium.kafka.core.config.ValidationContext;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.core.dispatch.DispatchLoop;
import com.jucius.cesium.kafka.core.dispatch.DispatchLoopConfig;
import com.jucius.cesium.kafka.core.dispatch.KafkaDispatchAdmin;
import com.jucius.cesium.kafka.core.fetch.KafkaSeekFetcher;
import com.jucius.cesium.kafka.core.fetch.SeekFetcher;
import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.RelayRecordFactory;
import com.jucius.cesium.kafka.core.ingest.IngestLoop;
import com.jucius.cesium.kafka.core.ingest.IngestLoopConfig;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import com.jucius.cesium.kafka.core.policy.IngestPolicyEngine;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import com.jucius.cesium.kafka.testkit.FakeStoreContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Assembles a real engine instance for integration tests: {@link CesiumConfig} records built
 * directly (no YAML), {@link CesiumConfigValidator} + {@link StartupValidator} (which performs the
 * tracker {@code CREATE} bootstrap), the real {@link KafkaTrackerStore} resolved through the
 * published {@link SchedulerStoreProvider} ServiceLoader hook, and — per the configured
 * {@linkplain Role roles} — the {@link IngestLoop} and/or the {@link DispatchLoop} (with its
 * {@link KafkaSeekFetcher} and {@link KafkaDispatchAdmin}) on daemon threads wired through
 * {@link KafkaClientFactory}.
 *
 * <p><strong>Restartable.</strong> {@link #start()} builds fresh clients, a fresh store, and fresh
 * loops each call, so crash tests restart the "process" by calling {@code start()} again on the
 * same harness — same application id, same {@code instanceId} (stable transactional.ids + static
 * membership), exactly the recovery a restarted deployment slot performs. The previous
 * incarnation's in-memory store state dies with it; the durable tracker log is the only carrier.
 *
 * <p><strong>First-run offsets (group A).</strong> {@code auto.offset.reset=none} is locked (D18),
 * so a brand-new ingest group has no position and the loop fail-fasts by design (I-9). The harness
 * performs the documented operator runbook step once per harness: it seeds committed offset 0 for
 * every source partition via {@code Admin.alterConsumerGroupOffsets} before the first ingest
 * start. Group B needs no seeding — the dispatch loop's provable-first-run seek (§3.6 step 2) is
 * production behavior and the harness exercises it as such.
 *
 * <p>Crash helpers wire to {@link CrashPoints}; tests must {@link CrashPoints#reset()} between
 * crash and restart (and in teardown). {@link #kill()} is the SIGKILL stand-in for points the
 * crash-point matrix does not cover: it interrupts the loop threads mid-flight (no graceful
 * commit, no store flush) and drops the in-memory store.
 */
final class EngineHarness implements AutoCloseable {

    static {
        // The crash-point seam refuses to arm without the explicit test-only guard property.
        System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
    }

    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);

    /**
     * IT-friendly poll ceiling (production default: 30 s). A quiet dispatch loop sleeps in
     * {@code poll()} up to this long (§6: the timeout is {@code min(nextDeadline - now, ceiling)}),
     * and idle cursor advancement only runs between polls — a 30 s ceiling would make tests
     * awaiting an idle commit wait out a near-full poll cycle. 2 s keeps idle advancement and
     * shutdown prompt without changing any transactional behavior.
     */
    private static final Duration IT_MAX_POLL_TIMEOUT = Duration.ofSeconds(2);

    private final CesiumConfig config;
    private final Admin admin;
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    /**
     * Optional wrapper around the real {@link KafkaSeekFetcher} (default identity). The M6
     * penalty-box isolation scenario (§11.3-10) injects a decorator that deterministically stamps
     * one source partition's fetch outcomes {@code TRANSIENT} — far more reliable than racing a
     * Toxiproxy latency toxic, and the single-broker substrate cannot degrade one partition's leader
     * in isolation anyway (R21 prefers deterministic injection). Set via the builder; unset for the
     * 28 pre-M6 ITs, so their fetch path is exactly the production {@code KafkaSeekFetcher}.
     */
    private UnaryOperator<SeekFetcher> seekFetcherDecorator = UnaryOperator.identity();

    private LoopHandle ingest;
    private LoopHandle dispatch;
    private TrackerBackedStore store;
    private IdentityBlob identity;
    private boolean offsetsSeeded;

    private EngineHarness(CesiumConfig config) {
        this.config = config;
        String bootstrap = config.kafka().properties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        this.admin = Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    static Builder builder() {
        return new Builder();
    }

    // ------------------------------------------------------------------ lifecycle

    /**
     * Runs config validation (throws on errors — that's a test bug) and the environment startup
     * checks (returned for assertions; includes the tracker CREATE bootstrap side effect).
     */
    StartupValidationResult validate() {
        ValidationReport configReport = new CesiumConfigValidator().validate(config, ValidationContext.runtime(16));
        if (configReport.hasErrors()) {
            throw new IllegalStateException("test-assembled config is invalid:\n" + configReport.render());
        }
        return new StartupValidator(new KafkaClusterAdmin(admin)).validate(config);
    }

    /**
     * Starts (or restarts) the engine: validates, builds the real store from the provider, seeds
     * group A's first-run offsets, builds fresh clients and fresh loops for the configured roles,
     * and spawns each loop on a daemon thread.
     */
    void start() {
        if (isAlive(ingest) || isAlive(dispatch)) {
            throw new IllegalStateException("harness already running");
        }
        StartupValidationResult result = validate();
        if (result.report().hasErrors()) {
            throw new AssertionError(
                    "startup validation failed:\n" + result.report().render());
        }
        this.identity = result.identity().orElseThrow();
        closeStoreQuietly(); // the previous incarnation's in-memory state dies with the "process"
        this.store = buildStore();
        KafkaClientFactory clients = new KafkaClientFactory(config);
        Clock clock = Clock.systemUTC();
        if (config.roles().contains(Role.INGEST)) {
            seedFirstRunOffsets();
            IngestLoop loop = new IngestLoop(
                    // Tracker partitions == source partitions once the R-7 parity check passed.
                    IngestLoopConfig.from(config, result.sourcePartitions().orElseThrow()),
                    clients.newIngestConsumer(),
                    () -> clients.newIngestProducer(0),
                    store,
                    new DelayHeaderCodec(config.delay().max(), config.headers().acceptBinaryLongValues()),
                    new IngestPolicyEngine(
                            config.delay().max(),
                            config.delay().onMalformedHeader(),
                            config.delay().onOverMax()),
                    relayFactory(),
                    identity,
                    meterRegistry,
                    clock);
            ingest = spawn("it-ingest-", loop, loop::stop);
        }
        if (config.roles().contains(Role.DISPATCH)) {
            DispatchLoop loop = new DispatchLoop(
                    dispatchLoopConfig(),
                    clients.newTrackerConsumer(),
                    () -> clients.newDispatchProducer(0),
                    store,
                    seekFetcherDecorator.apply(new KafkaSeekFetcher(
                            sourceTopic(),
                            config.dispatch().fetch().partitionTimeFloor(),
                            clients.newSeekConsumer(),
                            meterRegistry,
                            clock)),
                    relayFactory(),
                    new KafkaDispatchAdmin(admin, trackerTopic(), dispatchGroupId()),
                    meterRegistry,
                    clock);
            dispatch = spawn("it-dispatch-", loop, loop::stop);
        }
    }

    /** Graceful stop: raises each loop's stop flag, joins the threads, and closes the store. */
    void stop() {
        stopLoop(ingest, "ingest");
        stopLoop(dispatch, "dispatch");
        closeStoreQuietly();
    }

    /**
     * SIGKILL stand-in: interrupts the loop threads mid-flight — no graceful commit, no further
     * transaction work; an open transaction stays dangling until the restart's
     * {@code initTransactions()} resolves it — and drops the in-memory store with the "process".
     */
    void kill() {
        interruptLoop(ingest, "ingest");
        interruptLoop(dispatch, "dispatch");
        closeStoreQuietly();
    }

    /**
     * Waits for the ingest loop thread to die (crash tests) and returns the throwable that killed
     * it. Fails when the thread survives the timeout or exited without a failure.
     */
    Throwable awaitDeath(Duration timeout) {
        return awaitLoopDeath(ingest, "ingest", timeout);
    }

    /** The dispatch analog of {@link #awaitDeath}: waits for the dispatch loop thread to die. */
    Throwable awaitDispatchDeath(Duration timeout) {
        return awaitLoopDeath(dispatch, "dispatch", timeout);
    }

    @Override
    public void close() {
        bestEffortStop(ingest);
        bestEffortStop(dispatch);
        closeStoreQuietly();
        admin.close();
        meterRegistry.close();
    }

    // ------------------------------------------------------------------ crash helper

    /**
     * Installs a {@link CrashPoints} handler that kills the loop with a
     * {@link CrashPoints.SimulatedCrash} the <em>first</em> time {@code pointId} fires; later
     * firings (e.g. after a restart that forgot to reset) pass through harmlessly.
     */
    static void installCrashOnce(String pointId) {
        AtomicBoolean fired = new AtomicBoolean();
        CrashPoints.install(point -> {
            if (pointId.equals(point) && fired.compareAndSet(false, true)) {
                throw new CrashPoints.SimulatedCrash(point);
            }
        });
    }

    // ------------------------------------------------------------------ observation surface

    MeterRegistry meterRegistry() {
        return meterRegistry;
    }

    CesiumConfig config() {
        return config;
    }

    /** The captured §3.1 identity (cluster id + source topic id); available after start/validate. */
    IdentityBlob identity() {
        return Objects.requireNonNull(identity, "identity is captured by start()");
    }

    String ingestGroupId() {
        return KafkaClientFactory.ingestGroupId(config.applicationId());
    }

    String dispatchGroupId() {
        return KafkaClientFactory.dispatchGroupId(config.applicationId());
    }

    String trackerTopic() {
        return config.route().tracker().resolvedTopic(config.applicationId());
    }

    String sourceTopic() {
        return config.route().source().topic();
    }

    String destinationTopic() {
        return config.route().destination().topic();
    }

    String dlqTopic() {
        return config.route().dlq().orElseThrow().topic();
    }

    // ------------------------------------------------------------------ internals

    private static boolean isAlive(LoopHandle handle) {
        return handle != null && handle.thread.isAlive();
    }

    private LoopHandle spawn(String prefix, Runnable body, Runnable stopper) {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        body.run();
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                },
                prefix + config.applicationId());
        thread.setDaemon(true);
        LoopHandle handle = new LoopHandle(thread, stopper, failure);
        thread.start();
        return handle;
    }

    private static void stopLoop(LoopHandle handle, String name) {
        if (handle == null) {
            return;
        }
        if (handle.thread.isAlive()) {
            try {
                handle.stopper.run();
            } catch (RuntimeException ignored) {
                // The loop may already have died and closed its consumer; the join verifies.
            }
        }
        join(handle.thread, STOP_TIMEOUT);
        if (handle.thread.isAlive()) {
            throw new AssertionError(name + " loop did not stop within " + STOP_TIMEOUT);
        }
    }

    private static void interruptLoop(LoopHandle handle, String name) {
        if (handle == null || !handle.thread.isAlive()) {
            return;
        }
        handle.thread.interrupt();
        join(handle.thread, STOP_TIMEOUT);
        if (handle.thread.isAlive()) {
            throw new AssertionError(name + " loop survived the interrupt for " + STOP_TIMEOUT);
        }
    }

    private static void bestEffortStop(LoopHandle handle) {
        if (handle == null || !handle.thread.isAlive()) {
            return;
        }
        try {
            handle.stopper.run();
        } catch (RuntimeException ignored) {
            // Best effort: a crashed loop already closed its clients.
        }
        join(handle.thread, STOP_TIMEOUT);
    }

    private static Throwable awaitLoopDeath(LoopHandle handle, String name, Duration timeout) {
        Objects.requireNonNull(handle, name + " loop was never started");
        join(handle.thread, timeout);
        if (handle.thread.isAlive()) {
            throw new AssertionError(name + " loop is still alive after " + timeout);
        }
        Throwable failure = handle.failure.get();
        if (failure == null) {
            throw new AssertionError(name + " loop exited cleanly; expected a crash");
        }
        return failure;
    }

    private static void join(Thread thread, Duration timeout) {
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while joining the loop thread", e);
        }
    }

    /**
     * Builds, configures, validates, and starts the real store: resolved by {@code store.type}
     * through the published ServiceLoader hook (exactly the production selection path), with a
     * {@link RouteDescriptor} whose cluster id comes from {@code Admin.describeCluster()} and
     * whose topic ids come from {@code Admin.describeTopics()}.
     */
    private TrackerBackedStore buildStore() {
        TrackerBackedStore created = createStore(config.store().type());
        created.configure(new HarnessStoreContext(
                routeDescriptor(),
                new FakeStoreContext.MapConfigView(config.store().properties()),
                meterRegistry));
        created.validate();
        created.start();
        return created;
    }

    private static TrackerBackedStore createStore(String typeId) {
        for (SchedulerStoreProvider provider : ServiceLoader.load(SchedulerStoreProvider.class)) {
            if (provider.typeId().equals(typeId)) {
                SchedulerStore store = provider.create();
                if (store instanceof TrackerBackedStore trackerBacked) {
                    return trackerBacked;
                }
                throw new AssertionError("store type " + typeId + " is not tracker-backed: " + store.getClass());
            }
        }
        throw new AssertionError("no SchedulerStoreProvider registered for store.type " + typeId);
    }

    /** The route identity triple + topic facts, from the same admin plane production uses (§3.5). */
    private RouteDescriptor routeDescriptor() {
        // Mirrors production (CesiumEngine.buildRouteDescriptor): all three topics were described
        // successfully during validation, so an unknown-topic answer now is broker metadata lag.
        TopicVisibility visibility = new TopicVisibility(new KafkaClusterAdmin(admin));
        TopicFacts source = describeOrFail(visibility, sourceTopic());
        TopicFacts destination = describeOrFail(visibility, destinationTopic());
        // The tracker exists here: the CREATE bootstrap ran inside the startup validation.
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
                source.partitionCount());
    }

    private static TopicFacts describeOrFail(TopicVisibility visibility, String topic) {
        return visibility
                .awaitVisible(topic)
                .orElseThrow(() -> new AssertionError("topic vanished between validation and store build: " + topic));
    }

    /**
     * The production {@code DispatchLoopConfig} derivation with the {@link #IT_MAX_POLL_TIMEOUT}
     * ceiling swapped in (records have no withers; every other component is exactly what
     * {@code DispatchLoopConfig.from} derived).
     */
    private DispatchLoopConfig dispatchLoopConfig() {
        DispatchLoopConfig derived =
                DispatchLoopConfig.from(config, Runtime.getRuntime().maxMemory());
        return new DispatchLoopConfig(
                derived.trackerTopic(),
                derived.sourceTopic(),
                derived.maxBatchEntries(),
                derived.maxBatchBytes(),
                IT_MAX_POLL_TIMEOUT,
                derived.drainSlice(),
                derived.idleCursorInterval(),
                derived.fetchTimeout(),
                derived.penaltyBackoff(),
                derived.penaltyBackoffMax(),
                derived.onUnfetchablePayload(),
                derived.onUnrelayable(),
                derived.maxPendingPerPartition(),
                derived.maxPendingTotal(),
                derived.commitRetryLimit(),
                derived.parkThreshold(),
                derived.retryBackoffInitial(),
                derived.retryBackoffMax());
    }

    private void closeStoreQuietly() {
        if (store == null) {
            return;
        }
        try {
            store.close();
        } catch (RuntimeException ignored) {
            // The store is being discarded with its "process"; close failures are not the test.
        }
        store = null;
    }

    private RelayRecordFactory relayFactory() {
        String dlq = config.route().hasDlq() ? config.route().dlq().get().topic() : null;
        return new RelayRecordFactory(
                config.route().destination().topic(),
                dlq,
                config.headers().stampProvenance(),
                config.route().relay().timestamp(),
                config.route().relay().partitioning());
    }

    /**
     * The documented first-run operator step (I-9 runbook): seed committed offset 0 for every
     * source partition that has none, exactly once per harness. Restarts after the loop committed
     * find offsets present and never rewind.
     */
    private void seedFirstRunOffsets() {
        if (offsetsSeeded) {
            return;
        }
        String source = sourceTopic();
        try {
            int partitions = admin.describeTopics(List.of(source))
                    .allTopicNames()
                    .get()
                    .get(source)
                    .partitions()
                    .size();
            Map<TopicPartition, OffsetAndMetadata> existing = admin.listConsumerGroupOffsets(ingestGroupId())
                    .partitionsToOffsetAndMetadata()
                    .get();
            Map<TopicPartition, OffsetAndMetadata> seed = new HashMap<>();
            for (int p = 0; p < partitions; p++) {
                TopicPartition tp = new TopicPartition(source, p);
                if (!existing.containsKey(tp)) {
                    seed.put(tp, new OffsetAndMetadata(0L));
                }
            }
            if (!seed.isEmpty()) {
                admin.alterConsumerGroupOffsets(ingestGroupId(), seed).all().get();
            }
            offsetsSeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while seeding first-run offsets", e);
        } catch (Exception e) {
            throw new AssertionError("seeding first-run group offsets failed", e);
        }
    }

    // ------------------------------------------------------------------ plumbing types

    /** One running loop: its thread, its graceful stopper, and the throwable that killed it. */
    private static final class LoopHandle {
        final Thread thread;
        final Runnable stopper;
        final AtomicReference<Throwable> failure;

        LoopHandle(Thread thread, Runnable stopper, AtomicReference<Throwable> failure) {
            this.thread = thread;
            this.stopper = stopper;
            this.failure = failure;
        }
    }

    /**
     * The production-shaped {@link StoreContext}: real route identity, the {@code store.properties}
     * subtree, the system clock (the store keeps no clock state; due decisions arrive as
     * {@code nowMs} from the engine), and a placeholder epoch (tracker-backed stores ride the
     * engine's transactions and never consult it).
     */
    private record HarnessStoreContext(RouteDescriptor route, ConfigView config, MeterRegistry meterRegistry)
            implements StoreContext {

        @Override
        public Clock clock() {
            return Clock.systemUTC();
        }

        @Override
        public OwnershipEpoch epoch(int partition) {
            return new OwnershipEpoch(0, "it-harness");
        }
    }

    // ------------------------------------------------------------------ builder

    /** Assembles the {@link CesiumConfig} records directly — the §8 defaults fill absent parts. */
    static final class Builder {

        /**
         * Default store-side pending cap: small enough that the worst-case footprint check
         * ({@code partitions × cap × 64 B ≤ heap/4}, §5.3) passes in CI JVMs of any size, large
         * enough that no IT load ever brushes it.
         */
        private static final String DEFAULT_STORE_MAX_PENDING = "100000";

        private String applicationId = KafkaIT.unique("app");
        private InstanceId instanceId = InstanceId.of("it-1");
        private KafkaConfig.GroupProtocol groupProtocol = KafkaConfig.GroupProtocol.CLASSIC;
        private Set<Role> roles = Set.of(Role.INGEST);
        private String sourceTopic;
        private String destinationTopic;
        private String dlqTopic;
        private String trackerTopic = "";
        private Duration delayMax = DelayConfig.DEFAULT_MAX;
        private MalformedHeaderPolicy onMalformedHeader = MalformedHeaderPolicy.DLQ;
        private OverMaxPolicy onOverMax = OverMaxPolicy.DLQ;
        private TrackerConfig.Bootstrap bootstrap = TrackerConfig.Bootstrap.CREATE;
        private CheckMode retentionCheck;
        private StartupChecks.SizeBasedRetention sizeBasedRetention;
        private Duration idleCursorInterval;
        private long dispatchMaxPendingPerPartition = 10_000L;
        private String bootstrapServers;
        private Duration transactionTimeout;
        private Duration drainMaxSlice;
        private Long batchMaxBytes;
        private final Map<String, String> kafkaProperties = new HashMap<>();
        private final Map<String, String> storeProperties = new HashMap<>();
        private UnaryOperator<SeekFetcher> seekFetcherDecorator = UnaryOperator.identity();

        Builder applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        /**
         * The stable deployment-slot id (default {@code it-1}) that drives this instance's
         * {@code transactional.id}s and static {@code group.instance.id}s. M6 multi-instance
         * harnesses give each member a distinct id so two engines form one real consumer group with
         * two static members under the same {@code applicationId} (design D10/D21).
         */
        Builder instanceId(String instanceId) {
            this.instanceId = InstanceId.of(instanceId);
            return this;
        }

        /**
         * Selects the consumer group protocol both loops run under (default
         * {@link KafkaConfig.GroupProtocol#CLASSIC}). The KIP-848 lane (design §3.4.5, §11.3-11,
         * D12) passes {@link KafkaConfig.GroupProtocol#CONSUMER} so the same scenario body re-runs
         * under {@code group.protocol=consumer}; {@link KafkaClientFactory} then sets
         * {@code group.protocol=consumer} and drops the classic-only {@code CooperativeStickyAssignor}.
         * Leaving it at the default keeps all 28 pre-M6 ITs and every classic scenario byte-identical.
         */
        Builder groupProtocol(KafkaConfig.GroupProtocol groupProtocol) {
            this.groupProtocol = Objects.requireNonNull(groupProtocol, "groupProtocol");
            return this;
        }

        /**
         * Overrides the bootstrap brokers (default {@link KafkaIT#bootstrap()}). M6 fault-injection
         * harnesses point an instance at a dedicated proxied broker, or at a per-instance Toxiproxy
         * endpoint, so a single member's client&#8596;broker link can be cut while the rest of the
         * group keeps heartbeating. Resolved lazily, so an instance that sets it never triggers the
         * shared {@link KafkaIT} broker's static startup.
         */
        Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        /**
         * One common {@code kafka.properties} passthrough entry applied to every client (e.g. a
         * shorter {@code session.timeout.ms}/{@code heartbeat.interval.ms} so a killed static member
         * is evicted in seconds rather than the 45 s production default). Locked correctness keys are
         * still re-asserted by {@link KafkaClientFactory}; passthrough cannot override them.
         */
        Builder kafkaProperty(String key, String value) {
            this.kafkaProperties.put(key, value);
            return this;
        }

        /**
         * Wraps the real {@link KafkaSeekFetcher} this instance builds (default: no wrapping). The
         * §11.3-10 penalty-box isolation scenario injects a decorator that stamps a chosen source
         * partition's fetch outcomes {@code TRANSIENT} so the loop penalty-boxes exactly that
         * partition while healthy partitions keep dispatching — the deterministic substrate the
         * single-broker cluster cannot provide by degrading one partition's leader (R21).
         */
        Builder seekFetcherDecorator(UnaryOperator<SeekFetcher> decorator) {
            this.seekFetcherDecorator = Objects.requireNonNull(decorator, "decorator");
            return this;
        }

        /** The loops this instance runs; default ingest-only (the M4 suite's shape). */
        Builder roles(Role... roles) {
            this.roles = Set.of(roles);
            return this;
        }

        Builder source(String topic) {
            this.sourceTopic = topic;
            return this;
        }

        Builder destination(String topic) {
            this.destinationTopic = topic;
            return this;
        }

        Builder dlq(String topic) {
            this.dlqTopic = topic;
            return this;
        }

        Builder trackerTopic(String topic) {
            this.trackerTopic = topic;
            return this;
        }

        Builder delayMax(Duration delayMax) {
            this.delayMax = delayMax;
            return this;
        }

        Builder onMalformedHeader(MalformedHeaderPolicy policy) {
            this.onMalformedHeader = policy;
            return this;
        }

        Builder onOverMax(OverMaxPolicy policy) {
            this.onOverMax = policy;
            return this;
        }

        Builder trackerBootstrap(TrackerConfig.Bootstrap mode) {
            this.bootstrap = mode;
            return this;
        }

        Builder retentionCheck(CheckMode mode) {
            this.retentionCheck = mode;
            return this;
        }

        Builder acknowledgeSizeBasedRetention() {
            this.sizeBasedRetention = StartupChecks.SizeBasedRetention.ACKNOWLEDGED;
            return this;
        }

        /** §3.5 idle cursor advancement cadence; tests shrink it from the 30 s default. */
        Builder idleCursorInterval(Duration interval) {
            this.idleCursorInterval = interval;
            return this;
        }

        /**
         * The §5.3 per-ACTIVE-shard backpressure high-water mark ({@code
         * dispatch.max-pending-per-partition}); intake pauses above it and resumes below half. The
         * IT default (10,000) sits far above any normal IT load; the backpressure scenario shrinks
         * it to a handful so a tiny synthetic backlog trips the pause without producing 10k records.
         */
        Builder maxPendingPerPartition(long highWater) {
            this.dispatchMaxPendingPerPartition = highWater;
            return this;
        }

        /** One {@code store.properties} entry (e.g. a tiny sidecar budget for overflow tests). */
        Builder storeProperty(String key, String value) {
            this.storeProperties.put(key, value);
            return this;
        }

        /**
         * Raises the typed {@code kafka.transactions.timeout} (D9 default 30 s; the locked key that
         * {@link KafkaClientFactory} re-asserts over any passthrough, so it cannot be set via
         * {@link #kafkaProperty}). The §11.3-5 I8 barrier-ordering scenario deliberately stalls a
         * dispatch transaction at the post-{@code sendOffsetsToTransaction}/pre-{@code commit} seam
         * across a failover; without a longer timeout the broker would auto-abort the dangling
         * transaction before the test releases it, collapsing the "predecessor commits after takeover
         * begins" interleaving the test must exercise.
         */
        Builder transactionTimeout(Duration timeout) {
            this.transactionTimeout = timeout;
            return this;
        }

        /**
         * Shrinks the §6 drain {@code max-slice} (default 1 min). The validator requires
         * {@code max-slice <= max.poll.interval.ms / 3} (R2); a scenario that lowers
         * {@code max.poll.interval.ms} to evict a deliberately stalled member in seconds (the §11.3-5
         * I8 failover) must lower the slice in step so the assembled config stays valid. Smaller slices
         * only make the loop interleave polls more often — no transactional behavior changes.
         */
        Builder drainMaxSlice(Duration slice) {
            this.drainMaxSlice = slice;
            return this;
        }

        /**
         * Shrinks the §7.2 per-transaction decompressed-payload byte budget
         * ({@code dispatch.batch.max-bytes}; default 32 MiB). The §11.4 large-payload macro test
         * (R8) sets a small budget so a backlog of multi-megabyte due records provably
         * <em>truncates-and-carries-over</em> across several dispatch transactions — the fetch path
         * never materializes the whole backlog at once — rather than fetching everything into one
         * oversized batch. Behaviour-only: smaller batches change nothing transactional.
         */
        Builder batchMaxBytes(long maxBytes) {
            this.batchMaxBytes = maxBytes;
            return this;
        }

        EngineHarness build() {
            Objects.requireNonNull(sourceTopic, "source topic");
            Objects.requireNonNull(destinationTopic, "destination topic");
            Objects.requireNonNull(dlqTopic, "dlq topic (the default policies route to it)");
            Map<String, String> storeProps = new HashMap<>();
            storeProps.put(KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY, DEFAULT_STORE_MAX_PENDING);
            storeProps.putAll(storeProperties);
            Map<String, String> commonKafkaProps = new HashMap<>(kafkaProperties);
            commonKafkaProps.put(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers != null ? bootstrapServers : KafkaIT.bootstrap());
            CesiumConfig config = new CesiumConfig(
                    applicationId,
                    instanceId,
                    roles,
                    new KafkaConfig(
                            groupProtocol,
                            commonKafkaProps,
                            transactionTimeout == null ? null : new KafkaConfig.Transactions(transactionTimeout, null),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null),
                    new RouteConfig(
                            new TopicRef(sourceTopic),
                            new TopicRef(destinationTopic),
                            new TrackerConfig(trackerTopic, bootstrap, Optional.empty()),
                            Optional.of(new TopicRef(dlqTopic)),
                            null),
                    new DelayConfig(delayMax, onOverMax, onMalformedHeader),
                    null,
                    new StoreConfig(null, storeProps),
                    null,
                    // Small per-partition pending cap keeps the heap-budget check trivially green
                    // in CI JVMs of any size; IT loads never approach it (overridable for the §5.3
                    // backpressure scenario via Builder.maxPendingPerPartition).
                    new DispatchConfig(
                            null,
                            batchMaxBytes == null ? null : new DispatchConfig.Batch(null, batchMaxBytes),
                            drainMaxSlice == null ? null : new DispatchConfig.Drain(drainMaxSlice),
                            null,
                            idleCursorInterval,
                            null,
                            null,
                            null,
                            dispatchMaxPendingPerPartition,
                            null),
                    null,
                    // The integration brokers run PLAINTEXT with no authorizer, so the R12 tracker-ACL
                    // check cannot be verified there; SKIP it (the fix's default is FAIL in production).
                    new StartupChecks(retentionCheck, sizeBasedRetention, null, null, null, CheckMode.SKIP));
            EngineHarness harness = new EngineHarness(config);
            harness.seekFetcherDecorator = seekFetcherDecorator;
            return harness;
        }
    }
}
