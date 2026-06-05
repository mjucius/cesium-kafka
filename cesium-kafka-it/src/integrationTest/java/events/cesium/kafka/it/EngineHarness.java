package events.cesium.kafka.it;

import events.cesium.kafka.core.admin.IdentityBlob;
import events.cesium.kafka.core.admin.KafkaClusterAdmin;
import events.cesium.kafka.core.admin.StartupValidationResult;
import events.cesium.kafka.core.admin.StartupValidator;
import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.CesiumConfigValidator;
import events.cesium.kafka.core.config.CheckMode;
import events.cesium.kafka.core.config.DelayConfig;
import events.cesium.kafka.core.config.DispatchConfig;
import events.cesium.kafka.core.config.InstanceId;
import events.cesium.kafka.core.config.KafkaConfig;
import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.core.config.RouteConfig;
import events.cesium.kafka.core.config.StartupChecks;
import events.cesium.kafka.core.config.TopicRef;
import events.cesium.kafka.core.config.TrackerConfig;
import events.cesium.kafka.core.config.ValidationContext;
import events.cesium.kafka.core.config.ValidationReport;
import events.cesium.kafka.core.headers.DelayHeaderCodec;
import events.cesium.kafka.core.headers.RelayRecordFactory;
import events.cesium.kafka.core.ingest.IngestLoop;
import events.cesium.kafka.core.ingest.IngestLoopConfig;
import events.cesium.kafka.core.kafka.KafkaClientFactory;
import events.cesium.kafka.core.policy.IngestPolicyEngine;
import events.cesium.kafka.core.policy.MalformedHeaderPolicy;
import events.cesium.kafka.core.policy.OverMaxPolicy;
import events.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Assembles a real M4 engine instance for integration tests: {@link CesiumConfig} records built
 * directly (no YAML), {@link CesiumConfigValidator} + {@link StartupValidator} (which performs the
 * tracker {@code CREATE} bootstrap), and the {@link IngestLoop} on a daemon thread wired through
 * {@link KafkaClientFactory} — real group-A consumer, real transactional producer, real
 * {@code TrackerWireFormat} encoding via {@link MinimalTrackerStore}.
 *
 * <p><strong>Restartable.</strong> {@link #start()} builds fresh clients and a fresh loop each
 * call, so crash tests restart the "process" by calling {@code start()} again on the same harness
 * — same application id, same {@code instanceId} (stable transactional.id + static membership),
 * exactly the recovery a restarted deployment slot performs.
 *
 * <p><strong>First-run offsets.</strong> {@code auto.offset.reset=none} is locked (D18), so a
 * brand-new group has no position and the loop fail-fasts by design (I-9). The harness performs
 * the documented operator runbook step once per harness: it seeds committed offset 0 for every
 * source partition via {@code Admin.alterConsumerGroupOffsets} before the first start.
 *
 * <p>Crash helpers wire to {@link CrashPoints}; tests must {@link CrashPoints#reset()} between
 * crash and restart (and in teardown).
 */
final class EngineHarness implements AutoCloseable {

    static {
        // The crash-point seam refuses to arm without the explicit test-only guard property.
        System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
    }

    private final CesiumConfig config;
    private final Admin admin;
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private IngestLoop loop;
    private Thread thread;
    private AtomicReference<Throwable> loopFailure = new AtomicReference<>();
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
     * Starts (or restarts) the engine: validates, seeds first-run offsets, builds fresh clients
     * and a fresh {@link IngestLoop}, and spawns the loop on a daemon thread.
     */
    void start() {
        if (thread != null && thread.isAlive()) {
            throw new IllegalStateException("harness already running");
        }
        StartupValidationResult result = validate();
        if (result.report().hasErrors()) {
            throw new AssertionError(
                    "startup validation failed:\n" + result.report().render());
        }
        this.identity = result.identity().orElseThrow();
        seedFirstRunOffsets();

        KafkaClientFactory clients = new KafkaClientFactory(config);
        loopFailure = new AtomicReference<>();
        loop = new IngestLoop(
                // Tracker partitions == source partitions once the R-7 parity check passed.
                IngestLoopConfig.from(config, result.sourcePartitions().orElseThrow()),
                clients.newIngestConsumer(),
                () -> clients.newIngestProducer(0),
                new MinimalTrackerStore(),
                new DelayHeaderCodec(config.delay().max(), config.headers().acceptBinaryLongValues()),
                new IngestPolicyEngine(
                        config.delay().max(),
                        config.delay().onMalformedHeader(),
                        config.delay().onOverMax()),
                relayFactory(),
                identity,
                meterRegistry,
                Clock.systemUTC());
        AtomicReference<Throwable> failureSink = loopFailure;
        IngestLoop running = loop;
        thread = new Thread(
                () -> {
                    try {
                        running.run();
                    } catch (Throwable t) {
                        failureSink.set(t);
                    }
                },
                "it-ingest-" + config.applicationId());
        thread.setDaemon(true);
        thread.start();
    }

    /** Graceful stop: raises the loop's stop flag and joins the thread. */
    void stop() {
        if (loop == null || thread == null) {
            return;
        }
        try {
            loop.stop();
        } catch (RuntimeException ignored) {
            // The loop may already have died and closed its consumer; close() below verifies.
        }
        joinLoopThread(Duration.ofSeconds(30));
        if (thread.isAlive()) {
            throw new AssertionError("ingest loop did not stop within 30s");
        }
    }

    /**
     * Waits for the loop thread to die (crash tests) and returns the throwable that killed it.
     * Fails when the thread survives the timeout or exited without a failure.
     */
    Throwable awaitDeath(Duration timeout) {
        Objects.requireNonNull(thread, "harness was never started");
        joinLoopThread(timeout);
        if (thread.isAlive()) {
            throw new AssertionError("ingest loop is still alive after " + timeout);
        }
        Throwable failure = loopFailure.get();
        if (failure == null) {
            throw new AssertionError("ingest loop exited cleanly; expected a crash");
        }
        return failure;
    }

    @Override
    public void close() {
        if (thread != null && thread.isAlive() && loop != null) {
            try {
                loop.stop();
            } catch (RuntimeException ignored) {
                // Best effort: a crashed loop already closed its clients.
            }
            joinLoopThread(Duration.ofSeconds(30));
        }
        admin.close();
        meterRegistry.close();
    }

    private void joinLoopThread(Duration timeout) {
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while joining the loop thread", e);
        }
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
        return "cesium." + config.applicationId() + ".ingest";
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

    // ------------------------------------------------------------------ builder

    /** Assembles the {@link CesiumConfig} records directly — the §8 defaults fill absent parts. */
    static final class Builder {

        private String applicationId = KafkaIT.unique("app");
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

        Builder applicationId(String applicationId) {
            this.applicationId = applicationId;
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

        EngineHarness build() {
            Objects.requireNonNull(sourceTopic, "source topic");
            Objects.requireNonNull(destinationTopic, "destination topic");
            Objects.requireNonNull(dlqTopic, "dlq topic (the default policies route to it)");
            CesiumConfig config = new CesiumConfig(
                    applicationId,
                    InstanceId.of("it-1"),
                    Set.of(Role.INGEST),
                    new KafkaConfig(
                            null,
                            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIT.bootstrap()),
                            null,
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
                    null,
                    null,
                    // Small per-partition pending cap keeps the heap-budget check trivially green
                    // in CI JVMs of any size; M4 never builds the index anyway.
                    new DispatchConfig(null, null, null, null, null, null, null, null, 10_000L, null),
                    null,
                    new StartupChecks(retentionCheck, sizeBasedRetention, null, null, null));
            return new EngineHarness(config);
        }
    }
}
