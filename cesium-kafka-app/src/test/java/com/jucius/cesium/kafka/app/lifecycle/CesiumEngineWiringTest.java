package com.jucius.cesium.kafka.app.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.app.health.ShardRecovery;
import com.jucius.cesium.kafka.core.admin.StartupValidator;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The config-to-engine wiring decisions that do not need a broker (design §6, §10): the sidecar
 * budget, and the health and re-validation plumbing.
 */
class CesiumEngineWiringTest {

    @Nested
    class RecoverySnapshot {

        private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        private final Map<Integer, long[]> previous = new HashMap<>();

        private AtomicLong shard(int partition, long state, long remaining) {
            AtomicLong stateValue = new AtomicLong(state);
            AtomicLong remainingValue = new AtomicLong(remaining);
            Gauge.builder(KafkaTrackerStore.SHARD_STATE_METRIC, stateValue::get)
                    .tag("partition", Integer.toString(partition))
                    .register(registry);
            Gauge.builder(KafkaTrackerStore.REPLAY_REMAINING_METRIC, remainingValue::get)
                    .tag("partition", Integer.toString(partition))
                    .register(registry);
            return remainingValue;
        }

        @Test
        void reportsNonActiveShardsWithASlopeEta() {
            AtomicLong remaining = shard(1, 1, 1_000);
            shard(0, 0, 0);
            shard(2, 2, 0); // ACTIVE: omitted

            assertEquals(
                    List.of(
                            new ShardRecovery(0, ShardRecovery.State.ASSIGNED, 0, -1),
                            new ShardRecovery(1, ShardRecovery.State.RECOVERING, 1_000, -1)),
                    CesiumEngine.recoverySnapshot(registry, 10_000, previous),
                    "no ETA from a single sample");

            remaining.set(800); // 200 records in 1 s -> 800 records left ~ 4 s
            assertEquals(
                    new ShardRecovery(1, ShardRecovery.State.RECOVERING, 800, 4_000),
                    CesiumEngine.recoverySnapshot(registry, 11_000, previous).get(1));

            // A stalled backlog has no estimable ETA.
            assertEquals(
                    -1,
                    CesiumEngine.recoverySnapshot(registry, 12_000, previous)
                            .get(1)
                            .etaMillis());
        }

        @Test
        void emptyWithoutStoreGauges() {
            assertEquals(List.of(), CesiumEngine.recoverySnapshot(registry, 0, previous));
        }
    }

    // ------------------------------------------------------------------ re-validation (D23)

    @Nested
    class Revalidation {

        private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        private final CesiumEngine engine =
                new CesiumEngine(config(Set.of(Role.DISPATCH)), registry, Clock.systemUTC(), Duration.ofSeconds(1));

        private StartupValidator.Revalidation errors(String... paths) {
            List<ValidationReport.Finding> findings = new ArrayList<>();
            for (String path : paths) {
                findings.add(ValidationReport.Finding.error(path, "broken"));
            }
            return new StartupValidator.Revalidation(Optional.empty(), new ValidationReport(findings));
        }

        private double errorsGauge() {
            return registry.get(CesiumEngine.REVALIDATION_ERRORS_GAUGE).gauge().value();
        }

        @Test
        void degradedCauseCombinesLoopsAndRevalidation() {
            assertNull(CesiumEngine.degradedCause(Set.of(), null));
            assertEquals("parked-and-degraded loop(s): dispatch", CesiumEngine.degradedCause(Set.of("dispatch"), null));
            assertEquals("re-validation: x", CesiumEngine.degradedCause(Set.of(), "re-validation: x"));
            assertEquals(
                    "parked-and-degraded loop(s): ingest; re-validation: x",
                    CesiumEngine.degradedCause(Set.of("ingest"), "re-validation: x"));
        }

        @Test
        void anIdentityChangeIsFatal() {
            engine.onRevalidation(new StartupValidator.Revalidation(
                    Optional.of("re-validation found the route identity changed since startup"),
                    new ValidationReport(List.of())));

            Throwable cause = engine.fatalCause().orElseThrow();
            assertTrue(cause.getMessage().contains("identity changed"), cause::getMessage);
        }

        @Test
        void errorsDegradeAndACleanRoundClears() {
            engine.onRevalidation(errors("route.tracker", "route.tracker", "route.dlq.topic"));

            assertTrue(engine.fatalCause().isEmpty(), "errors degrade; they never stop the engine (D1)");
            assertEquals(3.0, errorsGauge());
            assertEquals("re-validation: 3 error(s) at route.tracker, route.dlq.topic", engine.revalidationCause());

            engine.onRevalidation(errors());

            assertEquals(0.0, errorsGauge());
            assertNull(engine.revalidationCause());
        }

        @Test
        void everythingIsIgnoredOnceStopping() {
            engine.stop(Duration.ofSeconds(1));

            engine.onRevalidation(errors("route.tracker"));
            engine.onRevalidation(new StartupValidator.Revalidation(
                    Optional.of("identity changed"), new ValidationReport(List.of())));

            assertTrue(engine.fatalCause().isEmpty());
            assertEquals(0.0, errorsGauge());
            assertNull(engine.revalidationCause());
        }

        @Test
        void aThrowingRoundIsReportedAndTheNextRoundStillRuns() throws Exception {
            AtomicInteger calls = new AtomicInteger();
            BlockingQueue<StartupValidator.Revalidation> results = new LinkedBlockingQueue<>();
            ScheduledExecutorService executor = CesiumEngine.scheduleRevalidation(
                    Duration.ofMillis(10),
                    "orders",
                    () -> {
                        if (calls.incrementAndGet() == 1) {
                            throw new IllegalStateException("admin exploded");
                        }
                        return errors();
                    },
                    results::add);
            try {
                StartupValidator.Revalidation first = results.poll(10, TimeUnit.SECONDS);
                StartupValidator.Revalidation second = results.poll(10, TimeUnit.SECONDS);

                assertNotNull(first);
                assertTrue(
                        first.report().errors().getFirst().message().contains("round could not complete"),
                        first.report()::render);
                assertNotNull(second, "a throwing round must not cancel the schedule");
                assertFalse(second.report().hasErrors());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    // ------------------------------------------------------------------ sidecar budget (§3.5)

    @Nested
    class SidecarBudget {

        @Test
        void theConfiguredBudgetIsClampedToTheBrokerCap() {
            assertEquals(2048, CesiumEngine.sidecarBudget(2048, OptionalInt.of(4096)));
            assertEquals(4096, CesiumEngine.sidecarBudget(8192, OptionalInt.of(4096)));
        }

        @Test
        void anUnknownBrokerCapPassesTheBudgetThrough() {
            assertEquals(8192, CesiumEngine.sidecarBudget(8192, OptionalInt.empty()));
        }

        @Test
        void aBrokerCapBelowTheStoreMinimumFloorsAtTheMinimum() {
            assertEquals(
                    DispatchConfig.Cursor.MIN_SIDECAR_MAX_BYTES, CesiumEngine.sidecarBudget(3072, OptionalInt.of(100)));
        }
    }

    /** A structurally valid config (route + DLQ + defaults) with the given roles. */
    private static CesiumConfig config(Set<Role> roles) {
        return new CesiumConfig(
                "orders",
                InstanceId.of("slot-0"),
                roles,
                null,
                new RouteConfig(new TopicRef("src"), new TopicRef("dst"), null, Optional.of(new TopicRef("dlq")), null),
                null,
                null,
                null,
                null,
                null);
    }
}
