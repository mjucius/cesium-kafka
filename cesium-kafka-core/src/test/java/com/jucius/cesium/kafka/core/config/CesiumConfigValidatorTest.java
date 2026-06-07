package com.jucius.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Decision tables for the aggregate validator (design §8, §11.1). */
class CesiumConfigValidatorTest {

    private final CesiumConfigValidator validator = new CesiumConfigValidator();

    private ValidationReport validate(CesiumConfig config) {
        return validator.validate(config, TestConfigs.ROOMY_CONTEXT);
    }

    private static List<String> errorPaths(ValidationReport report) {
        return report.errors().stream().map(Finding::path).toList();
    }

    @Test
    void minimalValidConfigHasNoErrors() {
        ValidationReport report = validate(TestConfigs.valid());
        assertFalse(report.hasErrors(), report::render);
    }

    @Test
    void validateOrThrowReturnsTheConfigWhenClean() {
        CesiumConfig config = TestConfigs.valid();
        assertSame(config, validator.validateOrThrow(config, TestConfigs.ROOMY_CONTEXT));
    }

    // ------------------------------------------------------------------ aggregate reporting

    @Test
    void everyViolationIsCollectedIntoOneReport() {
        // Missing applicationId + missing instanceId + missing topics + bad workers + DLQ policies
        // without a DLQ topic: all must surface in a single pass — never fail-on-first (§8).
        CesiumConfig broken = new CesiumConfig(
                null, null, null, null, null, null, null, null, new IngestConfig(0, null), null, null, null);
        ValidationReport report = validate(broken);
        List<String> paths = errorPaths(report);
        assertTrue(paths.contains("application-id"), report::render);
        assertTrue(paths.contains("instance-id"), report::render);
        assertTrue(paths.contains("route.source.topic"), report::render);
        assertTrue(paths.contains("route.destination.topic"), report::render);
        assertTrue(paths.contains("ingest.workers"), report::render);
        assertTrue(paths.contains("delay.on-malformed-header"), report::render);
        assertTrue(paths.contains("delay.on-over-max"), report::render);
        assertTrue(paths.contains("dispatch.on-unfetchable-payload"), report::render);
        assertTrue(paths.size() >= 8, report::render);
    }

    @Test
    void validateOrThrowCarriesTheFullReport() {
        CesiumConfig broken = new CesiumConfig(null, null, null, null, null, null, null, null, null, null, null, null);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> validator.validateOrThrow(broken, TestConfigs.ROOMY_CONTEXT));
        assertTrue(exception.report().hasErrors());
        assertTrue(exception.getMessage().contains("application-id"));
        assertTrue(exception.getMessage().contains("route.source.topic"));
    }

    @Test
    void instanceIdErrorMentionsTheRandomOptIn() {
        CesiumConfig broken = new CesiumConfig(
                "app", new InstanceId(""), null, null, TestConfigs.route(), null, null, null, null, null, null, null);
        ValidationReport report = validate(broken);
        Finding finding = report.errors().stream()
                .filter(f -> f.path().equals("instance-id"))
                .findFirst()
                .orElseThrow();
        assertTrue(finding.message().contains("random"));
    }

    @Test
    void emptyRolesAreRejected() {
        CesiumConfig broken = new CesiumConfig(
                "app",
                new InstanceId("slot-0"),
                Set.of(),
                null,
                TestConfigs.route(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        assertTrue(errorPaths(validate(broken)).contains("roles"));
    }

    // ------------------------------------------------------------------ locked Kafka keys

    @Nested
    class LockedKeys {

        @ParameterizedTest
        @MethodSource("com.jucius.cesium.kafka.core.config.CesiumConfigValidatorTest#allLockedKeys")
        void everyLockedKeyIsRejectedInCommonProperties(String key) {
            CesiumConfig config = TestConfigs.withKafkaProperties(Map.of(key, "anything"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            Finding finding = report.errors().stream()
                    .filter(f -> f.path().equals("kafka.properties." + key))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no rejection for " + key + "\n" + report.render()));
            assertTrue(finding.message().contains("locked"), finding::message);
        }

        @ParameterizedTest
        @MethodSource("com.jucius.cesium.kafka.core.config.CesiumConfigValidatorTest#allClientOverlays")
        void lockedKeysAreRejectedInEveryClientOverlay(int position, String pathPrefix) {
            CesiumConfig config = TestConfigs.withClientOverlay(position, Map.of("isolation.level", "read_committed"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            assertTrue(
                    errorPaths(report).contains(pathPrefix + ".isolation.level"),
                    () -> "expected rejection under " + pathPrefix + "\n" + report.render());
        }

        @Test
        void isolationLevelRejectionExplainsTheRequireStableDependency() {
            // D17: even setting the "correct" value is rejected — it is not a tuning knob.
            CesiumConfig config = TestConfigs.withKafkaProperties(Map.of("isolation.level", "read_committed"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            Finding finding = report.errors().getFirst();
            assertTrue(finding.message().contains("KIP-447"), finding::message);
            assertTrue(finding.message().contains("require_stable"), finding::message);
        }

        @Test
        void autoOffsetResetRejectionExplainsExplicitOperatorResets() {
            CesiumConfig config = TestConfigs.withKafkaProperties(Map.of("auto.offset.reset", "earliest"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            Finding finding = report.errors().getFirst();
            assertTrue(finding.message().contains("explicit operator decisions"), finding::message);
        }

        @Test
        void unlockedKeysPassThrough() {
            CesiumConfig config = TestConfigs.withKafkaProperties(
                    Map.of("bootstrap.servers", "localhost:9092", "max.poll.interval.ms", "300000"));
            assertFalse(validator.validate(config, TestConfigs.ROOMY_CONTEXT).hasErrors());
        }
    }

    static Stream<String> allLockedKeys() {
        return LockedKafkaKeys.all().stream();
    }

    static Stream<Arguments> allClientOverlays() {
        return Stream.of(
                Arguments.of(0, "kafka.ingest-consumer.properties"),
                Arguments.of(1, "kafka.tracker-consumer.properties"),
                Arguments.of(2, "kafka.seek-consumer.properties"),
                Arguments.of(3, "kafka.ingest-producer.properties"),
                Arguments.of(4, "kafka.dispatch-producer.properties"),
                Arguments.of(5, "kafka.admin.properties"));
    }

    // ------------------------------------------------------------------ cross-field: DLQ policies

    @Nested
    class DlqPolicies {

        private CesiumConfig withoutDlq(
                MalformedHeaderPolicy onMalformed, OverMaxPolicy onOverMax, UnfetchablePayloadPolicy onUnfetchable) {
            RouteConfig route =
                    new RouteConfig(new TopicRef("orders"), new TopicRef("orders-out"), null, Optional.empty(), null);
            DelayConfig delay = new DelayConfig(null, onOverMax, onMalformed);
            DispatchConfig dispatch =
                    new DispatchConfig(null, null, null, null, null, null, null, onUnfetchable, null, null);
            return new CesiumConfig(
                    "app", new InstanceId("slot-0"), null, null, route, delay, null, null, null, dispatch, null, null);
        }

        @Test
        void eachDlqPolicyRequiresTheDlqTopic() {
            ValidationReport report = validator.validate(
                    withoutDlq(MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ, UnfetchablePayloadPolicy.DLQ),
                    TestConfigs.ROOMY_CONTEXT);
            List<String> paths = errorPaths(report);
            assertTrue(paths.contains("delay.on-malformed-header"), report::render);
            assertTrue(paths.contains("delay.on-over-max"), report::render);
            assertTrue(paths.contains("dispatch.on-unfetchable-payload"), report::render);
        }

        @Test
        void nonDlqPoliciesNeedNoDlqTopic() {
            ValidationReport report = validator.validate(
                    withoutDlq(
                            MalformedHeaderPolicy.RELAY_IMMEDIATE, OverMaxPolicy.CLAMP, UnfetchablePayloadPolicy.DROP),
                    TestConfigs.ROOMY_CONTEXT);
            assertFalse(report.hasErrors(), report::render);
        }

        @Test
        void singleDlqPolicyIsAttributedToItsKey() {
            ValidationReport report = validator.validate(
                    withoutDlq(MalformedHeaderPolicy.DLQ, OverMaxPolicy.FAIL, UnfetchablePayloadPolicy.DROP),
                    TestConfigs.ROOMY_CONTEXT);
            assertEquals(List.of("delay.on-malformed-header"), errorPaths(report), report::render);
        }

        @Test
        void blankDlqTopicIsRejected() {
            RouteConfig route = new RouteConfig(
                    new TopicRef("orders"), new TopicRef("orders-out"), null, Optional.of(new TopicRef("  ")), null);
            CesiumConfig config = new CesiumConfig(
                    "app", new InstanceId("slot-0"), null, null, route, null, null, null, null, null, null, null);
            assertTrue(errorPaths(validator.validate(config, TestConfigs.ROOMY_CONTEXT))
                    .contains("route.dlq.topic"));
        }
    }

    // ------------------------------------------------------------------ cross-field: drain slice

    @Nested
    class DrainSlice {

        @Test
        void sliceAboveOneThirdOfMaxPollIntervalIsRejected() {
            // Default slice PT1M (60s) > 60000/3 ms.
            CesiumConfig config =
                    TestConfigs.withKafkaProperties(Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "60000"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            Finding finding = report.errors().stream()
                    .filter(f -> f.path().equals("dispatch.drain.max-slice"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(report.render()));
            assertTrue(finding.message().contains("max.poll.interval.ms / 3"), finding::message);
        }

        @Test
        void sliceAtTheBoundPasses() {
            // 300000/3 = 100000 ms; default slice 60000 ms is fine.
            CesiumConfig config =
                    TestConfigs.withKafkaProperties(Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "300000"));
            assertFalse(validator.validate(config, TestConfigs.ROOMY_CONTEXT).hasErrors());
        }

        @Test
        void defaultSlicePassesWhenMaxPollIntervalUnset() {
            // Default slice PT1M (60000 ms) <= kafka-clients default 300000 / 3 = 100000 ms.
            assertFalse(validator
                    .validate(TestConfigs.valid(), TestConfigs.ROOMY_CONTEXT)
                    .hasErrors());
        }

        @Test
        void capIsEnforcedAgainstTheClientDefaultWhenMaxPollIntervalUnset() {
            // §8 states the cap unconditionally: PT5M (300000 ms) > 300000 / 3 even when the
            // operator never touched max.poll.interval.ms — refuse at startup, not at runtime.
            DispatchConfig dispatch = new DispatchConfig(
                    null,
                    null,
                    new DispatchConfig.Drain(Duration.ofMinutes(5)),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            ValidationReport report = validate(TestConfigs.withDispatch(dispatch));
            Finding finding = report.errors().stream()
                    .filter(f -> f.path().equals("dispatch.drain.max-slice"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(report.render()));
            assertTrue(finding.message().contains("kafka-clients default"), finding::message);
        }

        @Test
        void trackerConsumerOverlayWinsOverCommonProperties() {
            // Common says 300000 (slice fine); overlay tightens to 60000 (slice violates).
            KafkaConfig kafka = TestConfigs.kafka(
                    Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "300000"),
                    new KafkaConfig.ClientOverrides(Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "60000")));
            ValidationReport report = validator.validate(TestConfigs.withKafka(kafka), TestConfigs.ROOMY_CONTEXT);
            assertTrue(errorPaths(report).contains("dispatch.drain.max-slice"), report::render);
        }

        @Test
        void unparseableMaxPollIntervalIsItselfAnError() {
            CesiumConfig config =
                    TestConfigs.withKafkaProperties(Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "five minutes"));
            ValidationReport report = validator.validate(config, TestConfigs.ROOMY_CONTEXT);
            assertTrue(
                    errorPaths(report).contains("kafka.properties." + CesiumConfigValidator.MAX_POLL_INTERVAL_MS),
                    report::render);
        }

        @Test
        void unparseableOverlayValueIsAttributedToTheOverlayPath() {
            // The overlay supplied the effective value, so the finding must name the overlay map,
            // not kafka.properties — the operator must be sent to the right place.
            KafkaConfig kafka = TestConfigs.kafka(
                    Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "300000"),
                    new KafkaConfig.ClientOverrides(
                            Map.of(CesiumConfigValidator.MAX_POLL_INTERVAL_MS, "five minutes")));
            ValidationReport report = validator.validate(TestConfigs.withKafka(kafka), TestConfigs.ROOMY_CONTEXT);
            assertTrue(
                    errorPaths(report)
                            .contains(
                                    "kafka.tracker-consumer.properties." + CesiumConfigValidator.MAX_POLL_INTERVAL_MS),
                    report::render);
        }
    }

    // ------------------------------------------------------------------ ranges

    @Test
    void sidecarBytesMustBePositive() {
        DispatchConfig dispatch =
                new DispatchConfig(null, null, null, null, null, new DispatchConfig.Cursor(0), null, null, null, null);
        ValidationReport report = validate(TestConfigs.withDispatch(dispatch));
        assertTrue(errorPaths(report).contains("dispatch.cursor.sidecar-max-bytes"), report::render);
    }

    @Test
    void penaltyBackoffMaxMustNotBeBelowBackoff() {
        DispatchConfig.Fetch fetch = new DispatchConfig.Fetch(
                null, null, new DispatchConfig.Fetch.Penalty(Duration.ofSeconds(5), Duration.ofSeconds(1)));
        DispatchConfig dispatch = new DispatchConfig(null, null, null, null, null, null, fetch, null, null, null);
        ValidationReport report = validate(TestConfigs.withDispatch(dispatch));
        assertTrue(errorPaths(report).contains("dispatch.fetch.penalty.backoff-max"), report::render);
    }

    @Test
    void negativeDelayMaxIsRejected() {
        CesiumConfig config = new CesiumConfig(
                "app",
                new InstanceId("slot-0"),
                null,
                null,
                TestConfigs.route(),
                new DelayConfig(Duration.ofDays(-1), OverMaxPolicy.FAIL, MalformedHeaderPolicy.FAIL),
                null,
                null,
                null,
                null,
                null,
                null);
        assertTrue(errorPaths(validate(config)).contains("delay.max"));
    }

    @Test
    void observabilityPortRangeIsEnforced() {
        CesiumConfig config = new CesiumConfig(
                "app",
                new InstanceId("slot-0"),
                null,
                null,
                TestConfigs.route(),
                null,
                null,
                null,
                null,
                null,
                new ObservabilityConfig(70000),
                null);
        assertTrue(errorPaths(validate(config)).contains("observability.port"));
    }

    @ParameterizedTest
    @EnumSource(value = CheckMode.class, names = "SKIP")
    void heapBudgetAndOutageChecksRejectSkip(CheckMode skip) {
        StartupChecks checks = new StartupChecks(null, null, null, skip, skip);
        CesiumConfig config = TestConfigs.withDispatch(DispatchConfig.defaults(), checks);
        List<String> paths = errorPaths(validate(config));
        assertTrue(paths.contains("startup-checks.outage-check"));
        assertTrue(paths.contains("startup-checks.heap-budget"));
    }

    @Test
    void reportRenderingListsEverySeverity() {
        ValidationReport report = validate(TestConfigs.valid());
        assertNotNull(report.render());
        assertEquals(Severity.INFO, report.infos().getFirst().severity());
    }
}
