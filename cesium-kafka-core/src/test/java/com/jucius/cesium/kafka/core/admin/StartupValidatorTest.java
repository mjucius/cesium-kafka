package com.jucius.cesium.kafka.core.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.CheckMode;
import com.jucius.cesium.kafka.core.config.DelayConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.RelayConfig;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.StartupChecks;
import com.jucius.cesium.kafka.core.config.TopicRef;
import com.jucius.cesium.kafka.core.config.TrackerConfig;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import com.jucius.cesium.kafka.core.headers.RelayPartitioning;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * {@link StartupValidator} against the fake admin boundary: every §2.1/§3.1/§7.6/§8 check gets a
 * pass case plus each failure mode, including the size-based-retention acknowledgment gate and
 * the CREATE-mode bootstrap config assembly.
 */
class StartupValidatorTest {

    private static final String SOURCE = "orders";
    private static final String DESTINATION = "orders-out";
    private static final String DLQ = "orders-dlq";
    private static final String TRACKER = "cesium.orders-delay.tracker";
    private static final int PARTITIONS = 3;

    /** 2 x delay.max for the default P1D. */
    private static final String FLOOR_DELETE_RETENTION = "172800000";

    private FakeClusterAdmin admin;
    private StartupValidator validator;

    @BeforeEach
    void setUp() {
        admin = new FakeClusterAdmin();
        validator = new StartupValidator(admin);
    }

    // ------------------------------------------------------------------ fixture helpers

    private static Map<String, String> goodSourceConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "delete");
        configs.put("retention.ms", "604800000");
        configs.put("retention.bytes", "-1");
        configs.put("remote.storage.enable", "false");
        return configs;
    }

    private static Map<String, String> goodTrackerConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        configs.put("delete.retention.ms", FLOOR_DELETE_RETENTION);
        configs.put("min.compaction.lag.ms", "3600000");
        configs.put("segment.ms", "3600000");
        configs.put("message.timestamp.type", "LogAppendTime");
        return configs;
    }

    /** Source, destination, DLQ, a spec-conformant tracker, and a 7-day offsets retention. */
    private void givenHealthyCluster() {
        admin.addTopic(SOURCE, PARTITIONS, goodSourceConfigs());
        admin.addTopic(DESTINATION, PARTITIONS, Map.of());
        admin.addTopic(DLQ, 1, Map.of());
        admin.addTopic(TRACKER, PARTITIONS, goodTrackerConfigs());
        admin.brokerConfigs.put("offsets.retention.minutes", "10080");
        // A correctly-deployed cluster has the R12 tracker write ACL in force, restricted to the
        // cesium principal, so the tracker-acl check produces no finding in any mode; the TrackerAcl
        // tests remove/adjust it to exercise the missing/foreign/unset cases.
        admin.putTrackerWriteAcl(TRACKER, "User:cesium");
    }

    private static final class ConfigBuilder {
        TrackerConfig.Bootstrap bootstrap = TrackerConfig.Bootstrap.FAIL;
        // The R12 principal a correctly-deployed cluster grants exclusive tracker WRITE to; tests that
        // exercise a missing/foreign/unset principal override this and adjust the live ACL to match.
        String aclPrincipal = "User:cesium";
        DelayConfig delay;
        RelayConfig relay;
        DispatchConfig dispatch;
        StartupChecks startupChecks;
        KafkaConfig kafka;

        CesiumConfig build() {
            TrackerConfig tracker = new TrackerConfig(
                    "", bootstrap, aclPrincipal == null ? Optional.empty() : Optional.of(aclPrincipal));
            RouteConfig route = new RouteConfig(
                    new TopicRef(SOURCE), new TopicRef(DESTINATION), tracker, Optional.of(new TopicRef(DLQ)), relay);
            return new CesiumConfig(
                    "orders-delay",
                    new InstanceId("slot-0"),
                    null,
                    kafka,
                    route,
                    delay,
                    null,
                    null,
                    null,
                    dispatch,
                    null,
                    startupChecks);
        }
    }

    private static CesiumConfig defaultConfig() {
        return new ConfigBuilder().build();
    }

    // ------------------------------------------------------------------ assertion helpers

    private static void assertNoErrors(StartupValidationResult result) {
        assertFalse(
                result.report().hasErrors(),
                () -> "expected no errors:\n" + result.report().render());
    }

    private static String findingsAt(List<Finding> findings, String path) {
        StringBuilder sb = new StringBuilder();
        for (Finding finding : findings) {
            if (finding.path().equals(path)) {
                sb.append(finding.message()).append('\n');
            }
        }
        return sb.toString();
    }

    private static void assertErrorContains(StartupValidationResult result, String path, String substring) {
        String messages = findingsAt(result.report().errors(), path);
        assertTrue(
                messages.contains(substring),
                () -> "expected an ERROR at '" + path + "' containing '" + substring + "':\n"
                        + result.report().render());
    }

    private static void assertWarningContains(StartupValidationResult result, String path, String substring) {
        String messages = findingsAt(result.report().warnings(), path);
        assertTrue(
                messages.contains(substring),
                () -> "expected a WARNING at '" + path + "' containing '" + substring + "':\n"
                        + result.report().render());
    }

    private static void assertNoFindingAt(StartupValidationResult result, String path) {
        assertEquals(
                "",
                findingsAt(result.report().errors(), path)
                        + findingsAt(result.report().warnings(), path),
                () -> "expected no error/warning at '" + path + "':\n"
                        + result.report().render());
    }

    // ------------------------------------------------------------------ the clean pass

    @Test
    void healthyClusterPassesAndCapturesIdentity() {
        // The healthy baseline already has the R12 tracker write ACL in force and acl-principal set to
        // the cesium principal, so the default FAIL ACL verification adds no finding.
        givenHealthyCluster();

        StartupValidationResult result = validator.validate(defaultConfig());

        assertNoErrors(result);
        assertTrue(result.report().warnings().isEmpty(), () -> result.report().render());
        assertEquals(OptionalInt.of(PARTITIONS), result.sourcePartitions());
        IdentityBlob identity = result.identity().orElseThrow();
        assertEquals(admin.clusterId, identity.clusterId());
        assertEquals(admin.topics.get(SOURCE).topicId(), identity.sourceTopicId());
        // The blob round-trips through the offset-metadata channel (§3.1).
        assertEquals(identity, IdentityBlob.decode(identity.encode()));
    }

    // ------------------------------------------------------------------ source topic

    @Nested
    class SourceTopic {

        @Test
        void missingSourceFails() {
            givenHealthyCluster();
            admin.topics.remove(SOURCE);

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "does not exist");
            assertTrue(result.identity().isEmpty());
            assertEquals(OptionalInt.empty(), result.sourcePartitions());
        }

        @Test
        void compactedSourceFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("cleanup.policy", "compact");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "must not be compacted");
        }

        @Test
        void compactDeleteSourceFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("cleanup.policy", "compact,delete");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "must not be compacted");
        }

        @Test
        void clusterUnavailableAggregatesErrorsWithoutIdentity() {
            admin.describeFailure = new ClusterAdminException("brokers unreachable");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertTrue(result.report().hasErrors());
            assertErrorContains(result, "kafka", "cluster metadata unavailable");
            assertTrue(result.identity().isEmpty());
        }
    }

    // ------------------------------------------------------------------ source retention (§7.6)

    @Nested
    class SourceRetention {

        @Test
        void sufficientTimeRetentionPasses() {
            givenHealthyCluster();
            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void infiniteRetentionWithoutSizeEvictionPasses() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.ms", "-1");

            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void retentionBelowDelayMaxPlusMarginFailsInFailMode() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.ms", "1000");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "delay.max + margin");
        }

        @Test
        void retentionBelowDelayMaxPlusMarginWarnsInWarnMode() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.ms", "1000");
            ConfigBuilder builder = new ConfigBuilder();
            builder.startupChecks = new StartupChecks(CheckMode.WARN, null, null, null, null, null);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertWarningContains(result, "route.source.topic", "delay.max + margin");
        }

        @Test
        void skipModePerformsNoRetentionChecks() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.ms", "1000");
            admin.topicConfigs.get(SOURCE).put("retention.bytes", "1073741824");
            ConfigBuilder builder = new ConfigBuilder();
            builder.startupChecks = new StartupChecks(CheckMode.SKIP, null, null, null, null, null);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertNoFindingAt(result, "startup-checks.size-based-retention");
            // The skip is named in the report, never silent (risk 19).
            assertTrue(
                    findingsAt(result.report().infos(), "startup-checks.retention")
                            .contains("SKIP"),
                    () -> result.report().render());
        }

        @Test
        void sizeBasedEvictionWithoutAcknowledgmentFailsEvenWithInfiniteTimeRetention() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.ms", "-1");
            admin.topicConfigs.get(SOURCE).put("retention.bytes", "1073741824");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "startup-checks.size-based-retention", "ACKNOWLEDGED");
            assertErrorContains(result, "startup-checks.size-based-retention", "retention.bytes=1073741824");
        }

        @Test
        void tieredStorageWithoutAcknowledgmentFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("remote.storage.enable", "true");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "startup-checks.size-based-retention", "remote.storage.enable=true");
        }

        @Test
        void acknowledgedSizeBasedEvictionDowngradesToNamedWarning() {
            givenHealthyCluster();
            admin.topicConfigs.get(SOURCE).put("retention.bytes", "1073741824");
            ConfigBuilder builder = new ConfigBuilder();
            builder.startupChecks =
                    new StartupChecks(null, StartupChecks.SizeBasedRetention.ACKNOWLEDGED, null, null, null, null);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertWarningContains(result, "startup-checks.size-based-retention", "ACKNOWLEDGED");
        }
    }

    // ------------------------------------------------------------------ tracker bootstrap + validation

    @Nested
    class TrackerTopic {

        @Test
        void createModeBootstrapsAbsentTrackerWithSpecConfigs() {
            givenHealthyCluster();
            admin.topics.remove(TRACKER);
            admin.topicConfigs.remove(TRACKER);
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.CREATE;

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertEquals(1, admin.created.size());
            FakeClusterAdmin.CreatedTopic created = admin.created.get(0);
            assertEquals(TRACKER, created.name());
            assertEquals(PARTITIONS, created.partitions());
            // The exact §2.1 config map the bootstrap applies (defaults: delay.max=P1D, txn=PT30S).
            assertEquals(
                    Map.of(
                            "cleanup.policy", "compact",
                            "delete.retention.ms", FLOOR_DELETE_RETENTION,
                            "min.compaction.lag.ms", "3600000",
                            "segment.ms", "3600000",
                            "message.timestamp.type", "LogAppendTime"),
                    created.configs());
        }

        @Test
        void createConfigAssemblyFollowsTheFloors() {
            // delete.retention.ms = 2 x delay.max; min.compaction.lag.ms = max(2 x txn timeout, 1h).
            assertEquals(
                    Map.of(
                            "cleanup.policy", "compact",
                            "delete.retention.ms", "14400000",
                            "min.compaction.lag.ms", "5400000",
                            "segment.ms", "3600000",
                            "message.timestamp.type", "LogAppendTime"),
                    StartupValidator.trackerCreateConfigs(Duration.ofHours(2), Duration.ofMinutes(45)));
            // Short transaction timeouts bottom out at the 1h lag floor.
            assertEquals(
                    "3600000",
                    StartupValidator.trackerCreateConfigs(Duration.ofDays(1), Duration.ofSeconds(30))
                            .get("min.compaction.lag.ms"));
        }

        @Test
        void createModeAppliesAclWhenPrincipalConfigured() {
            givenHealthyCluster();
            admin.topics.remove(TRACKER);
            admin.topicConfigs.remove(TRACKER);
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.CREATE;
            builder.aclPrincipal = "User:cesium";

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertEquals(List.of(new FakeClusterAdmin.AclGrant(TRACKER, "User:cesium")), admin.aclGrants);
        }

        @Test
        void aclFailureOnAuthorizerlessClusterIsAWarning() {
            givenHealthyCluster();
            admin.topics.remove(TRACKER);
            admin.topicConfigs.remove(TRACKER);
            admin.aclFailure =
                    new ClusterAdminException("createAcls failed", new SecurityDisabledException("no authorizer"));
            // A cluster with no authorizer also fails the every-startup DESCRIBE (L3), so the ACL
            // verification downgrades to a WARN just like the apply path.
            admin.describeAclsFailure =
                    new ClusterAdminException("describeAcls failed", new SecurityDisabledException("no authorizer"));
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.CREATE;
            builder.aclPrincipal = "User:cesium";
            // Default tracker-acl is WARN, so both the ACL *apply* failure and the every-startup
            // verification surface as warnings here — startup is not blocked on an authorizerless
            // cluster. (FAIL-mode enforcement is covered in the TrackerAcl nested class.)

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertWarningContains(result, "route.tracker.acl-principal", "no authorizer");
            assertWarningContains(result, "route.tracker.acl-principal", "R12");
        }

        @Test
        void aclFailureForOtherReasonsIsAnError() {
            givenHealthyCluster();
            admin.topics.remove(TRACKER);
            admin.topicConfigs.remove(TRACKER);
            admin.aclFailure = new ClusterAdminException("createAcls failed", new RuntimeException("denied"));
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.CREATE;
            builder.aclPrincipal = "User:cesium";

            StartupValidationResult result = validator.validate(builder.build());

            assertErrorContains(result, "route.tracker.acl-principal", "failed to apply the tracker write ACL");
        }

        @Test
        void createModeValidatesAnExistingTrackerWithoutRecreatingIt() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("delete.retention.ms", "1000");
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.CREATE;
            builder.aclPrincipal = "User:cesium";

            StartupValidationResult result = validator.validate(builder.build());

            assertTrue(admin.created.isEmpty());
            assertTrue(admin.aclGrants.isEmpty());
            assertErrorContains(result, "route.tracker", "D14");
        }

        @Test
        void failModeRequiresAnExistingTracker() {
            givenHealthyCluster();
            admin.topics.remove(TRACKER);
            admin.topicConfigs.remove(TRACKER);

            StartupValidationResult result = validator.validate(defaultConfig());

            assertTrue(admin.created.isEmpty());
            assertErrorContains(result, "route.tracker", "route.tracker.bootstrap is FAIL");
        }

        @Test
        void partitionCountDriftFailsWithGrowTrackerRunbook() {
            givenHealthyCluster();
            admin.addTopic(TRACKER, PARTITIONS + 2, goodTrackerConfigs());

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.tracker", "R-7");
            assertErrorContains(result, "route.tracker", "grow the tracker topic first");
        }

        @Test
        void compactDeleteCleanupPolicyFailsCitingD4() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("cleanup.policy", "compact,delete");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.tracker", "D4");
            assertErrorContains(result, "route.tracker", "silent loss");
        }

        @Test
        void deleteOnlyCleanupPolicyFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("cleanup.policy", "delete");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.tracker", "MUST be exactly 'compact'");
        }

        @Test
        void deleteRetentionBelowTheD14FloorFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("delete.retention.ms", "172799999");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.tracker", "D14");
            assertErrorContains(result, "route.tracker", FLOOR_DELETE_RETENTION);
        }

        @Test
        void minCompactionLagBelowTheFloorFails() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("min.compaction.lag.ms", "3599999");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.tracker", "min.compaction.lag.ms");
            assertErrorContains(result, "route.tracker", "LSO");
        }

        @Test
        void oversizedSegmentMsWarns() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("segment.ms", "604800000");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertWarningContains(result, "route.tracker", "segment.ms");
        }

        @Test
        void createTimeTimestampTypeWarns() {
            givenHealthyCluster();
            admin.topicConfigs.get(TRACKER).put("message.timestamp.type", "CreateTime");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertWarningContains(result, "route.tracker", "LogAppendTime");
        }
    }

    // ------------------------------------------------------------------ tracker write ACL (L3)

    @Nested
    class TrackerAcl {

        private static final String ACL_PATH = "route.tracker.acl-principal";

        private ConfigBuilder withPrincipal(String principal) {
            ConfigBuilder builder = new ConfigBuilder();
            builder.bootstrap = TrackerConfig.Bootstrap.FAIL;
            builder.aclPrincipal = principal;
            return builder;
        }

        @Test
        void matchingExclusiveGrantProducesNoFinding() {
            givenHealthyCluster();
            admin.putTrackerWriteAcl(TRACKER, "User:cesium");

            StartupValidationResult result =
                    validator.validate(withPrincipal("User:cesium").build());

            assertNoErrors(result);
            assertNoFindingAt(result, ACL_PATH);
        }

        private ConfigBuilder failMode(String principal) {
            ConfigBuilder builder = withPrincipal(principal);
            builder.startupChecks = new StartupChecks(null, null, null, null, null, CheckMode.FAIL);
            return builder;
        }

        // --- default (WARN): the R12 gap is surfaced on every boot, never blocking ---------------

        @Test
        void missingConfiguredGrantWarnsByDefault() {
            // The every-startup DESCRIBE (L3) surfaces a missing R12 grant. No grant is ever applied
            // on the FAIL-bootstrap validate path.
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);

            StartupValidationResult result =
                    validator.validate(withPrincipal("User:cesium").build());

            assertTrue(admin.aclGrants.isEmpty(), "verification never applies an ACL");
            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "no matching ALLOW WRITE ACL");
        }

        @Test
        void foreignWriterAlongsideTheConfiguredPrincipalWarnsByDefault() {
            givenHealthyCluster();
            admin.putTrackerWriteAcl(TRACKER, "User:intruder");

            StartupValidationResult result =
                    validator.validate(withPrincipal("User:cesium").build());

            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "foreign principal");
            assertWarningContains(result, ACL_PATH, "User:intruder");
        }

        @Test
        void unsetPrincipalWarnsThatTheR12ControlIsUnenforced() {
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);

            StartupValidationResult result =
                    validator.validate(withPrincipal(null).build());

            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "unset");
            assertWarningContains(result, ACL_PATH, "any principal can");
        }

        @Test
        void unsetPrincipalNamesExistingForeignWriters() {
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);
            admin.putTrackerWriteAcl(TRACKER, "User:intruder");

            StartupValidationResult result =
                    validator.validate(withPrincipal(null).build());

            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "unset");
            assertWarningContains(result, ACL_PATH, "User:intruder");
        }

        @Test
        void noAuthorizerDowngradesVerificationToWarning() {
            givenHealthyCluster();
            admin.describeAclsFailure =
                    new ClusterAdminException("describeAcls failed", new SecurityDisabledException("no authorizer"));

            StartupValidationResult result =
                    validator.validate(withPrincipal("User:cesium").build());

            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "no authorizer");
            assertWarningContains(result, ACL_PATH, "cannot be verified");
        }

        @Test
        void describeFailureForOtherReasonsWarnsToVerifyManually() {
            givenHealthyCluster();
            admin.describeAclsFailure = new ClusterAdminException("describeAcls timed out");

            StartupValidationResult result =
                    validator.validate(withPrincipal("User:cesium").build());

            assertNoErrors(result);
            assertWarningContains(result, ACL_PATH, "verify manually");
        }

        // --- FAIL (opt-in): startup refuses an unverified R12 restriction (VULN-015) -------------

        @Test
        void failModeErrorsWhenTheConfiguredGrantIsMissing() {
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);

            StartupValidationResult result =
                    validator.validate(failMode("User:cesium").build());

            assertErrorContains(result, ACL_PATH, "no matching ALLOW WRITE ACL");
        }

        @Test
        void failModeErrorsOnAForeignWriter() {
            givenHealthyCluster();
            admin.putTrackerWriteAcl(TRACKER, "User:intruder");

            StartupValidationResult result =
                    validator.validate(failMode("User:cesium").build());

            assertErrorContains(result, ACL_PATH, "foreign principal");
            assertErrorContains(result, ACL_PATH, "User:intruder");
        }

        @Test
        void failModeErrorsWhenPrincipalIsUnset() {
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);

            StartupValidationResult result = validator.validate(failMode(null).build());

            assertErrorContains(result, ACL_PATH, "unset");
        }

        @Test
        void failModeErrorsWhenNoAuthorizerCanVerify() {
            givenHealthyCluster();
            admin.describeAclsFailure =
                    new ClusterAdminException("describeAcls failed", new SecurityDisabledException("no authorizer"));

            StartupValidationResult result =
                    validator.validate(failMode("User:cesium").build());

            assertErrorContains(result, ACL_PATH, "no authorizer");
        }

        // --- SKIP: omitted entirely, but named in the report -------------------------------------

        @Test
        void skipModeOmitsTheAclCheckButNamesTheSkip() {
            givenHealthyCluster();
            admin.trackerWriteAcls.remove(TRACKER);
            ConfigBuilder builder = withPrincipal(null);
            builder.startupChecks = new StartupChecks(null, null, null, null, null, CheckMode.SKIP);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertNoFindingAt(result, ACL_PATH);
            // The skip is named in the report, never silent.
            assertTrue(
                    findingsAt(result.report().infos(), "startup-checks.tracker-acl")
                            .contains("SKIP"),
                    () -> result.report().render());
        }
    }

    // ------------------------------------------------------------------ recorded identity (R-10)

    @Nested
    class RecordedIdentity {

        private static final String GROUP_A = "cesium.orders-delay.ingest";

        private IdentityBlob liveIdentity() {
            return new IdentityBlob(admin.clusterId, admin.topics.get(SOURCE).topicId());
        }

        @Test
        void matchingRecordedIdentityPasses() {
            givenHealthyCluster();
            admin.putGroupOffset(
                    GROUP_A,
                    new TopicPartition(SOURCE, 0),
                    new OffsetAndMetadata(42L, liveIdentity().encode()));

            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void recreatedSourceTopicIdMismatchFailsWithRunbook() {
            givenHealthyCluster();
            // The blob recorded by the previous run names a different topic id: the source was
            // deleted and recreated under the same name between runs (R-10).
            IdentityBlob stale = new IdentityBlob(admin.clusterId, Uuid.randomUuid());
            admin.putGroupOffset(GROUP_A, new TopicPartition(SOURCE, 0), new OffsetAndMetadata(42L, stale.encode()));

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "recreated under the same name");
            assertErrorContains(result, "route.source.topic", "R-10");
        }

        @Test
        void clusterIdMismatchFails() {
            givenHealthyCluster();
            IdentityBlob foreignCluster = new IdentityBlob(
                    Uuid.randomUuid().toString(), admin.topics.get(SOURCE).topicId());
            admin.putGroupOffset(
                    GROUP_A, new TopicPartition(SOURCE, 0), new OffsetAndMetadata(42L, foreignCluster.encode()));

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "cluster id changed");
        }

        @Test
        void undecodableMetadataIsAnIntegrityError() {
            givenHealthyCluster();
            admin.putGroupOffset(GROUP_A, new TopicPartition(SOURCE, 0), new OffsetAndMetadata(42L, "!!garbage!!"));

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "not a readable §3.1 identity blob");
        }

        @Test
        void blankMetadataAndForeignTopicOffsetsAreTolerated() {
            givenHealthyCluster();
            // Operator-seeded first-run offsets carry no blob; offsets a group holds for other
            // topics are out of scope for the source identity check.
            admin.putGroupOffset(GROUP_A, new TopicPartition(SOURCE, 0), new OffsetAndMetadata(0L, ""));
            admin.putGroupOffset(
                    GROUP_A, new TopicPartition("some-other-topic", 0), new OffsetAndMetadata(7L, "not-a-blob"));

            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void absentGroupOffsetsPassAsFirstRun() {
            givenHealthyCluster();
            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void unreadableGroupOffsetsAreAnError() {
            givenHealthyCluster();
            admin.groupOffsetsFailure = new ClusterAdminException("offsets fetch failed");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.source.topic", "could not read consumer group");
            assertErrorContains(result, "route.source.topic", "R-10");
        }
    }

    // ------------------------------------------------------------------ dlq + destination

    @Nested
    class DlqAndDestination {

        @Test
        void missingDlqFailsWhenAnyPolicyRoutesToIt() {
            givenHealthyCluster();
            admin.topics.remove(DLQ);

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.dlq.topic", "does not exist");
        }

        @Test
        void dlqNotRequiredWhenNoPolicyRoutesToIt() {
            givenHealthyCluster();
            admin.topics.remove(DLQ);
            ConfigBuilder builder = new ConfigBuilder();
            builder.delay = new DelayConfig(null, OverMaxPolicy.CLAMP, MalformedHeaderPolicy.RELAY_IMMEDIATE);
            builder.dispatch = new DispatchConfig(
                    null, null, null, null, null, null, null, UnfetchablePayloadPolicy.DROP, null, null);
            // route.relay.on-unrelayable defaults to DLQ, which IS a DLQ-routing policy (M2), so the
            // "no policy routes to DLQ" premise also requires it to be a non-DLQ disposition.
            builder.relay = new RelayConfig(null, null, UnrelayablePolicy.DROP);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
        }

        @Test
        void onUnrelayableDlqAloneRequiresTheDlqTopicToExist() {
            // M2 regression guard: with every delay/dispatch policy set to a non-DLQ value but
            // on-unrelayable left at its DLQ default, the broker-side DLQ existence check MUST still
            // run — otherwise a permanent-rejection DLQ write to a nonexistent topic wedges the
            // partition (UnknownTopicOrPartition is not a permanent record rejection).
            givenHealthyCluster();
            admin.topics.remove(DLQ);
            ConfigBuilder builder = new ConfigBuilder();
            builder.delay = new DelayConfig(null, OverMaxPolicy.CLAMP, MalformedHeaderPolicy.RELAY_IMMEDIATE);
            builder.dispatch = new DispatchConfig(
                    null, null, null, null, null, null, null, UnfetchablePayloadPolicy.DROP, null, null);
            builder.relay = new RelayConfig(null, null, UnrelayablePolicy.DLQ);

            StartupValidationResult result = validator.validate(builder.build());

            assertErrorContains(result, "route.dlq.topic", "does not exist");
        }

        @Test
        void onUnrelayableDlqAloneRunsTheL4UnboundedRetentionCheck() {
            // The same M2 gap silently skipped the L4 unbounded-DLQ WARN for this path; with the gate
            // fixed it fires when on-unrelayable=DLQ is the only DLQ-routing policy.
            givenHealthyCluster();
            admin.addTopic(DLQ, 1, Map.of("retention.ms", "-1", "retention.bytes", "-1"));
            ConfigBuilder builder = new ConfigBuilder();
            builder.delay = new DelayConfig(null, OverMaxPolicy.CLAMP, MalformedHeaderPolicy.RELAY_IMMEDIATE);
            builder.dispatch = new DispatchConfig(
                    null, null, null, null, null, null, null, UnfetchablePayloadPolicy.DROP, null, null);
            builder.relay = new RelayConfig(null, null, UnrelayablePolicy.DLQ);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertWarningContains(result, "route.dlq.topic", "unbounded retention");
        }

        @Test
        void unboundedDlqRetentionWarns() {
            givenHealthyCluster();
            admin.addTopic(DLQ, 1, Map.of("retention.ms", "-1", "retention.bytes", "-1"));

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertWarningContains(result, "route.dlq.topic", "unbounded retention");
        }

        @Test
        void boundedDlqRetentionDoesNotWarn() {
            givenHealthyCluster();
            // retention.bytes=-1 but a finite retention.ms is a bound: no warning.
            admin.addTopic(DLQ, 1, Map.of("retention.ms", "604800000", "retention.bytes", "-1"));

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertNoFindingAt(result, "route.dlq.topic");
        }

        @Test
        void missingDestinationFails() {
            givenHealthyCluster();
            admin.topics.remove(DESTINATION);

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "route.destination.topic", "does not exist");
        }

        @Test
        void sourcePartitionRelayRequiresEqualPartitionCounts() {
            givenHealthyCluster();
            admin.addTopic(DESTINATION, PARTITIONS + 1, Map.of());
            ConfigBuilder builder = new ConfigBuilder();
            builder.relay = new RelayConfig(null, RelayPartitioning.SOURCE_PARTITION, null);

            StartupValidationResult result = validator.validate(builder.build());

            assertErrorContains(result, "route.relay.partitioning", "§2.4");
        }

        @Test
        void sourcePartitionRelayPassesWithEqualPartitionCounts() {
            givenHealthyCluster();
            ConfigBuilder builder = new ConfigBuilder();
            builder.relay = new RelayConfig(null, RelayPartitioning.SOURCE_PARTITION, null);

            assertNoErrors(validator.validate(builder.build()));
        }

        @Test
        void byKeyRelayIgnoresPartitionCountDrift() {
            givenHealthyCluster();
            admin.addTopic(DESTINATION, PARTITIONS + 5, Map.of());

            assertNoErrors(validator.validate(defaultConfig()));
        }
    }

    // ------------------------------------------------------------------ broker offsets retention (D18)

    @Nested
    class OffsetsRetention {

        @Test
        void sufficientOffsetsRetentionPasses() {
            givenHealthyCluster();
            assertNoErrors(validator.validate(defaultConfig()));
        }

        @Test
        void offsetsRetentionBelowMaxToleratedOutageFails() {
            givenHealthyCluster();
            admin.brokerConfigs.put("offsets.retention.minutes", "60");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertErrorContains(result, "startup-checks.max-tolerated-outage", "KIP-211");
        }

        @Test
        void offsetsRetentionBreachWarnsInWarnMode() {
            givenHealthyCluster();
            admin.brokerConfigs.put("offsets.retention.minutes", "60");
            ConfigBuilder builder = new ConfigBuilder();
            builder.startupChecks = new StartupChecks(null, null, null, CheckMode.WARN, null, null);

            StartupValidationResult result = validator.validate(builder.build());

            assertNoErrors(result);
            assertWarningContains(result, "startup-checks.max-tolerated-outage", "KIP-211");
        }

        @Test
        void lowerOutageTolerancePassesWithShortBrokerRetention() {
            givenHealthyCluster();
            admin.brokerConfigs.put("offsets.retention.minutes", "60");
            ConfigBuilder builder = new ConfigBuilder();
            builder.startupChecks = new StartupChecks(null, null, Duration.ofMinutes(30), null, null, null);

            assertNoErrors(validator.validate(builder.build()));
        }

        @Test
        void unreadableBrokerConfigWarnsToVerifyManually() {
            givenHealthyCluster();
            admin.brokerConfigs.remove("offsets.retention.minutes");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertWarningContains(result, "startup-checks.max-tolerated-outage", "verify manually");
        }

        @Test
        void unparseableBrokerConfigWarns() {
            givenHealthyCluster();
            admin.brokerConfigs.put("offsets.retention.minutes", "not-a-number");

            StartupValidationResult result = validator.validate(defaultConfig());

            assertNoErrors(result);
            assertWarningContains(result, "startup-checks.max-tolerated-outage", "not a number");
        }
    }

    // ------------------------------------------------------------------ report aggregation

    @Test
    void multipleViolationsAggregateIntoOneReport() {
        givenHealthyCluster();
        admin.topicConfigs.get(SOURCE).put("retention.ms", "1000");
        admin.topicConfigs.get(TRACKER).put("cleanup.policy", "compact,delete");
        admin.topicConfigs.get(TRACKER).put("delete.retention.ms", "1000");
        admin.topics.remove(DESTINATION);
        admin.brokerConfigs.put("offsets.retention.minutes", "60");

        ValidationReport report = validator.validate(defaultConfig()).report();

        assertTrue(report.errors().size() >= 5, report::render);
    }
}
