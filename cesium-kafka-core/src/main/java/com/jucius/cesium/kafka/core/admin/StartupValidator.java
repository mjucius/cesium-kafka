package com.jucius.cesium.kafka.core.admin;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.CheckMode;
import com.jucius.cesium.kafka.core.config.StartupChecks;
import com.jucius.cesium.kafka.core.config.TrackerConfig;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import com.jucius.cesium.kafka.core.headers.RelayPartitioning;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jspecify.annotations.Nullable;

/**
 * Admin startup validation and tracker bootstrap (design §2.1, §3.1, §3.6, §7.6, §8; rows
 * R-7..R-11). Aggregates <em>every</em> environment finding into one
 * {@link ValidationReport} — the caller fail-fasts on errors with the report intact, so an
 * operator fixes a broken environment in one round trip.
 *
 * <p>Checks performed:
 *
 * <ul>
 *   <li><strong>Source</strong> (§2.1): exists; not compacted (pointer-only storage re-fetches
 *       payloads by offset — compaction makes offsets unfetchable); partition count {@code P},
 *       topic ID, and cluster ID captured for the §3.1 identity blob.
 *   <li><strong>Source retention</strong> (§7.6, R-5/R-11/R13): {@code retention.ms} vs
 *       {@code delay.max + margin} at {@code startup-checks.retention} strictness;
 *       {@code retention.ms=-1} alone is never a pass when {@code retention.bytes != -1} or
 *       {@code remote.storage.enable=true} — those require the explicit, named
 *       {@code startup-checks.size-based-retention: ACKNOWLEDGED} acceptance.
 *   <li><strong>Tracker</strong> (§2.1, D4, D14, R-7/R-8): {@code CREATE} bootstrap creates the
 *       topic when absent with the §2.1 configs ({@link #trackerCreateConfigs}) and applies the
 *       R12 write ACL when {@code acl-principal} is configured; {@code FAIL} requires it to
 *       pre-exist. Either way the live topic is validated: partition parity with {@code P} (R-7
 *       runbook: grow the tracker first), {@code cleanup.policy} exactly {@code compact} (D4),
 *       and {@code delete.retention.ms} at or above the startup-validatable D14 floor
 *       {@code 2 × delay.max}.
 *   <li><strong>DLQ</strong> (§2.3): exists whenever any policy routes to it.
 *   <li><strong>Destination</strong> (§2.4): exists; partition parity with {@code P} when
 *       {@code route.relay.partitioning = SOURCE_PARTITION}.
 *   <li><strong>Broker offsets retention</strong> (D18, R6, KIP-211):
 *       {@code offsets.retention.minutes} vs {@code startup-checks.max-tolerated-outage} at
 *       {@code startup-checks.outage-check} strictness.
 *   <li><strong>Recorded identity</strong> (§3.1, R-10/R17): every non-blank metadata string in
 *       group A's committed offsets is decoded as an {@link IdentityBlob} and compared against
 *       the freshly captured live identity — a mismatch means the source topic was recreated
 *       under the same name (or the engine points at a different cluster) and is a fail-fast
 *       with the R-10 runbook; undecodable metadata is an integrity fail-fast (§3.6). Absent
 *       offsets and blank metadata (first run, operator-seeded offsets) pass.
 * </ul>
 */
public final class StartupValidator {

    /**
     * The safety margin added to {@code delay.max} for the §7.6 source-retention comparison: the
     * payload must remain fetchable through dispatch lag, fetch budgets, and penalty backoff after
     * its dispatch time arrives.
     */
    public static final Duration RETENTION_MARGIN = Duration.ofHours(1);

    /** The §2.1 {@code min.compaction.lag.ms} floor independent of the transaction timeout: 1h. */
    public static final Duration MIN_COMPACTION_LAG_FLOOR = Duration.ofHours(1);

    /** The §2.1 tracker {@code segment.ms} target (≈ 1h, so cleaning actually happens). */
    public static final Duration TRACKER_SEGMENT_MS = Duration.ofHours(1);

    /** The broker config bounding committed-offset lifetime (D18, KIP-211). */
    public static final String OFFSETS_RETENTION_MINUTES = "offsets.retention.minutes";

    /** The §2.1 tracker {@code message.timestamp.type}. */
    public static final String LOG_APPEND_TIME = "LogAppendTime";

    // Kafka broker defaults assumed when a (fake) config map omits a key; real describeConfigs
    // returns resolved entries including defaults.
    private static final long DEFAULT_RETENTION_MS = 604_800_000L;
    private static final long DEFAULT_RETENTION_BYTES = -1L;
    private static final long DEFAULT_DELETE_RETENTION_MS = 86_400_000L;
    private static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0L;
    private static final long DEFAULT_SEGMENT_MS = 604_800_000L;
    private static final String DEFAULT_CLEANUP_POLICY = TopicConfig.CLEANUP_POLICY_DELETE;
    private static final String DEFAULT_TIMESTAMP_TYPE = "CreateTime";

    private final ClusterAdmin admin;
    private final TopicVisibility visibility;

    /** Creates a validator over the given admin boundary. */
    public StartupValidator(ClusterAdmin admin) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.visibility = new TopicVisibility(admin);
    }

    /** Test seam: a virtual clock and a non-sleeping waiter make the metadata waits deterministic. */
    StartupValidator(ClusterAdmin admin, Clock clock, TopicVisibility.Waiter waiter) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.visibility = new TopicVisibility(admin, clock, waiter);
    }

    /**
     * A topic's effective configs, waiting out metadata-propagation lag.
     *
     * <p>Every call site runs <em>after</em> the topic was described successfully, so an
     * unknown-topic error here is lag rather than absence — see {@link TopicVisibility}.
     */
    private Map<String, String> topicConfigs(String topic) {
        return visibility.awaitTopicConfigs(topic);
    }

    /** Runs every startup check, aggregating all findings; the caller fail-fasts on errors. */
    public StartupValidationResult validate(CesiumConfig config) {
        List<Finding> findings = new ArrayList<>();

        String clusterId = fetchClusterId(findings);
        TopicFacts source = checkSource(config, findings);
        checkTracker(config, source, findings);
        checkDlq(config, findings);
        checkDestination(config, source, findings);
        checkOffsetsRetention(config, findings);

        IdentityBlob identity = null;
        if (clusterId != null && source != null) {
            identity = new IdentityBlob(clusterId, source.topicId());
            findings.add(Finding.info(
                    "route.source.topic",
                    "identity captured for the §3.1 offset-metadata blob: cluster id '" + clusterId
                            + "', source topic id " + source.topicId() + ", " + source.partitionCount()
                            + " partition(s)."));
            checkRecordedIdentity(config, identity, findings);
        }
        return new StartupValidationResult(
                new ValidationReport(findings),
                Optional.ofNullable(identity),
                source == null ? OptionalInt.empty() : OptionalInt.of(source.partitionCount()));
    }

    // ------------------------------------------------------------------ recorded identity (R-10)

    /**
     * §3.1's startup half: compare the identity blob recorded in group A's committed offset
     * metadata against the live identity just captured from the cluster. Kafka topic IDs survive
     * nothing — if the source topic was deleted and recreated under the same name between runs,
     * resuming from the stale committed offsets against the new incarnation silently relays
     * wrong payloads (the exact R-10/R17 hazard); a mismatch is therefore a fail-fast with the
     * source-was-recreated runbook. First runs (no committed offsets) and operator-seeded offsets
     * (blank metadata) pass; metadata that cannot be decoded is an integrity fail-fast (§3.6).
     */
    private void checkRecordedIdentity(CesiumConfig config, IdentityBlob live, List<Finding> findings) {
        String groupId = KafkaClientFactory.ingestGroupId(config.applicationId());
        String sourceTopic = config.route().source().topic();
        Map<TopicPartition, OffsetAndMetadata> committed;
        try {
            committed = admin.groupOffsets(groupId);
        } catch (ClusterAdminException e) {
            findings.add(Finding.error(
                    "route.source.topic",
                    "could not read consumer group '" + groupId + "' committed offsets to verify the §3.1 identity"
                            + " binding: " + e.getMessage()
                            + " — the R-10 source-recreation check requires them; fix connectivity and retry."));
            return;
        }
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            TopicPartition partition = entry.getKey();
            if (!partition.topic().equals(sourceTopic)) {
                continue;
            }
            String metadata = entry.getValue().metadata();
            if (metadata == null || metadata.isBlank()) {
                continue; // first run or operator-seeded offsets carry no blob
            }
            IdentityBlob recorded;
            try {
                recorded = IdentityBlob.decode(metadata);
            } catch (IllegalArgumentException e) {
                findings.add(Finding.error(
                        "route.source.topic",
                        "committed offset metadata for " + partition + " (group '" + groupId + "') is not a readable"
                                + " §3.1 identity blob: " + e.getMessage()
                                + " — refusing to start on unverifiable identity (§3.6 integrity); inspect and repair"
                                + " the group offsets."));
                continue;
            }
            if (!recorded.matches(live)) {
                findings.add(Finding.error(
                        "route.source.topic",
                        "committed offset identity mismatch for " + partition + " (group '" + groupId + "'): "
                                + recorded.describeMismatch(live)));
            }
        }
    }

    // ------------------------------------------------------------------ tracker bootstrap assembly

    /**
     * The exact topic configs the {@code CREATE} bootstrap applies (§2.1): compaction-only
     * cleanup (D4), the startup-validatable D14 tombstone-retention floor
     * {@code 2 × delay.max}, {@code min.compaction.lag.ms = max(2 × transaction timeout, 1h)}
     * (keeps the cleaner away from the active tail/LSO region), {@code segment.ms ≈ 1h} (so
     * cleaning actually happens), and {@code LogAppendTime}.
     */
    public static Map<String, String> trackerCreateConfigs(Duration delayMax, Duration transactionTimeout) {
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(deleteRetentionFloorMs(delayMax)));
        configs.put(
                TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, Long.toString(minCompactionLagFloorMs(transactionTimeout)));
        configs.put(TopicConfig.SEGMENT_MS_CONFIG, Long.toString(TRACKER_SEGMENT_MS.toMillis()));
        configs.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, LOG_APPEND_TIME);
        return Collections.unmodifiableMap(configs);
    }

    /** The startup-validatable term of the D14 tombstone-retention floor: {@code 2 × delay.max}. */
    public static long deleteRetentionFloorMs(Duration delayMax) {
        return saturatedMultiply(delayMax.toMillis(), 2);
    }

    /** The §2.1 compaction-lag floor: {@code max(2 × transaction timeout, 1h)}. */
    public static long minCompactionLagFloorMs(Duration transactionTimeout) {
        return Math.max(saturatedMultiply(transactionTimeout.toMillis(), 2), MIN_COMPACTION_LAG_FLOOR.toMillis());
    }

    // ------------------------------------------------------------------ cluster + source

    private @Nullable String fetchClusterId(List<Finding> findings) {
        try {
            return admin.clusterId();
        } catch (ClusterAdminException e) {
            findings.add(Finding.error(
                    "kafka",
                    "cluster metadata unavailable: " + e.getMessage()
                            + " — the §3.1 identity blob requires the cluster id; fix connectivity and retry."));
            return null;
        }
    }

    private @Nullable TopicFacts checkSource(CesiumConfig config, List<Finding> findings) {
        String topic = config.route().source().topic();
        try {
            Optional<TopicFacts> facts = admin.describeTopic(topic);
            if (facts.isEmpty()) {
                findings.add(Finding.error(
                        "route.source.topic",
                        "source topic '" + topic + "' does not exist (§2.1); create it or fix the configuration."));
                return null;
            }
            Map<String, String> topicConfig = topicConfigs(topic);
            if (cleanupPolicies(topicConfig).contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
                findings.add(Finding.error(
                        "route.source.topic",
                        "source topic '" + topic + "' has cleanup.policy='"
                                + topicConfig.getOrDefault(TopicConfig.CLEANUP_POLICY_CONFIG, DEFAULT_CLEANUP_POLICY)
                                + "' — the source must not be compacted (§2.1, §7.6): cesium stores pointers only and"
                                + " re-fetches payloads by offset; compaction makes offsets unfetchable. Use a"
                                + " non-compacted source topic."));
            }
            checkSourceRetention(config, topic, topicConfig, findings);
            return facts.get();
        } catch (ClusterAdminException e) {
            findings.add(Finding.error(
                    "route.source.topic", "could not validate the source topic: " + e.getMessage() + "."));
            return null;
        }
    }

    /** §7.6: time-, size-, and tier-aware source-retention validation (R-5, R-11, R13). */
    private static void checkSourceRetention(
            CesiumConfig config, String topic, Map<String, String> topicConfig, List<Finding> findings) {
        CheckMode mode = config.startupChecks().retention();
        if (mode == CheckMode.SKIP) {
            findings.add(Finding.info(
                    "startup-checks.retention",
                    "SKIP: source retention was not validated against delay.max (§7.6) — payload expiry before"
                            + " dispatch is the operator's responsibility."));
            return;
        }
        long retentionMs =
                parseLongConfig(topicConfig, TopicConfig.RETENTION_MS_CONFIG, DEFAULT_RETENTION_MS, findings);
        long retentionBytes =
                parseLongConfig(topicConfig, TopicConfig.RETENTION_BYTES_CONFIG, DEFAULT_RETENTION_BYTES, findings);
        boolean remoteStorage =
                Boolean.parseBoolean(topicConfig.getOrDefault(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"));

        long requiredMs = saturatedAdd(config.delay().max().toMillis(), RETENTION_MARGIN.toMillis());
        if (retentionMs != -1 && retentionMs < requiredMs) {
            findings.add(finding(
                    mode,
                    "route.source.topic",
                    "source topic '" + topic + "' retention.ms (" + retentionMs + " ms) is below delay.max + margin ("
                            + requiredMs + " ms = " + config.delay().max() + " + " + RETENTION_MARGIN
                            + ") — a payload can expire before its dispatch time (§7.6, R-5); raise source"
                            + " retention.ms or lower delay.max."));
        }

        boolean sizeOrTierEviction = retentionBytes != -1 || remoteStorage;
        if (sizeOrTierEviction) {
            String detected = (retentionBytes != -1 ? "retention.bytes=" + retentionBytes : "")
                    + (retentionBytes != -1 && remoteStorage ? ", " : "")
                    + (remoteStorage ? "remote.storage.enable=true" : "");
            if (config.startupChecks().sizeBasedRetention() == StartupChecks.SizeBasedRetention.ACKNOWLEDGED) {
                findings.add(Finding.warning(
                        "startup-checks.size-based-retention",
                        "ACKNOWLEDGED: source topic '" + topic + "' has " + detected
                                + " — time-based validation cannot bound payload lifetime (§7.6, R13). This named"
                                + " acceptance is logged and surfaced; the runtime earliest-available-age probe feeds"
                                + " cesium_retention_margin_seconds, and expired payloads resolve via the"
                                + " unfetchable-payload policy."));
            } else {
                findings.add(Finding.error(
                        "startup-checks.size-based-retention",
                        "source topic '" + topic + "' has " + detected
                                + " — size/tiered eviction means retention.ms cannot bound payload lifetime, so this"
                                + " cannot pass silently (§7.6, R13, R-11). Set startup-checks.size-based-retention:"
                                + " ACKNOWLEDGED to explicitly accept early payload eviction, or disable size/tier"
                                + " eviction on the source."));
            }
        }
    }

    // ------------------------------------------------------------------ tracker

    private void checkTracker(CesiumConfig config, @Nullable TopicFacts source, List<Finding> findings) {
        String topic = config.route().tracker().resolvedTopic(config.applicationId());
        try {
            Optional<TopicFacts> existing = admin.describeTopic(topic);
            if (existing.isEmpty()) {
                if (config.route().tracker().bootstrap() == TrackerConfig.Bootstrap.CREATE) {
                    existing = bootstrapTracker(config, source, topic, findings);
                    if (existing.isEmpty()) {
                        return;
                    }
                } else {
                    findings.add(Finding.error(
                            "route.tracker",
                            "tracker topic '" + topic + "' does not exist and route.tracker.bootstrap is FAIL"
                                    + " (§2.1); pre-provision it with the §2.1 configs (cleanup.policy=compact,"
                                    + " delete.retention.ms >= 2 x delay.max, min.compaction.lag.ms >= max(2 x"
                                    + " transaction timeout, 1h), segment.ms ~ 1h, LogAppendTime, partitions ="
                                    + " source partitions) or set route.tracker.bootstrap: CREATE."));
                    return;
                }
            }
            validateTracker(config, source, existing.get(), findings);
        } catch (ClusterAdminException e) {
            findings.add(
                    Finding.error("route.tracker", "could not validate the tracker topic: " + e.getMessage() + "."));
        }
    }

    /** §2.1 {@code CREATE} bootstrap: create with the spec configs, then apply the R12 ACL. */
    private Optional<TopicFacts> bootstrapTracker(
            CesiumConfig config, @Nullable TopicFacts source, String topic, List<Finding> findings) {
        if (source == null) {
            findings.add(Finding.error(
                    "route.tracker",
                    "cannot bootstrap tracker topic '" + topic + "': the source topic could not be described, so the"
                            + " partition count to mirror is unknown (§2.1); fix the source finding first."));
            return Optional.empty();
        }
        Map<String, String> configs = trackerCreateConfigs(
                config.delay().max(), config.kafka().transactions().timeout());
        admin.createTopic(topic, source.partitionCount(), configs);
        findings.add(Finding.info(
                "route.tracker",
                "created tracker topic '" + topic + "' with " + source.partitionCount()
                        + " partition(s) (mirroring the source) and the §2.1 configs " + configs + "."));
        applyTrackerAcl(config, topic, findings);
        // The topic provably exists — CreateTopics was acknowledged by the controller above. A broker
        // still serving a metadata image from before that record answers UNKNOWN_TOPIC_OR_PARTITION,
        // which describeTopic necessarily reports as empty; wait that window out rather than calling a
        // healthy cluster degraded. See TopicVisibility for why the wait lives there and not here.
        Optional<TopicFacts> created = visibility.awaitCreated(topic, source.partitionCount());
        if (created.isEmpty()) {
            findings.add(Finding.error(
                    "route.tracker",
                    "tracker topic '" + topic + "' was created, but " + TopicVisibility.BUDGET.toMillis()
                            + " ms later no broker will describe it with its " + source.partitionCount()
                            + " partition(s) — far longer than normal asynchronous metadata propagation."
                            + " Retry startup; if it recurs, investigate broker metadata lag (a stalled"
                            + " metadata publisher) or controller availability."));
        } else if (visibility.lastWaitedMillis() > 0) {
            // Never absorb the wait silently: a cluster whose propagation has degraded from
            // sub-millisecond to seconds is something the operator has to hear about, even though
            // startup succeeded.
            findings.add(trackerVisibilityWaitFinding(topic, visibility.lastWaitedMillis()));
        }
        return created;
    }

    /** Reports how long the tracker's metadata took to propagate — a warning once it is non-trivial. */
    private static Finding trackerVisibilityWaitFinding(String topic, long waitedMillis) {
        String message = "tracker topic '" + topic + "' became describable only after waiting " + waitedMillis
                + " ms for metadata propagation (normally sub-millisecond).";
        return waitedMillis > TopicVisibility.WARN_THRESHOLD_MS
                ? Finding.warning(
                        "route.tracker",
                        message + " Startup succeeded, but a metadata publisher this slow is a sign of a"
                                + " loaded or degrading cluster — investigate before it crosses the"
                                + " startup budget.")
                : Finding.info("route.tracker", message);
    }

    private void applyTrackerAcl(CesiumConfig config, String topic, List<Finding> findings) {
        Optional<String> principal = config.route().tracker().aclPrincipal();
        if (principal.isEmpty() || principal.get().isBlank()) {
            return;
        }
        try {
            admin.grantTrackerWriteAcl(topic, principal.get());
            findings.add(Finding.info(
                    "route.tracker.acl-principal",
                    "granted tracker write/read/describe on '" + topic + "' to principal '" + principal.get()
                            + "' (R12: tracker write access restricted to the cesium principal is a normative"
                            + " deployment requirement)."));
        } catch (ClusterAdminException e) {
            if (e.hasCause(SecurityDisabledException.class) || e.hasCause(UnsupportedVersionException.class)) {
                findings.add(Finding.warning(
                        "route.tracker.acl-principal",
                        "the cluster has no authorizer, so the tracker write ACL could not be applied ("
                                + e.getMessage() + "). R12: tracker write access restricted to the cesium principal"
                                + " is a normative deployment requirement — a forged ADD is a duplicate-injection"
                                + " primitive and a forged tombstone is a data-loss primitive; enable an authorizer"
                                + " or enforce the restriction by other means."));
            } else {
                findings.add(Finding.error(
                        "route.tracker.acl-principal",
                        "failed to apply the tracker write ACL on '" + topic + "': " + e.getMessage()
                                + " — R12 requires tracker write access restricted to the cesium principal; fix the"
                                + " ACL permissions or apply it manually, then retry."));
            }
        }
    }

    /** §2.1 tracker validation, run for both bootstrap modes once the topic exists. */
    private void validateTracker(
            CesiumConfig config, @Nullable TopicFacts source, TopicFacts tracker, List<Finding> findings) {
        if (source != null && tracker.partitionCount() != source.partitionCount()) {
            findings.add(Finding.error(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has " + tracker.partitionCount()
                            + " partition(s) but the source has " + source.partitionCount()
                            + " — counts MUST be equal (§2.1, R-7): tracker records land on the same partition number"
                            + " as their source record. Runbook: grow the tracker topic first, then the source;"
                            + " ingest fail-fasts when a source partition has no tracker partition (I-7)."));
        }

        Map<String, String> topicConfig = topicConfigs(tracker.name());

        Set<String> policies = cleanupPolicies(topicConfig);
        String rawPolicy = topicConfig.getOrDefault(TopicConfig.CLEANUP_POLICY_CONFIG, DEFAULT_CLEANUP_POLICY);
        if (!(policies.size() == 1 && policies.contains(TopicConfig.CLEANUP_POLICY_COMPACT))) {
            String detail = policies.contains(TopicConfig.CLEANUP_POLICY_COMPACT)
                            && policies.contains(TopicConfig.CLEANUP_POLICY_DELETE)
                    ? "compact,delete reintroduces silent loss: a pending ADD older than retention would be deleted"
                    : "compaction-only retention is what guarantees a lone pending ADD can never expire";
            findings.add(Finding.error(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has cleanup.policy='" + rawPolicy
                            + "' but it MUST be exactly 'compact' (D4, §2.1): " + detail
                            + ". Set cleanup.policy=compact on the tracker topic."));
        }

        long floorMs = deleteRetentionFloorMs(config.delay().max());
        long deleteRetentionMs = parseLongConfig(
                topicConfig, TopicConfig.DELETE_RETENTION_MS_CONFIG, DEFAULT_DELETE_RETENTION_MS, findings);
        if (deleteRetentionMs < floorMs) {
            findings.add(Finding.error(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has delete.retention.ms=" + deleteRetentionMs
                            + " ms, below the tombstone-retention floor 2 x delay.max = " + floorMs
                            + " ms (D14, §3.7, R-8): a replayer in overflow-fallback mode may need tombstones as old"
                            + " as the oldest pending entry, and cleaning them earlier converts a replay into a"
                            + " duplicate. Raise delete.retention.ms to >= " + floorMs
                            + " ms, or lower delay.max (drain entries scheduled under the old maximum first — see"
                            + " the lowering-delay.max runbook)."));
        }

        long lagFloorMs = minCompactionLagFloorMs(config.kafka().transactions().timeout());
        long lagMs = parseLongConfig(
                topicConfig, TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, DEFAULT_MIN_COMPACTION_LAG_MS, findings);
        if (lagMs < lagFloorMs) {
            findings.add(Finding.error(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has min.compaction.lag.ms=" + lagMs
                            + " ms, below max(2 x transaction timeout, 1h) = " + lagFloorMs
                            + " ms (§2.1): the floor keeps the cleaner away from the active tail/LSO region. Raise"
                            + " min.compaction.lag.ms to >= " + lagFloorMs + " ms."));
        }

        long segmentMs = parseLongConfig(topicConfig, TopicConfig.SEGMENT_MS_CONFIG, DEFAULT_SEGMENT_MS, findings);
        if (segmentMs > TRACKER_SEGMENT_MS.toMillis()) {
            findings.add(Finding.warning(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has segment.ms=" + segmentMs + " ms; §2.1 recommends ~"
                            + TRACKER_SEGMENT_MS.toMillis()
                            + " ms so the compaction cleaner actually runs — large segments delay tombstone"
                            + " collection and grow tracker disk."));
        }

        String timestampType =
                topicConfig.getOrDefault(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, DEFAULT_TIMESTAMP_TYPE);
        if (!LOG_APPEND_TIME.equals(timestampType)) {
            findings.add(Finding.warning(
                    "route.tracker",
                    "tracker topic '" + tracker.name() + "' has message.timestamp.type=" + timestampType
                            + "; §2.1 recommends " + LOG_APPEND_TIME
                            + " so tracker record timestamps reflect append order."));
        }

        checkTrackerAcl(config, tracker.name(), findings);
    }

    /**
     * R12 tracker write-ACL verification, run on <em>every</em> startup once the topic exists — for
     * both bootstrap modes. Kafka enforces ACLs continuously regardless of whether cesium re-applies
     * them, so cesium DESCRIBEs the live grant and, at the {@code startup-checks.tracker-acl}
     * strictness, reports when the configured principal's exclusive WRITE grant is missing, when a
     * foreign principal holds WRITE, when {@code acl-principal} is unset entirely, or when the cluster
     * has no authorizer to verify against. The default {@code WARN} surfaces any of these on every
     * boot without blocking (backward-compatible); {@code FAIL} — recommended for production — refuses
     * to start unless the R12 restriction is verifiably in force, since the whole security posture
     * delegates to this ACL and a forged ADD is a duplicate-injection primitive while a forged
     * tombstone is a data-loss primitive; {@code SKIP} omits the check for operators who enforce the
     * restriction out of band.
     */
    private void checkTrackerAcl(CesiumConfig config, String topic, List<Finding> findings) {
        CheckMode mode = config.startupChecks().trackerAcl();
        if (mode == CheckMode.SKIP) {
            findings.add(Finding.info(
                    "startup-checks.tracker-acl",
                    "SKIP: the R12 tracker write-ACL restriction was not verified — restricting tracker write access"
                            + " to the cesium principal is the operator's responsibility (a forged ADD is a"
                            + " duplicate-injection primitive and a forged tombstone is a data-loss primitive)."));
            return;
        }
        Optional<String> principal = config.route().tracker().aclPrincipal().filter(p -> !p.isBlank());
        Set<String> writePrincipals;
        try {
            writePrincipals = admin.describeTrackerWritePrincipals(topic);
        } catch (ClusterAdminException e) {
            if (e.hasCause(SecurityDisabledException.class) || e.hasCause(UnsupportedVersionException.class)) {
                findings.add(finding(
                        mode,
                        "route.tracker.acl-principal",
                        "the cluster has no authorizer, so the tracker write ACL (R12) cannot be verified ("
                                + e.getMessage() + "). A forged ADD is a duplicate-injection primitive and a forged"
                                + " tombstone is a data-loss primitive — enable an authorizer and restrict tracker"
                                + " write access to the cesium principal, or set startup-checks.tracker-acl: WARN/SKIP"
                                + " once you enforce the restriction by other means."));
            } else {
                findings.add(finding(
                        mode,
                        "route.tracker.acl-principal",
                        "could not describe the tracker write ACL on '" + topic + "' to verify R12: " + e.getMessage()
                                + " — verify manually that tracker write access is restricted to the cesium"
                                + " principal."));
            }
            return;
        }
        if (principal.isEmpty()) {
            // L3(c): the normative R12 control is not configured. Under the default FAIL this blocks
            // startup; WARN surfaces it without blocking (the escape hatch for out-of-band enforcement).
            String liveState = writePrincipals.isEmpty()
                    ? "no ALLOW WRITE ACL is present on the topic, so on a permissive authorizer any principal can"
                            + " forge tracker records"
                    : "the topic currently grants ALLOW WRITE to " + new TreeSet<>(writePrincipals)
                            + ", none of which cesium asserts or verifies";
            findings.add(finding(
                    mode,
                    "route.tracker.acl-principal",
                    "route.tracker.acl-principal is unset, so cesium neither applies nor verifies the R12 tracker"
                            + " write restriction — " + liveState
                            + ". Set route.tracker.acl-principal to the cesium principal (a forged ADD is a"
                            + " duplicate-injection primitive and a forged tombstone is a data-loss primitive), or"
                            + " set startup-checks.tracker-acl: WARN/SKIP if you enforce it out of band."));
            return;
        }
        String configured = principal.get();
        Set<String> foreign = new TreeSet<>(writePrincipals);
        foreign.remove(configured);
        if (!writePrincipals.contains(configured)) {
            // L3(a): the operator asked for the restriction but it is not actually in force.
            findings.add(finding(
                    mode,
                    "route.tracker.acl-principal",
                    "route.tracker.acl-principal='" + configured + "' is configured but no matching ALLOW WRITE ACL"
                            + " exists on tracker topic '" + topic + "' (live ALLOW WRITE principals: "
                            + (writePrincipals.isEmpty() ? "none" : new TreeSet<>(writePrincipals))
                            + ") — the R12 write restriction is not in force. Re-apply the grant (or set"
                            + " route.tracker.bootstrap: CREATE on a run where the topic is absent), or restore it"
                            + " out of band."));
        }
        if (!foreign.isEmpty()) {
            // L3(b): a principal other than the configured cesium principal can write tracker
            // records — a duplicate-injection / data-loss primitive.
            findings.add(finding(
                    mode,
                    "route.tracker.acl-principal",
                    "foreign principal(s) " + foreign + " hold ALLOW WRITE on tracker topic '" + topic + "' besides"
                            + " the configured cesium principal '" + configured + "' — a non-cesium writer can forge"
                            + " ADDs (duplicate injection) and tombstones (data loss). Revoke the foreign tracker"
                            + " write grant(s) (R12)."));
        }
    }

    // ------------------------------------------------------------------ dlq + destination

    private void checkDlq(CesiumConfig config, List<Finding> findings) {
        if (!routesToDlq(config) || !config.route().hasDlq()) {
            // A DLQ-routing policy without a configured topic is already an aggregate config
            // error (CesiumConfigValidator); nothing to check against the cluster here.
            return;
        }
        String topic = config.route().dlq().get().topic();
        try {
            if (admin.describeTopic(topic).isEmpty()) {
                findings.add(Finding.error(
                        "route.dlq.topic",
                        "a configured policy routes to the DLQ but topic '" + topic
                                + "' does not exist (§2.1, §2.3); create it or change"
                                + " delay.on-malformed-header / delay.on-over-max /"
                                + " dispatch.on-unfetchable-payload / route.relay.on-unrelayable."));
                return;
            }
            checkDlqRetention(topic, findings);
        } catch (ClusterAdminException e) {
            findings.add(Finding.error("route.dlq.topic", "could not validate the DLQ topic: " + e.getMessage() + "."));
        }
    }

    /**
     * L4: an unbounded DLQ ({@code retention.ms=-1} and {@code retention.bytes=-1}) can be filled
     * by a malformed-header flood — each rejected record copies its full payload 1:1 into the DLQ
     * (§7.4) — so warn (never hard-fail; the DLQ is operator-owned) when the topic has no bound.
     */
    private void checkDlqRetention(String topic, List<Finding> findings) {
        Map<String, String> topicConfig = topicConfigs(topic);
        long retentionMs =
                parseLongConfig(topicConfig, TopicConfig.RETENTION_MS_CONFIG, DEFAULT_RETENTION_MS, findings);
        long retentionBytes =
                parseLongConfig(topicConfig, TopicConfig.RETENTION_BYTES_CONFIG, DEFAULT_RETENTION_BYTES, findings);
        if (retentionMs == -1 && retentionBytes == -1) {
            findings.add(Finding.warning(
                    "route.dlq.topic",
                    "DLQ topic '" + topic + "' has unbounded retention (retention.ms=-1 and retention.bytes=-1):"
                            + " a malformed-header flood routes one full-payload copy per rejected record to the DLQ"
                            + " (§7.4, L4) and can fill it without bound. Set a retention.ms and/or retention.bytes"
                            + " bound on the DLQ topic."));
        }
    }

    /** True when any policy routes records to the DLQ (§2.3, §7.4). */
    static boolean routesToDlq(CesiumConfig config) {
        return config.delay().onMalformedHeader() == MalformedHeaderPolicy.DLQ
                || config.delay().onOverMax() == OverMaxPolicy.DLQ
                || config.dispatch().onUnfetchablePayload() == UnfetchablePayloadPolicy.DLQ
                // route.relay.on-unrelayable defaults to DLQ (M2): the broker-side existence and L4
                // retention checks MUST run for it too, otherwise a permanent-rejection DLQ write to a
                // nonexistent topic fails with UnknownTopicOrPartition — not a permanent record
                // rejection — and wedges the partition (the exact wedge M2 set out to eliminate).
                || config.route().relay().onUnrelayable() == UnrelayablePolicy.DLQ;
    }

    private void checkDestination(CesiumConfig config, @Nullable TopicFacts source, List<Finding> findings) {
        String topic = config.route().destination().topic();
        try {
            Optional<TopicFacts> destination = admin.describeTopic(topic);
            if (destination.isEmpty()) {
                findings.add(Finding.error(
                        "route.destination.topic",
                        "destination topic '" + topic + "' does not exist (§2.1); create it or fix the"
                                + " configuration."));
                return;
            }
            if (config.route().relay().partitioning() == RelayPartitioning.SOURCE_PARTITION
                    && source != null
                    && destination.get().partitionCount() != source.partitionCount()) {
                findings.add(Finding.error(
                        "route.relay.partitioning",
                        "route.relay.partitioning=SOURCE_PARTITION pins the destination partition to the source"
                                + " partition number, which requires equal partition counts (§2.4): destination '"
                                + topic + "' has " + destination.get().partitionCount() + " partition(s), source has "
                                + source.partitionCount() + ". Grow the destination to match or use BY_KEY."));
            }
        } catch (ClusterAdminException e) {
            findings.add(Finding.error(
                    "route.destination.topic", "could not validate the destination topic: " + e.getMessage() + "."));
        }
    }

    // ------------------------------------------------------------------ broker offsets retention

    /** D18/R6 (KIP-211): committed offsets must outlive the longest tolerated outage. */
    private void checkOffsetsRetention(CesiumConfig config, List<Finding> findings) {
        CheckMode mode = config.startupChecks().outageCheck();
        Duration maxOutage = config.startupChecks().maxToleratedOutage();
        Optional<String> raw;
        try {
            raw = admin.brokerConfig(OFFSETS_RETENTION_MINUTES);
        } catch (ClusterAdminException e) {
            raw = Optional.empty();
        }
        if (raw.isEmpty()) {
            findings.add(Finding.warning(
                    "startup-checks.max-tolerated-outage",
                    "could not read broker " + OFFSETS_RETENTION_MINUTES + "; verify manually that it covers"
                            + " startup-checks.max-tolerated-outage (" + maxOutage
                            + ") — committed offsets expiring during an outage fail-fast under"
                            + " auto.offset.reset=none (D18, R6, KIP-211)."));
            return;
        }
        long minutes;
        try {
            minutes = Long.parseLong(raw.get().trim());
        } catch (NumberFormatException e) {
            findings.add(Finding.warning(
                    "startup-checks.max-tolerated-outage",
                    "broker " + OFFSETS_RETENTION_MINUTES + "='" + raw.get()
                            + "' is not a number; verify manually that it covers"
                            + " startup-checks.max-tolerated-outage (" + maxOutage + ") (D18, R6)."));
            return;
        }
        long retentionMs = saturatedMultiply(minutes, 60_000L);
        if (retentionMs < maxOutage.toMillis()) {
            findings.add(finding(
                    mode,
                    "startup-checks.max-tolerated-outage",
                    "broker " + OFFSETS_RETENTION_MINUTES + " (" + minutes + " min = " + retentionMs
                            + " ms) is below startup-checks.max-tolerated-outage (" + maxOutage
                            + ") (D18, R6, KIP-211): an outage longer than the broker's offsets retention expires"
                            + " the committed offsets, and auto.offset.reset=none then fail-fasts — silent skip or"
                            + " mass duplication is never an option. Raise " + OFFSETS_RETENTION_MINUTES
                            + " or lower startup-checks.max-tolerated-outage."));
        }
    }

    // ------------------------------------------------------------------ helpers

    private static Finding finding(CheckMode mode, String path, String message) {
        return new Finding(mode == CheckMode.WARN ? Severity.WARNING : Severity.ERROR, path, message);
    }

    private static Set<String> cleanupPolicies(Map<String, String> topicConfig) {
        String raw = topicConfig.getOrDefault(TopicConfig.CLEANUP_POLICY_CONFIG, DEFAULT_CLEANUP_POLICY);
        Set<String> policies = new LinkedHashSet<>();
        for (String policy : raw.split(",", -1)) {
            String trimmed = policy.trim();
            if (!trimmed.isEmpty()) {
                policies.add(trimmed);
            }
        }
        return policies;
    }

    private static long parseLongConfig(
            Map<String, String> topicConfig, String key, long defaultValue, List<Finding> findings) {
        String raw = topicConfig.get(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            findings.add(Finding.warning(
                    "route",
                    "could not parse topic config " + key + "='" + raw + "' as a number; assuming the broker default "
                            + defaultValue + "."));
            return defaultValue;
        }
    }

    private static long saturatedMultiply(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatedAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
