package com.jucius.cesium.kafka.core.config;

import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Aggregate configuration validator (design §8): collects <em>every</em> violation — required
 * fields, ranges, cross-field rules, locked Kafka keys, and the heap-budget check — into one
 * {@link ValidationReport}, never failing on the first error.
 *
 * <p>The heap-budget check (§5.3, R7/R18) compares the worst-case index footprint
 * {@code assignedPartitionEstimate × dispatch.max-pending-per-partition ×}
 * {@value DispatchConfig#INDEX_BYTES_PER_ENTRY}{@code  B/entry} against the index heap budget
 * (25% of max heap). Strictness is the {@code startup-checks.heap-budget} flag (FAIL default,
 * WARN opt-down); the computed footprint is always included in the report as an INFO finding so
 * startup logs print it (§5.3).
 */
public final class CesiumConfigValidator {

    /** The group-B consumer property bounding the time-sliced drain (§6, R2). */
    static final String MAX_POLL_INTERVAL_MS = "max.poll.interval.ms";

    /**
     * The kafka-clients default for {@code ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG} (300000 ms,
     * classic protocol). §8 states the drain-slice cap unconditionally, so when the operator
     * leaves the key unset the {@code / 3} bound is enforced against this default rather than
     * skipped — an oversized slice must refuse at startup, not evict the member at runtime.
     */
    static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000L;

    /** Validates {@code config}, returning the aggregate report. */
    public ValidationReport validate(CesiumConfig config, ValidationContext context) {
        List<Finding> findings = new ArrayList<>();
        checkRequired(config, findings);
        checkRanges(config, findings);
        checkCrossField(config, findings);
        checkLockedKafkaKeys(config, findings);
        checkHeapBudget(config, context, findings);
        return new ValidationReport(findings);
    }

    /**
     * Validates {@code config} and returns it when clean.
     *
     * @throws ConfigValidationException carrying the full report when any ERROR finding exists
     */
    public CesiumConfig validateOrThrow(CesiumConfig config, ValidationContext context) {
        ValidationReport report = validate(config, context);
        if (report.hasErrors()) {
            throw new ConfigValidationException(report);
        }
        return config;
    }

    // ------------------------------------------------------------------ required fields

    private static void checkRequired(CesiumConfig config, List<Finding> findings) {
        if (config.applicationId().isBlank()) {
            findings.add(Finding.error(
                    "application-id",
                    "required: namespaces consumer groups, transactional ids, and default topic names."));
        }
        if (config.instanceId().isBlank()) {
            findings.add(Finding.error(
                    "instance-id",
                    "required: a stable deployment-slot id (e.g. a StatefulSet ordinal) keeps transactional ids and"
                            + " group.instance.id stable across restarts; set the literal 'random' to explicitly opt"
                            + " in to non-stable ids (design D10)."));
        }
        if (config.roles().isEmpty()) {
            findings.add(Finding.error("roles", "must name at least one of [ingest, dispatch]."));
        }
        if (!config.route().source().isConfigured()) {
            findings.add(Finding.error("route.source.topic", "required: the topic records are consumed from."));
        }
        if (!config.route().destination().isConfigured()) {
            findings.add(Finding.error("route.destination.topic", "required: the topic records are relayed to."));
        }
        if (config.route().dlq().isPresent() && !config.route().dlq().get().isConfigured()) {
            findings.add(Finding.error("route.dlq.topic", "must not be blank when route.dlq is configured."));
        }
        if (config.store().type().isBlank()) {
            findings.add(Finding.error("store.type", "required: the scheduler store must be selected explicitly."));
        }
    }

    // ------------------------------------------------------------------ ranges

    private static void checkRanges(CesiumConfig config, List<Finding> findings) {
        atLeast(findings, "ingest.workers", config.ingest().workers(), 1);
        atLeast(findings, "ingest.max-batch", config.ingest().maxBatch(), 1);
        atLeast(findings, "dispatch.workers", config.dispatch().workers(), 1);
        atLeast(
                findings,
                "dispatch.batch.max-entries",
                config.dispatch().batch().maxEntries(),
                1);
        atLeast(findings, "dispatch.batch.max-bytes", config.dispatch().batch().maxBytes(), 1);
        // The sidecar carries the pinned-entry encoding in offset metadata (§3.5); a zero budget
        // would silently force every partition into the overflow fallback.
        atLeast(
                findings,
                "dispatch.cursor.sidecar-max-bytes",
                config.dispatch().cursor().sidecarMaxBytes(),
                1);
        atLeast(
                findings,
                "dispatch.max-pending-per-partition",
                config.dispatch().maxPendingPerPartition(),
                1);
        positiveDuration(findings, "delay.max", config.delay().max());
        positiveDuration(
                findings, "dispatch.drain.max-slice", config.dispatch().drain().maxSlice());
        nonNegativeDuration(findings, "dispatch.coalesce", config.dispatch().coalesce());
        positiveDuration(
                findings, "dispatch.idle-cursor-interval", config.dispatch().idleCursorInterval());
        positiveDuration(
                findings, "dispatch.fetch.timeout", config.dispatch().fetch().timeout());
        positiveDuration(
                findings,
                "dispatch.fetch.partition-time-floor",
                config.dispatch().fetch().partitionTimeFloor());
        positiveDuration(
                findings,
                "dispatch.fetch.penalty.backoff",
                config.dispatch().fetch().penalty().backoff());
        positiveDuration(
                findings,
                "dispatch.fetch.penalty.backoff-max",
                config.dispatch().fetch().penalty().backoffMax());
        positiveDuration(
                findings,
                "kafka.transactions.timeout",
                config.kafka().transactions().timeout());
        if (config.kafka().transactions().commitRetry() < 0) {
            findings.add(Finding.error(
                    "kafka.transactions.commit-retry",
                    "must be >= 0, got " + config.kafka().transactions().commitRetry() + "."));
        }
        if (config.dispatch().maxPendingTotal() < 0) {
            findings.add(Finding.error(
                    "dispatch.max-pending-total",
                    "must be >= 0 (0 = AUTO: heap-derived), got "
                            + config.dispatch().maxPendingTotal() + "."));
        }
        int port = config.observability().port();
        if (port < 1 || port > 65535) {
            findings.add(Finding.error("observability.port", "must be in [1, 65535], got " + port + "."));
        }
        positiveDuration(
                findings,
                "startup-checks.max-tolerated-outage",
                config.startupChecks().maxToleratedOutage());
        failOrWarnOnly(
                findings, "startup-checks.outage-check", config.startupChecks().outageCheck());
        failOrWarnOnly(
                findings, "startup-checks.heap-budget", config.startupChecks().heapBudget());
    }

    private static void atLeast(List<Finding> findings, String path, long value, long min) {
        if (value < min) {
            findings.add(Finding.error(path, "must be >= " + min + ", got " + value + "."));
        }
    }

    private static void positiveDuration(List<Finding> findings, String path, Duration value) {
        if (value.isZero() || value.isNegative()) {
            findings.add(Finding.error(path, "must be a positive duration, got " + value + "."));
        }
    }

    private static void nonNegativeDuration(List<Finding> findings, String path, Duration value) {
        if (value.isNegative()) {
            findings.add(Finding.error(path, "must not be negative, got " + value + "."));
        }
    }

    private static void failOrWarnOnly(List<Finding> findings, String path, CheckMode mode) {
        if (mode == CheckMode.SKIP) {
            findings.add(Finding.error(path, "must be FAIL or WARN; this check cannot be skipped."));
        }
    }

    // ------------------------------------------------------------------ cross-field rules

    private static void checkCrossField(CesiumConfig config, List<Finding> findings) {
        checkDlqPolicies(config, findings);
        checkPenaltyOrdering(config, findings);
        checkDrainSlice(config, findings);
    }

    /** Every policy routing to DLQ requires the DLQ topic to exist (§2.3, §7.4). */
    private static void checkDlqPolicies(CesiumConfig config, List<Finding> findings) {
        if (config.route().hasDlq()) {
            return;
        }
        String requirement = "policy DLQ requires route.dlq.topic to be configured (§2.3: the DLQ keeps the"
                + " pipeline alive while making violations explicit).";
        if (config.delay().onMalformedHeader() == MalformedHeaderPolicy.DLQ) {
            findings.add(Finding.error("delay.on-malformed-header", requirement));
        }
        if (config.delay().onOverMax() == OverMaxPolicy.DLQ) {
            findings.add(Finding.error("delay.on-over-max", requirement));
        }
        if (config.dispatch().onUnfetchablePayload() == UnfetchablePayloadPolicy.DLQ) {
            findings.add(Finding.error("dispatch.on-unfetchable-payload", requirement));
        }
    }

    private static void checkPenaltyOrdering(CesiumConfig config, List<Finding> findings) {
        DispatchConfig.Fetch.Penalty penalty = config.dispatch().fetch().penalty();
        if (penalty.backoffMax().compareTo(penalty.backoff()) < 0) {
            findings.add(Finding.error(
                    "dispatch.fetch.penalty.backoff-max",
                    "must be >= dispatch.fetch.penalty.backoff (" + penalty.backoff() + "), got " + penalty.backoffMax()
                            + "."));
        }
    }

    /**
     * The drain slice must be ≤ {@code max.poll.interval.ms / 3} (§6, R2) — back-to-back
     * transactions without polling evict the member. The bound is enforced unconditionally: when
     * the operator leaves the poll interval unset, the cap falls back to the kafka-clients default
     * ({@value DEFAULT_MAX_POLL_INTERVAL_MS} ms). When a configured value is unparseable, the
     * finding names the map that actually supplied it (overlay wins over the common map).
     */
    private static void checkDrainSlice(CesiumConfig config, List<Finding> findings) {
        Map.Entry<String, String> configured = config.kafka().effectiveTrackerConsumerProperty(MAX_POLL_INTERVAL_MS);
        long maxPollIntervalMs;
        String source;
        if (configured == null) {
            maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL_MS;
            source = "the kafka-clients default " + DEFAULT_MAX_POLL_INTERVAL_MS + " ms";
        } else {
            Long parsed = parseLong(configured.getValue());
            if (parsed == null || parsed <= 0) {
                findings.add(Finding.error(
                        configured.getKey(),
                        "must be a positive integer number of milliseconds, got '" + configured.getValue() + "'."));
                return;
            }
            maxPollIntervalMs = parsed;
            source = maxPollIntervalMs + " ms (" + configured.getKey() + ")";
        }
        long sliceMs = config.dispatch().drain().maxSlice().toMillis();
        long boundMs = maxPollIntervalMs / 3;
        if (sliceMs > boundMs) {
            findings.add(Finding.error(
                    "dispatch.drain.max-slice",
                    "must be <= max.poll.interval.ms / 3 (" + boundMs + " ms of " + source + "), got " + sliceMs
                            + " ms: the time-sliced drain interleaves polls between transactions so group"
                            + " membership survives due-storms (design §6, R2)."));
        }
    }

    private static @Nullable Long parseLong(String raw) {
        try {
            return Long.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------ locked Kafka keys

    /** Rejects locked keys in the common map and in every per-client overlay (§8, D17/D18). */
    private static void checkLockedKafkaKeys(CesiumConfig config, List<Finding> findings) {
        for (Map.Entry<String, Map<String, String>> client :
                config.kafka().clientPropertyMaps().entrySet()) {
            for (String key : client.getValue().keySet()) {
                String explanation = LockedKafkaKeys.explanation(key);
                if (explanation != null) {
                    findings.add(Finding.error(client.getKey() + "." + key, explanation));
                }
            }
        }
    }

    // ------------------------------------------------------------------ heap budget

    /**
     * Worst-case index footprint vs the 25%-of-heap index budget (§5.3): always reported as INFO;
     * a breach is an ERROR (or WARNING under {@code startup-checks.heap-budget: WARN}).
     */
    private static void checkHeapBudget(CesiumConfig config, ValidationContext context, List<Finding> findings) {
        long perPartition = config.dispatch().maxPendingPerPartition();
        int partitions = context.assignedPartitionEstimate();
        long footprint =
                saturatedMultiply(saturatedMultiply(partitions, perPartition), DispatchConfig.INDEX_BYTES_PER_ENTRY);
        long budget = DispatchConfig.heapBudgetBytes(context.maxHeapBytes());

        findings.add(Finding.info(
                "dispatch.max-pending-per-partition",
                "worst-case index footprint " + footprint + " bytes (" + partitions + " partition(s) x "
                        + perPartition + " entries x " + DispatchConfig.INDEX_BYTES_PER_ENTRY
                        + " B/entry) vs heap budget " + budget + " bytes (25% of max heap "
                        + context.maxHeapBytes() + " bytes)."));

        Severity breachSeverity =
                config.startupChecks().heapBudget() == CheckMode.WARN ? Severity.WARNING : Severity.ERROR;
        if (footprint > budget) {
            findings.add(new Finding(
                    breachSeverity,
                    "dispatch.max-pending-per-partition",
                    "worst-case index footprint " + footprint + " bytes exceeds the heap budget " + budget
                            + " bytes (25% of max heap " + context.maxHeapBytes()
                            + " bytes); lower dispatch.max-pending-per-partition, assign fewer partitions per"
                            + " instance, or raise -Xmx (design §5.3)."));
        }

        long maxPendingTotal = config.dispatch().maxPendingTotal();
        if (maxPendingTotal != DispatchConfig.AUTO_MAX_PENDING_TOTAL) {
            long capFootprint = saturatedMultiply(maxPendingTotal, DispatchConfig.INDEX_BYTES_PER_ENTRY);
            if (capFootprint > budget) {
                findings.add(new Finding(
                        breachSeverity,
                        "dispatch.max-pending-total",
                        "the configured global cap admits " + capFootprint + " bytes of index ("
                                + maxPendingTotal + " entries x " + DispatchConfig.INDEX_BYTES_PER_ENTRY
                                + " B/entry), exceeding the heap budget " + budget + " bytes (25% of max heap "
                                + context.maxHeapBytes() + " bytes); lower the cap or raise -Xmx (design §5.3,"
                                + " R18)."));
            }
        }
    }

    private static long saturatedMultiply(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
