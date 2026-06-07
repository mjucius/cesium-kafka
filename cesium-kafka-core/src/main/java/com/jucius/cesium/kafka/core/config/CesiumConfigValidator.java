package com.jucius.cesium.kafka.core.config;

import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
 *
 * <p><strong>Scope — this check bounds steady-state ACTIVE backpressure, not recovery (H1).</strong>
 * The worst-case footprint assumes the per-partition cap holds, which it does only for ACTIVE
 * shards: a RECOVERING shard is never backpressure-paused (it must replay to the barrier, I4), so
 * this startup check alone does <em>not</em> bound the replay footprint. Recovery is instead bounded
 * <em>at runtime</em> by the store's resident-pending ceiling (the same heap budget ÷
 * {@value DispatchConfig#INDEX_BYTES_PER_ENTRY} B/entry): an over-budget durable backlog fails fast
 * and attributably ({@code cesium_recovery_over_budget} + a runbook log line) instead of allocating
 * into an {@code OutOfMemoryError} crash-loop. The durable backlog that recovery replays is itself
 * bounded outside cesium by broker client/produce quotas + source {@code retention.bytes} +
 * {@code delay.max} (the L4 deployment guidance, §5.3/§5.4) — this validator cannot see those, which
 * is why the runtime ceiling is the load-bearing recovery guard.
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
        bindAddress(
                findings, "observability.bind-address", config.observability().bindAddress());
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

    /**
     * L1: the observability bind address must be an IP literal ({@code 0.0.0.0}, {@code 127.0.0.1},
     * {@code ::}, {@code ::1}) or {@code localhost}. Hostnames are rejected here without a DNS lookup
     * (the validator never touches the network) — a bind address is a local interface, and parsing it
     * eagerly turns a typo into a clear config error rather than a {@code BindException} at startup.
     */
    private static void bindAddress(List<Finding> findings, String path, String value) {
        if (value.isBlank()) {
            findings.add(Finding.error(path, "must not be blank."));
            return;
        }
        if (value.equalsIgnoreCase("localhost")) {
            return;
        }
        if (value.indexOf(':') >= 0) {
            // IPv6 literal: getByName parses the literal without a DNS lookup (a hostname cannot
            // contain a ':'), so an unparseable value fails here rather than reaching for the network.
            try {
                InetAddress.getByName(value);
                return;
            } catch (UnknownHostException e) {
                findings.add(Finding.error(path, "is not a valid IPv6 literal: " + value + "."));
                return;
            }
        }
        if (!isIpv4Literal(value)) {
            findings.add(Finding.error(
                    path, "must be an IP literal (e.g. 0.0.0.0, 127.0.0.1, ::1) or localhost, got " + value + "."));
        }
    }

    /** Strict dotted-quad check (no DNS): exactly four octets, each 0-255. */
    private static boolean isIpv4Literal(String value) {
        String[] octets = value.split("\\.", -1);
        if (octets.length != 4) {
            return false;
        }
        for (String octet : octets) {
            if (octet.isEmpty() || octet.length() > 3) {
                return false;
            }
            int parsed;
            try {
                parsed = Integer.parseInt(octet);
            } catch (NumberFormatException e) {
                return false;
            }
            if (parsed < 0 || parsed > 255) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ cross-field rules

    private static void checkCrossField(CesiumConfig config, List<Finding> findings) {
        checkDlqPolicies(config, findings);
        checkUnsafeFailPolicies(config, findings);
        checkPenaltyOrdering(config, findings);
        checkDrainSlice(config, findings);
    }

    /**
     * L5: warn when an ingest delay policy is set to {@code FAIL}. A single crafted record (a
     * non-decimal or out-of-range {@code cesium-delay-ms}, or a delay above {@code delay.max})
     * then fatally stops the ingest loop, tearing down every worker; because the aborting batch
     * never commits offsets and {@code auto.offset.reset=none} is locked, the poison record stays
     * at the committed source offset and re-fails on every restart — a pipeline-wide,
     * restart-persistent outage from one record. {@code FAIL} is safe only when the source topic
     * has exclusively trusted producers; multi-tenant ingress must use {@code DLQ} /
     * {@code RELAY_IMMEDIATE} / {@code CLAMP}. WARN, never ERROR: {@code FAIL} is a deliberate,
     * non-default opt-in.
     */
    private static void checkUnsafeFailPolicies(CesiumConfig config, List<Finding> findings) {
        if (config.delay().onMalformedHeader() == MalformedHeaderPolicy.FAIL) {
            findings.add(Finding.warning(
                    "delay.on-malformed-header",
                    "FAIL turns a single crafted record (a malformed cesium-delay-ms) into a pipeline-wide,"
                            + " restart-persistent outage (L5): the poison record stays at the committed source offset"
                            + " and auto.offset.reset=none re-fails it on every restart. Unsafe when the SOURCE topic"
                            + " has untrusted producers — prefer DLQ or RELAY_IMMEDIATE for multi-tenant ingress."));
        }
        if (config.delay().onOverMax() == OverMaxPolicy.FAIL) {
            findings.add(Finding.warning(
                    "delay.on-over-max",
                    "FAIL turns a single crafted record (a delay above delay.max) into a pipeline-wide,"
                            + " restart-persistent outage (L5): the poison record stays at the committed source offset"
                            + " and auto.offset.reset=none re-fails it on every restart. Unsafe when the SOURCE topic"
                            + " has untrusted producers — prefer DLQ or CLAMP for multi-tenant ingress."));
        }
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
        if (config.route().relay().onUnrelayable() == UnrelayablePolicy.DLQ) {
            findings.add(Finding.error("route.relay.on-unrelayable", requirement));
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
     *
     * <p>This bounds the ACTIVE backpressure caps only; the replay footprint of a RECOVERING shard
     * is bounded at runtime by the store's resident-pending ceiling, not here (H1 — see the class
     * javadoc).
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
