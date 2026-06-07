package com.jucius.cesium.kafka.core.dispatch;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * The slice of configuration the {@link DispatchLoop} consumes, flattened from
 * {@link CesiumConfig} so the loop has no reach into unrelated config (and tests construct it
 * directly) — the dispatch analog of {@code IngestLoopConfig}.
 *
 * @param trackerTopic the resolved tracker topic group B subscribes to; COMPLETE tombstones land
 *     on the same partition number as the entry's source partition (design §2.1)
 * @param sourceTopic the source topic name, carried into payload-expired loss notices (§2.4)
 * @param maxBatchEntries per-transaction entry bound ({@code dispatch.batch.max-entries}, D8)
 * @param maxBatchBytes decompressed payload byte budget handed to the seek fetcher
 *     ({@code dispatch.batch.max-bytes}, §7.2)
 * @param maxPollTimeout ceiling on the event loop's poll timeout (§6's
 *     {@code clamp(nextDeadline - now, 0, 30s)})
 * @param drainSlice maximum back-to-back transaction time before the loop returns to a real poll
 *     ({@code dispatch.drain.max-slice}; validated ≤ {@code max.poll.interval.ms / 3} upstream)
 * @param idleCursorInterval how long a partition may go untouched before its cursor advances in a
 *     records-free transaction (§3.5); also the rate limit on idle-advancement attempts
 * @param fetchTimeout overall seek-fetch deadline per pass ({@code dispatch.fetch.timeout})
 * @param penaltyBackoff initial per-source-partition penalty after a TRANSIENT fetch outcome
 *     (§7.3, D22); doubles per consecutive transient failure of the same partition
 * @param penaltyBackoffMax penalty ceiling; the stamp never exceeds {@code now + this}
 * @param onUnfetchablePayload policy for provably-expired payloads (§7.4, D-9)
 * @param onUnrelayable policy for records the destination <em>permanently</em> rejects on the
 *     delayed-relay produce ({@code route.relay.on-unrelayable}, §3.8 I-8): {@code DLQ} (default)
 *     writes an unrelayable DLQ notice + COMPLETE tombstone + cursor atomically, {@code DROP}
 *     completes the entry without a notice, {@code FAIL} stops the loop. Transient produce failures
 *     stay on the abort/restore retry path
 * @param maxPendingPerPartition backpressure high-water mark per ACTIVE shard (§5.3); intake for
 *     the partition pauses above it and resumes below {@code maxPendingPerPartition / 2}
 * @param maxPendingTotal global pending cap pausing all ACTIVE shards ({@code ≤ 0} disables; the
 *     caller resolves the {@code AUTO} sentinel against the heap budget before constructing this)
 * @param commitRetryLimit bounded retries of a timed-out {@code commitTransaction} before the
 *     outcome is declared in-doubt and the I9 procedure runs ({@code kafka.transactions
 *     .commit-retry}, §3.8)
 * @param parkThreshold consecutive batch failures after which the loop is parked-and-degraded
 *     (§3.8 retry-exhaustion, R15) — retries continue with capped backoff either way
 * @param retryBackoffInitial first retry backoff; doubles per consecutive failure
 * @param retryBackoffMax backoff cap — recovery after a long broker outage stays prompt
 */
public record DispatchLoopConfig(
        String trackerTopic,
        String sourceTopic,
        int maxBatchEntries,
        long maxBatchBytes,
        Duration maxPollTimeout,
        Duration drainSlice,
        Duration idleCursorInterval,
        Duration fetchTimeout,
        Duration penaltyBackoff,
        Duration penaltyBackoffMax,
        UnfetchablePayloadPolicy onUnfetchablePayload,
        UnrelayablePolicy onUnrelayable,
        long maxPendingPerPartition,
        long maxPendingTotal,
        int commitRetryLimit,
        int parkThreshold,
        Duration retryBackoffInitial,
        Duration retryBackoffMax) {

    /** Default poll-timeout ceiling (§6 pseudocode's 30 s clamp). */
    public static final Duration DEFAULT_MAX_POLL_TIMEOUT = Duration.ofSeconds(30);

    /** Default consecutive-failure count that flips the loop into park-and-degrade. */
    public static final int DEFAULT_PARK_THRESHOLD = 5;

    /** Default initial retry backoff. */
    public static final Duration DEFAULT_RETRY_BACKOFF_INITIAL = Duration.ofMillis(100);

    /** Default retry backoff cap (§3.8: capped backoff while parked). */
    public static final Duration DEFAULT_RETRY_BACKOFF_MAX = Duration.ofSeconds(10);

    /** Validates the loop's operating invariants. */
    public DispatchLoopConfig {
        Objects.requireNonNull(trackerTopic, "trackerTopic");
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        Objects.requireNonNull(maxPollTimeout, "maxPollTimeout");
        Objects.requireNonNull(drainSlice, "drainSlice");
        Objects.requireNonNull(idleCursorInterval, "idleCursorInterval");
        Objects.requireNonNull(fetchTimeout, "fetchTimeout");
        Objects.requireNonNull(penaltyBackoff, "penaltyBackoff");
        Objects.requireNonNull(penaltyBackoffMax, "penaltyBackoffMax");
        Objects.requireNonNull(onUnfetchablePayload, "onUnfetchablePayload");
        Objects.requireNonNull(onUnrelayable, "onUnrelayable");
        Objects.requireNonNull(retryBackoffInitial, "retryBackoffInitial");
        Objects.requireNonNull(retryBackoffMax, "retryBackoffMax");
        if (trackerTopic.isBlank()) {
            throw new IllegalArgumentException("trackerTopic must be configured");
        }
        if (sourceTopic.isBlank()) {
            throw new IllegalArgumentException("sourceTopic must be configured");
        }
        if (maxBatchEntries < 1) {
            throw new IllegalArgumentException("maxBatchEntries must be at least 1: " + maxBatchEntries);
        }
        if (maxBatchBytes < 1) {
            throw new IllegalArgumentException("maxBatchBytes must be at least 1: " + maxBatchBytes);
        }
        requirePositive(maxPollTimeout, "maxPollTimeout");
        requirePositive(drainSlice, "drainSlice");
        requirePositive(idleCursorInterval, "idleCursorInterval");
        requirePositive(fetchTimeout, "fetchTimeout");
        requirePositive(penaltyBackoff, "penaltyBackoff");
        requirePositive(retryBackoffInitial, "retryBackoffInitial");
        if (penaltyBackoffMax.compareTo(penaltyBackoff) < 0) {
            throw new IllegalArgumentException(
                    "penaltyBackoffMax " + penaltyBackoffMax + " must be >= penaltyBackoff " + penaltyBackoff);
        }
        if (maxPendingPerPartition < 1) {
            throw new IllegalArgumentException("maxPendingPerPartition must be at least 1: " + maxPendingPerPartition);
        }
        if (commitRetryLimit < 1) {
            throw new IllegalArgumentException("commitRetryLimit must be at least 1: " + commitRetryLimit);
        }
        if (parkThreshold < 1) {
            throw new IllegalArgumentException("parkThreshold must be at least 1: " + parkThreshold);
        }
        if (retryBackoffMax.compareTo(retryBackoffInitial) < 0) {
            throw new IllegalArgumentException(
                    "retryBackoffMax " + retryBackoffMax + " must be >= retryBackoffInitial " + retryBackoffInitial);
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }

    /**
     * Derives the loop settings from a validated {@link CesiumConfig}. The global pending cap's
     * {@code AUTO} sentinel is resolved against {@code maxHeapBytes} (≈ 25% of the heap ÷ the
     * per-entry budget, §5.3/R18).
     *
     * @param config the frozen configuration
     * @param maxHeapBytes the JVM max heap ({@code Runtime.getRuntime().maxMemory()}) the AUTO
     *     pending cap derives from
     */
    public static DispatchLoopConfig from(CesiumConfig config, long maxHeapBytes) {
        DispatchConfig dispatch = config.dispatch();
        return new DispatchLoopConfig(
                config.route().tracker().resolvedTopic(config.applicationId()),
                config.route().source().topic(),
                dispatch.batch().maxEntries(),
                dispatch.batch().maxBytes(),
                DEFAULT_MAX_POLL_TIMEOUT,
                dispatch.drain().maxSlice(),
                dispatch.idleCursorInterval(),
                dispatch.fetch().timeout(),
                dispatch.fetch().penalty().backoff(),
                dispatch.fetch().penalty().backoffMax(),
                dispatch.onUnfetchablePayload(),
                config.route().relay().onUnrelayable(),
                dispatch.maxPendingPerPartition(),
                dispatch.resolveMaxPendingTotal(maxHeapBytes),
                config.kafka().transactions().commitRetry(),
                DEFAULT_PARK_THRESHOLD,
                DEFAULT_RETRY_BACKOFF_INITIAL,
                DEFAULT_RETRY_BACKOFF_MAX);
    }
}
