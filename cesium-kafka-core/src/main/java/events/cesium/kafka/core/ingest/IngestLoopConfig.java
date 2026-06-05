package events.cesium.kafka.core.ingest;

import events.cesium.kafka.core.config.CesiumConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * The slice of configuration the {@link IngestLoop} consumes, flattened from {@link CesiumConfig}
 * so the loop has no reach into unrelated config (and tests can construct it directly).
 *
 * @param sourceTopic the topic group A subscribes to
 * @param trackerTopic the resolved tracker topic ADDs are produced to (same partition number as
 *     the source record, design §2.1)
 * @param trackerPartitions the tracker topic's partition count as validated at startup (equal to
 *     the source's, R-7) — the I-7 fail-fast baseline: producing an ADD for a source partition
 *     {@code >= trackerPartitions} fails fast with the grow-tracker-first runbook instead of
 *     parking on a generic metadata timeout (design §3.9 I-7)
 * @param pollTimeout the steady-state {@code consumer.poll} timeout — the loop's sleep, wakeup
 *     channel, and liveness heartbeat in one (design §6); shutdown interrupts it via
 *     {@code wakeup()}
 * @param commitRetryLimit bounded retries of a timed-out {@code commitTransaction} before the
 *     outcome is declared in-doubt and the I-4 recovery procedure runs
 *     ({@code kafka.transactions.commit-retry}, design §3.8)
 * @param parkThreshold consecutive batch failures after which the loop is <em>parked</em>:
 *     degraded gauge raised and alert-level logging — retries continue with capped backoff either
 *     way; the threshold only moves the line between "bounded retries" and "park-and-degrade"
 *     (design §3.8 retry-exhaustion, R15)
 * @param retryBackoffInitial first retry backoff; doubles per consecutive failure
 * @param retryBackoffMax backoff cap — the loop never waits longer than this between retries, so
 *     recovery after a long broker outage is prompt
 */
public record IngestLoopConfig(
        String sourceTopic,
        String trackerTopic,
        int trackerPartitions,
        Duration pollTimeout,
        int commitRetryLimit,
        int parkThreshold,
        Duration retryBackoffInitial,
        Duration retryBackoffMax) {

    /** Default steady-state poll timeout. */
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);

    /** Default consecutive-failure count that flips the loop into park-and-degrade. */
    public static final int DEFAULT_PARK_THRESHOLD = 5;

    /** Default initial retry backoff. */
    public static final Duration DEFAULT_RETRY_BACKOFF_INITIAL = Duration.ofMillis(100);

    /** Default retry backoff cap (§3.8: capped backoff while parked). */
    public static final Duration DEFAULT_RETRY_BACKOFF_MAX = Duration.ofSeconds(10);

    /** Validates the loop's operating invariants. */
    public IngestLoopConfig {
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        Objects.requireNonNull(trackerTopic, "trackerTopic");
        Objects.requireNonNull(pollTimeout, "pollTimeout");
        Objects.requireNonNull(retryBackoffInitial, "retryBackoffInitial");
        Objects.requireNonNull(retryBackoffMax, "retryBackoffMax");
        if (sourceTopic.isBlank()) {
            throw new IllegalArgumentException("sourceTopic must be configured");
        }
        if (trackerTopic.isBlank()) {
            throw new IllegalArgumentException("trackerTopic must be configured");
        }
        if (trackerPartitions < 1) {
            throw new IllegalArgumentException("trackerPartitions must be at least 1: " + trackerPartitions);
        }
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive: " + pollTimeout);
        }
        if (commitRetryLimit < 1) {
            throw new IllegalArgumentException("commitRetryLimit must be at least 1: " + commitRetryLimit);
        }
        if (parkThreshold < 1) {
            throw new IllegalArgumentException("parkThreshold must be at least 1: " + parkThreshold);
        }
        if (retryBackoffInitial.isNegative() || retryBackoffInitial.isZero()) {
            throw new IllegalArgumentException("retryBackoffInitial must be positive: " + retryBackoffInitial);
        }
        if (retryBackoffMax.compareTo(retryBackoffInitial) < 0) {
            throw new IllegalArgumentException(
                    "retryBackoffMax " + retryBackoffMax + " must be >= retryBackoffInitial " + retryBackoffInitial);
        }
    }

    /**
     * Derives the loop settings from a validated {@link CesiumConfig} (§8 defaults elsewhere).
     *
     * @param config the frozen configuration
     * @param trackerPartitions the tracker partition count from startup validation (equal to the
     *     source partition count {@code P} once the R-7 parity check passed)
     */
    public static IngestLoopConfig from(CesiumConfig config, int trackerPartitions) {
        return new IngestLoopConfig(
                config.route().source().topic(),
                config.route().tracker().resolvedTopic(config.applicationId()),
                trackerPartitions,
                DEFAULT_POLL_TIMEOUT,
                config.kafka().transactions().commitRetry(),
                DEFAULT_PARK_THRESHOLD,
                DEFAULT_RETRY_BACKOFF_INITIAL,
                DEFAULT_RETRY_BACKOFF_MAX);
    }
}
