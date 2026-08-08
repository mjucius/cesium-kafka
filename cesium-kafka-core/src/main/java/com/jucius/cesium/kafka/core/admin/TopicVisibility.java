package com.jucius.cesium.kafka.core.admin;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * A bounded wait for a topic whose existence is <em>already proven</em> to become visible in the
 * metadata a broker serves.
 *
 * <p>{@code CreateTopics} is answered by the KRaft <em>controller</em> once the record is committed
 * to the metadata log; {@code DescribeTopics} / {@code DescribeConfigs} are answered by a
 * <em>broker</em> from its asynchronously published metadata image, on a node the AdminClient picks
 * per call. A broker that has not applied the record yet answers {@code
 * UNKNOWN_TOPIC_OR_PARTITION}, which {@link KafkaClusterAdmin#describeTopic} necessarily reports as
 * {@link Optional#empty()} — indistinguishable, at that layer, from a topic that genuinely does not
 * exist. The window is normally sub-millisecond, so a single-shot describe passes; it is lost when
 * the broker's metadata publisher stalls (CPU contention, a GC pause, a disk stall creating the new
 * partitions' log dirs), and startup then fails on a perfectly healthy cluster.
 *
 * <p><strong>The rule this class exists to enforce: retry only where existence is already proven —
 * we just created the topic, or we just described it successfully.</strong> Establishing whether an
 * <em>operator-provisioned</em> topic exists must stay a single shot: retrying there only makes
 * cesium slower at telling an operator the truth about a topic they forgot to create. That is why
 * the wait lives here rather than inside {@link KafkaClusterAdmin#describeTopic}, which every caller
 * — including the fail-fast existence checks — would otherwise inherit.
 *
 * <p>Not thread-safe: one instance belongs to one validation/startup pass on the {@code
 * cesium-admin} thread, and {@link #lastWaitedMillis()} reports the most recent call's wait.
 */
public final class TopicVisibility {

    /**
     * The total wait for proven-existing metadata to arrive. Deliberately a constant and not a
     * config knob: the value only has to exceed normal KRaft propagation, and a misconfigured large
     * value would hang a rollout. Note that {@link KafkaClusterAdmin#DEFAULT_TIMEOUT} applies to
     * each individual call underneath, so the worst-case wall-clock is this budget plus one
     * in-flight call timeout.
     */
    static final Duration BUDGET = Duration.ofSeconds(10);

    private static final long INITIAL_BACKOFF_MS = 50;
    private static final long MAX_BACKOFF_MS = 1_000;

    /**
     * A wait longer than this is reported as a warning rather than an informational finding: a
     * cluster whose metadata propagation has degraded from sub-millisecond to seconds is something
     * an operator must hear about, even though cesium started successfully.
     */
    static final long WARN_THRESHOLD_MS = 1_000;

    /** How the poll loop waits between attempts; the test seam that keeps unit tests instant. */
    @FunctionalInterface
    public interface Waiter {
        /** Waits approximately {@code millis}. */
        void awaitMillis(long millis) throws InterruptedException;
    }

    private final ClusterAdmin admin;
    private final Clock clock;
    private final Waiter waiter;

    private long lastWaitedMillis;

    /** Production wiring: a real clock and {@link Thread#sleep(long)}. */
    public TopicVisibility(ClusterAdmin admin) {
        this(admin, Clock.systemUTC(), Thread::sleep);
    }

    /** Test seam: a virtual clock and a waiter that records instead of sleeping. */
    TopicVisibility(ClusterAdmin admin, Clock clock, Waiter waiter) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
    }

    /**
     * Waits for a topic this process just created to become describable with at least {@code
     * expectedPartitions} partitions.
     *
     * <p>The partition-count condition is not cosmetic: a {@code DescribeTopicPartitions} response
     * is cursor-paginated and reassembled client-side, so a partially propagated topic can be
     * described with fewer partitions than it has. Accepting that would feed the §2.1 parity check
     * a short count and tell the operator to grow their topics.
     *
     * @return the topic's facts, or empty when the budget expired without it becoming visible
     */
    Optional<TopicFacts> awaitCreated(String topic, int expectedPartitions) {
        return poll(topic, facts -> facts.partitionCount() >= expectedPartitions);
    }

    /**
     * Waits for a topic proven to exist earlier in this startup (it was described successfully) to
     * be visible again — the metadata may since have been served by a different, laggier node.
     *
     * @return the topic's facts, or empty when the budget expired without it becoming visible
     */
    public Optional<TopicFacts> awaitVisible(String topic) {
        return poll(topic, facts -> true);
    }

    /**
     * Reads a topic's effective configs, waiting out an {@code UnknownTopicOrPartitionException}.
     *
     * <p>Every caller runs after a successful {@code describeTopic}, so an unknown-topic error here
     * is definitionally propagation lag — possibly on a different node, since {@code
     * describeConfigs} for a TOPIC resource picks its node independently of {@code describeTopics}.
     * Any other failure (authorization, transport, timeout) propagates on the first occurrence.
     */
    Map<String, String> awaitTopicConfigs(String topic) {
        long deadlineMs = deadlineMillis();
        long backoffMs = INITIAL_BACKOFF_MS;
        ClusterAdminException lastLag;
        while (true) {
            try {
                return admin.topicConfigs(topic);
            } catch (ClusterAdminException e) {
                if (!e.hasCause(UnknownTopicOrPartitionException.class)) {
                    throw e;
                }
                lastLag = e;
            }
            long waitMs = nextWaitMillis(deadlineMs, backoffMs);
            if (waitMs < 0) {
                throw lastLag;
            }
            if (!sleep(waitMs)) {
                throw lastLag;
            }
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    /**
     * How long the most recent {@code await*} call spent waiting, in milliseconds; {@code 0} when
     * its first attempt succeeded. Callers surface a non-zero value so a degrading cluster cannot
     * be absorbed in silence.
     */
    public long lastWaitedMillis() {
        return lastWaitedMillis;
    }

    // ------------------------------------------------------------------ internals

    /** Describes until {@code visible} accepts the result, the budget expires, or we are interrupted. */
    private Optional<TopicFacts> poll(String topic, TopicPredicate visible) {
        long deadlineMs = deadlineMillis();
        long backoffMs = INITIAL_BACKOFF_MS;
        while (true) {
            Optional<TopicFacts> facts = admin.describeTopic(topic);
            if (facts.isPresent() && visible.test(facts.get())) {
                return facts;
            }
            long waitMs = nextWaitMillis(deadlineMs, backoffMs);
            if (waitMs < 0 || !sleep(waitMs)) {
                // Budget expired, or a shutdown interrupted us: report what we last saw. A partially
                // propagated description is deliberately not returned as if it were complete.
                return facts.filter(visible::test);
            }
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    /** Resets the wait accounting and returns this call's deadline. */
    private long deadlineMillis() {
        lastWaitedMillis = 0;
        return clock.millis() + BUDGET.toMillis();
    }

    /**
     * The next wait, clamped so it never overshoots the deadline, or {@code -1} when the budget is
     * exhausted.
     */
    private long nextWaitMillis(long deadlineMs, long backoffMs) {
        long remainingMs = deadlineMs - clock.millis();
        return remainingMs <= 0 ? -1 : Math.min(backoffMs, remainingMs);
    }

    /** Waits and accounts for it; false when interrupted (the flag is restored and the caller bails). */
    private boolean sleep(long millis) {
        try {
            waiter.awaitMillis(millis);
        } catch (InterruptedException e) {
            // A SIGTERM during startup is a shutdown, not a cluster finding: restore the flag and
            // let the caller report the last observation.
            Thread.currentThread().interrupt();
            return false;
        }
        lastWaitedMillis += millis;
        return true;
    }

    /** {@link java.util.function.Predicate} over {@link TopicFacts}, named for readability. */
    @FunctionalInterface
    private interface TopicPredicate {
        boolean test(TopicFacts facts);
    }
}
