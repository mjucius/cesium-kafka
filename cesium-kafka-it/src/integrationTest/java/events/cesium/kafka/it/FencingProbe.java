package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * EOS observation utilities for the M6 fencing/rebalance scenarios, bound to a bootstrap that stays
 * <em>reachable</em> while faults are injected elsewhere (use {@link ToxiproxySupport#directBootstrap()},
 * never a proxied endpoint — the probe must keep seeing the log while a member is partitioned).
 *
 * <p>The core EOS facts a fencing scenario asserts are differential:
 *
 * <ul>
 *   <li><strong>"instance X's in-flight transaction aborted"</strong> &mdash; an aborted transaction
 *       leaves its records in the log but invisible under {@code read_committed}; so a
 *       {@code read_uncommitted} count strictly greater than the {@code read_committed} count is
 *       positive proof the abort happened (rather than nothing happening). See
 *       {@link #assertAbortsObserved}.
 *   <li><strong>no duplicate delivery</strong> (the EOS violation) &mdash; under {@code read_committed}
 *       every destination/tracker key must appear exactly once. {@link #assertNoDuplicateKeys} and
 *       {@link #assertExactlyOnceByKey} make a duplicate a deterministic failure with the offending
 *       key named.
 * </ul>
 */
final class FencingProbe {

    private final String bootstrap;

    FencingProbe(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    // ------------------------------------------------------------------ counts

    /** Drains {@code topic} under {@code isolation} until two quiet seconds pass; returns the count. */
    int count(String topic, String isolation, Duration overallTimeout) {
        return drain(topic, isolation, overallTimeout).size();
    }

    /** Drains every record visible under {@code isolation} until two quiet seconds pass. */
    List<ConsumerRecord<byte[], byte[]>> drain(String topic, String isolation, Duration overallTimeout) {
        try (Consumer<byte[], byte[]> consumer = ToxiproxySupport.newAssignedConsumer(bootstrap, topic, isolation)) {
            List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
            long quietNanos = Duration.ofSeconds(2).toNanos();
            long deadline = System.nanoTime() + overallTimeout.toNanos();
            long lastNew = System.nanoTime();
            while (System.nanoTime() < deadline && System.nanoTime() - lastNew < quietNanos) {
                var records = consumer.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    records.forEach(out::add);
                    lastNew = System.nanoTime();
                }
            }
            return out;
        }
    }

    /** Records visible under {@code read_committed} (committed only). */
    int committedCount(String topic) {
        return count(topic, "read_committed", Duration.ofSeconds(30));
    }

    /** Records present in the log under {@code read_uncommitted} (committed + aborted). */
    int uncommittedCount(String topic) {
        return count(topic, "read_uncommitted", Duration.ofSeconds(30));
    }

    // ------------------------------------------------------------------ assertions

    /**
     * Asserts an abort is visible on {@code topic}: more records exist in the log
     * ({@code read_uncommitted}) than are committed ({@code read_committed}), i.e. at least one
     * transaction wrote then aborted. Proves a zombie's in-flight work became invisible rather than
     * the scenario simply producing nothing.
     */
    void assertAbortsObserved(String topic) {
        int committed = committedCount(topic);
        int uncommitted = uncommittedCount(topic);
        assertTrue(
                uncommitted > committed,
                () -> "expected aborted records on " + topic + " (read_uncommitted=" + uncommitted
                        + " should exceed read_committed=" + committed + ")");
    }

    /**
     * Asserts no committed key on {@code topic} is delivered more than once (the EOS invariant) by
     * draining the whole committed log until quiet and indexing by key. Use when the exact committed
     * count is not known a priori; prefer {@link #assertExactlyOnceByKey} when it is (it also catches
     * a late duplicate via the grace window).
     */
    void assertNoDuplicateKeys(String topic) {
        byKey(drain(topic, "read_committed", Duration.ofSeconds(20)));
    }

    /**
     * Asserts exactly {@code expected} committed records on {@code topic}, each key once, then keeps
     * polling for {@code grace} to catch a late duplicate from an asynchronous re-dispatch. Returns
     * the committed records indexed by key.
     */
    Map<String, ConsumerRecord<byte[], byte[]>> assertExactlyOnceByKey(
            String topic, int expected, Duration timeout, Duration grace) {
        return byKey(readCommitted(topic, expected, timeout, grace));
    }

    /**
     * The exactly-{@code expected} committed verifier ({@code expected} &ge; 1): reads {@code topic}
     * under {@code read_committed} from the beginning until {@code expected} records are seen (failing
     * if {@code timeout} elapses first), then keeps polling for {@code grace} and asserts no extra
     * record arrived &mdash; sizing {@code grace} to the scenario's worst-case asynchronous
     * re-dispatch latency is what makes a late duplicate a failure rather than a miss.
     */
    List<ConsumerRecord<byte[], byte[]>> readCommitted(String topic, int expected, Duration timeout, Duration grace) {
        try (Consumer<byte[], byte[]> consumer =
                ToxiproxySupport.newAssignedConsumer(bootstrap, topic, "read_committed")) {
            List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
            long deadline = System.nanoTime() + timeout.toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(out::add);
            }
            if (expected > 0 && out.size() < expected) {
                throw new AssertionError(
                        "expected " + expected + " committed record(s) on " + topic + " but saw " + out.size());
            }
            long graceDeadline = System.nanoTime() + grace.toNanos();
            while (System.nanoTime() < graceDeadline) {
                consumer.poll(Duration.ofMillis(100)).forEach(out::add);
            }
            if (expected > 0) {
                assertEquals(
                        expected,
                        out.size(),
                        () -> "read_committed verifier on " + topic + " saw extra records: " + describe(out));
            }
            return out;
        }
    }

    // ------------------------------------------------------------------ helpers

    /** Indexes records by key, asserting every key appears exactly once (names a duplicate). */
    static Map<String, ConsumerRecord<byte[], byte[]>> byKey(List<ConsumerRecord<byte[], byte[]>> records) {
        Map<String, ConsumerRecord<byte[], byte[]>> out = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            String key = record.key() == null ? "<null>" : new String(record.key(), StandardCharsets.UTF_8);
            ConsumerRecord<byte[], byte[]> previous = out.put(key, record);
            if (previous != null) {
                throw new AssertionError("duplicate delivery: key '" + key + "' seen more than once on "
                        + record.topic() + " (EOS violation): " + describe(records));
            }
        }
        return out;
    }

    private static String describe(List<ConsumerRecord<byte[], byte[]>> records) {
        StringBuilder sb = new StringBuilder();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            sb.append("\n  ")
                    .append(record.topic())
                    .append('-')
                    .append(record.partition())
                    .append('@')
                    .append(record.offset())
                    .append(" key=")
                    .append(record.key() == null ? "null" : new String(record.key(), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }
}
