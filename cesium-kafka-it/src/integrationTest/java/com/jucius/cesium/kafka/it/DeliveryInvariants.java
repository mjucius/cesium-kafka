package com.jucius.cesium.kafka.it;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The soak invariant checker (design §11.3-12) shared by {@link SoakPerfIT} and
 * {@link CrashLoopSoakIT}. It checks two things on every delivered record:
 *
 * <ol>
 *   <li><strong>exactly once</strong>: each produced key appears on the destination once, with no
 *       duplicate, missing or unknown key;
 *   <li><strong>never early</strong>: under {@code relay.timestamp: DISPATCH} (D8) the record
 *       timestamp is the dispatch instant, and it is never before the requested delivery time minus
 *       {@link #SKEW_TOLERANCE_MS}.
 * </ol>
 *
 * <p>It references nothing in {@link KafkaIT}, whose static initializer starts the broker, so
 * {@code DeliveryInvariantsTest} runs without Docker.
 */
final class DeliveryInvariants {

    /** Clock-skew tolerance for the not-before-time invariant (§11.3-12 "minus skew tolerance"). */
    static final long SKEW_TOLERANCE_MS = 150;

    private DeliveryInvariants() {}

    /**
     * Asserts both invariants over a {@code read_committed} drain of the destination.
     *
     * @param expected how many distinct keys were produced
     * @param requestedAt the requested delivery instant (epoch ms) for a produced key, or
     *     {@code null} for a key the test never produced
     * @return the worst lateness seen, in ms
     * @throws AssertionError on a duplicate, unknown or missing key, or a record delivered early
     */
    static long check(
            List<ConsumerRecord<byte[], byte[]>> delivered, int expected, Function<String, Long> requestedAt) {
        Set<String> seen = new HashSet<>();
        long maxLateMs = 0;
        long worstEarlyMs = 0;
        int earlyViolations = 0;
        for (ConsumerRecord<byte[], byte[]> record : delivered) {
            String key = record.key() == null ? "<null>" : new String(record.key(), StandardCharsets.UTF_8);
            if (!seen.add(key)) {
                throw new AssertionError("exactly-once: key '" + key + "' delivered more than once");
            }
            Long requested = requestedAt.apply(key);
            if (requested == null) {
                throw new AssertionError("exactly-once: key '" + key + "' was delivered but never produced");
            }
            long earlyBy = requested - record.timestamp(); // positive: delivered BEFORE requested
            if (earlyBy > SKEW_TOLERANCE_MS) {
                earlyViolations++;
                worstEarlyMs = Math.max(worstEarlyMs, earlyBy);
            }
            maxLateMs = Math.max(maxLateMs, -earlyBy);
        }
        if (seen.size() != expected) {
            throw new AssertionError("exactly-once: expected " + expected + " distinct keys, saw " + seen.size());
        }
        if (earlyViolations > 0) {
            throw new AssertionError("not-before-time invariant violated for " + earlyViolations
                    + " record(s); worst early by " + worstEarlyMs + "ms (skew tolerance " + SKEW_TOLERANCE_MS
                    + "ms)");
        }
        return maxLateMs;
    }
}
