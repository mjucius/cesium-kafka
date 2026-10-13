package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sub-second delays are best-effort and never early (design open question 5, revision 6). A trickle
 * of records asks for {@code cesium-delay-ms} 0, 50, 300 and 900, one of each per round. A
 * read_committed consumer polls the destination while they are produced and stamps the moment it
 * first sees each relay. Lateness is that moment minus the requested time (source record timestamp +
 * delay), so it covers the whole pipeline: ingest commit, tracker consume, payload fetch, the
 * dispatch commit and the consumer's fetch. {@code cesium_dispatch_lag_seconds} measures only up to
 * the start of the dispatch batch.
 *
 * <p>The test asserts exactly-once, and never-early with zero tolerance: the engine, the producer and
 * the consumer all read this JVM's clock, so there is no skew to allow for. It logs p50/p99/max
 * lateness per delay in one line and asserts no upper bound, since that depends on the machine.
 * docs/performance.md §3.4 quotes the numbers. The percentiles cover the steady trickle only. The
 * first {@value #WARMUP_ROUNDS} rounds land in the engine's first second, while it warms up,
 * and the drain rounds keep the source busy until the last measured record is due, because a quiet
 * source adds up to the seek consumer's {@code fetch.max.wait.ms} to each due batch. Both are
 * checked, and the summary reports their worst lateness separately. A record whose requested time has
 * passed when ingest reads it relays at once from ingest, without a {@code cesium-scheduled-for}
 * header; the summary counts those per delay.
 *
 * <p>Untagged: about 25 s, and nothing it asserts depends on timing, so it runs on the PR lane.
 */
class SubSecondDelayIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(SubSecondDelayIT.class);

    private static final long[] DELAYS_MS = {0, 50, 300, 900};

    /** Measured rounds: one record per delay each, enough for a stable p99. */
    private static final int MEASURED_ROUNDS = Integer.getInteger("perf.subsecond.rounds", 200);

    private static final int WARMUP_ROUNDS = 25;

    /** Pause between rounds, so the engine sees a trickle, not a backlog. */
    private static final long ROUND_SPACING_MS = 15;

    /** Drain rounds span more than the longest delay even if sends took no time. */
    private static final int DRAIN_ROUNDS = (int) ((DELAYS_MS[DELAYS_MS.length - 1] + 200) / ROUND_SPACING_MS);

    private static final int ROUNDS = WARMUP_ROUNDS + MEASURED_ROUNDS + DRAIN_ROUNDS;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void subSecondDelaysAreNeverEarly() {
        String source = createTopic("subsec-src", 1);
        String destination = createTopic("subsec-dst", 1);
        String dlq = createTopic("subsec-dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        harness.start();

        int total = DELAYS_MS.length * ROUNDS;
        Map<String, Long> requestedAt = new ConcurrentHashMap<>();
        Map<String, Long> firstSeenAt = new HashMap<>();
        List<ConsumerRecord<byte[], byte[]>> delivered = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = newAssignedConsumer(destination, "read_committed")) {
            consumer.assignment().forEach(consumer::position); // resolve the start position up front
            CompletableFuture<Void> producing = CompletableFuture.runAsync(() -> produceTrickle(source, requestedAt));
            // A plain loop, not awaitility: its 100 ms poll interval would add up to 100 ms to every
            // first-seen stamp, because records reach the consumer only inside poll().
            long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
            while (delivered.size() < total && !producing.isCompletedExceptionally()) {
                assertTrue(System.nanoTime() < deadline, () -> delivered.size() + " of " + total + " relays seen");
                poll(consumer, delivered, firstSeenAt);
            }
            producing.join();
            long graceDeadline = System.nanoTime() + RETRY_GRACE.toNanos();
            while (System.nanoTime() < graceDeadline) {
                poll(consumer, delivered, firstSeenAt);
            }
        }

        DeliveryInvariants.check(delivered, total, requestedAt::get);

        Map<Long, PerfSupport.LatencyDigest> latenessByDelay = new TreeMap<>();
        Map<Long, Integer> relayedAtIngest = new TreeMap<>();
        long warmupMaxMs = 0;
        long drainMaxMs = 0;
        for (ConsumerRecord<byte[], byte[]> record : delivered) {
            String key = utf8(record.key());
            long requested = requestedAt.get(key);
            long seen = firstSeenAt.get(key);
            assertTrue(seen >= requested, () -> key + " became visible " + (requested - seen) + " ms early");
            int dash = key.indexOf('-');
            long delay = Long.parseLong(key.substring(1, dash));
            int round = Integer.parseInt(key.substring(dash + 1));
            if (round < WARMUP_ROUNDS) {
                warmupMaxMs = Math.max(warmupMaxMs, seen - requested);
            } else if (round >= WARMUP_ROUNDS + MEASURED_ROUNDS) {
                drainMaxMs = Math.max(drainMaxMs, seen - requested);
            } else {
                latenessByDelay
                        .computeIfAbsent(delay, d -> new PerfSupport.LatencyDigest())
                        .add(seen, requested);
                if (headerValue(record.headers(), CesiumHeaders.SCHEDULED_FOR) == null) {
                    relayedAtIngest.merge(delay, 1, Integer::sum);
                }
            }
        }

        StringBuilder summary = new StringBuilder();
        latenessByDelay.forEach((delay, digest) -> summary.append(String.format(
                " | delay %dms: p50=%dms p99=%dms max=%dms, relayed at ingest %d/%d",
                delay,
                digest.percentileMillis(0.50),
                digest.percentileMillis(0.99),
                digest.maxMillis(),
                relayedAtIngest.getOrDefault(delay, 0),
                digest.size())));
        log.info(
                "MEASURED sub-second lateness (first read_committed sight minus requested time){}"
                        + " | not in the percentiles: warm-up max={}ms, drain max={}ms",
                summary,
                warmupMaxMs,
                drainMaxMs);
        harness.stop();
    }

    /** Produces {@link #ROUNDS} rounds of one record per delay, recording each requested time. */
    private static void produceTrickle(String source, Map<String, Long> requestedAt) {
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int round = 0; round < ROUNDS; round++) {
                for (long delay : DELAYS_MS) {
                    long timestamp = System.currentTimeMillis();
                    String key = "d" + delay + "-" + round;
                    requestedAt.put(key, timestamp + delay); // before the send: a 0 ms relay can beat it back
                    produce(producer, source, 0, timestamp, key, "v", h(CesiumHeaders.DELAY_MS, Long.toString(delay)));
                }
                Thread.sleep(ROUND_SPACING_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted producing the trickle", e);
        }
    }

    /** One short poll; stamps the first time each key is seen. */
    private static void poll(
            Consumer<byte[], byte[]> consumer,
            List<ConsumerRecord<byte[], byte[]>> delivered,
            Map<String, Long> firstSeenAt) {
        var records = consumer.poll(Duration.ofMillis(10));
        long now = System.currentTimeMillis();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            firstSeenAt.putIfAbsent(utf8(record.key()), now);
            delivered.add(record);
        }
    }
}
