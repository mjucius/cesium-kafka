package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Soak — <strong>sustained mixed delayed traffic with an exactly-once + not-before-time invariant
 * checker</strong> (design §11.3-12 / §11.4 soak lane). Drives a few hundred thousand heterogeneously
 * delayed entries through the full {@link EngineHarness} (ingest + dispatch + re-fetch) over a few
 * minutes and asserts the two soak invariants on every delivered record:
 *
 * <ol>
 *   <li><strong>exactly once</strong> — each delayed input appears on the destination exactly once
 *       (read_committed, indexed by key);
 *   <li><strong>never early</strong> — its dispatch instant (relay timestamp = DISPATCH, D8) is never
 *       before its requested delivery time minus a small clock-skew tolerance.
 * </ol>
 *
 * <p><strong>Scale.</strong> This is the <em>scaled-down</em> soak: it proves the harness and the
 * invariant checker work end to end. The full design soak — <b>100 M pending, 12 GB heap, ZGC
 * generational, 24 h, GC pause p99 &lt; 5 ms, no OOM, stable accuracy (§11.4)</b> — is a dedicated-
 * environment lane, not runnable on a CI box or this dev box. Run it manually on a sized host with:
 *
 * <pre>{@code
 *   # dedicated host: 16+ GB RAM, fast local disk for the broker, JDK 21.
 *   # Heap is sized via -Dsoak.maxHeap / -Dsoak.minHeap (the soakPerf task emits them as -Xmx/-Xms
 *   # on the forked worker, which WIN over JAVA_TOOL_OPTIONS); JAVA_TOOL_OPTIONS carries only the
 *   # non-conflicting GC / pre-touch / JFR flags. Do NOT put -Xmx/-Xms in JAVA_TOOL_OPTIONS here:
 *   # the worker's command-line -Xmx (default 1536m) would clobber it and the JVM would fail to start.
 *   JAVA_TOOL_OPTIONS="-XX:+UseZGC -XX:+ZGenerational \
 *       -XX:+AlwaysPreTouch -XX:StartFlightRecording=filename=soak.jfr,dumponexit=true" \
 *   ./gradlew :cesium-kafka-it:soakPerf \
 *       -Dsoak.maxHeap=12g -Dsoak.minHeap=12g \
 *       -Dsoak.total=100000000 -Dsoak.windowMs=86400000 -Dsoak.partitions=64 \
 *       -Dsoak.timeoutMin=1560
 * }</pre>
 *
 * <p>{@code @Tag("soak")}: excluded from every default and {@code -PincludeNightly} lane; runs only
 * through the dedicated {@code soakPerf} Gradle task (see {@code cesium-kafka-it/build.gradle.kts}).
 */
@Tag("soak")
class SoakPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(SoakPerfIT.class);

    /** Scaled-down default: a few hundred k entries (override {@code -Dsoak.total} for the full lane). */
    private static final int TOTAL = Integer.getInteger("soak.total", 200_000);

    /** Spread of due times so the dispatcher drains a steady, heterogeneous stream (default ~2 min). */
    private static final int WINDOW_MS = Integer.getInteger("soak.windowMs", 120_000);

    private static final int PARTITIONS = Integer.getInteger("soak.partitions", 4);

    /** Minimum lead before the earliest entry is due — every entry is pending before its due time. */
    private static final int MIN_LEAD_MS = 2_000;

    /** Clock-skew tolerance for the not-before-time invariant (§11.3-12 "minus skew tolerance"). */
    private static final long SKEW_TOLERANCE_MS = 150;

    private static final int TIMEOUT_MIN = Integer.getInteger("soak.timeoutMin", 15);

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void scaledSoakHoldsExactlyOnceAndNeverEarly() {
        String source = createTopic("soak-src", PARTITIONS);
        String destination = createTopic("soak-dst", PARTITIONS);
        String dlq = createTopic("soak-dlq", 1);

        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .delayMax(Duration.ofMillis(MIN_LEAD_MS + WINDOW_MS).plusMinutes(5))
                .build();
        harness.start();

        // Produce TOTAL entries with heterogeneous due times spread across the window; record each
        // requested instant so the not-before-time invariant can be checked per record.
        long[] deliverAt = new long[TOTAL];
        Random random = new Random(7);
        long base = System.currentTimeMillis() + MIN_LEAD_MS;
        long produceStartNanos = System.nanoTime();
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            byte[] value = "soak".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < TOTAL; i++) {
                long when = base + random.nextInt(WINDOW_MS);
                deliverAt[i] = when;
                var unused = producer.send(new ProducerRecord<>(
                        source,
                        i % PARTITIONS,
                        null,
                        ("s-" + i).getBytes(StandardCharsets.UTF_8),
                        value,
                        List.of(new RecordHeader(
                                CesiumHeaders.DELIVER_AT, Long.toString(when).getBytes(StandardCharsets.UTF_8)))));
            }
            producer.flush();
        }
        double produceSeconds = (System.nanoTime() - produceStartNanos) / 1e9;

        // Drain the whole stream; read_committed gives exactly-once already, the invariant loop re-proves it.
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, TOTAL, Duration.ofMinutes(TIMEOUT_MIN), RETRY_GRACE);

        // Invariant 1: exactly once — byKey throws on any duplicate key, and the size must equal TOTAL.
        var byKey = byKey(delivered);
        assertTrue(byKey.size() == TOTAL, "exactly-once: expected " + TOTAL + " distinct keys, saw " + byKey.size());

        // Invariant 2: never early — dispatch instant ≥ requested time − skew tolerance, for every record.
        long worstEarlyMs = 0;
        long maxLateMs = 0;
        int earlyViolations = 0;
        for (ConsumerRecord<byte[], byte[]> record : delivered) {
            int i = Integer.parseInt(utf8(record.key()).substring("s-".length()));
            long earlyBy = deliverAt[i] - record.timestamp(); // positive ⇒ delivered BEFORE requested
            if (earlyBy > SKEW_TOLERANCE_MS) {
                earlyViolations++;
                worstEarlyMs = Math.max(worstEarlyMs, earlyBy);
            }
            maxLateMs = Math.max(maxLateMs, record.timestamp() - deliverAt[i]);
        }

        log.info(
                "SOAK (scaled): total={} partitions={} window={}ms | produced in {}s; exactly-once OK ({} keys); "
                        + "not-before-time: {} violations (worst early {}ms, tol {}ms); maxLate={}ms",
                TOTAL,
                PARTITIONS,
                WINDOW_MS,
                String.format("%.1f", produceSeconds),
                byKey.size(),
                earlyViolations,
                worstEarlyMs,
                SKEW_TOLERANCE_MS,
                maxLateMs);
        System.out.printf(
                "%n[SOAK scaled] total=%d: exactly-once OK (%d keys); never-early violations=%d (worst early %dms, "
                        + "tol %dms); maxLate=%dms. Full 100M/24h/ZGC lane: see SoakPerfIT javadoc.%n",
                TOTAL, byKey.size(), earlyViolations, worstEarlyMs, SKEW_TOLERANCE_MS, maxLateMs);

        if (earlyViolations > 0) {
            fail("not-before-time invariant violated for " + earlyViolations + " record(s); worst early by "
                    + worstEarlyMs + "ms (skew tolerance " + SKEW_TOLERANCE_MS + "ms)");
        }
        harness.stop();
    }
}
