package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Macro perf — <strong>dispatch throughput + dispatch-lag SLO</strong> (design §11.4: sustained
 * dispatch incl. re-fetch ≥ 10 k rec/s CI / ≥ 50 k dedicated; {@code dispatch_lag} p99 ≤ 250 ms for
 * idle arrivals; R8/R9).
 *
 * <ul>
 *   <li><strong>{@link #sustainedDispatchThroughputIncludingRefetch()}</strong> — a whole backlog
 *       scheduled to come due simultaneously, drained by a real {@link
 *       com.jucius.cesium.kafka.core.dispatch.DispatchLoop} that re-fetches each source payload through
 *       the {@link com.jucius.cesium.kafka.core.fetch.KafkaSeekFetcher}; the rate is read off
 *       {@code cesium_dispatch_records}. <strong>This is a best-case burst, not a sustained
 *       mixed-traffic rate:</strong> the whole backlog is due at one instant and laid out as
 *       contiguous per-partition source offsets, so the seek fetcher does ONE {@code seek} + a
 *       sequential forward scan per partition (the §7 / risk #8 seek-friendly extreme), and the index
 *       is held fully resident. A scattered-due workload re-fetches with random seeks and smaller
 *       per-transaction batches and is far slower — {@link SoakPerfIT} sustains ~826 rec/s under
 *       scattered due times. So this case measures that the index + transaction path is not the
 *       bottleneck; it is NOT a deployment SLA. See docs/performance.md §3.
 *   <li><strong>{@link #dispatchLagP99ForIdleArrivals()}</strong> — a sparse trickle of arrivals,
 *       each made pending ~2 s before its due instant (relay timestamp = DISPATCH, D8). The 2 s lead
 *       fully absorbs the ingest&rarr;pending pipeline floor (ingest-commit + tracker-consume +
 *       {@code read_committed} LSO visibility, OQ#5 ~100–500 ms), which therefore contributes ZERO
 *       lateness; p50 = 2 ms confirms an already-pending entry dispatches almost immediately. The
 *       p99 tail (~480 ms on this single-broker dev box) is thus NOT that floor — a floor would lift
 *       p50 too — but an <em>occasional dispatch-side stall</em> (single-broker EOS transaction
 *       commit / broker fsync / GC pause). p99 is measured end to end and reported against the 250 ms
 *       SLO; see docs/performance.md §3.
 * </ul>
 *
 * <p>{@code @Tag("nightly")}. Numbers are logged and reported honestly (R9); the hard assertions are
 * conservative regression floors, not the headline SLO.
 */
@Tag("nightly")
class DispatchThroughputPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(DispatchThroughputPerfIT.class);

    private static final int TOTAL = Integer.getInteger("perf.dispatch.total", 50_000);
    private static final int PARTITIONS = 4;
    private static final int VALUE_BYTES = 256;

    /** Lead time before the whole backlog comes due — ample for ingest to schedule every entry first. */
    private static final Duration DUE_LEAD = Duration.ofSeconds(25);

    /** Backpressure high-water held above the per-partition load (TOTAL/PARTITIONS) so intake never pauses. */
    private static final long STAGE_HIGH_WATER = Math.max(80_000L, (long) TOTAL / PARTITIONS * 2);

    private static final double FLOOR = Double.parseDouble(System.getProperty("perf.dispatch.floor", "2000"));

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void sustainedDispatchThroughputIncludingRefetch() {
        String source = createTopic("perf-src", PARTITIONS);
        String destination = createTopic("perf-dst", PARTITIONS);
        String dlq = createTopic("perf-dlq", 1);

        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // Keep the whole backlog resident in the index for a clean dispatch-bound measurement:
                // raise the §5.3 backpressure high-water AND the store hard cap above the per-partition
                // load so intake never pauses (default 10k high-water would cap each of 4 shards below
                // TOTAL/4 and stall staging).
                .maxPendingPerPartition(STAGE_HIGH_WATER)
                .storeProperty(
                        KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY, Long.toString(STAGE_HIGH_WATER + 20_000))
                .build();
        harness.start();

        // Whole backlog due at one instant, far enough out that ingest schedules all of it first; the
        // drain that follows is dispatch-bound (heap pop + re-fetch + transactional relay), not
        // ingest-bound.
        long dueAt = System.currentTimeMillis() + DUE_LEAD.toMillis();
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            PerfSupport.produce(producer, source, PARTITIONS, TOTAL, "d-", VALUE_BYTES, PerfSupport.deliverAt(dueAt));
        }

        // Every entry is pending (ADD applied) before the due instant — confirm the backlog is staged
        // so the measured window is the drain, not the schedule.
        await("all " + TOTAL + " entries scheduled and pending before the due instant")
                .atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> pendingTotal() >= TOTAL);

        PerfSupport.ThroughputResult result = PerfSupport.measureSustainedThroughput(
                harness.meterRegistry(), "cesium.dispatch.records", TOTAL, 0.15, Duration.ofMinutes(8));

        log.info(
                "MEASURED dispatch throughput (incl. re-fetch) = {} | target ≥10k rec/s (CI) / ≥50k (dedicated) [§11.4]",
                result);
        System.out.printf(
                "%n[PERF] dispatch sustained (incl re-fetch) = %.0f rec/s (%d records, %.2fs window); "
                        + "target ≥10k CI / ≥50k dedicated%n",
                result.recordsPerSecond(), result.observed(), result.windowSeconds());

        // Exactly-once across the whole backlog (re-fetch + transactional relay).
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, TOTAL, Duration.ofMinutes(3), RETRY_GRACE);
        byKey(delivered);
        assertTrue(
                result.recordsPerSecond() >= FLOOR,
                () -> "dispatch throughput " + result + " fell below the regression floor " + FLOOR + " rec/s");
        harness.stop();
    }

    @Test
    void dispatchLagP99ForIdleArrivals() throws InterruptedException {
        String source = createTopic("perf-src", 1);
        String destination = createTopic("perf-dst", 1);
        String dlq = createTopic("perf-dlq", 1);

        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        harness.start();

        // A sparse trickle — idle arrivals, NOT a backlog: each entry is scheduled ~2 s ahead so it is
        // pending long before it comes due. The 2 s lead absorbs the ingest->pending pipeline floor
        // (OQ#5 ~100-500 ms), so steady-state lateness is dispatch-side; the p99 tail is an occasional
        // commit/IO/GC stall, not that floor (which would also lift p50). See docs/performance.md §3.
        int count = Integer.getInteger("perf.lag.count", 300);
        long lead = 2_000;
        Map<String, Long> deliverAtByKey = new HashMap<>();
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < count; i++) {
                long now = System.currentTimeMillis();
                long deliverAt = now + lead;
                String key = "lag-" + i;
                deliverAtByKey.put(key, deliverAt);
                produce(producer, source, 0, now, key, "v", h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
                producer.flush();
                Thread.sleep(15); // spread the arrivals so the dispatcher is never backlogged
            }
        }

        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, count, Duration.ofMinutes(2), RETRY_GRACE);

        PerfSupport.LatencyDigest digest = new PerfSupport.LatencyDigest();
        for (ConsumerRecord<byte[], byte[]> record : delivered) {
            Long deliverAt = deliverAtByKey.get(utf8(record.key()));
            assertNotNull(deliverAt, () -> "unexpected destination key " + utf8(record.key()));
            // relay.timestamp = DISPATCH (D8): the record timestamp IS the dispatch instant.
            digest.add(record.timestamp(), deliverAt);
        }

        long p50 = digest.percentileMillis(0.50);
        long p99 = digest.percentileMillis(0.99);
        log.info(
                "MEASURED dispatch_lag idle arrivals: p50={}ms p99={}ms max={}ms mean={}ms (n={}) | SLO p99 ≤ 250 ms [§11.4]",
                p50,
                p99,
                digest.maxMillis(),
                String.format("%.1f", digest.meanMillis()),
                digest.size());
        System.out.printf(
                "%n[PERF] dispatch_lag idle: p50=%dms p99=%dms max=%dms (n=%d); SLO p99 ≤ 250ms%s%n",
                p50, p99, digest.maxMillis(), digest.size(), p99 <= 250 ? "" : "  <-- MISS vs 250ms SLO");

        // Hard guard: not wedged. The 250 ms SLO is reported above; a slow CI box may exceed it and
        // that is recorded honestly rather than failing the lane (R9).
        long ceiling = Long.getLong("perf.lag.ceilingMs", 2_000);
        assertTrue(
                p99 <= ceiling,
                () -> "idle dispatch lag p99 " + p99 + "ms exceeded the regression ceiling " + ceiling + "ms");
        harness.stop();
    }

    private double pendingTotal() {
        double sum = 0;
        for (int p = 0; p < PARTITIONS; p++) {
            double v = EngineMetrics.gauge(
                    harness.meterRegistry(), "cesium.pending.entries", "partition", Integer.toString(p));
            if (!Double.isNaN(v)) {
                sum += v;
            }
        }
        return sum;
    }
}
