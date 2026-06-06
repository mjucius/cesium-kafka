package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.store.tracker.KafkaTrackerStore;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Macro perf — <strong>simultaneous-due burst drain + exactly-once</strong> (design §11.4: a 100 k
 * simultaneously-due burst drains within {@code burst / batch-throughput} ± tolerance). The whole
 * burst is scheduled to come due at one instant; the drain rate is measured from that instant to the
 * last delivery and exactly-once is verified across the whole burst.
 *
 * <p><strong>Membership stability is a sanity check here, NOT a proof of the R2 poll-gap property.</strong>
 * The assertions that {@code cesium_dispatch_rebalances} takes no {@code revoked}/{@code lost}/extra
 * {@code assigned} step and that {@code cesium_dispatch_poll_gap_seconds} stays low are kept as a
 * cheap guard against a catastrophic rebalance storm — but at this scale they <em>cannot fail</em> and
 * so do not discriminate an R2-correct dispatcher from a broken one. On this dev box the 100 k burst
 * drains in well under a second (≈ 0.6 s, measured), whereas the dispatch consumer's
 * {@code max.poll.interval.ms} is the kafka-clients default 300 s: even a single fully-blocking drain
 * with zero interleaved {@code poll()}s would finish ~500× inside that window, so the measured
 * {@code pollGap} is ≈ 0 s either way. Lowering {@code max.poll.interval.ms} would not help — to make
 * a non-interleaving drain trip it, the interval would have to drop below the sub-second drain time,
 * which is not viable for a real consumer. Genuinely discriminating R2 coverage therefore lives where
 * the drain is forced to outlast the interval: <strong>{@link BarrierOrderingI8IT}</strong> evicts a
 * deliberately stalled member at {@code max.poll.interval.ms = 7 s} (with a matching
 * {@code drain.max-slice}), and <strong>RebalanceScaleIT</strong> exercises assignment churn. See
 * docs/performance.md §3 for the reconciled verdict.
 *
 * <p>{@code @Tag("nightly")}.
 */
@Tag("nightly")
class BurstPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(BurstPerfIT.class);

    /** Design §11.4 burst size. Tunable down for a constrained box; the dedicated figure is 100 k. */
    private static final int BURST = Integer.getInteger("perf.burst.total", 100_000);

    private static final int PARTITIONS = 4;
    private static final int VALUE_BYTES = 128;
    private static final Duration DUE_LEAD = Duration.ofSeconds(35);

    /** Backpressure high-water held above the per-partition burst (BURST/PARTITIONS) so intake never pauses. */
    private static final long STAGE_HIGH_WATER = Math.max(80_000L, (long) BURST / PARTITIONS * 2);

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void simultaneousDueBurstDrainsWithStableGroupMembership() {
        String source = createTopic("burst-src", PARTITIONS);
        String destination = createTopic("burst-dst", PARTITIONS);
        String dlq = createTopic("burst-dlq", 1);

        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // Hold the whole simultaneously-due burst resident: raise the §5.3 high-water and store
                // hard cap above the per-partition burst so backpressure never pauses intake mid-stage.
                .maxPendingPerPartition(STAGE_HIGH_WATER)
                .storeProperty(
                        KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY, Long.toString(STAGE_HIGH_WATER + 20_000))
                .build();
        harness.start();

        long dueAt = System.currentTimeMillis() + DUE_LEAD.toMillis();
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            PerfSupport.produce(producer, source, PARTITIONS, BURST, "b-", VALUE_BYTES, PerfSupport.deliverAt(dueAt));
        }

        // Stage the whole burst as pending BEFORE the due instant, so the drain is a true
        // simultaneous-due storm rather than a steady arrival.
        await("all " + BURST + " entries pending before the simultaneous due instant")
                .atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> pendingTotal() >= BURST);

        // Snapshot group-B membership right before the storm (initial assignment has already happened).
        double assignedBefore = rebalances("assigned");
        double revokedBefore = rebalances("revoked");
        double lostBefore = rebalances("lost");

        // Wait out the due instant, then measure the drain from due-time to the last dispatch.
        await("burst due instant reached")
                .atMost(DUE_LEAD.plusSeconds(5))
                .until(() -> System.currentTimeMillis() >= dueAt);
        long drainStart = System.currentTimeMillis();
        await("burst drained — all " + BURST + " dispatched")
                .atMost(Duration.ofMinutes(8))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> EngineMetrics.counterSum(harness.meterRegistry(), "cesium.dispatch.records") >= BURST);
        double drainSeconds = (System.currentTimeMillis() - drainStart) / 1000.0;
        double drainRate = BURST / drainSeconds;

        // Group-B membership stayed alive across the whole drain. NB: a sanity guard against a
        // rebalance storm, NOT a proof of the R2 poll-gap property — at a sub-second drain vs the
        // 300 s default max.poll.interval.ms this cannot fail (see class javadoc; R2 proper is
        // BarrierOrderingI8IT / RebalanceScaleIT).
        double assignedDelta = rebalances("assigned") - assignedBefore;
        double pollGap = EngineMetrics.gauge(harness.meterRegistry(), "cesium.dispatch.poll.gap.seconds");

        log.info(
                "MEASURED burst: {} entries drained in {}s ({} rec/s); membership Δ assigned={} revoked={} lost={}; pollGap={}s",
                BURST,
                String.format("%.2f", drainSeconds),
                String.format("%.0f", drainRate),
                assignedDelta,
                rebalances("revoked") - revokedBefore,
                rebalances("lost") - lostBefore,
                Double.isNaN(pollGap) ? "n/a" : String.format("%.2f", pollGap));
        System.out.printf(
                "%n[PERF] burst %d due-simultaneously: drained in %.2fs = %.0f rec/s; no rebalance during drain "
                        + "(Δassigned=%.0f Δrevoked=%.0f Δlost=%.0f), pollGap=%s%n",
                BURST,
                drainSeconds,
                drainRate,
                assignedDelta,
                rebalances("revoked") - revokedBefore,
                rebalances("lost") - lostBefore,
                Double.isNaN(pollGap) ? "n/a" : String.format("%.2fs", pollGap));

        // Sanity guards (NOT R2 discrimination — see class javadoc): the burst drain must not coincide
        // with a rebalance storm. R2's poll-gap property is proven by BarrierOrderingI8IT/RebalanceScaleIT.
        assertEquals(revokedBefore, rebalances("revoked"), "group B must not revoke during the drain (sanity)");
        assertEquals(lostBefore, rebalances("lost"), "no member may be fenced/lost during the drain (sanity)");
        assertEquals(
                assignedBefore,
                rebalances("assigned"),
                "the assignment generation must stay stable during the drain (no rebalance; sanity)");
        assertTrue(
                Double.isNaN(pollGap) || pollGap < 60.0,
                () -> "group-B poll gap must stay below max.poll.interval.ms during the storm, was " + pollGap + "s");

        // Exactly-once across the whole simultaneous-due burst.
        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, BURST, Duration.ofMinutes(3), RETRY_GRACE);
        byKey(delivered);
        harness.stop();
    }

    private double rebalances(String event) {
        return EngineMetrics.counter(harness.meterRegistry(), "cesium.dispatch.rebalances", "event", event);
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
