package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.fetch.FetchOutcome;
import com.jucius.cesium.kafka.core.fetch.FetchResult;
import com.jucius.cesium.kafka.core.fetch.SeekFetcher;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Penalty-box isolation at the real broker (design §7.3/D22, §11.3-10): one source partition's
 * payload fetch is forced {@code TRANSIENT} (a deterministic decorator over the real seek fetcher —
 * the single broker cannot degrade one partition's leader alone, R21); that partition is
 * penalty-boxed while the healthy partition keeps dispatching on time, and the degraded partition
 * drains exactly once after the fault heals.
 *
 * <p><strong>Park-and-degrade (R15/D-15) is NOT exercised here</strong> — it has no clean real-broker
 * trigger since M2: the one fast, definitively-abortable relay fault (an oversized relay →
 * {@code RecordTooLargeException}) is now an "unrelayable" fault routed to the DLQ (see
 * {@link UnrelayableDlqIT}), and every transient relay failure surfaces only as a delivery-timeout,
 * which the loop classifies as in-doubt (§3.8), not abortable-retry exhaustion. The park-and-degrade
 * mechanism (gauge raise/clear, membership-preserving, no crash-loop) is covered deterministically by
 * {@code DispatchLoopTest.parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess}.
 *
 * <p>Class-{@code @Tag("nightly")}: the penalty-box backoff window makes this a slow fault-injection
 * scenario, pushed off the PR lane to nightly (design §13).
 */
@Tag("nightly")
class DegradationIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    // A former real-broker `dispatchParksAndDegrades...` scenario was removed here. Its only clean
    // trigger — an oversized relay rejected with RecordTooLargeException — is, since M2, a record-level
    // "unrelayable" fault routed to the DLQ instead of parking (covered by UnrelayableDlqIT). No clean
    // real-broker driver for park-and-degrade remains: every *transient* relay failure (e.g.
    // min.insync.replicas not met) reaches the producer only as a delivery-timeout, which the loop
    // classifies as IN-DOUBT (§3.8 drop-and-recover), not abortable-retry exhaustion. Park-and-degrade
    // itself (R15 — gauge raise/clear, membership-preserving, no crash-loop) is covered deterministically
    // by DispatchLoopTest.parkAndDegradeRaisesGaugeAtThresholdAndClearsOnSuccess (with the
    // no-park-on-deterministic-rejection case beside it). A real-broker park test would need a
    // producer-fault-injection harness hook — a future enhancement, not a v1 coverage gap.

    @Test
    void penaltyBoxIsolatesADegradedSourcePartitionFromHealthyOnes() {
        String source = createTopic("src", 2);
        String destination = createTopic("dst", 2);
        String dlq = createTopic("dlq", 1);

        // While set, partition 1's payload fetch is forced TRANSIENT (penalty-boxed); partition 0 is
        // untouched. Cleared to heal partition 1.
        AtomicBoolean partition1Degraded = new AtomicBoolean(true);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .seekFetcherDecorator(delegate -> new PartitionFaultFetcher(delegate, 1, partition1Degraded))
                .build();
        harness.start();

        int perPartition = 3;
        long ts = System.currentTimeMillis();
        long deliverAt = ts + 4_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < perPartition; i++) {
                produce(producer, source, 0, ts, "p0-" + i, "v", h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
                produce(producer, source, 1, ts, "p1-" + i, "v", h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
            }
            producer.flush();
        }

        // The healthy partition (0) dispatches on time despite partition 1's fetch failing — the
        // penalty box prevents head-of-line blocking (R9/D22). Partition 1 is held in the box.
        await("the degraded partition 1 entered the fetch penalty box")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> EngineMetrics.gauge(harness.meterRegistry(), "cesium.fetch.penalized.partitions") >= 1.0);

        // Partition 0's entries arrive (and on time); partition 1's are still parked in the box.
        Map<String, ConsumerRecord<byte[], byte[]>> healthy = byKey(new FencingProbe(bootstrap())
                .readCommitted(destination, perPartition, Duration.ofSeconds(60), Duration.ofSeconds(2)));
        for (int i = 0; i < perPartition; i++) {
            ConsumerRecord<byte[], byte[]> record = healthy.get("p0-" + i);
            assertTrue(record != null, "healthy partition 0 entry p0-" + i + " should dispatch while p1 is penalized");
            assertTrue(
                    record.timestamp() >= deliverAt && record.timestamp() <= deliverAt + 20_000,
                    () -> "healthy partition dispatch_lag must be unaffected by the degraded partition, got "
                            + (record.timestamp() - deliverAt) + " ms");
        }
        assertTrue(
                healthy.keySet().stream().noneMatch(k -> k.startsWith("p1-")),
                "the degraded partition's entries must stay penalty-boxed, not dispatched");

        // Heal partition 1: it leaves the box and drains exactly once; no duplicates anywhere.
        partition1Degraded.set(false);
        List<ConsumerRecord<byte[], byte[]>> all =
                readExactlyCommitted(destination, perPartition * 2, Duration.ofSeconds(90), RETRY_GRACE);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(all);
        for (int i = 0; i < perPartition; i++) {
            assertTrue(
                    byKey.containsKey("p1-" + i), "degraded partition entry p1-" + i + " must dispatch after healing");
        }
        harness.stop();
    }

    /**
     * A {@link SeekFetcher} decorator that, while {@code degraded} is set, rewrites every settled
     * fetch outcome for {@code degradedSourcePartition} to {@link FetchOutcome#TRANSIENT} — the
     * signal the dispatch loop reads to penalty-box that source partition (§7.3). Other partitions
     * pass through untouched, so the healthy ones keep dispatching. Deterministic and instantly
     * reversible, which the single-broker Toxiproxy substrate cannot achieve per-partition (R21).
     */
    static final class PartitionFaultFetcher implements SeekFetcher {

        private final SeekFetcher delegate;
        private final int degradedSourcePartition;
        private final AtomicBoolean degraded;

        PartitionFaultFetcher(SeekFetcher delegate, int degradedSourcePartition, AtomicBoolean degraded) {
            this.delegate = delegate;
            this.degradedSourcePartition = degradedSourcePartition;
            this.degraded = degraded;
        }

        @Override
        public FetchResult fetch(DueBatch candidates, long maxBytes, long deadlineMs) {
            FetchResult real = delegate.fetch(candidates, maxBytes, deadlineMs);
            if (!degraded.get()) {
                return real;
            }
            return new FaultResult(candidates, real, degradedSourcePartition);
        }

        @Override
        public void close() {
            delegate.close();
        }

        /**
         * Overrides {@link #outcome(int)} for the degraded partition's settled entries; everything
         * else (records, carry-over derivation, the metrics-only summaries the loop never reads)
         * delegates to the real pass.
         */
        private record FaultResult(DueBatch candidates, FetchResult real, int degradedPartition)
                implements FetchResult {

            @Override
            public int size() {
                return real.size();
            }

            @Override
            public FetchOutcome outcome(int i) {
                FetchOutcome actual = real.outcome(i);
                boolean settled = actual == FetchOutcome.FOUND || actual == FetchOutcome.GONE;
                if (settled && candidates.sourcePartition(i) == degradedPartition) {
                    return FetchOutcome.TRANSIENT;
                }
                return actual;
            }

            @Override
            public ConsumerRecord<byte[], byte[]> record(int i) {
                return real.record(i); // only ever called for FOUND (healthy-partition) entries
            }

            @Override
            public List<PartitionSummary> partitionSummaries() {
                return real.partitionSummaries();
            }
        }
    }
}
