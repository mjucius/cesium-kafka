package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.core.fetch.FetchOutcome;
import events.cesium.kafka.core.fetch.FetchResult;
import events.cesium.kafka.core.fetch.SeekFetcher;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Park-and-degrade and penalty-box isolation (design §3.8/R15 D-15, §11.3-10, §7.3/D22).
 *
 * <ul>
 *   <li><strong>Park-and-degrade:</strong> every dispatch relay is rejected by the destination (the
 *       deterministic, fast, definitively-abortable fault — a relay larger than the destination's
 *       {@code max.message.bytes}, broker-rejected immediately with no delivery-timeout churn); the
 *       loop aborts and retries with capped backoff and, after {@code parkThreshold} consecutive
 *       failures, parks-and-degrades — {@code cesium.degraded} rises, membership stays alive (no
 *       crash-loop, no replay storm, nothing double-sent), and it recovers the instant the
 *       destination accepts the relay again. (D-15 is abortable-retry <em>exhaustion</em>; an ISR
 *       shortage is just one example — an oversized relay is another and is far more stable to inject
 *       than a {@code min.insync.replicas} delivery-timeout storm.)
 *   <li><strong>Penalty-box isolation:</strong> one source partition's payload fetch is forced
 *       {@code TRANSIENT} (a deterministic decorator over the real seek fetcher — the single broker
 *       cannot degrade one partition's leader alone, R21); that partition is penalty-boxed while the
 *       healthy partition keeps dispatching on time, and the degraded partition drains exactly once
 *       after the fault heals.
 * </ul>
 *
 * <p>Class-{@code @Tag("nightly")}: the park-and-degrade and penalty-box backoff windows make this a
 * slow fault-injection scenario, pushed off the PR lane to nightly (design §13).
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

    @Test
    void dispatchParksAndDegradesWhenTheDestinationRejectsRelaysThenRecovers() {
        String source = createTopic("src", 1);
        // A destination whose max.message.bytes is far below the relay size ⇒ every relay is rejected
        // immediately (RecordTooLargeException) — a definitively-abortable failure with no
        // delivery-timeout wait. Relaxing the limit later "returns" the destination.
        String destination = createTopic("dst", 1, Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1024"));
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        harness.start();

        int total = 3;
        // Incompressible (lz4 producer compression can't shrink it) so the relay genuinely exceeds
        // the destination's 1 KiB ceiling — a repeated byte would compress to well under it and slip
        // through.
        String bigValue = incompressible(8192);
        long ts = System.currentTimeMillis();
        long deliverAt = ts + 3_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < total; i++) {
                produce(
                        producer,
                        source,
                        0,
                        ts,
                        "k" + i,
                        bigValue,
                        h(CesiumHeaders.DELIVER_AT, Long.toString(deliverAt)));
            }
            producer.flush();
        }

        // The entries come due, every relay is rejected, and after the park threshold the loop degrades
        // (no crash — a dead loop thread could not flip the gauge).
        await("dispatch loop parked-and-degraded after the destination rejected every relay")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() ->
                        EngineMetrics.gaugeIs(harness.meterRegistry(), "cesium.degraded", 1.0, "loop", "dispatch"));

        // Membership stays alive while degraded: group B still has its member owning the partition.
        ConsumerGroupDescription dispatchGroup = get(ADMIN.describeConsumerGroups(List.of(harness.dispatchGroupId()))
                        .all())
                .get(harness.dispatchGroupId());
        assertEquals(
                1,
                dispatchGroup.members().size(),
                "the degraded dispatch member must stay in the group (membership-preserving park, R15)");
        assertTrue(
                dispatchGroup.members().stream()
                        .anyMatch(m -> !m.assignment().topicPartitions().isEmpty()),
                "the degraded member must keep its tracker assignment (no fence, no crash-loop)");

        // Nothing was *committed* while degraded — the batch is parked, not dropped, not double-sent
        // (aborted relay attempts and their abort markers do advance the raw log end offset, but a
        // read_committed consumer sees none of them).
        assertEquals(
                0,
                drainCount(destination, "read_committed"),
                "nothing committed to the destination while parked (aborted, never double-sent)");

        // Relax the limit so relays fit; dispatch recovers, the flag clears, every entry lands once.
        setTopicConfig(destination, TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(1024 * 1024));

        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, total, Duration.ofSeconds(90), RETRY_GRACE);
        byKey(delivered); // exactly-once across the whole park/retry/recover cycle

        await("degraded flag cleared after recovery")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() ->
                        EngineMetrics.gaugeIs(harness.meterRegistry(), "cesium.degraded", 0.0, "loop", "dispatch"));
        harness.stop();
    }

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

    /** A pseudo-random printable-ASCII string the producer's lz4 compression cannot shrink. */
    private static String incompressible(int length) {
        java.util.Random random = new java.util.Random(7);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('!' + random.nextInt(94)));
        }
        return sb.toString();
    }

    private static void setTopicConfig(String topic, String key, String value) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET);
        get(ADMIN.incrementalAlterConfigs(Map.of(resource, List.of(op))).all());
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
