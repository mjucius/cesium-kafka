package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.Role;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Macro perf — <strong>large-payload fetch budget</strong> (design §11.4 + §5.4 + R8): a backlog of
 * 1 MiB <em>incompressible</em> source records, all due, drained with a deliberately small
 * {@code dispatch.batch.max-bytes} budget. The proof that the fetch path enforces its
 * decompressed-byte budget (§7.2 "truncate-and-carry-over") rather than materializing the whole
 * backlog at once is that the drain takes <em>several</em> bounded dispatch transactions — about
 * {@code totalBytes / budget} of them — instead of one oversized batch; observed heap is reported
 * alongside as corroboration.
 *
 * <p>Random payloads (not runs of one byte) so the wire batch ≈ the decompressed size: the §5.4
 * budget is about <em>decompressed</em> resident bytes, and a highly compressible filler would let an
 * unbounded implementation pass undetected. {@code @Tag("nightly")}.
 */
@Tag("nightly")
class LargePayloadPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(LargePayloadPerfIT.class);

    private static final int RECORD_BYTES = 1024 * 1024; // 1 MiB
    private static final int COUNT = Integer.getInteger("perf.large.count", 32); // ~32 MiB of payload
    private static final long BATCH_MAX_BYTES = 4L * 1024 * 1024; // 4 MiB fetch budget → ~4 records/batch
    private static final int TOPIC_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void largePayloadDrainRespectsTheFetchByteBudget() {
        Map<String, String> bigMsg =
                Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(TOPIC_MAX_MESSAGE_BYTES));
        String source = createTopic("lp-src", 2, bigMsg);
        String destination = createTopic("lp-dst", 2, bigMsg);
        String dlq = createTopic("lp-dlq", 1, bigMsg);

        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // The §11.4 R8 lever: a small decompressed-byte budget so the fetch pass must
                // truncate-and-carry-over rather than fetch all 32 MiB into one batch.
                .batchMaxBytes(BATCH_MAX_BYTES)
                // The relay producer re-emits 1 MiB records: raise max.request.size above the 1 MiB default.
                .kafkaProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.toString(2 * 1024 * 1024))
                .build();
        harness.start();

        long dueAt = System.currentTimeMillis() + 10_000;
        Random random = new Random(42);
        try (Producer<byte[], byte[]> producer = bigRecordProducer()) {
            for (int i = 0; i < COUNT; i++) {
                byte[] value = new byte[RECORD_BYTES];
                random.nextBytes(value); // incompressible: wire size ≈ decompressed size
                var unused = producer.send(new ProducerRecord<>(
                        source,
                        i % 2,
                        null,
                        ("lp-" + i).getBytes(StandardCharsets.UTF_8),
                        value,
                        List.of(new RecordHeader(
                                CesiumHeaders.DELIVER_AT, Long.toString(dueAt).getBytes(StandardCharsets.UTF_8)))));
            }
            producer.flush();
        }

        // Stage the backlog as pending, then snapshot the committed-dispatch-transaction count just
        // before the drain so the delta counts only drain transactions (not idle-cursor commits).
        await("all " + COUNT + " large entries pending before due")
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> pendingTotal() >= COUNT);
        System.gc();
        long heapBeforeBytes = usedHeapBytes();
        double committedBefore = committedDispatchTxns();

        List<ConsumerRecord<byte[], byte[]>> delivered =
                readExactlyCommitted(destination, COUNT, Duration.ofMinutes(3), RETRY_GRACE);
        byKey(delivered); // exactly-once across the whole large-payload backlog

        double drainTxns = committedDispatchTxns() - committedBefore;
        long heapAfterBytes = usedHeapBytes();
        // With a 4 MiB budget and 1 MiB records the fetch pass carries ~4 records/transaction, so a
        // 32-record backlog needs ~8 transactions; an unbounded "materialize everything" path would
        // drain in ONE. Allow slack for record/relay overhead trimming a record off some batches.
        double expectedBatches = Math.ceil((double) COUNT * RECORD_BYTES / BATCH_MAX_BYTES);
        double minBatches = Math.max(2, expectedBatches * 0.6);

        log.info(
                "MEASURED large-payload: {} × 1 MiB drained in {} dispatch txns (expected ≈{}); "
                        + "budget {} MiB; heap used {}→{} MiB | §5.4 fetch budget holds [R8]",
                COUNT,
                String.format("%.0f", drainTxns),
                String.format("%.0f", expectedBatches),
                BATCH_MAX_BYTES / (1024 * 1024),
                heapBeforeBytes / (1024 * 1024),
                heapAfterBytes / (1024 * 1024));
        System.out.printf(
                "%n[PERF] large-payload %d×1MiB, budget %dMiB: %.0f dispatch txns (expected ≈%.0f, "
                        + "min %.0f for truncation); heap %dMiB→%dMiB%n",
                COUNT,
                BATCH_MAX_BYTES / (1024 * 1024),
                drainTxns,
                expectedBatches,
                minBatches,
                heapBeforeBytes / (1024 * 1024),
                heapAfterBytes / (1024 * 1024));

        assertTrue(
                drainTxns >= minBatches,
                () -> "fetch budget not enforced: " + COUNT + " × 1 MiB drained in only " + drainTxns
                        + " dispatch transactions (expected ≈" + expectedBatches
                        + "); the path materialized more than the " + (BATCH_MAX_BYTES / (1024 * 1024))
                        + " MiB budget at once (R8 regression)");
        harness.stop();
    }

    private static Producer<byte[], byte[]> bigRecordProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"); // keep wire ≈ decompressed
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.toString(2 * 1024 * 1024));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.toString(64 * 1024 * 1024));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private double committedDispatchTxns() {
        return EngineMetrics.counter(
                harness.meterRegistry(),
                "cesium.transactions",
                "loop",
                "dispatch",
                "result",
                "committed",
                "cause",
                "none");
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private double pendingTotal() {
        double sum = 0;
        for (int p = 0; p < 2; p++) {
            double v = EngineMetrics.gauge(
                    harness.meterRegistry(), "cesium.pending.entries", "partition", Integer.toString(p));
            if (!Double.isNaN(v)) {
                sum += v;
            }
        }
        return sum;
    }
}
