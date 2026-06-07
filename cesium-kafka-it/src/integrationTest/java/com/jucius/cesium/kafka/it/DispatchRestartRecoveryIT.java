package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Restart recovery (design §3.6, I8 — §11.3-2/-7 shapes): the committed cursor + sidecar replay
 * path exercised for real across a process boundary. A SIGKILL-style {@link EngineHarness#kill()}
 * before due time loses every byte of in-memory state; the restarted instance recovers the pending
 * set from the committed cursor (sidecar pins + bounded replay) and dispatches each entry on time,
 * exactly once under read_committed. A graceful restart after a partial dispatch additionally
 * proves completed entries stay completed (their tombstones/cursor committed atomically) while
 * still-pending pins survive into the new incarnation.
 */
class DispatchRestartRecoveryIT extends KafkaIT {

    private static final long LATENESS_BOUND_MS = 20_000;

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void killBeforeDueTimeThenRestartDispatchesOnTimeExactlyOnce() {
        killBeforeDueTimeThenRestartDispatchesOnTimeExactlyOnce(KafkaConfig.GroupProtocol.CLASSIC);
    }

    /**
     * KIP-848 variant (design §11.3-11, D12): the same restart-recovery body under
     * {@code group.protocol=consumer}. Non-blocking nightly lane ({@code @Tag("kip848")}); proves the
     * committed-cursor + sidecar replay and the {@code initTransactions()} in-doubt resolution are
     * protocol-agnostic (the shard state machine is, per §3.4.5).
     */
    @Test
    @Tag("kip848")
    void killBeforeDueTimeThenRestartDispatchesOnTimeExactlyOnceUnderConsumerProtocol() {
        killBeforeDueTimeThenRestartDispatchesOnTimeExactlyOnce(KafkaConfig.GroupProtocol.CONSUMER);
    }

    private void killBeforeDueTimeThenRestartDispatchesOnTimeExactlyOnce(KafkaConfig.GroupProtocol protocol) {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .groupProtocol(protocol)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // Fast idle advancement: the cursor (offset + pinned-entry sidecar) commits well
                // before the kill, so the restart recovers from the sidecar, not a full replay.
                .idleCursorInterval(Duration.ofSeconds(1))
                .build();

        // Start FIRST so the clock toward the due time only starts once the pipeline is hot —
        // the kill-lands-before-due invariant must not race harness startup on slow CI.
        harness.start();
        String tracker = harness.trackerTopic();
        TopicPartition trackerTp = new TopicPartition(tracker, 0);

        long ts = System.currentTimeMillis();
        long scheduledFor = ts + 12_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < 3; i++) {
                produce(
                        producer,
                        source,
                        0,
                        ts,
                        "k" + i,
                        "v" + i,
                        h(CesiumHeaders.DELIVER_AT, Long.toString(scheduledFor)));
            }
            producer.flush();
        }

        // Ingest done (3 committed ADDs) and the dispatch cursor committed with the 3 pins in the
        // sidecar — the durable state a real deployment would have when the process dies.
        readExactlyCommitted(tracker, 3, WAIT);
        await("group-B cursor committed before the kill").atMost(WAIT).untilAsserted(() -> {
            OffsetAndMetadata committed =
                    committedOffsets(harness.dispatchGroupId()).get(trackerTp);
            assertNotNull(committed, "no committed dispatch cursor yet");
            assertTrue(!committed.metadata().isEmpty(), "cursor metadata must carry the pinned-entry sidecar");
        });

        // SIGKILL stand-in, well before the due time: all in-memory state is gone.
        harness.kill();
        assertTrue(
                System.currentTimeMillis() < scheduledFor,
                "test invariant: the kill must land before the entries become due");

        harness.start();

        List<ConsumerRecord<byte[], byte[]>> out = readExactlyCommitted(destination, 3, WAIT);
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(out);
        for (int i = 0; i < 3; i++) {
            ConsumerRecord<byte[], byte[]> record = byKey.get("k" + i);
            assertNotNull(record, "missing input k" + i);
            assertTrue(
                    record.timestamp() >= scheduledFor,
                    () -> "dispatched before the requested instant by " + (scheduledFor - record.timestamp()) + " ms");
            assertTrue(
                    record.timestamp() <= scheduledFor + LATENESS_BOUND_MS,
                    () -> "dispatched " + (record.timestamp() - scheduledFor) + " ms late");
        }

        // The completions are durable: 3 ADDs + 3 tombstones, nothing extra.
        readExactlyCommitted(tracker, 6, WAIT);
        harness.stop();
    }

    @Test
    void restartAfterPartialDispatchReplaysCompletionsAsNoOpsAndKeepsPinsPending() {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.INGEST, Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long ts = System.currentTimeMillis();
        // Wide gap between the early and late waves: the early-only observation + graceful stop
        // must finish well before the pins become due, even on a slow CI box.
        long lateFor = ts + 30_000;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            for (int i = 0; i < 3; i++) {
                produce(producer, source, 0, ts, "k-early-" + i, "v", h(CesiumHeaders.DELAY_MS, "3000"));
            }
            for (int i = 0; i < 2; i++) {
                produce(producer, source, 0, ts, "k-late-" + i, "v", h(CesiumHeaders.DELAY_MS, "30000"));
            }
            producer.flush();
        }

        harness.start();

        // The early entries dispatch; their tombstones + the cursor committed atomically. The
        // cursor's position-tracking offset predates the transaction's own tombstones, so the
        // restart's replay re-delivers those tombstones — they must apply as R2 no-ops.
        readExactlyCommitted(destination, 3, WAIT);
        harness.stop();
        harness.start();

        List<ConsumerRecord<byte[], byte[]>> out = readExactlyCommitted(destination, 5, Duration.ofSeconds(90));
        Map<String, ConsumerRecord<byte[], byte[]>> byKey = byKey(out); // also asserts no duplicates
        for (int i = 0; i < 2; i++) {
            ConsumerRecord<byte[], byte[]> late = byKey.get("k-late-" + i);
            assertNotNull(late, "missing pinned input k-late-" + i);
            assertTrue(
                    late.timestamp() >= lateFor,
                    () -> "pin dispatched before its requested instant by " + (lateFor - late.timestamp()) + " ms");
        }

        // Lifecycle complete for all five: 5 ADDs + 5 tombstones, exactly once each.
        assertEquals(10, readExactlyCommitted(harness.trackerTopic(), 10, WAIT).size());
        harness.stop();
    }
}
