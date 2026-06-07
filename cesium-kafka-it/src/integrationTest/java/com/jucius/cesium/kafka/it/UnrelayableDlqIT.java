package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Security audit M2 / design §3.8 I-8: a record the <em>destination</em> broker permanently rejects
 * on produce (here: a relay larger than the destination topic's {@code max.message.bytes}) must NOT
 * wedge the source partition behind an infinite abort→rewind→retry loop. Under the default
 * {@code route.relay.on-unrelayable: DLQ} it is routed to the DLQ <em>exactly once</em>, atomically
 * with advancing the source offset past it, and a following normal record on the same partition is
 * delivered to the destination — the partition makes progress.
 *
 * <p>PR-lane (untagged), like the sibling DLQ ITs ({@code HeaderPolicyIT}, {@code PayloadExpiryDlqIT}):
 * fast and deterministic, no fault injection or timing windows.
 */
class UnrelayableDlqIT extends KafkaIT {

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void oversizedRelayLandsInTheDlqExactlyOnceAndTheFollowingRecordIsDelivered() {
        String source = createTopic("src", 1);
        // The destination caps records at 4 KiB: an 8 KiB relay is permanently rejected
        // (MESSAGE_TOO_LARGE ⇒ RecordTooLargeException), while the producer's 1 MiB max.request.size
        // and the source/DLQ topics (default ~1 MiB) accept the same record — so the rejection is
        // genuinely a destination record-level limit, not a client-side or source limit.
        String destination = createTopic("dst", 1, Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4096"));
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        // 8 KiB of INCOMPRESSIBLE bytes: cesium relays with compression.type=lz4, and the broker
        // checks the compressed batch against max.message.bytes — a repeated character would compress
        // under the cap and not be rejected. Random bytes stay ~8 KiB > 4 KiB ⇒ a genuine rejection.
        byte[] oversized = new byte[8192];
        new Random(20260605L).nextBytes(oversized);
        long timestamp = System.currentTimeMillis();
        try (Producer<byte[], byte[]> producer = newProducer()) {
            producer.send(new ProducerRecord<>(source, 0, timestamp, bytes("poison"), oversized)) // offset 0: poison
                    .get();
            produce(producer, source, 0, timestamp, "ok", "small"); // offset 1: a normal record behind it
            producer.flush();
        } catch (Exception e) {
            throw new AssertionError("producing the source records failed", e);
        }
        harness.start();

        // The poison record lands in the DLQ exactly once, reusing the §2.4 header-error shape with
        // reason = unrelayable + the destination rejection detail.
        List<ConsumerRecord<byte[], byte[]>> dlqRecords = readExactlyCommitted(dlq, 1, WAIT);
        ConsumerRecord<byte[], byte[]> dead = dlqRecords.get(0);
        assertArrayEquals(bytes("poison"), dead.key(), "the original key is preserved on the DLQ record");
        assertArrayEquals(oversized, dead.value(), "the original (oversized) value is preserved");
        assertEquals(DlqReasons.UNRELAYABLE, headerValue(dead.headers(), CesiumHeaders.ERROR_REASON));
        String detail = headerValue(dead.headers(), CesiumHeaders.ERROR_DETAIL);
        assertNotNull(detail, "the destination rejection reason is carried as the error detail: " + detail);
        assertEquals(source, headerValue(dead.headers(), CesiumHeaders.SOURCE_TOPIC));
        assertEquals("0", headerValue(dead.headers(), CesiumHeaders.SOURCE_OFFSET));

        // The partition is NOT wedged: the following normal record reaches the destination, and it is
        // the ONLY thing there (the poison relay never committed).
        List<ConsumerRecord<byte[], byte[]>> relayed = readExactlyCommitted(destination, 1, WAIT);
        assertArrayEquals(bytes("small"), relayed.get(0).value(), "the record behind the poison record IS delivered");

        // The source offset advanced past BOTH records — exactly-once, no infinite retry.
        awaitCommittedOffset(harness.ingestGroupId(), new TopicPartition(source, 0), 2L);
        assertEquals(
                1.0,
                harness.meterRegistry()
                        .get("cesium.dlq.records")
                        .tag("reason", DlqReasons.UNRELAYABLE)
                        .counter()
                        .count(),
                "exactly one unrelayable DLQ record");

        harness.stop();
    }
}
