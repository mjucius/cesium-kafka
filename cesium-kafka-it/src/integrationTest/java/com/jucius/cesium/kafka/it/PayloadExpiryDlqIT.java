package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import com.jucius.cesium.kafka.store.tracker.TrackerWireFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Retention-expiry resolution (design §7.4, D-9, §11.3-9): a scheduled payload that provably
 * vanishes from the source before its due time (here: {@code Admin.deleteRecords} advancing the
 * log-start offset past it — the deterministic stand-in for retention) resolves through the
 * default DLQ policy as a §2.4 loss-notice JSON + a {@code PAYLOAD_MISSING_DLQ} COMPLETE
 * tombstone, exactly once, in one transaction — and the pipeline stays alive: a surviving entry
 * on the same partition still dispatches to the destination.
 *
 * <p>Runs split roles deterministically: an ingest-only instance schedules and stops, the records
 * are deleted, then a dispatch-only instance (same application id) recovers and resolves — no race
 * between the delete and the fetch.
 */
class PayloadExpiryDlqIT extends KafkaIT {

    private EngineHarness ingestHarness;
    private EngineHarness dispatchHarness;

    @AfterEach
    void tearDown() {
        if (ingestHarness != null) {
            ingestHarness.close();
        }
        if (dispatchHarness != null) {
            dispatchHarness.close();
        }
    }

    @Test
    void retentionExpiredPayloadResolvesViaDlqLossNoticeExactlyOnce() {
        String appId = unique("app");
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        ingestHarness = EngineHarness.builder()
                .applicationId(appId)
                .roles(Role.INGEST)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        long ts = System.currentTimeMillis();
        long goneFor = ts + 8_000;
        long aliveFor = ts + 10_000;
        RecordMetadata gone;
        RecordMetadata alive;
        try (Producer<byte[], byte[]> producer = newProducer()) {
            gone = produce(
                    producer, source, 0, ts, "k-gone", "v-gone", h(CesiumHeaders.DELIVER_AT, Long.toString(goneFor)));
            alive = produce(
                    producer,
                    source,
                    0,
                    ts,
                    "k-alive",
                    "v-alive",
                    h(CesiumHeaders.DELIVER_AT, Long.toString(aliveFor)));
            producer.flush();
        }

        ingestHarness.start();
        String tracker = ingestHarness.trackerTopic();
        readExactlyCommitted(tracker, 2, WAIT); // both ADDs durable
        ingestHarness.stop();

        // Force the first payload away: advance the log-start offset past it (the clean
        // deterministic "retention caught up" signal — beginningOffsets > wantedOffset, §7.3).
        get(ADMIN.deleteRecords(Map.of(new TopicPartition(source, 0), RecordsToDelete.beforeOffset(gone.offset() + 1)))
                .all());

        dispatchHarness = EngineHarness.builder()
                .applicationId(appId)
                .roles(Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();
        dispatchHarness.start();

        // The loss notice: §2.4 contract — key=null, hand-rolled JSON value, error-reason header.
        // RETRY_GRACE: the gone payload penalty-boxes its source partition; a wider quiet window
        // catches a duplicate notice from a delayed re-dispatch that a 1 s window would miss.
        List<ConsumerRecord<byte[], byte[]>> notices = readExactlyCommitted(dlq, 1, WAIT, RETRY_GRACE);
        ConsumerRecord<byte[], byte[]> notice = notices.get(0);
        assertNull(notice.key(), "loss notices have no key — only the pointer survived");
        assertEquals(DlqReasons.PAYLOAD_EXPIRED, headerValue(notice.headers(), CesiumHeaders.ERROR_REASON));
        String json = utf8(notice.value());
        assertJsonField(json, "\"v\":1");
        assertJsonField(json, "\"sourceTopic\":\"" + source + "\"");
        assertJsonField(json, "\"sourcePartition\":0");
        assertJsonField(json, "\"sourceOffset\":" + gone.offset());
        assertJsonField(json, "\"scheduledFor\":" + goneFor);
        assertJsonField(json, "\"reason\":\"" + DlqReasons.PAYLOAD_EXPIRED + "\"");

        // Pipeline alive: the surviving entry still dispatches, exactly once, at-or-after its time.
        List<ConsumerRecord<byte[], byte[]>> relayed = readExactlyCommitted(destination, 1, WAIT, RETRY_GRACE);
        assertArrayEquals(bytes("k-alive"), relayed.get(0).key());
        assertArrayEquals(bytes("v-alive"), relayed.get(0).value());
        assertTrue(
                relayed.get(0).timestamp() >= aliveFor,
                "the surviving entry must not dispatch before its requested instant");

        // Durable lifecycle: 2 ADDs + 2 tombstones; the lost entry completed PAYLOAD_MISSING_DLQ,
        // the survivor DISPATCHED — both resolved exactly once.
        List<ConsumerRecord<byte[], byte[]>> trackerLog = readExactlyCommitted(tracker, 4, WAIT);
        assertEquals(
                CompletionReason.PAYLOAD_MISSING_DLQ.name(),
                completionReason(trackerLog, gone.offset()),
                "the lost entry's tombstone carries PAYLOAD_MISSING_DLQ");
        assertEquals(
                CompletionReason.DISPATCHED.name(),
                completionReason(trackerLog, alive.offset()),
                "the survivor's tombstone carries DISPATCHED");

        dispatchHarness.stop();
    }

    private static void assertJsonField(String json, String fragment) {
        assertTrue(json.contains(fragment), () -> "loss-notice JSON missing " + fragment + ": " + json);
    }

    /** The completion-reason header of the single tombstone for {@code sourceOffset}. */
    private static String completionReason(List<ConsumerRecord<byte[], byte[]>> trackerLog, long sourceOffset) {
        byte[] key = TrackerWireFormat.encodeKey(sourceOffset);
        List<ConsumerRecord<byte[], byte[]>> tombstones = trackerLog.stream()
                .filter(r -> r.value() == null && Arrays.equals(key, r.key()))
                .toList();
        assertEquals(1, tombstones.size(), () -> "expected exactly one tombstone for source offset " + sourceOffset);
        return headerValue(tombstones.get(0).headers(), TrackerWireFormat.COMPLETION_REASON_HEADER);
    }
}
