package events.cesium.kafka.core.headers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import events.cesium.kafka.api.headers.CesiumHeaders;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/** Design §2.4: relay fidelity, provenance stamping, timestamp policy, and the DLQ contract. */
class RelayRecordFactoryTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long TS = NOW - 5_000;
    private static final byte[] KEY = {1, 2, 3};
    private static final byte[] VALUE = {9, 8, 7, 6};

    private static RelayRecordFactory factory() {
        return new RelayRecordFactory("dest", "dlq", true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
    }

    private static ConsumerRecord<byte[], byte[]> source(Headers headers) {
        return source(headers, TS, TimestampType.CREATE_TIME);
    }

    private static ConsumerRecord<byte[], byte[]> source(Headers headers, long timestamp, TimestampType type) {
        return new ConsumerRecord<>(
                "orders", 7, 42L, timestamp, type, KEY.length, VALUE.length, KEY, VALUE, headers, Optional.empty());
    }

    private static Headers typicalHeaders() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("app-header", new byte[] {0x01});
        headers.add(CesiumHeaders.DELAY_MS, ascii("60000"));
        headers.add("cesium-custom", ascii("user-owned"));
        headers.add(CesiumHeaders.DELIVER_AT, ascii("1700000060000"));
        return headers;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] headerValue(ProducerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertNotNull(header, "expected header " + key);
        return header.value();
    }

    private static void assertNoHeader(ProducerRecord<?, ?> record, String key) {
        assertNull(record.headers().lastHeader(key), "header " + key + " must be absent");
    }

    // --- relay fidelity (§2.4) --------------------------------------------------------------------

    @Test
    void relay_preservesKeyAndValueByteForByte() {
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(source(typicalHeaders()), NOW);
        assertEquals("dest", relayed.topic());
        assertSame(KEY, relayed.key());
        assertSame(VALUE, relayed.value());
    }

    @Test
    void relay_stripsExactlyTheTwoControlHeaders_keepsOrderAndOtherCesiumPrefixes() {
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(source(typicalHeaders()), NOW);
        assertNoHeader(relayed, CesiumHeaders.DELAY_MS);
        assertNoHeader(relayed, CesiumHeaders.DELIVER_AT);
        // The cesium- prefix names a namespace, not the strip set: user headers under it survive.
        Header[] headers = relayed.headers().toArray();
        assertEquals("app-header", headers[0].key());
        assertArrayEquals(new byte[] {0x01}, headers[0].value());
        assertEquals("cesium-custom", headers[1].key());
        assertArrayEquals(ascii("user-owned"), headers[1].value());
    }

    @Test
    void relay_stampsProvenance() {
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(source(typicalHeaders()), NOW);
        assertArrayEquals(ascii(Long.toString(NOW)), headerValue(relayed, CesiumHeaders.RELAYED_AT));
        assertArrayEquals("orders".getBytes(StandardCharsets.UTF_8), headerValue(relayed, CesiumHeaders.SOURCE_TOPIC));
        assertArrayEquals(ascii("7"), headerValue(relayed, CesiumHeaders.SOURCE_PARTITION));
        assertArrayEquals(ascii("42"), headerValue(relayed, CesiumHeaders.SOURCE_OFFSET));
        assertArrayEquals(ascii(Long.toString(TS)), headerValue(relayed, CesiumHeaders.SOURCE_TIMESTAMP));
    }

    @Test
    void relayImmediate_hasNoScheduledForAndNoClamped() {
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(source(typicalHeaders()), NOW);
        assertNoHeader(relayed, CesiumHeaders.SCHEDULED_FOR);
        assertNoHeader(relayed, CesiumHeaders.CLAMPED);
    }

    @Test
    void relayScheduled_stampsScheduledFor() {
        long scheduledFor = NOW - 50;
        ProducerRecord<byte[], byte[]> relayed =
                factory().relayScheduled(source(typicalHeaders()), NOW, scheduledFor, false);
        assertArrayEquals(ascii(Long.toString(scheduledFor)), headerValue(relayed, CesiumHeaders.SCHEDULED_FOR));
        assertNoHeader(relayed, CesiumHeaders.CLAMPED);
    }

    @Test
    void relayScheduled_clamped_stampsClampedTrue() {
        ProducerRecord<byte[], byte[]> relayed =
                factory().relayScheduled(source(typicalHeaders()), NOW, NOW - 50, true);
        assertArrayEquals(ascii("true"), headerValue(relayed, CesiumHeaders.CLAMPED));
    }

    @Test
    void provenanceOff_stampsNothingButStillStripsControlHeaders() {
        RelayRecordFactory noProvenance =
                new RelayRecordFactory("dest", "dlq", false, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
        ProducerRecord<byte[], byte[]> relayed = noProvenance.relayScheduled(source(typicalHeaders()), NOW, NOW, false);
        assertNoHeader(relayed, CesiumHeaders.DELAY_MS);
        assertNoHeader(relayed, CesiumHeaders.DELIVER_AT);
        assertNoHeader(relayed, CesiumHeaders.RELAYED_AT);
        assertNoHeader(relayed, CesiumHeaders.SOURCE_TOPIC);
        assertNoHeader(relayed, CesiumHeaders.SOURCE_PARTITION);
        assertNoHeader(relayed, CesiumHeaders.SOURCE_OFFSET);
        assertNoHeader(relayed, CesiumHeaders.SOURCE_TIMESTAMP);
        assertNoHeader(relayed, CesiumHeaders.SCHEDULED_FOR);
        assertEquals(2, relayed.headers().toArray().length);
    }

    @Test
    void clampedStampSurvivesProvenanceOff_itIsPolicyContractNotProvenance() {
        RelayRecordFactory noProvenance =
                new RelayRecordFactory("dest", "dlq", false, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
        ProducerRecord<byte[], byte[]> relayed = noProvenance.relayScheduled(source(typicalHeaders()), NOW, NOW, true);
        assertArrayEquals(ascii("true"), headerValue(relayed, CesiumHeaders.CLAMPED));
    }

    @Test
    void sourceTimestampProvenanceOmitted_whenRecordHasNoTimestamp() {
        ConsumerRecord<byte[], byte[]> timestampless =
                source(typicalHeaders(), ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(timestampless, NOW);
        assertNoHeader(relayed, CesiumHeaders.SOURCE_TIMESTAMP);
        // The rest of the provenance set is unaffected.
        assertArrayEquals(ascii("42"), headerValue(relayed, CesiumHeaders.SOURCE_OFFSET));
    }

    // --- timestamp policy (§2.4) ------------------------------------------------------------------

    @Test
    void timestampPolicyDispatch_stampsNow() {
        ProducerRecord<byte[], byte[]> relayed = factory().relayImmediate(source(typicalHeaders()), NOW);
        assertEquals(NOW, relayed.timestamp());
    }

    @Test
    void timestampPolicySource_carriesSourceTimestamp() {
        RelayRecordFactory sourceTime =
                new RelayRecordFactory("dest", "dlq", true, RelayTimestampPolicy.SOURCE, RelayPartitioning.BY_KEY);
        ProducerRecord<byte[], byte[]> relayed = sourceTime.relayImmediate(source(typicalHeaders()), NOW);
        assertEquals(TS, relayed.timestamp());
    }

    @Test
    void timestampPolicySource_withoutTimestamp_leavesAssignmentToProducer() {
        RelayRecordFactory sourceTime =
                new RelayRecordFactory("dest", "dlq", true, RelayTimestampPolicy.SOURCE, RelayPartitioning.BY_KEY);
        ConsumerRecord<byte[], byte[]> timestampless =
                source(typicalHeaders(), ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE);
        assertNull(sourceTime.relayImmediate(timestampless, NOW).timestamp());
    }

    // --- partitioning (§2.4) ----------------------------------------------------------------------

    @Test
    void partitioningByKey_leavesPartitionUnset() {
        assertNull(factory().relayImmediate(source(typicalHeaders()), NOW).partition());
    }

    @Test
    void partitioningSourcePartition_pinsSourcePartition() {
        RelayRecordFactory pinned = new RelayRecordFactory(
                "dest", "dlq", true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.SOURCE_PARTITION);
        assertEquals(7, pinned.relayImmediate(source(typicalHeaders()), NOW).partition());
    }

    // --- header-error DLQ contract (§2.4) ---------------------------------------------------------

    @Test
    void headerErrorDlq_carriesOriginalKeyValueAndAllHeadersIncludingOffendingControls() {
        ProducerRecord<byte[], byte[]> dlq =
                factory().headerErrorDlqRecord(source(typicalHeaders()), DlqReasons.MALFORMED_HEADER, "bad value", NOW);
        assertEquals("dlq", dlq.topic());
        assertSame(KEY, dlq.key());
        assertSame(VALUE, dlq.value());
        // The offending control headers are evidence and must survive on the DLQ record.
        assertArrayEquals(ascii("60000"), headerValue(dlq, CesiumHeaders.DELAY_MS));
        assertArrayEquals(ascii("1700000060000"), headerValue(dlq, CesiumHeaders.DELIVER_AT));
        assertArrayEquals(new byte[] {0x01}, headerValue(dlq, "app-header"));
    }

    @Test
    void headerErrorDlq_stampsReasonDetailAndProvenance() {
        ProducerRecord<byte[], byte[]> dlq =
                factory().headerErrorDlqRecord(source(typicalHeaders()), DlqReasons.OVER_MAX_DELAY, "way too far", NOW);
        assertArrayEquals(ascii("over-max-delay"), headerValue(dlq, CesiumHeaders.ERROR_REASON));
        assertArrayEquals("way too far".getBytes(StandardCharsets.UTF_8), headerValue(dlq, CesiumHeaders.ERROR_DETAIL));
        assertArrayEquals(ascii(Long.toString(NOW)), headerValue(dlq, CesiumHeaders.RELAYED_AT));
        assertArrayEquals(ascii("7"), headerValue(dlq, CesiumHeaders.SOURCE_PARTITION));
        assertArrayEquals(ascii("42"), headerValue(dlq, CesiumHeaders.SOURCE_OFFSET));
        assertEquals(NOW, dlq.timestamp());
    }

    @Test
    void headerErrorDlq_provenanceIsPartOfTheContract_notGatedByStampProvenance() {
        RelayRecordFactory noProvenance =
                new RelayRecordFactory("dest", "dlq", false, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
        ProducerRecord<byte[], byte[]> dlq =
                noProvenance.headerErrorDlqRecord(source(typicalHeaders()), DlqReasons.MALFORMED_HEADER, "detail", NOW);
        assertArrayEquals(ascii(Long.toString(NOW)), headerValue(dlq, CesiumHeaders.RELAYED_AT));
    }

    @Test
    void headerErrorDlq_withoutDlqTopic_throws() {
        RelayRecordFactory noDlq =
                new RelayRecordFactory("dest", null, true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
        assertThrows(
                IllegalStateException.class,
                () -> noDlq.headerErrorDlqRecord(source(typicalHeaders()), DlqReasons.MALFORMED_HEADER, "d", NOW));
    }

    // --- payload-expired loss notice (§2.4) -------------------------------------------------------

    @Test
    void payloadExpiredDlq_matchesTheJsonContractExactly() {
        ProducerRecord<byte[], byte[]> notice = factory().payloadExpiredDlqRecord("orders", 7, 42, NOW - 1_000, NOW);
        assertEquals("dlq", notice.topic());
        assertNull(notice.key());
        assertEquals(
                "{\"v\":1,\"sourceTopic\":\"orders\",\"sourcePartition\":7,\"sourceOffset\":42,"
                        + "\"scheduledFor\":" + (NOW - 1_000) + ",\"detectedAt\":" + NOW
                        + ",\"reason\":\"payload-expired\"}",
                new String(notice.value(), StandardCharsets.UTF_8));
        assertArrayEquals(ascii("payload-expired"), headerValue(notice, CesiumHeaders.ERROR_REASON));
        assertEquals(NOW, notice.timestamp());
    }

    @Test
    void payloadExpiredDlq_escapesJsonStringSpecials() {
        ProducerRecord<byte[], byte[]> notice =
                // quote, backslash, popular escape (\n), and a bare control character (0x01)
                factory().payloadExpiredDlqRecord("a\"b\\c\nd" + (char) 1 + "e", 0, 0, 0, 0);
        String json = new String(notice.value(), StandardCharsets.UTF_8);
        assertEquals(
                "{\"v\":1,\"sourceTopic\":\"a\\\"b\\\\c\\nd\\u0001e\",\"sourcePartition\":0,\"sourceOffset\":0,"
                        + "\"scheduledFor\":0,\"detectedAt\":0,\"reason\":\"payload-expired\"}",
                json);
    }

    @Test
    void payloadExpiredDlq_withoutDlqTopic_throws() {
        RelayRecordFactory noDlq =
                new RelayRecordFactory("dest", null, true, RelayTimestampPolicy.DISPATCH, RelayPartitioning.BY_KEY);
        assertThrows(IllegalStateException.class, () -> noDlq.payloadExpiredDlqRecord("orders", 7, 42, 0, 0));
    }
}
