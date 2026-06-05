package events.cesium.kafka.store.tracker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.TrackerRecordData;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Design §2.2 / D15: the tracker-topic wire format. */
class TrackerWireFormatTest {

    private static final Headers NO_HEADERS = new RecordHeaders();
    private static final byte[] KEY = TrackerWireFormat.encodeKey(42);

    private static TrackerDecodeResult decodeValue(byte[] value) {
        return TrackerWireFormat.decode(KEY, value, NO_HEADERS);
    }

    // --- Golden bytes: the exact encoding is pinned so the format can never silently drift -------

    @Test
    void goldenKeyBytes() {
        assertArrayEquals(
                new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
                TrackerWireFormat.encodeKey(0x0102030405060708L));
        assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 42}, TrackerWireFormat.encodeKey(42));
        assertArrayEquals(
                new byte[] {
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
                },
                TrackerWireFormat.encodeKey(-1));
    }

    @Test
    void goldenAddValueBytes() {
        assertArrayEquals(
                new byte[] {
                    (byte) 0xC5, // magic
                    0x01, // version
                    0x01, // type ADD
                    0x01, // flags: CLAMPED
                    0x11,
                    0x22,
                    0x33,
                    0x44,
                    0x55,
                    0x66,
                    0x77,
                    (byte) 0x88 // dispatchAtMs BE
                },
                TrackerWireFormat.encodeAddValue(0x1122334455667788L, true));
        assertArrayEquals(
                new byte[] {(byte) 0xC5, 0x01, 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 0},
                TrackerWireFormat.encodeAddValue(0, false));
    }

    @Test
    void goldenCompletionRecord() {
        TrackerRecordData record = TrackerWireFormat.encodeCompletion(7, CompletionReason.DISPATCHED);
        assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 7}, record.key());
        assertNull(record.value(), "completion must be a null-value tombstone (compaction, D15)");
        Header header = record.headers().lastHeader("cesium-completion-reason");
        assertNotNull(header, "reason must travel in the cesium-completion-reason header");
        assertArrayEquals("DISPATCHED".getBytes(StandardCharsets.US_ASCII), header.value());
    }

    @Test
    void goldenConstants() {
        assertEquals((byte) 0xC5, TrackerWireFormat.MAGIC);
        assertEquals(0x01, TrackerWireFormat.VERSION);
        assertEquals(0x01, TrackerWireFormat.TYPE_ADD);
        assertEquals(0x02, TrackerWireFormat.TYPE_CANCEL);
        assertEquals(0x01, TrackerWireFormat.FLAG_CLAMPED);
        assertEquals(8, TrackerWireFormat.KEY_LENGTH);
        assertEquals(12, TrackerWireFormat.V1_VALUE_LENGTH);
        assertEquals("cesium-completion-reason", TrackerWireFormat.COMPLETION_REASON_HEADER);
    }

    // --- Round trips ------------------------------------------------------------------------------

    @Test
    void keyRoundTripsAtBoundaries() {
        for (long offset : new long[] {0, 1, 42, -1, Long.MAX_VALUE, Long.MIN_VALUE}) {
            assertEquals(offset, TrackerWireFormat.decodeKey(TrackerWireFormat.encodeKey(offset)));
        }
    }

    @Test
    void addValueRoundTripsWithFlags() {
        for (long dispatchAtMs : new long[] {0, 1_700_000_060_000L, Long.MAX_VALUE, Long.MIN_VALUE}) {
            for (boolean clamped : new boolean[] {false, true}) {
                TrackerDecodeResult result = decodeValue(TrackerWireFormat.encodeAddValue(dispatchAtMs, clamped));
                TrackerDecodeResult.Add add = assertInstanceOf(TrackerDecodeResult.Add.class, result);
                assertEquals(dispatchAtMs, add.dispatchAtMs());
                assertEquals(clamped, add.clamped());
            }
        }
    }

    @Test
    void encodeAddProducesFullRecord() {
        TrackerRecordData record = TrackerWireFormat.encodeAdd(99, 1_700_000_060_000L, true);
        assertEquals(99, TrackerWireFormat.decodeKey(record.key()));
        byte[] value = record.value();
        assertNotNull(value);
        TrackerDecodeResult result = TrackerWireFormat.decode(record.key(), value, record.headers());
        assertEquals(new TrackerDecodeResult.Add(1_700_000_060_000L, true), result);
        assertTrue(!record.headers().iterator().hasNext(), "ADD records carry no headers");
    }

    @ParameterizedTest
    @EnumSource(CompletionReason.class)
    void completionRoundTripsEveryReason(CompletionReason reason) {
        TrackerRecordData record = TrackerWireFormat.encodeCompletion(7, reason);
        TrackerDecodeResult result = TrackerWireFormat.decode(record.key(), record.value(), record.headers());
        assertEquals(new TrackerDecodeResult.Complete(reason), result);
    }

    // --- Forward compatibility: unknown flags ignored, unknown versions rejected ------------------

    @Test
    void unknownFlagBitsAreIgnored() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        value[3] = (byte) 0xFE; // every unknown bit set, CLAMPED clear
        assertEquals(new TrackerDecodeResult.Add(123_456L, false), decodeValue(value));

        value[3] = (byte) 0xFF; // every unknown bit set, CLAMPED set
        assertEquals(new TrackerDecodeResult.Add(123_456L, true), decodeValue(value));
    }

    @Test
    void unknownVersionIsRejected() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        value[1] = 0x02;
        TrackerDecodeResult.Invalid invalid = assertInstanceOf(TrackerDecodeResult.Invalid.class, decodeValue(value));
        assertTrue(invalid.detail().contains("version"), invalid.detail());
    }

    @Test
    void wrongMagicIsRejected() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        value[0] = 0x5C;
        TrackerDecodeResult.Invalid invalid = assertInstanceOf(TrackerDecodeResult.Invalid.class, decodeValue(value));
        assertTrue(invalid.detail().contains("magic"), invalid.detail());
    }

    // --- Reserved CANCEL and unknown types ---------------------------------------------------------

    @Test
    void reservedCancelTypeDecodesDistinctlyWithoutThrowing() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        value[2] = TrackerWireFormat.TYPE_CANCEL;
        assertSame(TrackerDecodeResult.Cancel.INSTANCE, decodeValue(value));
    }

    @Test
    void unknownTypeIsRejected() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        value[2] = 0x03;
        TrackerDecodeResult.Invalid invalid = assertInstanceOf(TrackerDecodeResult.Invalid.class, decodeValue(value));
        assertTrue(invalid.detail().contains("type"), invalid.detail());
    }

    // --- Length validation -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 11, 13, 64})
    void truncatedAndOversizedValuesAreRejected(int length) {
        byte[] value = Arrays.copyOf(TrackerWireFormat.encodeAddValue(123_456L, false), length);
        assertInstanceOf(TrackerDecodeResult.Invalid.class, decodeValue(value));
    }

    @Test
    void badKeyLengthIsRejectedEvenWithValidValue() {
        byte[] value = TrackerWireFormat.encodeAddValue(123_456L, false);
        TrackerDecodeResult.Invalid invalid = assertInstanceOf(
                TrackerDecodeResult.Invalid.class, TrackerWireFormat.decode(new byte[7], value, NO_HEADERS));
        assertTrue(invalid.detail().contains("key"), invalid.detail());
    }

    @Test
    void decodeKeyThrowsOnWrongLengthAsProgrammingError() {
        assertThrows(IllegalArgumentException.class, () -> TrackerWireFormat.decodeKey(new byte[7]));
        assertThrows(IllegalArgumentException.class, () -> TrackerWireFormat.decodeKey(new byte[9]));
    }

    // --- Tombstone reason-header validation --------------------------------------------------------

    @Test
    void tombstoneWithoutReasonHeaderIsInvalid() {
        TrackerDecodeResult.Invalid invalid = assertInstanceOf(
                TrackerDecodeResult.Invalid.class, TrackerWireFormat.decode(KEY, null, new RecordHeaders()));
        assertTrue(invalid.detail().contains("cesium-completion-reason"), invalid.detail());
    }

    @Test
    void tombstoneWithUnknownReasonStillCompletesCarryingTheRawName() {
        // A future writer's new CompletionReason constant must not convert into a skipped
        // tombstone (= duplicate dispatch) on this reader: the completion applies, the novelty
        // is surfaced distinctly for a dedicated metric/WARN.
        RecordHeaders headers = new RecordHeaders();
        headers.add(TrackerWireFormat.COMPLETION_REASON_HEADER, "EXPLODED".getBytes(StandardCharsets.US_ASCII));
        TrackerDecodeResult.CompleteUnknownReason complete = assertInstanceOf(
                TrackerDecodeResult.CompleteUnknownReason.class, TrackerWireFormat.decode(KEY, null, headers));
        assertEquals("EXPLODED", complete.rawReason());
    }

    @Test
    void tombstoneWithNullReasonValueIsInvalid() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(TrackerWireFormat.COMPLETION_REASON_HEADER, null);
        assertInstanceOf(TrackerDecodeResult.Invalid.class, TrackerWireFormat.decode(KEY, null, headers));
    }

    @Test
    void tombstoneUsesLastReasonHeaderValue() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(TrackerWireFormat.COMPLETION_REASON_HEADER, "DISPATCHED".getBytes(StandardCharsets.US_ASCII));
        headers.add(TrackerWireFormat.COMPLETION_REASON_HEADER, "DROPPED".getBytes(StandardCharsets.US_ASCII));
        assertEquals(
                new TrackerDecodeResult.Complete(CompletionReason.DROPPED),
                TrackerWireFormat.decode(KEY, null, headers));
    }
}
