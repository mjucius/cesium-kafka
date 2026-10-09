package com.jucius.cesium.kafka.store.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.store.tracker.SidecarCodec.DecodedSidecar;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.EncodedCursor;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.Encoder;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.SidecarEntry;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

/**
 * Sidecar codec units (design §3.5, §11.1): golden bytes pin the v1 layout; greedy budget cuts
 * are exercised at exact boundaries; the overflow fallback degenerates to the min-pending
 * watermark; corrupt input — including unknown versions — is an explicit error (§3.6: a
 * corrupted committed cursor is never silently ignored).
 */
class SidecarCodecTest {

    private static final String CLUSTER_ID = "ck";
    private static final Uuid SOURCE_ID = new Uuid(1, 2);
    private static final Uuid TRACKER_ID = new Uuid(3, 4);

    /**
     * Hand-derived golden layout for two entries under cluster id {@code "ck"}:
     *
     * <pre>
     *   01                      version
     *   02 63 6b                cluster id length + "ck"
     *   00..01 00..02           source topic id Uuid(1, 2)
     *   00..03 00..04           tracker topic id Uuid(3, 4)
     *   05 0a d0 0f 00          entry 1: trk=5, src=10, zigzag(1000)=2000 -> d0 0f, flags 0
     *   02 03 c7 01 01          entry 2: dTrk=2 (7), dSrc=3 (13), zigzag(-100)=199 -> c7 01, clamped
     * </pre>
     */
    private static final String GOLDEN_HEX = "0102636b"
            + "0000000000000001" + "0000000000000002"
            + "0000000000000003" + "0000000000000004"
            + "050ad00f00"
            + "0203c70101";

    private static Encoder encoder(int budget) {
        return SidecarCodec.encoder(CLUSTER_ID, SOURCE_ID, TRACKER_ID, budget);
    }

    @Test
    void goldenBytes() {
        Encoder encoder = encoder(3072);
        assertTrue(encoder.offer(5, 10, 1_000, false));
        assertTrue(encoder.offer(7, 13, 900, true));
        EncodedCursor encoded = encoder.finish(42);

        String expected = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(HexFormat.of().parseHex(GOLDEN_HEX));
        assertEquals(expected, encoded.metadata(), "v1 sidecar layout is pinned by the golden hex");
        assertEquals(2, encoded.encodedCount());
        assertEquals(expected.length(), encoded.encodedBytes());
        assertFalse(encoded.overflowed());
        assertEquals(42, encoded.cursorOffset(), "all pending encoded => cursor = live position");
    }

    @Test
    void goldenBytesDecode() {
        String metadata = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(HexFormat.of().parseHex(GOLDEN_HEX));
        DecodedSidecar decoded = SidecarCodec.decode(metadata);
        assertEquals(CLUSTER_ID, decoded.clusterId());
        assertEquals(SOURCE_ID, decoded.sourceTopicId());
        assertEquals(TRACKER_ID, decoded.trackerTopicId());
        assertEquals(
                List.of(new SidecarEntry(5, 10, 1_000, false), new SidecarEntry(7, 13, 900, true)), decoded.entries());
    }

    @Test
    void headerOnlyRoundTrip() {
        EncodedCursor encoded = encoder(3072).finish(17);
        assertEquals(0, encoded.encodedCount());
        assertFalse(encoded.overflowed());
        assertEquals(17, encoded.cursorOffset());
        assertFalse(encoded.metadata().isEmpty(), "the identity header is always present");

        DecodedSidecar decoded = SidecarCodec.decode(encoded.metadata());
        assertEquals(CLUSTER_ID, decoded.clusterId());
        assertEquals(SOURCE_ID, decoded.sourceTopicId());
        assertEquals(TRACKER_ID, decoded.trackerTopicId());
        assertTrue(decoded.entries().isEmpty());
    }

    @Test
    void roundTripWithNegativeDeltasAndClampedMix() {
        Encoder encoder = encoder(3072);
        List<SidecarEntry> entries = List.of(
                new SidecarEntry(0, 0, -5, true),
                new SidecarEntry(1, 7, 1_700_000_000_000L, false),
                new SidecarEntry(100, 8, 12, true),
                new SidecarEntry(101, 1_000_000, Long.MAX_VALUE, false),
                new SidecarEntry(1_000_000_000_000L, 1_000_001, Long.MIN_VALUE, true));
        for (SidecarEntry e : entries) {
            assertTrue(encoder.offer(e.trackerAddOffset(), e.sourceOffset(), e.dispatchAtMs(), e.clamped()));
        }
        EncodedCursor encoded = encoder.finish(2_000_000_000_000L);
        DecodedSidecar decoded = SidecarCodec.decode(encoded.metadata());
        assertEquals(entries, decoded.entries());
        assertTrue(encoded.encodedBytes() <= 3072, "Base64 size accounting holds the budget");
    }

    /**
     * Boundary math for the golden entries: header = 36 raw bytes, each entry 5 raw bytes. The
     * raw limit is {@code floor(3*budget/4)}: budget 62 -> 46 (both entries fit exactly), 61 ->
     * 45 (the second entry is the greedy cut), 54 -> 40 (nothing fits: min-pending fallback).
     */
    @Test
    void greedyCutAtExactBudgetBoundary() {
        Encoder exact = encoder(62);
        assertTrue(exact.offer(5, 10, 1_000, false));
        assertTrue(exact.offer(7, 13, 900, true));
        EncodedCursor both = exact.finish(42);
        assertEquals(2, both.encodedCount());
        assertFalse(both.overflowed());
        assertEquals(42, both.cursorOffset());
        assertTrue(both.encodedBytes() <= 62);

        Encoder oneByteShort = encoder(61);
        assertTrue(oneByteShort.offer(5, 10, 1_000, false));
        assertFalse(oneByteShort.offer(7, 13, 900, true), "the NEXT entry would exceed the budget");
        EncodedCursor cut = oneByteShort.finish(42);
        assertEquals(1, cut.encodedCount());
        assertTrue(cut.overflowed());
        assertEquals(7, cut.cursorOffset(), "overflow => first NON-encoded pending trackerAddOffset");
        assertTrue(cut.encodedBytes() <= 61);
        assertEquals(
                List.of(new SidecarEntry(5, 10, 1_000, false)),
                SidecarCodec.decode(cut.metadata()).entries());
    }

    @Test
    void overflowFallbackEqualsMinPendingWatermark() {
        Encoder encoder = encoder(54); // raw limit 40 < header(36) + first entry(5)
        assertFalse(encoder.offer(5, 10, 1_000, false), "nothing fits");
        assertFalse(encoder.offer(7, 13, 900, true), "stop signal is sticky");
        EncodedCursor encoded = encoder.finish(42);
        assertEquals(0, encoded.encodedCount());
        assertTrue(encoded.overflowed());
        assertEquals(5, encoded.cursorOffset(), "degenerates to the classic min-pending watermark");
        assertTrue(SidecarCodec.decode(encoded.metadata()).entries().isEmpty());
    }

    @Test
    void budgetTooSmallForIdentityHeaderIsAConfigError() {
        assertThrows(IllegalArgumentException.class, () -> encoder(47)); // raw limit 35 < header 36
    }

    @Test
    void outOfOrderOffersThrow() {
        Encoder byTracker = encoder(3072);
        assertTrue(byTracker.offer(10, 10, 0, false));
        assertThrows(IllegalStateException.class, () -> byTracker.offer(10, 11, 0, false));

        Encoder bySource = encoder(3072);
        assertTrue(bySource.offer(10, 10, 0, false));
        assertThrows(IllegalStateException.class, () -> bySource.offer(11, 9, 0, false));

        Encoder negative = encoder(3072);
        assertThrows(IllegalStateException.class, () -> negative.offer(-1, 0, 0, false));
    }

    @Test
    void unknownVersionIsRejected() {
        byte[] raw = HexFormat.of().parseHex(GOLDEN_HEX);
        raw[0] = 0x02;
        String metadata = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(metadata));
        assertTrue(e.getMessage().contains("version"), e.getMessage());
    }

    @Test
    void malformedInputIsAnExplicitError() {
        // Not Base64 at all.
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode("%%%not-base64%%%"));
        // Empty blob.
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(""));
        // Truncated inside the identity header.
        byte[] full = HexFormat.of().parseHex(GOLDEN_HEX);
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(base64(full, 20)));
        // Truncated inside an entry (header + first entry minus its flags byte).
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(base64(full, 36 + 4)));
        // Truncated inside a varint (header + one dangling continuation byte).
        byte[] dangling = new byte[37];
        System.arraycopy(full, 0, dangling, 0, 36);
        dangling[36] = (byte) 0x80;
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(base64(dangling, 37)));
    }

    @Test
    void tenByteVarintWithHighBitsInTheFinalByteIsRejected() {
        // A 10-byte varint's final byte may only contribute bit 63: any higher value bit would
        // be shifted out of range, and silently dropping it would decode a corrupted blob to a
        // WRONG value instead of failing fast (§3.6 posture). Self-produced blobs never hit
        // this — the case exists exactly for corrupted/forged metadata.
        byte[] raw = HexFormat.of()
                .parseHex("0102636b"
                        + "0000000000000001" + "0000000000000002"
                        + "0000000000000003" + "0000000000000004"
                        + "80808080808080808002"); // 10-byte varint, final byte 0x02
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(base64(raw, raw.length)));
        assertTrue(e.getMessage().contains("overflows 64 bits"), e.getMessage());
    }

    @Test
    void nonIncreasingEntriesAreRejected() {
        // Header + entry(trk=5,src=10) + entry with zero tracker delta.
        byte[] raw = HexFormat.of()
                .parseHex("0102636b"
                        + "0000000000000001" + "0000000000000002"
                        + "0000000000000003" + "0000000000000004"
                        + "050a0000"
                        + "00010000");
        assertThrows(IllegalArgumentException.class, () -> SidecarCodec.decode(base64(raw, raw.length)));
    }

    @Test
    void unknownEntryFlagBitsAreIgnoredForwardCompatibly() {
        byte[] raw = HexFormat.of()
                .parseHex("0102636b"
                        + "0000000000000001" + "0000000000000002"
                        + "0000000000000003" + "0000000000000004"
                        + "050a00" + "83"); // flags 0x83: clamped + two unknown bits
        DecodedSidecar decoded = SidecarCodec.decode(base64(raw, raw.length));
        assertEquals(List.of(new SidecarEntry(5, 10, 0, true)), decoded.entries());
    }

    private static String base64(byte[] raw, int length) {
        byte[] slice = new byte[length];
        System.arraycopy(raw, 0, slice, 0, length);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(slice);
    }
}
