package com.jucius.cesium.kafka.store.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.store.tracker.SidecarCodec.DecodedSidecar;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.EncodedCursor;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.Encoder;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.SidecarEntry;
import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.kafka.common.Uuid;

/** Property tests: every encodable pending set round-trips bit-exactly through the sidecar. */
class SidecarCodecProperties {

    record EntrySeed(long trackerGap, long sourceGap, long dispatchAtMs, boolean clamped) {}

    @Property(tries = 300)
    void roundTrip(
            @ForAll("entrySeeds") List<EntrySeed> seeds,
            @ForAll("clusterIds") String clusterId,
            @ForAll long sourceMsb,
            @ForAll long sourceLsb,
            @ForAll long trackerMsb,
            @ForAll long trackerLsb) {
        Uuid sourceId = new Uuid(sourceMsb, sourceLsb);
        Uuid trackerId = new Uuid(trackerMsb, trackerLsb);
        List<SidecarEntry> entries = materialize(seeds);

        Encoder encoder = SidecarCodec.encoder(clusterId, sourceId, trackerId, 1 << 18);
        for (SidecarEntry entry : entries) {
            assertTrue(encoder.offer(
                    entry.trackerAddOffset(), entry.sourceOffset(), entry.dispatchAtMs(), entry.clamped()));
        }
        EncodedCursor encoded = encoder.finish(123_456_789L);
        assertFalse(encoded.overflowed());
        assertEquals(entries.size(), encoded.encodedCount());
        assertEquals(123_456_789L, encoded.cursorOffset());
        assertTrue(encoded.encodedBytes() <= (1 << 18), "Base64 size within budget");

        DecodedSidecar decoded = SidecarCodec.decode(encoded.metadata());
        assertEquals(clusterId, decoded.clusterId());
        assertEquals(sourceId, decoded.sourceTopicId());
        assertEquals(trackerId, decoded.trackerTopicId());
        assertEquals(entries, decoded.entries());
    }

    /** Strictly increasing offsets are built from positive gaps so generation cannot violate order. */
    private static List<SidecarEntry> materialize(List<EntrySeed> seeds) {
        List<SidecarEntry> entries = new ArrayList<>(seeds.size());
        long trackerAddOffset = -1;
        long sourceOffset = -1;
        for (EntrySeed seed : seeds) {
            trackerAddOffset += seed.trackerGap();
            sourceOffset += seed.sourceGap();
            entries.add(new SidecarEntry(trackerAddOffset, sourceOffset, seed.dispatchAtMs(), seed.clamped()));
        }
        return entries;
    }

    @Provide
    Arbitrary<List<EntrySeed>> entrySeeds() {
        Arbitrary<Long> gap = Arbitraries.longs().between(1, 1_000_000_000L);
        Arbitrary<Long> dispatchAt = Arbitraries.oneOf(
                Arbitraries.longs(), Arbitraries.longs().between(1_600_000_000_000L, 1_900_000_000_000L));
        Arbitrary<Boolean> clamped = Arbitraries.of(true, false);
        return Combinators.combine(gap, gap, dispatchAt, clamped)
                .as(EntrySeed::new)
                .list()
                .ofMaxSize(400);
    }

    @Provide
    Arbitrary<String> clusterIds() {
        // Cluster ids are ASCII in practice (KRaft: 22-char base64url); unpaired surrogates would
        // not survive UTF-8 anyway, which is a String.getBytes property, not a codec one.
        return Arbitraries.strings().ascii().ofMaxLength(40);
    }
}
