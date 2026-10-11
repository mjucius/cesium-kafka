package com.jucius.cesium.kafka.store.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** jqwik properties for the tracker wire format (design §11.1). */
class TrackerWireFormatProperties {

    /** Every source offset survives the key round trip exactly. */
    @Property
    void keyRoundTrips(@ForAll long sourceOffset) {
        assertEquals(sourceOffset, TrackerWireFormat.decodeKey(TrackerWireFormat.encodeKey(sourceOffset)));
    }

    /** Every (dispatchAtMs, clamped) pair survives the ADD round trip exactly. */
    @Property
    void addRoundTrips(@ForAll long dispatchAtMs, @ForAll boolean clamped) {
        TrackerDecodeResult result = TrackerWireFormat.decode(
                TrackerWireFormat.encodeKey(0),
                TrackerWireFormat.encodeAddValue(dispatchAtMs, clamped),
                new RecordHeaders());
        assertEquals(new TrackerDecodeResult.Add(dispatchAtMs, clamped), result);
    }

    /**
     * Whatever a future writer puts in the unknown flag bits, decode preserves the dispatch
     * instant and derives {@code clamped} from bit 0 alone (forward compatibility, design §2.2).
     */
    @Property
    void unknownFlagBitsNeverChangeTheDecode(@ForAll long dispatchAtMs, @ForAll byte flags) {
        byte[] value = TrackerWireFormat.encodeAddValue(dispatchAtMs, false);
        value[3] = flags;
        TrackerDecodeResult result =
                TrackerWireFormat.decode(TrackerWireFormat.encodeKey(0), value, new RecordHeaders());
        boolean clamped = (flags & TrackerWireFormat.FLAG_CLAMPED) != 0;
        assertEquals(new TrackerDecodeResult.Add(dispatchAtMs, clamped), result);
    }

    /** Decode is total: arbitrary garbage never throws, it classifies (count-and-skip, §2.2). */
    @Property
    void decodeNeverThrowsOnGarbage(@ForAll @Size(max = 64) byte[] key, @ForAll @Size(max = 64) byte[] value) {
        assertNotNull(TrackerWireFormat.decode(key, value, new RecordHeaders()));
    }

    /**
     * Every structurally valid tombstone with a <em>present</em> reason header completes —
     * known reasons as {@code Complete}, anything else as {@code CompleteUnknownReason} — never
     * {@code Invalid}: skipping a real completion is a duplicate-dispatch vector, and a future
     * writer's new reason constant must stay a completion on this reader.
     */
    @Property
    void presentReasonTombstonesAlwaysComplete(@ForAll @Size(max = 32) byte[] reasonBytes) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(TrackerWireFormat.COMPLETION_REASON_HEADER, reasonBytes);
        TrackerDecodeResult result = TrackerWireFormat.decode(TrackerWireFormat.encodeKey(0), null, headers);
        boolean completes = result instanceof TrackerDecodeResult.Complete
                || result instanceof TrackerDecodeResult.CompleteUnknownReason;
        assertTrue(completes, () -> "expected a completion, got " + result);
    }
}
