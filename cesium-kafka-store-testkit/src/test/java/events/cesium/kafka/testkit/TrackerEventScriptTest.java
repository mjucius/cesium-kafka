package events.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import events.cesium.kafka.testkit.TrackerEventScript.PendingTruth;
import events.cesium.kafka.testkit.TrackerEventScript.ScriptRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class TrackerEventScriptTest {

    private static final StubTrackerStore CODEC = new StubTrackerStore();

    private static TrackerEventScript script() {
        // offsets:        0          1            2          3          4            5
        return TrackerEventScript.forPartition(3)
                .add(10, 1_000)
                .complete(10)
                .add(11, 2_000, true)
                .add(12, 3_000)
                .complete(12)
                .add(13, 4_000);
    }

    @Test
    void recordsCarryOriginalOffsetsAndStoreEncodedBytes() {
        List<ScriptRecord> records = script().records(CODEC);
        assertEquals(6, records.size());
        for (int i = 0; i < records.size(); i++) {
            assertEquals(i, records.get(i).trackerOffset());
        }
        assertEquals(10, StubTrackerStore.sourceOffsetOf(records.get(0).key()));
        assertNotNull(records.get(0).value(), "ADD records have values");
        assertEquals(10, StubTrackerStore.sourceOffsetOf(records.get(1).key()));
        assertNull(records.get(1).value(), "COMPLETE records are tombstones");
        byte[] clampedValue = records.get(2).value();
        assertNotNull(clampedValue);
        assertEquals(1, clampedValue[8], "the clamp marker reaches the store's encoder");
    }

    @Test
    void truthIsAddsWithoutLaterCompletes() {
        assertEquals(
                List.of(new PendingTruth(11, 2_000, 2, true), new PendingTruth(13, 4_000, 5, false)),
                script().expectedPending());
    }

    @Test
    void compactAddsOfCompletedPairsKeepsOrphanTombstones() {
        TrackerEventScript compacted = script().compactAddsOfCompletedPairs();
        List<ScriptRecord> records = compacted.records(CODEC);
        // ADDs of 10 and 12 removed; their tombstones and the live ADDs remain.
        assertEquals(4, records.size());
        assertEquals(1, records.get(0).trackerOffset());
        assertNull(records.get(0).value());
        assertEquals(2, records.get(1).trackerOffset());
        assertEquals(4, records.get(2).trackerOffset());
        assertNull(records.get(2).value());
        assertEquals(5, records.get(3).trackerOffset());
        assertEquals(script().expectedPending(), compacted.expectedPending(), "compaction never changes truth");
        assertEquals(6, compacted.replayBarrier());
        assertEquals(6, compacted.endOffset());
    }

    @Test
    void compactCompletedPairsRemovesBothSides() {
        TrackerEventScript compacted = script().compactCompletedPairs();
        List<ScriptRecord> records = compacted.records(CODEC);
        assertEquals(2, records.size());
        assertEquals(2, records.get(0).trackerOffset());
        assertEquals(11, StubTrackerStore.sourceOffsetOf(records.get(0).key()));
        assertEquals(5, records.get(1).trackerOffset());
        assertEquals(13, StubTrackerStore.sourceOffsetOf(records.get(1).key()));
        assertEquals(script().expectedPending(), compacted.expectedPending());
        assertEquals(6, compacted.replayBarrier(), "the live tail survives: barrier = last survivor + 1");
    }

    @Test
    void barrierShrinksWhenTheTailIsCompactedAway() {
        TrackerEventScript tailCompleted =
                TrackerEventScript.forPartition(0).add(1, 100).add(2, 200).complete(2);
        TrackerEventScript compacted = tailCompleted.compactCompletedPairs();
        assertEquals(1, compacted.replayBarrier(), "only the offset-0 ADD survives");
        assertEquals(3, compacted.endOffset());
        assertEquals(List.of(new PendingTruth(1, 100, 0, false)), compacted.expectedPending());
    }

    @Test
    void invalidRecordsPassThroughVerbatimAndNeverReachTruth() {
        byte[] key = {1, 2, 3};
        byte[] value = {(byte) 0xBA, (byte) 0xD0};
        TrackerEventScript script =
                TrackerEventScript.forPartition(0).add(7, 100).invalid(key, value);
        List<ScriptRecord> records = script.records(CODEC);
        assertEquals(2, records.size());
        assertArrayEquals(key, records.get(1).key());
        assertArrayEquals(value, records.get(1).value());
        assertEquals(List.of(new PendingTruth(7, 100, 0, false)), script.expectedPending());
    }

    @Test
    void emptyScriptHasZeroBarrierAndEmptyTruth() {
        TrackerEventScript empty = TrackerEventScript.forPartition(0);
        assertEquals(0, empty.replayBarrier());
        assertEquals(0, empty.endOffset());
        assertEquals(List.of(), empty.expectedPending());
        assertEquals(List.of(), empty.records(CODEC));
    }
}
