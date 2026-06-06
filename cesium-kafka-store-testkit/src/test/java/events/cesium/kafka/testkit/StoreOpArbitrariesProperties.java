package events.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Shape checks for the generators the contract's properties rely on. */
class StoreOpArbitrariesProperties {

    @Property(tries = 100)
    void sequencesAreWellFormed(@ForAll("ops") List<StoreOp> ops) {
        for (StoreOp op : ops) {
            switch (op) {
                case StoreOp.Add(int partition, int gap, long delayMs, boolean ignored) -> {
                    assertTrue(partition >= 0 && partition < 4);
                    assertTrue(gap >= 1, "source offsets must strictly increase (§3.1)");
                    assertTrue(delayMs >= -60_000 && delayMs <= 600_000);
                }
                case StoreOp.DuplicateAdd(int partition, int pickIndex, long ignored) -> {
                    assertTrue(partition >= 0 && partition < 4);
                    assertTrue(pickIndex >= 0);
                }
                case StoreOp.AdvanceTime(long deltaMs) -> assertTrue(deltaMs >= 0, "time only moves forward");
                case StoreOp.DrainCommit(int maxBatch, var ignored) -> assertTrue(maxBatch >= 1);
                case StoreOp.DrainCommitPartial(int maxBatch, int settleSeed, var ignored) -> {
                    assertTrue(maxBatch >= 2, "a strict subset needs at least two drained entries");
                    assertTrue(settleSeed >= 0);
                }
                case StoreOp.DrainAbort(int maxBatch) -> assertTrue(maxBatch >= 1);
                case StoreOp.IdleCommit(int partition) -> assertTrue(partition >= 0 && partition < 4);
                case StoreOp.Rerecover(int partition) -> assertTrue(partition >= 0 && partition < 4);
            }
        }
    }

    @Property(tries = 50)
    void compactionScriptsAreHealthyLogs(@ForAll("scripts") TrackerEventScript script) {
        // §3.1 shape: at most one ADD per source offset; completes only for prior, pending adds.
        Set<Long> seen = new HashSet<>();
        for (TrackerEventScript.PendingTruth truth : script.expectedPending()) {
            assertTrue(seen.add(truth.sourceOffset()), "duplicate pending source offset");
        }
        assertTrue(script.replayBarrier() <= script.endOffset());
        // Truth is invariant under both compaction transforms by construction.
        var compactedAdds = script.compactAddsOfCompletedPairs().expectedPending();
        var compactedPairs = script.compactCompletedPairs().expectedPending();
        assertTrue(compactedAdds.equals(script.expectedPending()));
        assertTrue(compactedPairs.equals(script.expectedPending()));
    }

    @Provide
    Arbitrary<List<StoreOp>> ops() {
        return StoreOpArbitraries.sequences(4);
    }

    @Provide
    Arbitrary<TrackerEventScript> scripts() {
        return StoreOpArbitraries.compactionScripts(0);
    }
}
