package events.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.SplittableRandom;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.openjdk.jol.info.GraphLayout;

/**
 * JOL footprint gate (design §11.4, post-approval revision 1: ≤ 56 B per pending entry). The
 * shard is built with a realistic mix — geometric growth left as-is (no pre-sizing), the heap
 * fully populated, and ~5% completed-held entries pinning log slots and stale heap copies.
 *
 * <p>The 10M variant is tagged {@code soak} and disabled by default; run it with:
 * {@code CESIUM_SOAK=true ./gradlew :cesium-kafka-store-kafka:test --tests '*ShardFootprintTest*'}.
 */
class ShardFootprintTest {

    private static final double MAX_BYTES_PER_ENTRY = 56.0;

    @Test
    void oneMillionPendingEntriesStayUnderJolBudget() {
        assertFootprint(1_000_000);
    }

    @Test
    @Tag("soak")
    @EnabledIfEnvironmentVariable(named = "CESIUM_SOAK", matches = "true")
    void tenMillionPendingEntriesStayUnderJolBudget() {
        assertFootprint(10_000_000);
    }

    private static void assertFootprint(int pendingTarget) {
        int completedHeld = pendingTarget / 20; // ~5% completed-held, realistic steady-state mix
        PartitionShard shard = buildShard(pendingTarget, completedHeld);
        assertEquals(pendingTarget, shard.pendingCount());
        assertEquals(completedHeld, shard.completedHeldInLog());

        long totalBytes = GraphLayout.parseInstance(shard).totalSize();
        double perEntry = (double) totalBytes / pendingTarget;
        System.out.printf(
                "JOL footprint: pending=%,d completedHeld=%,d total=%,d bytes → %.2f B/entry (gate %.0f)%n",
                pendingTarget, completedHeld, totalBytes, perEntry, MAX_BYTES_PER_ENTRY);
        assertTrue(
                perEntry <= MAX_BYTES_PER_ENTRY,
                "JOL footprint " + perEntry + " B/entry exceeds the " + MAX_BYTES_PER_ENTRY + " B gate");
    }

    private static PartitionShard buildShard(int pendingTarget, int completedHeld) {
        PartitionShard shard = new PartitionShard();
        SplittableRandom random = new SplittableRandom(0xCE51_04CAFEL);
        long base = 1_700_000_000_000L;
        int total = pendingTarget + completedHeld;
        for (int i = 0; i < total; i++) {
            shard.applyAdd(i, base + random.nextLong(7L * 24 * 3600 * 1000), i);
        }
        // Complete a scattered subset pre-dispatch (skipping the head so they stay log-held and
        // their heap copies stay resident as lazy-deletion debt — the realistic worst mix).
        int stride = total / (completedHeld + 1);
        int completed = 0;
        for (long src = stride; src < total && completed < completedHeld; src += stride) {
            if (shard.applyComplete(src)) {
                completed++;
            }
        }
        assertEquals(completedHeld, completed, "test setup: scattered completes");
        return shard;
    }
}
