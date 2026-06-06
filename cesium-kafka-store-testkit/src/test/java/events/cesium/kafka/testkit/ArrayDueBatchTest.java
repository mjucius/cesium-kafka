package events.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.store.DueBatch;
import org.junit.jupiter.api.Test;

class ArrayDueBatchTest {

    @Test
    void emptyBatch() {
        assertEquals(0, ArrayDueBatch.EMPTY.size());
        assertThrows(IndexOutOfBoundsException.class, () -> ArrayDueBatch.EMPTY.sourceOffset(0));
    }

    @Test
    void clampedDefaultsToFalseOnNonOverridingImplementations() {
        // Pins the M3 additive SPI evolution: DueBatch.clamped(i) must default to false so
        // implementations predating the method (external stores, engine views) stay
        // behavior-compatible.
        DueBatch legacy = new DueBatch() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public int sourcePartition(int i) {
                return 0;
            }

            @Override
            public long sourceOffset(int i) {
                return 42;
            }

            @Override
            public long dispatchAtMs(int i) {
                return 1_000;
            }

            @Override
            public long trackerOffset(int i) {
                return -1;
            }
        };
        assertFalse(legacy.clamped(0), "the additive default must be false");
    }

    @Test
    void builderGrowsAndPreservesOrder() {
        ArrayDueBatch.Builder builder = ArrayDueBatch.builder();
        for (int i = 0; i < 20; i++) {
            builder.add(i % 3, 100 + i, 1_000 + i, 10 + i, i % 2 == 0);
        }
        ArrayDueBatch batch = builder.build();
        assertEquals(20, batch.size());
        for (int i = 0; i < 20; i++) {
            assertEquals(i % 3, batch.sourcePartition(i));
            assertEquals(100 + i, batch.sourceOffset(i));
            assertEquals(1_000 + i, batch.dispatchAtMs(i));
            assertEquals(10 + i, batch.trackerOffset(i));
            assertEquals(i % 2 == 0, batch.clamped(i));
        }
    }

    @Test
    void snapshotCopiesEveryField() {
        ArrayDueBatch source = ArrayDueBatch.builder()
                .add(1, 7, 70, 700, true)
                .add(2, 8, 80, 800, false)
                .build();
        ArrayDueBatch copy = ArrayDueBatch.snapshot(source);
        assertEquals(2, copy.size());
        assertEquals(1, copy.sourcePartition(0));
        assertEquals(7, copy.sourceOffset(0));
        assertEquals(70, copy.dispatchAtMs(0));
        assertEquals(700, copy.trackerOffset(0));
        assertTrue(copy.clamped(0));
        assertEquals(8, copy.sourceOffset(1));
        assertFalse(copy.clamped(1));
    }

    @Test
    void selectBuildsSubBatchViewsInGivenOrder() {
        ArrayDueBatch source = ArrayDueBatch.builder()
                .add(0, 1, 10, 100, false)
                .add(0, 2, 20, 200, true)
                .add(0, 3, 30, 300, false)
                .add(0, 4, 40, 400, false)
                .build();
        ArrayDueBatch view = ArrayDueBatch.select(source, 3, 1);
        assertEquals(2, view.size());
        assertEquals(4, view.sourceOffset(0));
        assertEquals(2, view.sourceOffset(1));
        assertTrue(view.clamped(1));
        assertThrows(IndexOutOfBoundsException.class, () -> view.sourceOffset(2));
    }
}
