package com.jucius.cesium.kafka.store.tracker;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;

/** Shared helpers for {@code KafkaTrackerStore} unit tests. */
final class StoreFixtures {

    /** An empty engine-synthesized batch (the records-free idle-advancement transaction shape). */
    static final DueBatch EMPTY_BATCH = new DueBatch() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public int sourcePartition(int i) {
            throw new IndexOutOfBoundsException("empty batch");
        }

        @Override
        public long sourceOffset(int i) {
            throw new IndexOutOfBoundsException("empty batch");
        }

        @Override
        public long dispatchAtMs(int i) {
            throw new IndexOutOfBoundsException("empty batch");
        }

        @Override
        public long trackerOffset(int i) {
            throw new IndexOutOfBoundsException("empty batch");
        }
    };

    private StoreFixtures() {}

    static KafkaTrackerStore startedStore(FixedIdentityStoreContext context) {
        KafkaTrackerStore store = new KafkaTrackerStore();
        store.configure(context);
        store.validate();
        store.start();
        return store;
    }

    /**
     * Feeds one schedule through the REAL ingest encoding and the engine's call path:
     * encodeSchedule -> headers-carrying onTrackerRecord.
     */
    static void feedSchedule(KafkaTrackerStore store, long trackerOffset, ScheduledRef ref) {
        TrackerRecordData data = store.encodeSchedule(ref);
        store.onTrackerRecord(ref.sourcePartition(), trackerOffset, data.key(), data.value(), data.headers());
    }

    /** Feeds one completion tombstone through the real wire encoding, reason header included. */
    static void feedComplete(KafkaTrackerStore store, int partition, long trackerOffset, long sourceOffset) {
        TrackerRecordData data = TrackerWireFormat.encodeCompletion(sourceOffset, CompletionReason.DISPATCHED);
        store.onTrackerRecord(partition, trackerOffset, data.key(), data.value(), data.headers());
    }
}
