package com.jucius.cesium.kafka.store.tracker;

import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.testkit.FakeStoreContext;
import com.jucius.cesium.kafka.testkit.TrackerBackedStoreContract;

/**
 * Runs every test and property of {@link TrackerBackedStoreContract} against
 * {@link KafkaTrackerStore} (design §11.2).
 */
class KafkaTrackerStoreContractTest extends TrackerBackedStoreContract {

    @Override
    protected TrackerBackedStore createStore(FakeStoreContext context) {
        FakeStoreContext.Identity id = context.identity();
        return new KafkaTrackerStore(
                id.clusterId(),
                id.sourceTopicId(),
                id.trackerTopicId(),
                context.sidecarMaxBytes(),
                context.heapBudgetBytes(),
                context.meterRegistry());
    }
}
