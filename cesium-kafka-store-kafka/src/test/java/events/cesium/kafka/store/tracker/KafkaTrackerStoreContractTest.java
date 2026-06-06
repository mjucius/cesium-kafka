package events.cesium.kafka.store.tracker;

import events.cesium.kafka.api.store.StoreContext;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.testkit.TrackerBackedStoreContract;
import java.util.Map;
import java.util.Optional;

/**
 * Proof that the flagship store passes the published contract kit through the exact door third
 * parties use (design §11.2, D13): every test and property of
 * {@link TrackerBackedStoreContract} runs against {@link KafkaTrackerStore} with nothing but the
 * public SPI in between.
 */
class KafkaTrackerStoreContractTest extends TrackerBackedStoreContract {

    @Override
    protected TrackerBackedStore createStore(StoreContext context) {
        KafkaTrackerStore store = new KafkaTrackerStore();
        store.configure(context);
        store.validate();
        store.start();
        return store;
    }

    @Override
    protected Map<String, String> storeProperties() {
        // Deterministic heap budget: the validate() worst-case footprint check must not depend on
        // the test JVM's -Xmx (1 GiB covers partitionCount x default max-pending x 64 B).
        return Map.of(KafkaTrackerStore.HEAP_BUDGET_BYTES_KEY, Long.toString(1L << 30));
    }

    @Override
    protected Optional<Map<String, String>> overflowForcingProperties() {
        // The smallest supported sidecar budget (128 B Base64 => 96 raw bytes) combined with the
        // contract's 60-byte OVERFLOW_CLUSTER_ID (94-byte identity header) leaves 2 raw bytes —
        // less than any encoded entry — so the cursor must degenerate to the min-pending
        // watermark (§3.5 overflow fallback).
        return Optional.of(Map.of(KafkaTrackerStore.SIDECAR_MAX_BYTES_KEY, "128"));
    }
}
