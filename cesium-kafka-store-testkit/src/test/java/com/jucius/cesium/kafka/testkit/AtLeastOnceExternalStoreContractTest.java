package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.ExternalSchedulerStore;
import com.jucius.cesium.kafka.api.store.StoreContext;
import java.util.Map;

/**
 * Proves {@link ExternalSchedulerStoreContract} is satisfiable in the pure <strong>at-least-once</strong>
 * mode: the reference {@link InMemoryExternalSchedulerStore} runs with cursor reconciliation disabled,
 * so {@code capabilities()} declares {@code AT_LEAST_ONCE}, the reconciliation tests are skipped, and
 * the crash-without-markDispatched test asserts the entry reappears and re-dispatches.
 */
class AtLeastOnceExternalStoreContractTest extends ExternalSchedulerStoreContract {

    @Override
    protected ExternalSchedulerStore createStore(StoreContext context) {
        InMemoryExternalSchedulerStore store = new InMemoryExternalSchedulerStore();
        store.configure(context);
        store.validate();
        store.start();
        return store;
    }

    @Override
    protected Map<String, String> storeProperties() {
        return Map.of(InMemoryExternalSchedulerStore.RECONCILIATION_KEY, "false");
    }
}
