package events.cesium.kafka.testkit;

import events.cesium.kafka.api.store.ExternalSchedulerStore;
import events.cesium.kafka.api.store.StoreContext;

/**
 * Proves {@link ExternalSchedulerStoreContract} is satisfiable in the <strong>effectively-once</strong>
 * mode: the reference {@link InMemoryExternalSchedulerStore} runs with cursor reconciliation enabled
 * (the default), so the two reconciliation tests run and the at-least-once-reappearance test is
 * skipped by assumption.
 */
class InMemoryExternalStoreContractTest extends ExternalSchedulerStoreContract {

    @Override
    protected ExternalSchedulerStore createStore(StoreContext context) {
        InMemoryExternalSchedulerStore store = new InMemoryExternalSchedulerStore();
        store.configure(context);
        store.validate();
        store.start();
        return store;
    }

    @Override
    protected boolean supportsCursorReconciliation() {
        return true;
    }

    @Override
    protected void deliverReconciliationCursor(ExternalSchedulerStore store, int partition, String committedCursor) {
        ((InMemoryExternalSchedulerStore) store).reconcileFrom(partition, committedCursor);
    }
}
