package events.cesium.kafka.store.tracker;

import events.cesium.kafka.api.store.SchedulerStore;
import events.cesium.kafka.api.store.SchedulerStoreProvider;

/**
 * {@link java.util.ServiceLoader} discovery hook for {@link KafkaTrackerStore} (design §4.5).
 * Selection is always explicit: operators choose this store with {@code store.type:
 * kafka-tracker} — the engine never auto-selects.
 */
public final class KafkaTrackerStoreProvider implements SchedulerStoreProvider {

    /** The stable {@code store.type} id of the flagship store (§8 defaults table). */
    public static final String TYPE_ID = "kafka-tracker";

    @Override
    public String typeId() {
        return TYPE_ID;
    }

    @Override
    public SchedulerStore create() {
        return new KafkaTrackerStore();
    }
}
