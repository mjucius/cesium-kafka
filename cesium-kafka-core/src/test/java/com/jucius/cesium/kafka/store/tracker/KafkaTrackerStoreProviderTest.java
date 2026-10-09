package com.jucius.cesium.kafka.store.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.SchedulerStoreProvider;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** Discovery contract (design §4.5): ServiceLoader registration with the stable type id. */
class KafkaTrackerStoreProviderTest {

    @Test
    void serviceLoaderDiscoversTheProvider() {
        boolean found = false;
        for (SchedulerStoreProvider provider : ServiceLoader.load(SchedulerStoreProvider.class)) {
            if (KafkaTrackerStoreProvider.TYPE_ID.equals(provider.typeId())) {
                assertInstanceOf(KafkaTrackerStoreProvider.class, provider);
                found = true;
            }
        }
        assertTrue(found, "META-INF/services must register KafkaTrackerStoreProvider");
    }

    @Test
    void createReturnsFreshUnconfiguredStores() {
        KafkaTrackerStoreProvider provider = new KafkaTrackerStoreProvider();
        assertEquals("kafka-tracker", provider.typeId());
        var first = provider.create();
        var second = provider.create();
        assertInstanceOf(KafkaTrackerStore.class, first);
        assertNotSame(first, second, "each engine lifecycle gets its own instance");
    }
}
