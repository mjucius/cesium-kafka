package com.jucius.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class FakeStoreContextTest {

    @Test
    void mintsAFreshIdentityPerBuild() {
        FakeStoreContext a = FakeStoreContext.builder().build();
        FakeStoreContext b = FakeStoreContext.builder().build();
        assertNotEquals(a.identity().sourceTopicId(), b.identity().sourceTopicId());
        assertNotEquals(a.identity().trackerTopicId(), b.identity().trackerTopicId());
    }

    @Test
    void reusesAnExplicitIdentity() {
        FakeStoreContext a = FakeStoreContext.builder().build();
        FakeStoreContext b = FakeStoreContext.builder().identity(a.identity()).build();
        assertSame(a.identity(), b.identity(), "same identity models a restart of the same route");
    }
}
