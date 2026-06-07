package com.jucius.cesium.kafka.core.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The crash-point seam: inert by default, observable and crash-capable when installed. */
class CrashPointsTest {

    static {
        System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
    }

    @AfterEach
    void reset() {
        CrashPoints.reset();
    }

    @Test
    void noOpWithoutHandler() {
        CrashPoints.maybeFire(CrashPoints.INGEST_BEFORE_BEGIN); // must not throw
    }

    @Test
    void installRefusesWithoutTheGuardProperty() {
        // core is a published artifact: without the explicit opt-in property, no classpath
        // dependency can arm crash points in a production engine.
        System.clearProperty(CrashPoints.ENABLE_PROPERTY);
        try {
            IllegalStateException refusal =
                    assertThrows(IllegalStateException.class, () -> CrashPoints.install(ignored -> {}));
            assertTrue(refusal.getMessage().contains(CrashPoints.ENABLE_PROPERTY), refusal.getMessage());
            CrashPoints.maybeFire(CrashPoints.INGEST_BEFORE_BEGIN); // still inert
        } finally {
            System.setProperty(CrashPoints.ENABLE_PROPERTY, "true");
        }
    }

    @Test
    void firesInstalledHandlerWithPointId() {
        List<String> seen = new ArrayList<>();
        CrashPoints.install(seen::add);
        CrashPoints.maybeFire(CrashPoints.INGEST_AFTER_SENDS);
        CrashPoints.maybeFire(CrashPoints.INGEST_DURING_COMMIT);
        assertEquals(List.of("I-2", "I-4"), seen);
    }

    @Test
    void resetUninstalls() {
        List<String> seen = new ArrayList<>();
        CrashPoints.install(seen::add);
        CrashPoints.reset();
        CrashPoints.maybeFire(CrashPoints.INGEST_BEFORE_BEGIN);
        assertTrue(seen.isEmpty());
    }

    @Test
    void simulatedCrashIsAnErrorSoTaxonomyHandlersNeverCatchIt() {
        CrashPoints.install(id -> {
            throw new CrashPoints.SimulatedCrash(id);
        });
        Error crash = assertThrows(
                CrashPoints.SimulatedCrash.class, () -> CrashPoints.maybeFire(CrashPoints.INGEST_AFTER_SEND_OFFSETS));
        assertTrue(crash.getMessage().contains("I-3"));
    }
}
