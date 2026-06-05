package events.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The {@code random} literal is the explicit opt-in to non-stable ids (design D10). */
class InstanceIdTest {

    @Test
    void randomLiteralIsRecognized() {
        assertTrue(InstanceId.of("random").isRandom());
    }

    @Test
    void stableIdsAreNotRandom() {
        assertFalse(InstanceId.of("slot-0").isRandom());
        assertFalse(InstanceId.of("0").isRandom());
    }

    @Test
    void randomLiteralIsCaseSensitive() {
        // The opt-in is a documented literal, not a fuzzy match.
        assertFalse(InstanceId.of("RANDOM").isRandom());
        assertFalse(InstanceId.of("Random").isRandom());
    }

    @Test
    void absentValueMaterializesAsBlank() {
        assertTrue(new InstanceId(null).isBlank());
        assertTrue(InstanceId.of("  ").isBlank());
        assertFalse(InstanceId.of("slot-0").isBlank());
    }
}
