package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.ConfigValidationException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Binding-failure message quality (design §11.1): a bad ISO-8601 duration must name the key path,
 * the offending value, and show what a correct value looks like — duration typos are the most
 * common operator mistake.
 */
class BindingErrorMessageTest {

    @TempDir
    Path dir;

    @Test
    void badIsoDurationNamesPathValueAndExpectedFormat() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        delay:
                          max: tomorrow
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        String message = exception.getMessage();
        assertTrue(message.contains("delay.max"), message);
        assertTrue(message.contains("tomorrow"), message);
        assertTrue(message.contains("ISO-8601"), message);
        assertTrue(message.contains("PT30S"), message);
    }

    @Test
    void badDurationInNestedDispatchPathIsAttributed() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        dispatch:
                          fetch:
                            timeout: 30 seconds
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        String message = exception.getMessage();
        assertTrue(message.contains("dispatch.fetch.timeout"), message);
        assertTrue(message.contains("30 seconds"), message);
        assertTrue(message.contains("ISO-8601"), message);
    }

    @Test
    void badDurationViaEnvOverrideIsAttributed() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of("CESIUM_DELAY__MAX", "1 day"))
                        .load(file));
        String message = exception.getMessage();
        assertTrue(message.contains("delay.max"), message);
        assertTrue(message.contains("1 day"), message);
        assertTrue(message.contains("ISO-8601"), message);
    }

    @Test
    void unknownEnumValueIsAttributedToItsPath() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        delay:
                          on-over-max: EXPLODE
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        String message = exception.getMessage();
        assertTrue(message.contains("delay.on-over-max"), message);
        assertTrue(message.contains("EXPLODE"), message);
    }
}
