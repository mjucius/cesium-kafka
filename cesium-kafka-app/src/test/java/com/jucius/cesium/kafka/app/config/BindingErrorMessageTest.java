package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.ConfigValidationException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Binding-failure message quality (design §11.1): a bad scalar must name the key path and show what
 * a correct value looks like. Since {@code ${env:VAR}} interpolation runs before binding, the
 * message must <em>not</em> echo the offending (post-interpolation) value — that could leak a secret
 * an operator mistakenly interpolated into a typed scalar (L6) — so these assert on the path and the
 * expected-format hint, and that the raw value is absent.
 */
class BindingErrorMessageTest {

    @TempDir
    Path dir;

    @Test
    void badIsoDurationNamesPathAndExpectedFormatWithoutEchoingValue() {
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
        assertTrue(message.contains("ISO-8601"), message);
        assertTrue(message.contains("PT30S"), message);
        assertFalse(message.contains("tomorrow"), message);
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
        assertTrue(message.contains("ISO-8601"), message);
        assertFalse(message.contains("30 seconds"), message);
    }

    @Test
    void badDurationViaEnvOverrideIsAttributed() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of("CESIUM_DELAY__MAX", "1 day"))
                        .load(file));
        String message = exception.getMessage();
        assertTrue(message.contains("delay.max"), message);
        assertTrue(message.contains("ISO-8601"), message);
        assertFalse(message.contains("1 day"), message);
    }

    @Test
    void unknownEnumValueIsAttributedToItsPathAndListsAcceptedValues() {
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
        // The accepted values are listed (helpful) while the rejected value is not echoed.
        assertTrue(message.contains("CLAMP"), message);
        assertFalse(message.contains("EXPLODE"), message);
    }

    @Test
    void interpolatedSecretIsNeverEchoedInABindingError() {
        // L6: an operator who mistakenly routes a secret into a typed scalar via ${env:VAR} and it
        // fails to parse must not see that secret echoed into ERROR-level startup logs.
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        delay:
                          max: ${env:LEAKY_SECRET}
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(
                                Map.of("LEAKY_SECRET", "p@ssw0rd-do-not-log"))
                        .load(file));
        String message = exception.getMessage();
        assertFalse(message.contains("p@ssw0rd-do-not-log"), message);
        assertTrue(message.contains("delay.max"), message);
        assertTrue(message.contains("ISO-8601"), message);
    }

    @Test
    void interpolatedSecretIsNeverEchoedInANonInvalidFormatBindingError() {
        // L6 residual: a secret routed into a STRUCTURED key (here a scalar where the kafka.properties
        // map is expected) fails binding with a MismatchedInputException, not an InvalidFormatException.
        // Jackson's original message embeds the offending value verbatim ("...from String value
        // ('<secret>')") — the fallback must scrub it just like the typed-scalar branch does.
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        kafka:
                          properties: ${env:LEAKY_SECRET}
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(
                                Map.of("LEAKY_SECRET", "p@ssw0rd-do-not-log"))
                        .load(file));
        String message = exception.getMessage();
        assertFalse(message.contains("p@ssw0rd-do-not-log"), message);
        // The field path is still attributed, and the structural cause is named without the value.
        assertTrue(message.contains("kafka.properties"), message);
        assertTrue(message.contains("could not be bound"), message);
    }
}
