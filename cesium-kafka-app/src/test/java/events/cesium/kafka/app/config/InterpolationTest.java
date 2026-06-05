package events.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.ConfigValidationException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@code ${env:VAR}} interpolation inside string values — secrets stay out of files (design §8). */
class InterpolationTest {

    @TempDir
    Path dir;

    @Test
    void envReferenceIsReplacedInsideStringValues() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        kafka:
                          properties:
                            sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="cesium" password="${env:KAFKA_PASSWORD}";
                        """);
        CesiumConfig config = LoaderTestSupport.loader(Map.of("KAFKA_PASSWORD", "hunter2"))
                .load(file)
                .config();
        String jaas = config.kafka().properties().get("sasl.jaas.config");
        assertTrue(jaas.contains("password=\"hunter2\""), jaas);
    }

    @Test
    void multipleReferencesInOneValueAllResolve() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        store:
                          properties:
                            combined: ${env:A}-${env:B}
                        """);
        CesiumConfig config = LoaderTestSupport.loader(Map.of("A", "left", "B", "right"))
                .load(file)
                .config();
        assertEquals("left-right", config.store().properties().get("combined"));
    }

    @Test
    void interpolationAppliesToOverlayValuesToo() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of(
                        "CESIUM_ROUTE__SOURCE__TOPIC", "${env:TOPIC_NAME}",
                        "TOPIC_NAME", "interpolated-topic"))
                .load(file)
                .config();
        assertEquals("interpolated-topic", config.route().source().topic());
    }

    @Test
    void undefinedVariableIsAStartupErrorNamingThePathAndVariable() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        kafka:
                          properties:
                            sasl.jaas.config: ${env:MISSING_SECRET}
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("MISSING_SECRET"), exception::getMessage);
        assertTrue(exception.getMessage().contains("sasl.jaas.config"), exception::getMessage);
    }

    @Test
    void plainStringsAreLeftUntouched() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        store:
                          properties:
                            plain: no interpolation here
                            dollar: cost is $100 {not a reference}
                        """);
        CesiumConfig config = LoaderTestSupport.loader(Map.of()).load(file).config();
        assertEquals("no interpolation here", config.store().properties().get("plain"));
        assertEquals(
                "cost is $100 {not a reference}", config.store().properties().get("dollar"));
    }
}
