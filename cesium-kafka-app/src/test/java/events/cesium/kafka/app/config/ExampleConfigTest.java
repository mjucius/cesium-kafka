package events.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import events.cesium.kafka.app.config.CesiumConfigLoader.LoadedConfig;
import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.core.config.ValidationContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Proves the shipped {@code config/cesium-example.yaml} (README quickstart, design §12) is a real,
 * loadable, valid configuration — not documentation that has drifted from the M1 schema. The example
 * is bound through the production {@link CesiumConfigLoader} (kebab keys, enums, durations, unknown-key
 * rejection) and run through the aggregate validator; only the broker-dependent {@code StartupValidator}
 * checks are deferred to the app integration test. Also exercises the container override path the
 * quickstart documents (an environment variable replacing a file value).
 */
class ExampleConfigTest {

    private static final Path EXAMPLE = locateExample();

    @Test
    void exampleConfigBindsAndValidates() {
        LoadedConfig loaded =
                new CesiumConfigLoader(Map.of(), new Properties(), ValidationContext.runtime(1)).load(EXAMPLE);

        // A clean load proves: parsed, no unknown keys, bound to records, aggregate-validated (it
        // throws ConfigValidationException otherwise). The report still carries WARN/INFO findings.
        assertFalse(loaded.report().hasErrors());
        assertTrue(
                loaded.report().infos().stream().anyMatch(f -> f.path().equals("dispatch.max-pending-per-partition")),
                "the §5.3 worst-case index-footprint INFO finding is always present");

        CesiumConfig config = loaded.config();
        assertEquals("orders-delay", config.applicationId());
        assertEquals("cesium-0", config.instanceId().value());
        assertEquals(Set.of(Role.INGEST, Role.DISPATCH), config.roles());
        assertEquals("orders", config.route().source().topic());
        assertEquals("orders-delayed", config.route().destination().topic());
        assertTrue(config.route().hasDlq());
        assertEquals("orders-dlq", config.route().dlq().orElseThrow().topic());
        assertEquals("kafka-tracker", config.store().type());
        assertEquals(Duration.ofHours(1), config.delay().max());
        assertEquals(8081, config.observability().port());
        assertEquals("localhost:9092", config.kafka().properties().get("bootstrap.servers"));
    }

    @Test
    void environmentOverridesAFileValue() {
        // The documented container override: CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS replaces the
        // file's localhost default without editing the mounted file (precedence: file < env).
        LoadedConfig loaded = new CesiumConfigLoader(
                        Map.of("CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS", "kafka:9092"),
                        new Properties(),
                        ValidationContext.runtime(1))
                .load(EXAMPLE);
        assertEquals("kafka:9092", loaded.config().kafka().properties().get("bootstrap.servers"));
    }

    /** Resolves {@code config/cesium-example.yaml} from the repo root regardless of the test CWD. */
    private static Path locateExample() {
        Path dir = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++) {
            Path candidate = dir.resolve("config").resolve("cesium-example.yaml");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        return fail("could not locate config/cesium-example.yaml above "
                + Path.of("").toAbsolutePath());
    }
}
