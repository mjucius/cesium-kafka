package com.jucius.cesium.kafka.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.jucius.cesium.kafka.app.config.CesiumConfigLoader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@link CesiumApp} entry point factored so the body returns an exit code instead of calling
 * {@link System#exit}: the config-error path ({@value CesiumConfigLoader#EX_CONFIG}) is exercised
 * without a broker, and {@code --config} resolution is unit-tested directly. The success path needs a
 * real cluster and is covered by the app integration test in the next phase.
 */
class CesiumAppEntryPointTest {

    @TempDir
    Path dir;

    @Test
    void missingConfigFileExitsWithConfigCode() {
        int code = CesiumApp.run(
                new String[] {"--config", dir.resolve("absent.yaml").toString()},
                Map.of(),
                new Properties(),
                Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void invalidConfigExitsWithConfigCode() throws IOException {
        // An empty file binds to pure defaults: application-id / instance-id / topics are blank, so
        // the aggregate validator reports errors and the loader throws — exit 78, before any Kafka.
        Path file = Files.writeString(dir.resolve("empty.yaml"), "# no required fields here\n");
        int code = CesiumApp.run(
                new String[] {"--config", file.toString()}, Map.of(), new Properties(), Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void danglingConfigFlagExitsWithConfigCode() {
        int code = CesiumApp.run(new String[] {"--config"}, Map.of(), new Properties(), Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void resolvesConfigPathFromArgsThenEnvThenDefault() {
        assertEquals("/a.yaml", CesiumApp.resolveConfigPath(new String[] {"--config", "/a.yaml"}, Map.of()));
        assertEquals("/b.yaml", CesiumApp.resolveConfigPath(new String[] {"--config=/b.yaml"}, Map.of()));
        assertEquals("/c.yaml", CesiumApp.resolveConfigPath(new String[] {}, Map.of(CesiumApp.CONFIG_ENV, "/c.yaml")));
        assertEquals(CesiumApp.DEFAULT_CONFIG_PATH, CesiumApp.resolveConfigPath(new String[] {}, Map.of()));
    }

    @Test
    void explicitConfigArgOutranksTheEnvironment() {
        assertEquals(
                "/arg.yaml",
                CesiumApp.resolveConfigPath(
                        new String[] {"--config", "/arg.yaml"}, Map.of(CesiumApp.CONFIG_ENV, "/env.yaml")));
    }

    @Test
    void danglingConfigFlagThrows() {
        assertThrows(
                IllegalArgumentException.class, () -> CesiumApp.resolveConfigPath(new String[] {"--config"}, Map.of()));
    }
}
