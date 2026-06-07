package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.ConfigValidationException;
import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unknown YAML keys are startup errors, reported in aggregate (design §8, §11.1). */
class UnknownKeyTest {

    @TempDir
    Path dir;

    @Test
    void unknownTopLevelKeyFails() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML + "aplication-id: typo\n");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("aplication-id"), exception::getMessage);
        assertTrue(exception.getMessage().contains("unknown configuration key"), exception::getMessage);
    }

    @Test
    void unknownNestedKeyReportsItsFullPath() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        delay:
                          maxx: P1D
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        List<String> paths =
                exception.report().errors().stream().map(Finding::path).toList();
        assertTrue(paths.contains("delay.maxx"), exception::getMessage);
    }

    @Test
    void everyUnknownKeyIsReportedInOnePass() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        delay:
                          maxx: P1D
                        dispatch:
                          batchh:
                            max-entries: 1
                        observability:
                          prt: 8081
                        """);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        List<String> paths =
                exception.report().errors().stream().map(Finding::path).toList();
        assertTrue(paths.contains("delay.maxx"), exception::getMessage);
        assertTrue(paths.contains("dispatch.batchh"), exception::getMessage);
        assertTrue(paths.contains("observability.prt"), exception::getMessage);
    }

    @Test
    void camelCaseKeySuggestsTheKebabSpelling() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML + "applicationId: oops\n");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("did you mean 'application-id'"), exception::getMessage);
    }

    @Test
    void legacyTopLevelTransactionsKeySuggestsItsNestedHome() {
        // The §8 defaults table once listed transactions.* at top level; the authoritative sketch
        // houses it at kafka.transactions.* — point the operator at the relocation.
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML + "transactions:\n  timeout: PT30S\n");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("did you mean 'kafka.transactions'"), exception::getMessage);
    }

    @Test
    void legacyTopLevelRelayKeySuggestsItsNestedHome() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML + "relay:\n  timestamp: SOURCE\n");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("did you mean 'route.relay'"), exception::getMessage);
    }

    @Test
    void duplicateYamlKeysAreRejectedNotLastWins() {
        // A block pasted twice must not silently lose half its settings (§8 typo-rejection).
        Path file = LoaderTestSupport.write(
                dir, LoaderTestSupport.MINIMAL_YAML + "delay:\n  max: PT1H\ndelay:\n  max: PT2H\n");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of())
                        .load(file));
        assertTrue(exception.getMessage().contains("delay"), exception::getMessage);
        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("duplicate"), exception::getMessage);
    }

    @Test
    void openMapSubtreesAcceptArbitraryKeys() {
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        kafka:
                          properties:
                            some.exotic.broker.tunable: 42
                        store:
                          properties:
                            anything.goes.here: yes
                        """);
        // Must not throw: map subtrees are the operator's namespace.
        LoaderTestSupport.loader(Map.of()).load(file);
    }
}
