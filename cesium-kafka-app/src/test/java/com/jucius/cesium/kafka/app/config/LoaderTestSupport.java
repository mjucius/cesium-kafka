package com.jucius.cesium.kafka.app.config;

import com.jucius.cesium.kafka.core.config.ValidationContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * Hermetic loader fixtures: the environment and system properties are plain injected maps — tests
 * never touch the real process environment (design §11.1).
 */
final class LoaderTestSupport {

    /** Generous heap so default configs pass the §5.3 budget check deterministically. */
    static final ValidationContext ROOMY_CONTEXT = new ValidationContext(4L * 1024 * 1024 * 1024, 1);

    /** A minimal valid YAML: required fields plus a DLQ (the default policies route to it). */
    static final String MINIMAL_YAML =
            """
            application-id: orders-delay
            instance-id: slot-0
            route:
              source:
                topic: orders
              destination:
                topic: orders-out
              dlq:
                topic: orders-dlq
            """;

    private LoaderTestSupport() {}

    static CesiumConfigLoader loader(Map<String, String> environment) {
        return new CesiumConfigLoader(environment, new Properties(), ROOMY_CONTEXT);
    }

    static CesiumConfigLoader loader(Map<String, String> environment, Properties systemProperties) {
        return new CesiumConfigLoader(environment, systemProperties, ROOMY_CONTEXT);
    }

    static Path write(Path dir, String yaml) {
        try {
            return Files.writeString(dir.resolve("cesium.yaml"), yaml);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
