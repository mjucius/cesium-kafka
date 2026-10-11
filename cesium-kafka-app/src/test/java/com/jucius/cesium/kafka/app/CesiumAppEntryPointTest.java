package com.jucius.cesium.kafka.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.jucius.cesium.kafka.app.config.CesiumConfigLoader;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
                new String[] {"--config", dir.resolve("absent.yaml").toString()}, Map.of(), Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void invalidConfigExitsWithConfigCode() throws IOException {
        // An empty file binds to pure defaults: application-id / instance-id / topics are blank, so
        // the aggregate validator reports errors and the loader throws — exit 78, before any Kafka.
        Path file = Files.writeString(dir.resolve("empty.yaml"), "# no required fields here\n");
        int code = CesiumApp.run(new String[] {"--config", file.toString()}, Map.of(), Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void danglingConfigFlagExitsWithConfigCode() {
        int code = CesiumApp.run(new String[] {"--config"}, Map.of(), Clock.systemUTC());
        assertEquals(CesiumConfigLoader.EX_CONFIG, code);
    }

    @Test
    void mainThreadLinesCarryTheApplicationIdMdc() throws IOException {
        // A valid config against an unreachable broker: startup validation fails fast (short admin
        // timeouts) and run() logs "engine refused to start" on the calling (main) thread.
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path file = Files.writeString(
                dir.resolve("unreachable.yaml"),
                """
                application-id: mdc-app
                instance-id: cesium-0
                roles: [ingest, dispatch]
                kafka:
                  properties:
                    bootstrap.servers: 127.0.0.1:1
                  admin:
                    properties:
                      request.timeout.ms: 200
                      default.api.timeout.ms: 500
                route:
                  source:
                    topic: src
                  destination:
                    topic: dst
                  dlq:
                    topic: dlq
                observability:
                  port: %d
                """
                        .formatted(port));
        Logger logger = (Logger) LoggerFactory.getLogger(CesiumApp.class);
        // Snapshot the MDC at append time: a ListAppender alone reads it lazily, after the scope closed.
        ListAppender<ILoggingEvent> appender = new ListAppender<>() {
            @Override
            protected void append(ILoggingEvent event) {
                event.prepareForDeferredProcessing();
                super.append(event);
            }
        };
        appender.start();
        logger.addAppender(appender);
        try {
            int code = CesiumApp.run(new String[] {"--config", file.toString()}, Map.of(), Clock.systemUTC());
            assertEquals(CesiumApp.EX_FATAL, code);
        } finally {
            logger.detachAppender(appender);
        }
        ILoggingEvent refused = appender.list.stream()
                .filter(e -> e.getFormattedMessage().startsWith("engine refused to start"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no 'engine refused to start' line in " + appender.list));
        assertEquals("mdc-app", refused.getMDCPropertyMap().get("applicationId"));
        assertNull(MDC.get("applicationId"), "the MDC scope closes when run() returns");
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
