package com.jucius.cesium.kafka.app;

import com.jucius.cesium.kafka.app.config.CesiumConfigLoader;
import com.jucius.cesium.kafka.app.config.CesiumConfigLoader.LoadedConfig;
import com.jucius.cesium.kafka.app.lifecycle.CesiumEngine;
import com.jucius.cesium.kafka.app.lifecycle.EngineStartupException;
import com.jucius.cesium.kafka.app.metrics.BuildInfo;
import com.jucius.cesium.kafka.app.metrics.ServiceInfo;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.ConfigValidationException;
import com.jucius.cesium.kafka.core.config.StartupChecks;
import com.jucius.cesium.kafka.core.config.ValidationContext;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the cesium-kafka service (design §6, §9, §10). {@link #main(String[])} is a thin
 * wrapper over the testable {@link #run(String[], Map, Properties, Clock)} body, which returns the
 * process exit code instead of calling {@link System#exit} so the config and exit-code paths are
 * unit-testable without forking a JVM.
 *
 * <p><strong>Sequence.</strong> parse {@code --config <path>} (default {@value #DEFAULT_CONFIG_PATH},
 * overridable via {@code CESIUM_CONFIG}) → load + validate the YAML (every violation at once; exit
 * {@value CesiumConfigLoader#EX_CONFIG} on error, printing the report; print WARN/INFO findings incl.
 * the §5.3 footprint on success) → create the {@link PrometheusMeterRegistry} → build the
 * {@link CesiumEngine} → start the observability HTTP server (before the engine, so probes answer
 * during startup) → install a SIGTERM/SIGINT hook calling {@code engine.stop(shutdown.timeout)} →
 * start the engine → block in {@link CesiumEngine#awaitDone()} → exit {@code 0} on a clean shutdown,
 * non-zero on a fatal loop failure or shutdown-budget overrun.
 *
 * <p><strong>Exit codes.</strong> {@code 0} normal, {@value CesiumConfigLoader#EX_CONFIG} config
 * (BSD {@code EX_CONFIG}), {@value #EX_FATAL} any other fatal (startup validation, engine failure).
 */
public final class CesiumApp {

    private static final Logger log = LoggerFactory.getLogger(CesiumApp.class);

    /** The default config path when neither {@code --config} nor {@code CESIUM_CONFIG} is set. */
    static final String DEFAULT_CONFIG_PATH = "/etc/cesium/cesium.yaml";

    /**
     * The environment variable overriding the config path (below an explicit {@code --config}). It is
     * the loader's reserved config-file selector — excluded from the {@code CESIUM_} override scan so
     * it never collides with the config-override grammar.
     */
    static final String CONFIG_ENV = CesiumConfigLoader.CONFIG_PATH_ENV;

    /** Generic fatal exit code (startup validation failure, engine failure, shutdown overrun). */
    static final int EX_FATAL = 1;

    /**
     * The §6 graceful-shutdown budget the signal handler and {@link CesiumEngine#awaitDone()} pass to
     * {@link CesiumEngine#stop(Duration)}. Comfortably above each loop's 10 s client-close bound so a
     * loop's in-flight transaction can commit/abort and its clients close before the hard cut.
     */
    static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    /**
     * The heartbeat-age ceiling the health layer treats as "wedged" — well above the dispatch poll
     * ceiling (§6 default 30 s) so a quiet, idle loop sleeping in {@code poll()} is never a false
     * negative; the 1 s health sampler refreshes far faster.
     */
    static final Duration LIVENESS_STALE_AFTER = Duration.ofSeconds(90);

    private CesiumApp() {}

    public static void main(String[] args) {
        System.exit(run(args, System.getenv(), System.getProperties(), Clock.systemUTC()));
    }

    /**
     * The testable {@code main} body: resolves and loads config, builds and runs the engine, and
     * returns the process exit code (never calls {@link System#exit}). Environment and system
     * properties are injected so tests run hermetically.
     *
     * @return {@code 0} clean, {@value CesiumConfigLoader#EX_CONFIG} config error, {@value #EX_FATAL}
     *     any other fatal
     */
    public static int run(String[] args, Map<String, String> environment, Properties systemProperties, Clock clock) {
        final LoadedConfig loaded;
        try {
            Path configPath = Path.of(resolveConfigPath(args, environment));
            log.info("loading configuration from {}", configPath);
            loaded = new CesiumConfigLoader(environment, systemProperties, ValidationContext.runtime(1))
                    .load(configPath);
        } catch (ConfigValidationException e) {
            log.error("configuration is invalid:\n{}", e.report().render());
            return CesiumConfigLoader.EX_CONFIG;
        } catch (IllegalArgumentException e) {
            log.error("invalid command line: {}", e.getMessage());
            return CesiumConfigLoader.EX_CONFIG;
        }
        printFindings(loaded.report());
        CesiumConfig config = loaded.config();

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        CesiumEngine engine = new CesiumEngine(config, registry, clock, SHUTDOWN_TIMEOUT);
        AutoCloseable observability = () -> {};
        try {
            ObservabilityRuntime runtime = new ObservabilityRuntime(
                    config.observability().bindAddress(),
                    config.observability().port(),
                    config.observability().detailedInfo(),
                    registry,
                    engine.health(),
                    clock,
                    LIVENESS_STALE_AFTER,
                    serviceInfoSupplier(config, engine));
            observability = startObservability(runtime);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> engine.stop(SHUTDOWN_TIMEOUT), "cesium-shutdown"));
            engine.start();
            boolean clean = engine.awaitDone();
            if (!clean) {
                engine.fatalCause().ifPresent(cause -> log.error("engine exited on a fatal loop failure", cause));
                return EX_FATAL;
            }
            log.info("cesium engine stopped cleanly");
            return 0;
        } catch (EngineStartupException e) {
            log.error("engine refused to start: {}", e.getMessage());
            ValidationReport report = e.report();
            if (report != null) {
                log.error("startup findings:\n{}", report.render());
            }
            return EX_FATAL;
        } catch (RuntimeException e) {
            log.error("fatal error running the engine", e);
            return EX_FATAL;
        } finally {
            closeQuietly(observability);
            registry.close();
        }
    }

    // ------------------------------------------------------------------ helpers

    /** {@code --config <path>} / {@code --config=<path>}, else {@code CESIUM_CONFIG}, else the default. */
    static String resolveConfigPath(String[] args, Map<String, String> environment) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--config")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("--config requires a path argument");
                }
                return args[i + 1];
            }
            if (arg.startsWith("--config=")) {
                return arg.substring("--config=".length());
            }
        }
        String fromEnv = environment.get(CONFIG_ENV);
        return fromEnv == null || fromEnv.isBlank() ? DEFAULT_CONFIG_PATH : fromEnv;
    }

    /** Logs WARN and INFO findings (incl. the always-present §5.3 footprint) of a clean load (M1). */
    private static void printFindings(ValidationReport report) {
        for (ValidationReport.Finding finding : report.warnings()) {
            log.warn("config {}: {}", finding.path(), finding.message());
        }
        for (ValidationReport.Finding finding : report.infos()) {
            log.info("config {}: {}", finding.path(), finding.message());
        }
    }

    /**
     * The {@code /info} supplier: build provenance + applicationId + lower-cased roles + store type
     * and (once the store is up) its capabilities + the named startup acknowledgments. Re-evaluated
     * per request so capabilities appear as soon as the engine resolves the store.
     */
    private static Supplier<ServiceInfo> serviceInfoSupplier(CesiumConfig config, CesiumEngine engine) {
        BuildInfo build = BuildInfo.load();
        List<String> roles = config.roles().stream()
                .map(role -> role.name().toLowerCase(Locale.ROOT))
                .sorted()
                .toList();
        List<String> acknowledgments = new ArrayList<>();
        if (config.startupChecks().sizeBasedRetention() == StartupChecks.SizeBasedRetention.ACKNOWLEDGED) {
            acknowledgments.add("size-based-retention");
        }
        String storeType = config.store().type();
        String applicationId = config.applicationId();
        return () ->
                new ServiceInfo(build, applicationId, roles, storeType, engine.storeCapabilities(), acknowledgments);
    }

    private static AutoCloseable startObservability(ObservabilityRuntime runtime) {
        List<ObservabilityServerFactory> factories = new ArrayList<>();
        ServiceLoader.load(ObservabilityServerFactory.class).forEach(factories::add);
        if (factories.isEmpty()) {
            log.warn("no ObservabilityServerFactory registered; running without the HTTP observability surface"
                    + " (/metrics, /health/live, /health/ready, /info)");
            return () -> {};
        }
        if (factories.size() > 1) {
            throw new IllegalStateException("multiple ObservabilityServerFactory providers registered: " + factories);
        }
        try {
            AutoCloseable server = factories.get(0).start(runtime);
            log.info("observability HTTP server listening on port {}", runtime.port());
            return server;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to start the observability HTTP server on port " + runtime.port(), e);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("failed to close a lifecycle component during shutdown", e);
        }
    }
}
