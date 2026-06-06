package events.cesium.kafka.app.logging;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.Status;
import java.net.URL;
import org.junit.jupiter.api.Test;

/**
 * Verifies the packaged {@code logback.xml} actually wires (design §9): it loads without errors, the
 * default profile selects the plain pattern encoder, and {@code LOG_FORMAT=json} selects the {@link
 * CompactJsonEncoder} via the profile include. This exercises the include + property-substitution
 * machinery the unit encoder tests skip.
 */
class LogbackConfigTest {

    @Test
    void defaultProfileUsesPlainPatternEncoder() throws Exception {
        LoggerContext context = configureFromClasspath();

        assertNoErrors(context);
        assertInstanceOf(PatternLayoutEncoder.class, consoleEncoder(context));
    }

    @Test
    void jsonProfileSelectsCompactJsonEncoder() throws Exception {
        System.setProperty("LOG_FORMAT", "json");
        try {
            LoggerContext context = configureFromClasspath();

            assertNoErrors(context);
            assertInstanceOf(CompactJsonEncoder.class, consoleEncoder(context));
        } finally {
            System.clearProperty("LOG_FORMAT");
        }
    }

    private static LoggerContext configureFromClasspath() throws Exception {
        LoggerContext context = new LoggerContext();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        URL config = LogbackConfigTest.class.getResource("/logback.xml");
        assertNotNull(config, "logback.xml must be on the classpath");
        configurator.doConfigure(config);
        return context;
    }

    private static Encoder<?> consoleEncoder(LoggerContext context) {
        Appender<ILoggingEvent> appender =
                context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("CONSOLE");
        assertNotNull(appender, "the root logger must have a CONSOLE appender");
        return ((ConsoleAppender<?>) appender).getEncoder();
    }

    private static void assertNoErrors(LoggerContext context) {
        boolean hasError = context.getStatusManager().getCopyOfStatusList().stream()
                .anyMatch(status -> status.getLevel() == Status.ERROR);
        assertFalse(
                hasError, () -> context.getStatusManager().getCopyOfStatusList().toString());
    }
}
