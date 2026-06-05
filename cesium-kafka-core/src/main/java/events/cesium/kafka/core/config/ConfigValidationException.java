package events.cesium.kafka.core.config;

/**
 * Thrown when configuration loading or validation produced at least one
 * {@link ValidationReport.Severity#ERROR} finding. Carries the full aggregate
 * {@link ValidationReport} so callers can render every violation at once.
 *
 * <p>Exit-code semantics belong to the app layer: the runnable service maps this exception to
 * exit code 78 ({@code EX_CONFIG}, design §8); the engine itself only throws.
 */
public final class ConfigValidationException extends RuntimeException {

    private final transient ValidationReport report;

    /** Creates the exception; the message is the report's full rendering. */
    public ConfigValidationException(ValidationReport report) {
        super(report.render());
        this.report = report;
    }

    /** The aggregate report carrying every finding. */
    public ValidationReport report() {
        return report;
    }
}
