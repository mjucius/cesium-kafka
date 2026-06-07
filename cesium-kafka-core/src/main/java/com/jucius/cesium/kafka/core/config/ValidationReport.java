package com.jucius.cesium.kafka.core.config;

import java.util.List;
import java.util.Objects;

/**
 * The aggregate result of configuration validation (design §8): <em>every</em> violation is
 * collected into one report — the validator never fails on the first error, so an operator fixes
 * a broken config in one round trip.
 *
 * <p>Findings carry a severity: {@code ERROR} refuses startup (the app maps it to exit code 78 /
 * {@code EX_CONFIG}), {@code WARNING} is surfaced but does not refuse, and {@code INFO} carries
 * always-reported facts such as the computed worst-case index footprint (§5.3 prints it at
 * startup).
 *
 * @param findings all findings in the order they were collected; immutable
 */
public record ValidationReport(List<Finding> findings) {

    /** Severity of a single finding. */
    public enum Severity {
        /** Refuses startup; the app exits with {@code EX_CONFIG} (78). */
        ERROR,
        /** Surfaced prominently; startup proceeds. */
        WARNING,
        /** Always-reported context, e.g. the computed heap footprint. */
        INFO
    }

    /**
     * One validation finding.
     *
     * @param severity see {@link Severity}
     * @param path the config path the finding is about ({@code delay.max},
     *     {@code kafka.properties.isolation.level}, an env var name, ...)
     * @param message human-readable explanation, including the rationale for locked keys
     */
    public record Finding(Severity severity, String path, String message) {

        /** Validates that no component is null. */
        public Finding {
            Objects.requireNonNull(severity, "severity");
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(message, "message");
        }

        /** Shorthand for an {@link Severity#ERROR} finding. */
        public static Finding error(String path, String message) {
            return new Finding(Severity.ERROR, path, message);
        }

        /** Shorthand for a {@link Severity#WARNING} finding. */
        public static Finding warning(String path, String message) {
            return new Finding(Severity.WARNING, path, message);
        }

        /** Shorthand for an {@link Severity#INFO} finding. */
        public static Finding info(String path, String message) {
            return new Finding(Severity.INFO, path, message);
        }

        @Override
        public String toString() {
            return severity + " " + path + ": " + message;
        }
    }

    /** Freezes the findings list. */
    public ValidationReport {
        findings = List.copyOf(findings);
    }

    /** True when at least one finding is an {@link Severity#ERROR}. */
    public boolean hasErrors() {
        return findings.stream().anyMatch(f -> f.severity() == Severity.ERROR);
    }

    /** All {@link Severity#ERROR} findings, in collection order. */
    public List<Finding> errors() {
        return findings.stream().filter(f -> f.severity() == Severity.ERROR).toList();
    }

    /** All {@link Severity#WARNING} findings, in collection order. */
    public List<Finding> warnings() {
        return findings.stream().filter(f -> f.severity() == Severity.WARNING).toList();
    }

    /** All {@link Severity#INFO} findings, in collection order. */
    public List<Finding> infos() {
        return findings.stream().filter(f -> f.severity() == Severity.INFO).toList();
    }

    /** A multi-line human-readable rendering, one finding per line. */
    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append("configuration validation: ")
                .append(errors().size())
                .append(" error(s), ")
                .append(warnings().size())
                .append(" warning(s)");
        for (Finding finding : findings) {
            sb.append(System.lineSeparator()).append("  ").append(finding);
        }
        return sb.toString();
    }
}
