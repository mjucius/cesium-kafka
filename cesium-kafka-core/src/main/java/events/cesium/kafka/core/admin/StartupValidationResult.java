package events.cesium.kafka.core.admin;

import events.cesium.kafka.core.config.ValidationReport;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Everything {@link StartupValidator} produced: the aggregate report (the caller fail-fasts on
 * {@link ValidationReport#hasErrors()}) plus the environment facts the engine needs afterwards —
 * the §3.1 identity blob and the source partition count {@code P} that sizes the tracker and the
 * shard set.
 *
 * @param report every finding, aggregated — never fail-on-first (§8 validation philosophy)
 * @param identity the captured {@code {v, clusterId, sourceTopicId}} identity; empty when the
 *     source topic or cluster metadata could not be resolved (the report then carries the error)
 * @param sourcePartitions the source topic's partition count {@code P}; empty when the source
 *     could not be described
 */
public record StartupValidationResult(
        ValidationReport report, Optional<IdentityBlob> identity, OptionalInt sourcePartitions) {

    /** Validates that no component is null. */
    public StartupValidationResult {
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(sourcePartitions, "sourcePartitions");
    }
}
