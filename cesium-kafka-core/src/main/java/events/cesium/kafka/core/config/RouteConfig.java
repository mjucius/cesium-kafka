package events.cesium.kafka.core.config;

import java.util.Objects;
import java.util.Optional;

/**
 * The single route this process serves (design §8; {@code route:} → {@code routes:} is the
 * reserved v2 schema evolution).
 *
 * @param source the user-owned topic records are consumed from (required)
 * @param destination the user-owned topic records are relayed to (required)
 * @param tracker tracker-topic settings
 * @param dlq the dead-letter topic receiving malformed-header, over-max, and payload-expired loss
 *     notices; must be configured whenever any policy routes to DLQ (validated, §2.3)
 * @param relay relay fidelity settings (§2.4)
 */
public record RouteConfig(
        TopicRef source, TopicRef destination, TrackerConfig tracker, Optional<TopicRef> dlq, RelayConfig relay) {

    /** Materializes defaults for absent components; required topics surface via validation. */
    public RouteConfig {
        source = Objects.requireNonNullElse(source, new TopicRef(""));
        destination = Objects.requireNonNullElse(destination, new TopicRef(""));
        tracker = Objects.requireNonNullElse(tracker, TrackerConfig.defaults());
        dlq = Objects.requireNonNullElse(dlq, Optional.empty());
        relay = Objects.requireNonNullElse(relay, RelayConfig.defaults());
    }

    /** Returns a {@code RouteConfig} with defaults and the required topics left unset. */
    public static RouteConfig defaults() {
        return new RouteConfig(
                new TopicRef(""), new TopicRef(""), TrackerConfig.defaults(), Optional.empty(), RelayConfig.defaults());
    }

    /** True when a DLQ topic is configured with a non-blank name. */
    public boolean hasDlq() {
        return dlq.isPresent() && dlq.get().isConfigured();
    }
}
