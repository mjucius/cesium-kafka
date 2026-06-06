package events.cesium.kafka.app.health;

/**
 * The result of a liveness or readiness evaluation: the boolean verdict plus the JSON detail body to
 * return. The HTTP handler maps {@code ok} to {@code 200} or {@code 503} and writes {@code json}.
 *
 * @param ok whether the probe passes ({@code 200}) or fails ({@code 503})
 * @param json the JSON body describing the verdict and its per-loop / per-shard detail
 */
public record HealthSnapshot(boolean ok, String json) {}
