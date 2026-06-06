package events.cesium.kafka.app.health;

import events.cesium.kafka.app.json.Json;

/**
 * A point-in-time recovery snapshot for one tracker partition, surfaced in the {@code /health/ready}
 * detail body (design §9). Recovery is <em>decoupled from readiness</em> (D21): this detail explains
 * why a ready instance is still catching up, it never makes the instance not-ready.
 *
 * @param partition the tracker (== source) partition number
 * @param state the shard's replay state (design §3.6)
 * @param recordsRemaining {@code barrier - position} — records left to replay before the shard goes
 *     {@link State#ACTIVE}; {@code 0} once active, mirrors {@code cesium_replay_remaining_records}
 * @param etaMillis projected milliseconds until this shard reaches the barrier, or {@code -1} when
 *     not yet estimable (feeds the replay-ETA alert, §3.5)
 */
public record ShardRecovery(int partition, State state, long recordsRemaining, long etaMillis) {

    /** The per-partition shard replay state machine (design §3.6, mirrors {@code cesium_shard_state}). */
    public enum State {
        /** Assignment recorded in an O(1) rebalance callback; cursor/barrier not yet resolved (I8). */
        ASSIGNED,
        /** Seeded from the cursor sidecar and replaying {@code [cursor, barrier)}; not dispatch-eligible (I4). */
        RECOVERING,
        /** Replay reached the barrier; the shard dispatches live. */
        ACTIVE
    }

    /** Renders this snapshot as a JSON object for the readiness detail body. */
    public String toJson() {
        return Json.object()
                .num("partition", partition)
                .str("state", state.name())
                .num("recordsRemaining", recordsRemaining)
                .num("etaMillis", etaMillis)
                .end();
    }
}
