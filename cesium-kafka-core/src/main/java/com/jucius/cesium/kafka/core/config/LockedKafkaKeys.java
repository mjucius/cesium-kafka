package com.jucius.cesium.kafka.core.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * The Kafka client properties the engine owns and refuses to let passthrough config override
 * (design §8, D17/D18). Each key carries the explanation returned in the rejection finding — the
 * operator learns <em>why</em> the key is locked, not just that it is.
 *
 * <p>This fixes the PoC's {@code Properties(defaults)} misuse class by construction: correctness-
 * critical client config can never silently drift via a passthrough map.
 */
public final class LockedKafkaKeys {

    private static final Map<String, String> EXPLANATIONS = buildExplanations();

    private LockedKafkaKeys() {}

    private static Map<String, String> buildExplanations() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(
                "group.id",
                "locked: cesium derives consumer group ids from applicationId; an override would detach the"
                        + " ingest/dispatch groups from their transactional offset commits.");
        m.put(
                "group.instance.id",
                "locked: static-membership ids are derived from instanceId (design D21); an override breaks the"
                        + " zero-partition-movement rolling-restart recipe.");
        m.put(
                "transactional.id",
                "locked: transactional ids follow the cesium.<applicationId>.<role>.<instanceId>.<ordinal> scheme"
                        + " (design D10); ad-hoc ids break immediate fencing of a predecessor's dangling"
                        + " transaction.");
        m.put(
                "enable.auto.commit",
                "locked to false: every cesium offset commit travels inside a Kafka transaction via"
                        + " sendOffsetsToTransaction; auto-commit would advance offsets outside the transaction and"
                        + " break exactly-once.");
        m.put(
                "isolation.level",
                "locked to read_committed on every cesium consumer: KIP-447 takeover ordering depends on the"
                        + " require_stable offset-fetch flag, which the consumer only sets under read_committed;"
                        + " read_uncommitted would also relay records from aborted upstream transactions"
                        + " (design D17).");
        m.put(
                "auto.offset.reset",
                "locked to none for both consumer groups: after committed-offset expiry (KIP-211) an automatic"
                        + " reset silently loses (latest) or mass-duplicates (earliest) records; offset resets must"
                        + " be explicit operator decisions per the runbook (design D18, §3.6).");
        m.put(
                "enable.idempotence",
                "locked to true: idempotence is a prerequisite of the transactional producer; disabling it breaks"
                        + " exactly-once.");
        m.put(
                "key.serializer",
                "locked: cesium relays keys as opaque byte arrays; serialization is owned by the engine.");
        m.put(
                "value.serializer",
                "locked: cesium relays values as opaque byte arrays; serialization is owned by the engine.");
        m.put(
                "key.deserializer",
                "locked: cesium consumes keys as opaque byte arrays; deserialization is owned by the engine.");
        m.put(
                "value.deserializer",
                "locked: cesium consumes values as opaque byte arrays; deserialization is owned by the engine.");
        return Collections.unmodifiableMap(m);
    }

    /** True when {@code key} is locked on every cesium client. */
    public static boolean isLocked(String key) {
        return EXPLANATIONS.containsKey(key);
    }

    /** The rejection explanation for a locked key, or {@code null} when the key is not locked. */
    public static @Nullable String explanation(String key) {
        return EXPLANATIONS.get(key);
    }

    /** All locked keys, in documentation order. */
    public static Set<String> all() {
        return EXPLANATIONS.keySet();
    }
}
