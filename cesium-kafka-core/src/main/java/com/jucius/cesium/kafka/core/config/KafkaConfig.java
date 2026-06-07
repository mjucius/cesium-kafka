package com.jucius.cesium.kafka.core.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Kafka client passthrough configuration (design §8): a common {@code kafka.properties} map plus
 * per-client overlay maps, with the correctness-critical keys locked
 * ({@link LockedKafkaKeys}) — the engine owns client config that EOS depends on.
 *
 * <p>Transaction settings live here (rather than at the top level) because
 * {@code transaction.timeout.ms} and the commit-retry budget are producer-level concerns shared by
 * both loops; the §8 sketch has no top-level {@code transactions} component.
 *
 * @param groupProtocol consumer group protocol; default {@code classic} — {@code consumer}
 *     (KIP-848) is a tested option (D12)
 * @param properties common client properties applied to every cesium client
 * @param transactions transaction timeout and commit-retry budget (D9, §3.8)
 * @param ingestConsumer overlay for the group-A source consumer
 * @param trackerConsumer overlay for the group-B tracker consumer
 * @param seekConsumer overlay for the payload seek-fetch consumer (§7)
 * @param ingestProducer overlay for the ingest transactional producer
 * @param dispatchProducer overlay for the dispatch transactional producer
 * @param admin overlay for the admin client (§6)
 */
public record KafkaConfig(
        GroupProtocol groupProtocol,
        Map<String, String> properties,
        Transactions transactions,
        ClientOverrides ingestConsumer,
        ClientOverrides trackerConsumer,
        ClientOverrides seekConsumer,
        ClientOverrides ingestProducer,
        ClientOverrides dispatchProducer,
        ClientOverrides admin) {

    /** Consumer group protocol (§8 defaults table; KIP-848 lane is D12). */
    public enum GroupProtocol {
        /** Classic group protocol (default). */
        CLASSIC,
        /** KIP-848 {@code group.protocol=consumer} — continuously tested, non-default in v1. */
        CONSUMER
    }

    /** Materializes the §8 defaults for absent components; the map is defensively copied. */
    public KafkaConfig {
        groupProtocol = Objects.requireNonNullElse(groupProtocol, GroupProtocol.CLASSIC);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        transactions = Objects.requireNonNullElse(transactions, Transactions.defaults());
        ingestConsumer = Objects.requireNonNullElse(ingestConsumer, ClientOverrides.empty());
        trackerConsumer = Objects.requireNonNullElse(trackerConsumer, ClientOverrides.empty());
        seekConsumer = Objects.requireNonNullElse(seekConsumer, ClientOverrides.empty());
        ingestProducer = Objects.requireNonNullElse(ingestProducer, ClientOverrides.empty());
        dispatchProducer = Objects.requireNonNullElse(dispatchProducer, ClientOverrides.empty());
        admin = Objects.requireNonNullElse(admin, ClientOverrides.empty());
    }

    /** Returns a {@code KafkaConfig} populated entirely from the §8 defaults table. */
    public static KafkaConfig defaults() {
        return new KafkaConfig(
                GroupProtocol.CLASSIC,
                Map.of(),
                Transactions.defaults(),
                ClientOverrides.empty(),
                ClientOverrides.empty(),
                ClientOverrides.empty(),
                ClientOverrides.empty(),
                ClientOverrides.empty(),
                ClientOverrides.empty());
    }

    /**
     * Every client property map keyed by its config path — the common map first, then each
     * overlay. Iteration order is stable so validation reports are deterministic.
     */
    public Map<String, Map<String, String>> clientPropertyMaps() {
        Map<String, Map<String, String>> maps = new LinkedHashMap<>();
        maps.put("kafka.properties", properties);
        maps.put("kafka.ingest-consumer.properties", ingestConsumer.properties());
        maps.put("kafka.tracker-consumer.properties", trackerConsumer.properties());
        maps.put("kafka.seek-consumer.properties", seekConsumer.properties());
        maps.put("kafka.ingest-producer.properties", ingestProducer.properties());
        maps.put("kafka.dispatch-producer.properties", dispatchProducer.properties());
        maps.put("kafka.admin.properties", admin.properties());
        return maps;
    }

    /**
     * The effective value of one client property for the group-B tracker consumer, with its
     * provenance: the overlay wins over the common map (§8). Returns an entry of
     * {@code (config path, value)} — e.g.
     * {@code ("kafka.tracker-consumer.properties.max.poll.interval.ms", "60000")} — or
     * {@code null} when neither map sets the key. Carrying the source path lets validation
     * findings name the map that actually supplied the value instead of guessing.
     */
    public Map.@Nullable Entry<String, String> effectiveTrackerConsumerProperty(String key) {
        String overlay = trackerConsumer.properties().get(key);
        if (overlay != null) {
            return Map.entry("kafka.tracker-consumer.properties." + key, overlay);
        }
        String common = properties.get(key);
        return common == null ? null : Map.entry("kafka.properties." + key, common);
    }

    /**
     * A per-client property overlay ({@code kafka.<client>.properties}, §8).
     *
     * @param properties client-specific keys layered over the common {@code kafka.properties}
     */
    public record ClientOverrides(Map<String, String> properties) {

        /** Materializes an absent map as empty; the map is defensively copied. */
        public ClientOverrides {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }

        /** Returns an overlay with no properties. */
        public static ClientOverrides empty() {
            return new ClientOverrides(Map.of());
        }
    }

    /**
     * Transaction settings (D9, §3.8).
     *
     * @param timeout {@code transaction.timeout.ms}; default {@code PT30S} — bounds the LSO-stall
     *     / barrier-gating window after crashes (D9)
     * @param commitRetry bounded retries before an in-doubt commit resolves to the I9 procedure or
     *     the loop parks-and-degrades (§3.8, R15); default 5
     */
    public record Transactions(Duration timeout, Integer commitRetry) {

        /** Default transaction timeout (§8 defaults table, D9). */
        public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

        /** Default commit-retry budget (§8 defaults table). */
        public static final int DEFAULT_COMMIT_RETRY = 5;

        /** Materializes the §8 defaults for absent components. */
        public Transactions {
            timeout = Objects.requireNonNullElse(timeout, DEFAULT_TIMEOUT);
            commitRetry = Objects.requireNonNullElse(commitRetry, DEFAULT_COMMIT_RETRY);
        }

        /** Returns a {@code Transactions} populated entirely from the §8 defaults table. */
        public static Transactions defaults() {
            return new Transactions(DEFAULT_TIMEOUT, DEFAULT_COMMIT_RETRY);
        }
    }
}
