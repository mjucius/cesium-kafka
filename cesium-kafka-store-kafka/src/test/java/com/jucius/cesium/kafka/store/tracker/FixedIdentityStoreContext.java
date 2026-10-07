package com.jucius.cesium.kafka.store.tracker;

import com.jucius.cesium.kafka.api.store.ConfigView;
import com.jucius.cesium.kafka.api.store.OwnershipEpoch;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.StoreContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Uuid;

/**
 * Deterministic {@link StoreContext} for store-level unit tests, with <em>fixed</em> identity
 * material ({@link #CLUSTER_ID}, {@link #SOURCE_TOPIC_ID}, {@link #TRACKER_TOPIC_ID}).
 *
 * <p>Deliberately distinct from the published testkit's
 * {@code com.jucius.cesium.kafka.testkit.FakeStoreContext} (also on this module's test classpath),
 * whose identity is <em>random per build</em> — the right default for contract tests, where
 * independently built contexts must model different clusters. These unit tests instead need
 * byte-exact, name-addressable identity constants (sidecar identity headers are asserted against
 * them, and forged/foreign sidecars are constructed from them), plus the
 * {@link #withoutTrackerTopicId no-tracker-topic-id} {@code validate()} case the testkit builder
 * deliberately does not expose. The distinct name keeps the two fixtures from being confused —
 * importing the testkit fake here would silently change what "foreign identity" means in a test.
 */
final class FixedIdentityStoreContext implements StoreContext {

    static final String CLUSTER_ID = "test-cluster";
    static final Uuid SOURCE_TOPIC_ID = new Uuid(0x0102030405060708L, 0x1112131415161718L);
    static final Uuid DESTINATION_TOPIC_ID = new Uuid(0x4142434445464748L, 0x5152535455565758L);
    static final Uuid TRACKER_TOPIC_ID = new Uuid(0x2122232425262728L, 0x3132333435363738L);

    private final RouteDescriptor route;
    private final ConfigView config;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000_000), ZoneOffset.UTC);

    FixedIdentityStoreContext(int partitionCount, Map<String, String> properties) {
        this(partitionCount, properties, TRACKER_TOPIC_ID);
    }

    FixedIdentityStoreContext(int partitionCount, Map<String, String> properties, Uuid trackerTopicId) {
        this.route = new RouteDescriptor(
                "test-app",
                CLUSTER_ID,
                "source",
                SOURCE_TOPIC_ID,
                "destination",
                DESTINATION_TOPIC_ID,
                "cesium.test-app.tracker",
                trackerTopicId,
                "dlq",
                partitionCount);
        Map<String, String> withDefaults = new HashMap<>(properties);
        // Deterministic heap budget: the worst-case footprint check must not depend on the test
        // JVM's -Xmx (1 GiB covers partitionCount x default max-pending x 64 B for small counts).
        withDefaults.putIfAbsent(KafkaTrackerStore.HEAP_BUDGET_BYTES_KEY, Long.toString(1L << 30));
        this.config = ConfigView.of(withDefaults);
    }

    static FixedIdentityStoreContext withPartitions(int partitionCount) {
        return new FixedIdentityStoreContext(partitionCount, Map.of());
    }

    static FixedIdentityStoreContext withoutTrackerTopicId(int partitionCount) {
        return new FixedIdentityStoreContext(partitionCount, Map.of(), null);
    }

    @Override
    public RouteDescriptor route() {
        return route;
    }

    @Override
    public ConfigView config() {
        return config;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public MeterRegistry meterRegistry() {
        return registry;
    }

    @Override
    public OwnershipEpoch epoch(int partition) {
        return new OwnershipEpoch(1, "test-member");
    }

    SimpleMeterRegistry registry() {
        return registry;
    }
}
