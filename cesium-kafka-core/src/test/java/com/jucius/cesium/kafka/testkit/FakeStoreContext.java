package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.ConfigView;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.StoreContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * Deterministic {@link StoreContext} for contract tests: a {@link RouteDescriptor} with random
 * topic ids (each {@link Builder#build()} mints a fresh identity unless an explicit
 * {@linkplain Builder#route(RouteDescriptor) route} is supplied), a {@link ConfigView#of(Map)}
 * view that parses exactly as the engine does, and a {@link SimpleMeterRegistry}.
 *
 * <p>Random ids per build are deliberate: identity material (cluster id + topic ids) must be
 * bound into committed cursor metadata (design §3.5/R17), so two contexts built independently
 * represent <em>different</em> clusters/topics and a cursor must not cross between them. Tests
 * that model a restart or rebalance of the <em>same</em> route pass the first context's
 * {@link #route()} to the next builder.
 */
public final class FakeStoreContext implements StoreContext {

    private final RouteDescriptor route;
    private final ConfigView config;
    private final MeterRegistry registry;

    private FakeStoreContext(RouteDescriptor route, ConfigView config, MeterRegistry registry) {
        this.route = route;
        this.config = config;
        this.registry = registry;
    }

    /** A fresh builder seeded with the conventional default route and meter registry. */
    public static Builder builder() {
        return new Builder();
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
    public MeterRegistry meterRegistry() {
        return registry;
    }

    /** Builder; every unset aspect has a deterministic default (random Uuids excepted). */
    public static final class Builder {

        private int partitions = 1;
        private String applicationId = "testkit-app";
        private String clusterId = "testkit-cluster";
        private @Nullable RouteDescriptor route;
        private final Map<String, String> properties = new HashMap<>();
        private MeterRegistry registry = new SimpleMeterRegistry();

        private Builder() {}

        /** Partition count of the generated route; ignored when {@link #route} is set. */
        public Builder partitions(int partitionCount) {
            this.partitions = partitionCount;
            return this;
        }

        /** Application id of the generated route; ignored when {@link #route} is set. */
        public Builder applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        /** Cluster id of the generated route; ignored when {@link #route} is set. */
        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Uses an explicit route instead of generating one — the way tests give a second store
         * incarnation the <em>same</em> identity as the first (restart/rebalance modelling).
         */
        public Builder route(RouteDescriptor route) {
            this.route = route;
            return this;
        }

        /** Adds {@code store.properties} entries (merged; later calls win per key). */
        public Builder properties(Map<String, String> entries) {
            this.properties.putAll(entries);
            return this;
        }

        /** Adds one {@code store.properties} entry. */
        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        /** Sets the meter registry the store records into (a {@code SimpleMeterRegistry} by default). */
        public Builder meterRegistry(MeterRegistry registry) {
            this.registry = registry;
            return this;
        }

        /** Builds the context, synthesizing a default {@link RouteDescriptor} when none was set. */
        public FakeStoreContext build() {
            RouteDescriptor resolved = route != null
                    ? route
                    : new RouteDescriptor(
                            applicationId,
                            clusterId,
                            "source",
                            Uuid.randomUuid(),
                            "destination",
                            Uuid.randomUuid(),
                            "cesium." + applicationId + ".tracker",
                            Uuid.randomUuid(),
                            "dlq",
                            partitions);
            return new FakeStoreContext(resolved, ConfigView.of(properties), registry);
        }
    }
}
