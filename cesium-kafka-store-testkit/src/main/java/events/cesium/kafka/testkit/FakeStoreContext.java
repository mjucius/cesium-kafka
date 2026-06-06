package events.cesium.kafka.testkit;

import events.cesium.kafka.api.store.ConfigView;
import events.cesium.kafka.api.store.OwnershipEpoch;
import events.cesium.kafka.api.store.RouteDescriptor;
import events.cesium.kafka.api.store.StoreContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * Deterministic {@link StoreContext} for contract tests: a {@link RouteDescriptor} with random
 * topic ids (each {@link Builder#build()} mints a fresh identity unless an explicit
 * {@linkplain Builder#route(RouteDescriptor) route} is supplied), a map-backed {@link ConfigView}
 * following the engine's parsing conventions, a {@link SimpleMeterRegistry}, and a
 * {@link MutableClock}.
 *
 * <p>Random ids per build are deliberate: identity material (cluster id + topic ids) must be
 * bound into committed cursor metadata (design §3.5/R17), so two contexts built independently
 * represent <em>different</em> clusters/topics and a cursor must not cross between them. Tests
 * that model a restart or rebalance of the <em>same</em> route pass the first context's
 * {@link #route()} to the next builder.
 */
public final class FakeStoreContext implements StoreContext {

    private final RouteDescriptor route;
    private final MapConfigView config;
    private final MutableClock clock;
    private final MeterRegistry registry;

    private FakeStoreContext(RouteDescriptor route, MapConfigView config, MutableClock clock, MeterRegistry registry) {
        this.route = route;
        this.config = config;
        this.clock = clock;
        this.registry = registry;
    }

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
    public Clock clock() {
        return clock;
    }

    /** The same clock as {@link #clock()}, typed for test-side advancement. */
    public MutableClock mutableClock() {
        return clock;
    }

    @Override
    public MeterRegistry meterRegistry() {
        return registry;
    }

    @Override
    public OwnershipEpoch epoch(int partition) {
        return new OwnershipEpoch(1, "testkit-member");
    }

    /** Builder; every unset aspect has a deterministic default (random Uuids excepted). */
    public static final class Builder {

        private int partitions = 1;
        private String applicationId = "testkit-app";
        private String clusterId = "testkit-cluster";
        private @Nullable RouteDescriptor route;
        private final Map<String, String> properties = new HashMap<>();
        private MutableClock clock = MutableClock.at(0);
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

        public Builder clock(MutableClock clock) {
            this.clock = clock;
            return this;
        }

        public Builder meterRegistry(MeterRegistry registry) {
            this.registry = registry;
            return this;
        }

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
            return new FakeStoreContext(resolved, new MapConfigView(properties), clock, registry);
        }
    }

    /** Map-backed {@link ConfigView} mirroring the engine's parsing conventions (design §8). */
    public static final class MapConfigView implements ConfigView {

        private final Map<String, String> values;

        public MapConfigView(Map<String, String> values) {
            this.values = Map.copyOf(values);
        }

        @Override
        public String getString(String key, String defaultValue) {
            return values.getOrDefault(key, defaultValue);
        }

        @Override
        public String getString(String key) {
            String value = values.get(key);
            if (value == null) {
                throw new NoSuchElementException("missing store.properties key: " + key);
            }
            return value;
        }

        @Override
        public int getInt(String key, int defaultValue) {
            String value = values.get(key);
            return value == null ? defaultValue : parseInt(key, value);
        }

        @Override
        public int getInt(String key) {
            return parseInt(key, getString(key));
        }

        @Override
        public long getLong(String key, long defaultValue) {
            String value = values.get(key);
            return value == null ? defaultValue : parseLong(key, value);
        }

        @Override
        public long getLong(String key) {
            return parseLong(key, getString(key));
        }

        @Override
        public Duration getDuration(String key, Duration defaultValue) {
            String value = values.get(key);
            return value == null ? defaultValue : parseDuration(key, value);
        }

        @Override
        public Duration getDuration(String key) {
            return parseDuration(key, getString(key));
        }

        @Override
        public boolean getBoolean(String key, boolean defaultValue) {
            String value = values.get(key);
            return value == null ? defaultValue : parseBoolean(key, value);
        }

        @Override
        public boolean getBoolean(String key) {
            return parseBoolean(key, getString(key));
        }

        @Override
        public Set<String> keys() {
            return values.keySet();
        }

        private static int parseInt(String key, String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("store.properties key " + key + " is not an int: " + value, e);
            }
        }

        private static long parseLong(String key, String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("store.properties key " + key + " is not a long: " + value, e);
            }
        }

        private static Duration parseDuration(String key, String value) {
            try {
                return Duration.parse(value);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("store.properties key " + key + " is not a duration: " + value, e);
            }
        }

        private static boolean parseBoolean(String key, String value) {
            if ("true".equals(value)) {
                return true;
            }
            if ("false".equals(value)) {
                return false;
            }
            throw new IllegalArgumentException("store.properties key " + key + " is not a boolean: " + value);
        }
    }
}
