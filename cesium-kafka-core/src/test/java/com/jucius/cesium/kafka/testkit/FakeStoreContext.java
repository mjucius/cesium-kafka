package com.jucius.cesium.kafka.testkit;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;

/**
 * What the contract hands a store factory: the route identity the store binds into its committed
 * cursors, the sidecar and heap budgets, and a {@link SimpleMeterRegistry}.
 *
 * <p>Each {@link Builder#build()} mints a fresh random identity unless an explicit
 * {@linkplain Builder#identity(Identity) identity} is supplied. That is deliberate: identity
 * material (cluster id + topic ids) is bound into committed cursor metadata (design §3.5/R17), so
 * two contexts built independently represent <em>different</em> clusters/topics and a cursor must
 * not cross between them. Tests that model a restart or rebalance of the <em>same</em> route pass
 * the first context's {@link #identity()} to the next builder.
 *
 * @param identity the cluster and topic ids the store binds into its sidecars
 * @param sidecarMaxBytes the committed-cursor sidecar budget
 * @param heapBudgetBytes the index heap budget (fixed, so the H1 ceiling never depends on -Xmx)
 * @param meterRegistry the registry the store records into
 */
public record FakeStoreContext(
        Identity identity, int sidecarMaxBytes, long heapBudgetBytes, MeterRegistry meterRegistry) {

    /** The route identity a store binds into its committed cursors. */
    public record Identity(String clusterId, Uuid sourceTopicId, Uuid trackerTopicId) {}

    /** A fresh builder: random identity, the default 3 KiB sidecar budget, a 1 GiB heap budget. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder; every unset aspect has a deterministic default (random Uuids excepted). */
    public static final class Builder {

        private String clusterId = "testkit-cluster";
        private @Nullable Identity identity;
        private int sidecarMaxBytes = 3072;
        private long heapBudgetBytes = 1L << 30;

        private Builder() {}

        /** Cluster id of a generated identity; ignored when {@link #identity} is set. */
        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Reuses an identity instead of generating one — the way tests give a second store
         * incarnation the <em>same</em> route as the first (restart/rebalance modelling).
         */
        public Builder identity(Identity identity) {
            this.identity = identity;
            return this;
        }

        /** Sets the committed-cursor sidecar budget. */
        public Builder sidecarMaxBytes(int sidecarMaxBytes) {
            this.sidecarMaxBytes = sidecarMaxBytes;
            return this;
        }

        /** Builds the context, generating a random identity when none was set. */
        public FakeStoreContext build() {
            Identity resolved =
                    identity != null ? identity : new Identity(clusterId, Uuid.randomUuid(), Uuid.randomUuid());
            return new FakeStoreContext(resolved, sidecarMaxBytes, heapBudgetBytes, new SimpleMeterRegistry());
        }
    }
}
