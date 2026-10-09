package com.jucius.cesium.kafka.store.tracker;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.Uuid;

/**
 * Store-level unit-test fixture with <em>fixed</em> identity material ({@link #CLUSTER_ID},
 * {@link #SOURCE_TOPIC_ID}, {@link #TRACKER_TOPIC_ID}) and its own registry.
 *
 * <p>Deliberately distinct from the contract kit's {@code FakeStoreContext}, whose identity is
 * <em>random per build</em> — the right default for contract tests, where independently built
 * contexts must model different clusters. These unit tests instead need byte-exact,
 * name-addressable identity constants (sidecar identity headers are asserted against them, and
 * forged/foreign sidecars are constructed from them).
 */
final class FixedIdentityStoreContext {

    static final String CLUSTER_ID = "test-cluster";
    static final Uuid SOURCE_TOPIC_ID = new Uuid(0x0102030405060708L, 0x1112131415161718L);
    static final Uuid DESTINATION_TOPIC_ID = new Uuid(0x4142434445464748L, 0x5152535455565758L);
    static final Uuid TRACKER_TOPIC_ID = new Uuid(0x2122232425262728L, 0x3132333435363738L);

    /** Deterministic heap budget: the H1 recovery ceiling must not depend on the test JVM's -Xmx. */
    static final long DEFAULT_HEAP_BUDGET_BYTES = 1L << 30;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final int sidecarMaxBytes;
    private final long heapBudgetBytes;

    private FixedIdentityStoreContext(int sidecarMaxBytes, long heapBudgetBytes) {
        this.sidecarMaxBytes = sidecarMaxBytes;
        this.heapBudgetBytes = heapBudgetBytes;
    }

    /** The default 3 KiB sidecar budget and a 1 GiB heap budget. */
    static FixedIdentityStoreContext defaults() {
        return new FixedIdentityStoreContext(3072, DEFAULT_HEAP_BUDGET_BYTES);
    }

    static FixedIdentityStoreContext withSidecarBudget(int sidecarMaxBytes) {
        return new FixedIdentityStoreContext(sidecarMaxBytes, DEFAULT_HEAP_BUDGET_BYTES);
    }

    static FixedIdentityStoreContext withHeapBudget(long heapBudgetBytes) {
        return new FixedIdentityStoreContext(3072, heapBudgetBytes);
    }

    /** A new store over this fixture's identity, budgets and registry. */
    KafkaTrackerStore newStore() {
        return new KafkaTrackerStore(
                CLUSTER_ID, SOURCE_TOPIC_ID, TRACKER_TOPIC_ID, sidecarMaxBytes, heapBudgetBytes, registry);
    }

    SimpleMeterRegistry registry() {
        return registry;
    }
}
