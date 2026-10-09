/**
 * The per-partition in-memory scheduler index backing {@code KafkaTrackerStore} (design §5,
 * post-approval revision 1: fastutil-backed primitive structures).
 *
 * <p>Layering: {@link com.jucius.cesium.kafka.store.tracker.index.EntryPool} (slot storage),
 * {@link com.jucius.cesium.kafka.store.tracker.index.DispatchHeap} (lazy-deletion min-heap on
 * {@code dispatchAtMs}), {@link com.jucius.cesium.kafka.store.tracker.index.ArrivalLog} (append-only
 * arrival order, doubly sorted, binary-searchable) compose into one
 * {@link com.jucius.cesium.kafka.store.tracker.index.PartitionShard} per tracker partition;
 * {@link com.jucius.cesium.kafka.store.tracker.index.TrackerIndex} is the multi-shard container with
 * cross-partition scheduling and the per-source-partition penalty box (design §5.5, D22).
 *
 * <p><strong>Threading.</strong> Everything in this package is confined to the single dispatch
 * thread that owns the affected tracker partitions (design §5.1). There is no locking anywhere;
 * concurrent use is a caller bug.
 *
 * <p><strong>Allocation discipline.</strong> Steady-state hot paths (apply/drain/finalize/
 * restore/next-deadline) allocate nothing and never box: all structures are fastutil primitive
 * collections or primitive arrays, batches are reused parallel-array views, and comparators are
 * primitive {@code IntComparator}s over pool cells.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.store.tracker.index;
