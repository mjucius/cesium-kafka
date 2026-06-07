package com.jucius.cesium.kafka.core.config;

import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * Dispatch-loop settings (design §3.2, §5.3, §6, §7, §8). The unfetchable-payload policy component
 * is the same enum type the dispatch loop consumes — one type per concept.
 *
 * <p>Boxed numeric component types are deliberate: binding layers pass {@code null} for absent
 * keys and the compact constructor materializes the documented default.
 *
 * @param workers number of dispatch threads, each owning a tracker consumer, dispatch producer,
 *     seek consumer, and disjoint shards (§6); default 1, useful N ≤ tracker partitions fleet-wide
 * @param batch per-transaction batch bounds (D8)
 * @param drain time-sliced drain settings (§6, R2)
 * @param coalesce intentional dispatch coalescing window; default {@code PT0S} — never early,
 *     never deliberately late
 * @param idleCursorInterval how long a partition may go untouched before its cursor advances in a
 *     records-free transaction (§3.5); default {@code PT30S}
 * @param cursor committed-cursor v2 settings (§3.5)
 * @param fetch seek-fetch budgets and the per-source-partition penalty box (§7)
 * @param onUnfetchablePayload policy for provably-expired payloads (§7.4); default DLQ
 * @param maxPendingPerPartition pause/resume backpressure threshold for ACTIVE shards (§5.3);
 *     default 2,000,000
 * @param maxPendingTotal global pending cap; {@link #AUTO_MAX_PENDING_TOTAL} (the default) derives
 *     it from the heap budget — ≈ 25% of {@code Xmx} ÷ {@value #INDEX_BYTES_PER_ENTRY} B/entry
 *     (§5.3, R18; 64 B/entry per the post-approval fastutil revision)
 */
public record DispatchConfig(
        Integer workers,
        Batch batch,
        Drain drain,
        Duration coalesce,
        Duration idleCursorInterval,
        Cursor cursor,
        Fetch fetch,
        UnfetchablePayloadPolicy onUnfetchablePayload,
        Long maxPendingPerPartition,
        Long maxPendingTotal) {

    /** Sentinel for {@code maxPendingTotal}: derive the cap from the heap budget (§8 "AUTO"). */
    public static final long AUTO_MAX_PENDING_TOTAL = 0L;

    /**
     * Capacity-planning bytes per pending index entry: 64 B typical per the post-approval fastutil
     * revision (doubling growth replaces bounded chunk slack; design revision block, §5.4).
     */
    public static final long INDEX_BYTES_PER_ENTRY = 64L;

    /** The index heap budget is 25% of max heap — i.e. max heap divided by this (§5.3). */
    public static final long HEAP_BUDGET_DIVISOR = 4L;

    /** Default worker count (§8 defaults table). */
    public static final int DEFAULT_WORKERS = 1;

    /** Default backpressure threshold per partition (§8 defaults table). */
    public static final long DEFAULT_MAX_PENDING_PER_PARTITION = 2_000_000L;

    /** Materializes the §8 defaults for absent components. */
    public DispatchConfig {
        workers = Objects.requireNonNullElse(workers, DEFAULT_WORKERS);
        batch = Objects.requireNonNullElse(batch, Batch.defaults());
        drain = Objects.requireNonNullElse(drain, Drain.defaults());
        coalesce = Objects.requireNonNullElse(coalesce, Duration.ZERO);
        idleCursorInterval = Objects.requireNonNullElse(idleCursorInterval, Duration.ofSeconds(30));
        cursor = Objects.requireNonNullElse(cursor, Cursor.defaults());
        fetch = Objects.requireNonNullElse(fetch, Fetch.defaults());
        onUnfetchablePayload = Objects.requireNonNullElse(onUnfetchablePayload, UnfetchablePayloadPolicy.DLQ);
        maxPendingPerPartition = Objects.requireNonNullElse(maxPendingPerPartition, DEFAULT_MAX_PENDING_PER_PARTITION);
        maxPendingTotal = Objects.requireNonNullElse(maxPendingTotal, AUTO_MAX_PENDING_TOTAL);
    }

    /** Returns a {@code DispatchConfig} populated entirely from the §8 defaults table. */
    public static DispatchConfig defaults() {
        return new DispatchConfig(
                DEFAULT_WORKERS,
                Batch.defaults(),
                Drain.defaults(),
                Duration.ZERO,
                Duration.ofSeconds(30),
                Cursor.defaults(),
                Fetch.defaults(),
                UnfetchablePayloadPolicy.DLQ,
                DEFAULT_MAX_PENDING_PER_PARTITION,
                AUTO_MAX_PENDING_TOTAL);
    }

    /** The heap budget reserved for the pending index: 25% of {@code maxHeapBytes} (§5.3). */
    public static long heapBudgetBytes(long maxHeapBytes) {
        return maxHeapBytes / HEAP_BUDGET_DIVISOR;
    }

    /**
     * The effective global pending cap: the configured value, or — when {@code AUTO} — the heap
     * budget divided by {@value #INDEX_BYTES_PER_ENTRY} B/entry (≈ 25% of {@code Xmx} ÷ 64).
     */
    public long resolveMaxPendingTotal(long maxHeapBytes) {
        return maxPendingTotal == AUTO_MAX_PENDING_TOTAL
                ? heapBudgetBytes(maxHeapBytes) / INDEX_BYTES_PER_ENTRY
                : maxPendingTotal;
    }

    /**
     * Per-transaction batch bounds (D8).
     *
     * @param maxEntries maximum entries per dispatch transaction; default 10,000 — sized so
     *     ~5–20 ms commit overhead amortizes to ≥10⁵ dispatches/s/thread
     * @param maxBytes decompressed payload byte budget; default 32 MiB, enforced in the fetch pass
     *     with truncate-and-carry-over because record sizes are only known there (§7.2, R8)
     */
    public record Batch(Integer maxEntries, Long maxBytes) {

        /** Default entries bound (§8 defaults table). */
        public static final int DEFAULT_MAX_ENTRIES = 10_000;

        /** Default decompressed bytes bound: 32 MiB (§8 defaults table). */
        public static final long DEFAULT_MAX_BYTES = 32L * 1024 * 1024;

        /** Materializes the §8 defaults for absent components. */
        public Batch {
            maxEntries = Objects.requireNonNullElse(maxEntries, DEFAULT_MAX_ENTRIES);
            maxBytes = Objects.requireNonNullElse(maxBytes, DEFAULT_MAX_BYTES);
        }

        /** Returns a {@code Batch} populated entirely from the §8 defaults table. */
        public static Batch defaults() {
            return new Batch(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES);
        }
    }

    /**
     * Time-sliced drain settings (§6, R2).
     *
     * @param maxSlice maximum back-to-back transaction time before the loop returns to a real
     *     {@code poll()}; default {@code PT1M}, validated ≤ {@code max.poll.interval.ms / 3} when
     *     the operator overrides the poll interval — group membership must survive due-storms
     */
    public record Drain(Duration maxSlice) {

        /** Default drain slice (§8 defaults table). */
        public static final Duration DEFAULT_MAX_SLICE = Duration.ofMinutes(1);

        /** Materializes the §8 defaults for absent components. */
        public Drain {
            maxSlice = Objects.requireNonNullElse(maxSlice, DEFAULT_MAX_SLICE);
        }

        /** Returns a {@code Drain} populated entirely from the §8 defaults table. */
        public static Drain defaults() {
            return new Drain(DEFAULT_MAX_SLICE);
        }
    }

    /**
     * Committed-cursor v2 settings (§3.5, D16).
     *
     * @param sidecarMaxBytes encoded pinned-entry sidecar budget in the offset metadata; default
     *     3072, validated ≤ broker {@code offset.metadata.max.bytes} at startup
     */
    public record Cursor(Integer sidecarMaxBytes) {

        /** Default sidecar budget in bytes (§8 defaults table). */
        public static final int DEFAULT_SIDECAR_MAX_BYTES = 3072;

        /** Materializes the §8 defaults for absent components. */
        public Cursor {
            sidecarMaxBytes = Objects.requireNonNullElse(sidecarMaxBytes, DEFAULT_SIDECAR_MAX_BYTES);
        }

        /** Returns a {@code Cursor} populated entirely from the §8 defaults table. */
        public static Cursor defaults() {
            return new Cursor(DEFAULT_SIDECAR_MAX_BYTES);
        }
    }

    /**
     * Seek-fetch budgets and per-source-partition isolation (§7, D22).
     *
     * @param timeout overall fetch deadline per batch; default {@code PT30S}
     * @param partitionTimeFloor minimum per-partition time slice — one slow partition must not
     *     consume the whole deadline; default {@code PT2S}
     * @param penalty per-source-partition penalty box backoff (§7.3)
     */
    public record Fetch(Duration timeout, Duration partitionTimeFloor, Penalty penalty) {

        /** Default overall fetch timeout (§8 defaults table). */
        public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

        /** Default per-partition time floor (§8 defaults table). */
        public static final Duration DEFAULT_PARTITION_TIME_FLOOR = Duration.ofSeconds(2);

        /** Materializes the §8 defaults for absent components. */
        public Fetch {
            timeout = Objects.requireNonNullElse(timeout, DEFAULT_TIMEOUT);
            partitionTimeFloor = Objects.requireNonNullElse(partitionTimeFloor, DEFAULT_PARTITION_TIME_FLOOR);
            penalty = Objects.requireNonNullElse(penalty, Penalty.defaults());
        }

        /** Returns a {@code Fetch} populated entirely from the §8 defaults table. */
        public static Fetch defaults() {
            return new Fetch(DEFAULT_TIMEOUT, DEFAULT_PARTITION_TIME_FLOOR, Penalty.defaults());
        }

        /**
         * Penalty-box backoff for TRANSIENT fetch outcomes (§7.3, D22): exponential per-partition
         * not-before deadlines, reset on success.
         *
         * @param backoff initial backoff; default {@code PT0.05S}
         * @param backoffMax backoff ceiling under consecutive failures; default {@code PT10S}
         */
        public record Penalty(Duration backoff, Duration backoffMax) {

            /** Default initial backoff: {@code PT0.05S} (§8 defaults table). */
            public static final Duration DEFAULT_BACKOFF = Duration.ofMillis(50);

            /** Default backoff ceiling: {@code PT10S} (§8 defaults table). */
            public static final Duration DEFAULT_BACKOFF_MAX = Duration.ofSeconds(10);

            /** Materializes the §8 defaults for absent components. */
            public Penalty {
                backoff = Objects.requireNonNullElse(backoff, DEFAULT_BACKOFF);
                backoffMax = Objects.requireNonNullElse(backoffMax, DEFAULT_BACKOFF_MAX);
            }

            /** Returns a {@code Penalty} populated entirely from the §8 defaults table. */
            public static Penalty defaults() {
                return new Penalty(DEFAULT_BACKOFF, DEFAULT_BACKOFF_MAX);
            }
        }
    }
}
