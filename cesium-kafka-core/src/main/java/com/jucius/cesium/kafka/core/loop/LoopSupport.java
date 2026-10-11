package com.jucius.cesium.kafka.core.loop;

import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

/** Helpers and value types shared verbatim by the ingest and dispatch loops (design §3.8). */
public final class LoopSupport {

    private LoopSupport() {}

    /**
     * True when any throwable in {@code error}'s cause chain is an instance of one of {@code types}
     * (clients wrap the classifying exception, e.g. a fenced producer under {@code KafkaException}).
     * {@code null} is treated as "no match".
     */
    // ReferenceEquality: `t.getCause() == t` is the self-referential-cause guard — a Throwable
    // whose getCause() returns itself would loop forever. Identity is the intended test, and Error
    // Prone's suggested .equals() is wrong (Throwable does not override it). Flagged from 2.50.0.
    @SuppressWarnings("ReferenceEquality")
    @SafeVarargs
    public static boolean anyCause(@Nullable Throwable error, Class<? extends Throwable>... types) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            for (Class<? extends Throwable> type : types) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Adds {@code n} committed DLQ records to {@code cesium_dlq_records_total{reason}}. A zero
     * count registers nothing, so a reason never written has no series.
     */
    public static void countDlq(MeterRegistry registry, String reason, long n) {
        if (n > 0) {
            registry.counter("cesium.dlq.records", "reason", reason).increment((double) n);
        }
    }

    /** The {@code cause} tag on abort metrics and backoff logs: the failure's simple class name. */
    public static String causeTag(Throwable error) {
        return error.getClass().getSimpleName();
    }

    /** Capped exponential backoff: {@code initial * 2^(streak-1)}, never above the cap. */
    public static long boundedExponential(long initialMs, long maxMs, int streak) {
        long backoff = initialMs;
        for (int i = 1; i < streak && backoff < maxMs; i++) {
            backoff <<= 1;
        }
        return Math.min(backoff, maxMs);
    }

    /**
     * Records (or escalates) an unrelayable record's disposition. A first rejection of a relay maps
     * to the policy route (DLQ, or DROP). A <em>second</em> rejection of an already-DLQ-routed record
     * means the unrelayable DLQ write was itself too large to produce (the relay exceeds {@code
     * max.request.size}, so the larger DLQ copy is rejected too) — escalate to DROP so the partition
     * still advances rather than wedging on the DLQ write, logged loudly (§3.8).
     */
    public static void promoteUnrelayable(
            Map<SourceCoord, Unrelayable> unrelayable,
            UnrelayableHit hit,
            UnrelayablePolicy policy,
            String sourceTopic,
            MeterRegistry registry,
            Logger log) {
        Unrelayable existing = unrelayable.get(hit.coord());
        if (existing == null) {
            UnrelayablePolicy route = policy == UnrelayablePolicy.DROP ? UnrelayablePolicy.DROP : UnrelayablePolicy.DLQ;
            unrelayable.put(hit.coord(), new Unrelayable(route, hit.detail()));
        } else if (existing.route() == UnrelayablePolicy.DLQ) {
            log.error(
                    "the unrelayable DLQ write for source {}-{}@{} was ALSO permanently rejected ({}); escalating to"
                            + " DROP so the partition is not wedged on the DLQ write (route.relay.on-unrelayable=DLQ,"
                            + " §3.8)",
                    sourceTopic,
                    hit.coord().partition(),
                    hit.coord().offset(),
                    hit.detail());
            registry.counter("cesium.unrelayable.dlq.rejected").increment();
            unrelayable.put(hit.coord(), new Unrelayable(UnrelayablePolicy.DROP, hit.detail()));
        }
    }

    /** A source-record identity within the (single) source topic: partition + offset. */
    public record SourceCoord(int partition, long offset) {}

    /**
     * A resolved disposition for a permanently-unrelayable record (§3.8 I-8).
     *
     * @param route {@code DLQ} or {@code DROP} — never {@code FAIL}, which stops the loop instead
     */
    public record Unrelayable(UnrelayablePolicy route, String detail) {}

    /** A permanent destination rejection attributed to a specific source record by a send callback. */
    public record UnrelayableHit(SourceCoord coord, String detail) {}

    /** Control-flow signal: {@code commitTransaction} stayed ambiguous past the retry budget. */
    public static final class InDoubtCommit extends RuntimeException {
        public final int attempts;
        public final RuntimeException failure;

        public InDoubtCommit(int attempts, RuntimeException failure) {
            super("commitTransaction outcome ambiguous after " + attempts + " attempts", failure);
            this.attempts = attempts;
            this.failure = failure;
        }
    }
}
