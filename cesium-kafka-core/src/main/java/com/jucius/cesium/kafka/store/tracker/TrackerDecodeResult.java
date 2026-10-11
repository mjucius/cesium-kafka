package com.jucius.cesium.kafka.store.tracker;

import com.jucius.cesium.kafka.api.store.CompletionReason;

/**
 * Outcome of decoding one tracker-topic record (design §2.2, D15).
 *
 * <p>Decoding is total: every {@code (key, value, headers)} triple maps to exactly one of these
 * variants and never throws on the hot path. Wire-format violations surface as {@link Invalid} so
 * the replay loop can count ({@code cesium_tracker_invalid_records_total}), WARN with the offset,
 * and skip — never apply, never crash (design §2.2, R12).
 */
public sealed interface TrackerDecodeResult {

    /**
     * A v1 ADD: schedule the source offset (carried in the record key) for dispatch.
     *
     * @param dispatchAtMs the instant the entry is due, epoch milliseconds UTC
     * @param clamped whether the ingest-time {@code delay.on-over-max: CLAMP} policy pinned the
     *     dispatch instant to {@code now + delay.max}; dispatch must stamp
     *     {@code cesium-clamped: true} on the relay (design §2.3) — the flag travels on the wire
     *     because the index is a cache and the relay may be produced by a different instance than
     *     the one that ingested the record
     */
    record Add(long dispatchAtMs, boolean clamped) implements TrackerDecodeResult {}

    /**
     * A structurally valid v1 record of the reserved type {@code 0x02 CANCEL} (D15). No v1 writer
     * produces it; decoding it distinctly (rather than as {@link Invalid}) lets the caller treat it
     * as not-yet-supported — e.g. count under a dedicated metric — without misclassifying a future
     * cancellation API's records as corruption.
     */
    record Cancel() implements TrackerDecodeResult {
        /** Shared instance — the variant carries no data. */
        public static final Cancel INSTANCE = new Cancel();
    }

    /**
     * A completion tombstone: the source offset (record key) is settled and its index entry must be
     * removed. The value is {@code null} for compaction; the reason rides in the
     * {@link TrackerWireFormat#COMPLETION_REASON_HEADER} record header (D15).
     *
     * @param reason why the entry was settled
     */
    record Complete(CompletionReason reason) implements TrackerDecodeResult {}

    /**
     * A structurally valid completion tombstone whose reason header names no known
     * {@link CompletionReason} — a tombstone written by a newer cesium with an added reason
     * constant. The completion <em>must still apply</em>: the reason header is informational and
     * the correctness payload of a tombstone is "this key completed"; classifying it
     * {@link Invalid} would count-and-skip it, leave the entry pending, and convert a legitimate
     * completion into a future duplicate dispatch. Decoding it distinctly (rather than as
     * {@link Complete} with a guessed reason) lets the replay loop apply the removal while
     * surfacing the novelty under a dedicated metric/WARN. This adds no R12 forgery surface: a
     * writer who can forge an unknown reason can equally forge a known one — the
     * missing-header case is different (it matches the forged-tombstone shape) and stays
     * {@link Invalid}.
     *
     * @param rawReason the unrecognized reason name as carried on the wire (US-ASCII decoded),
     *     for the WARN log and metric
     */
    record CompleteUnknownReason(String rawReason) implements TrackerDecodeResult {}

    /**
     * The record failed wire-format validation: bad key length, wrong magic, unsupported version,
     * bad value length, unknown type, or a tombstone whose completion-reason header is missing or
     * carries a null value. Count-and-skip per design §2.2 — never an exception. (A tombstone
     * with a <em>present but unrecognized</em> reason is {@link CompleteUnknownReason}, not
     * Invalid — skipping it would be a duplicate-dispatch vector.)
     *
     * @param detail human-readable diagnostic for the WARN log (names the violated rule and the
     *     offending bytes/length)
     */
    record Invalid(String detail) implements TrackerDecodeResult {}
}
