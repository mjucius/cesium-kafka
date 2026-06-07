package com.jucius.cesium.kafka.core.policy;

/**
 * What to do with a record the destination broker <em>permanently</em> rejects on produce — a
 * record-level rejection that no amount of retrying can clear ({@code RecordTooLargeException} when
 * the relay plus appended provenance headers exceed the producer's {@code max.request.size} or the
 * destination's {@code max.message.bytes}, {@code InvalidRecordException}, …); config
 * {@code route.relay.on-unrelayable} (design §2.4, §3.8 I-8, default {@link #DLQ}).
 *
 * <p>Mirrored on both relay paths: the ingest immediate-relay path and the dispatch delayed-relay
 * path. Transient produce failures (broker availability, ISR shortage, timeouts) are <em>not</em>
 * governed by this policy — they stay on the abort/retry and park-and-degrade path (§3.8) so a
 * blip never drops a message. Only the permanent, record-scoped rejections route here.
 *
 * <p>In every non-{@link #FAIL} mode the offending record is resolved exactly once and the source
 * partition makes progress past it, atomically with the source-offset advance (ingest) or the
 * COMPLETE tombstone + cursor advance (dispatch) — never an infinite abort/rewind/retry loop that
 * wedges the whole partition behind one poison record.
 */
public enum UnrelayablePolicy {
    /**
     * Dead-letter the record with reason {@code unrelayable} (original key/value/headers + error
     * reason/detail + provenance, the §2.4 header-error DLQ shape), produced inside the same
     * transaction that advances past it. Default. Requires a DLQ topic.
     */
    DLQ,
    /**
     * Discard the record with a metric and a loud log; no DLQ notice. The only escape when the DLQ
     * itself cannot hold the record (e.g. the relay exceeds {@code max.request.size}, so the larger
     * DLQ copy would be rejected too) — the engine escalates a {@code DLQ}-routed record to this
     * disposition rather than wedge the partition (logged at ERROR; see design §3.8).
     */
    DROP,
    /**
     * Stop the loop: a permanent destination rejection is an operator problem to surface. Like the
     * other {@code FAIL} policies this leaves the record at the committed offset, so an
     * untrusted-producer source turns one crafted record into a restart-persistent outage — use
     * only when the source is trusted (design §2.3 FAIL caveat).
     */
    FAIL
}
