package com.jucius.cesium.kafka.core.headers;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;

/**
 * Machine-readable values of the {@link CesiumHeaders#ERROR_REASON cesium-error-reason} DLQ header
 * (design §2.4 DLQ contract — versioned and public; these strings are wire format, not internal
 * identifiers, and must never change spelling).
 */
public final class DlqReasons {

    /** The winning control header failed the value grammar; produced inside the ingest transaction. */
    public static final String MALFORMED_HEADER = "malformed-header";

    /** The requested delay exceeded {@code delay.max}; produced inside the ingest transaction. */
    public static final String OVER_MAX_DELAY = "over-max-delay";

    /**
     * The destination broker permanently rejected the relay on produce (record too large for
     * {@code max.request.size}/{@code max.message.bytes}, invalid record, …) and
     * {@code route.relay.on-unrelayable: DLQ}; produced inside the relaying transaction (the ingest
     * transaction for an immediate relay, the dispatch transaction for a delayed relay), atomically
     * with the progress past the offending record (design §2.4, §3.8 I-8).
     */
    public static final String UNRELAYABLE = "unrelayable";

    /**
     * The payload was no longer fetchable at dispatch time (retention/compaction/size or tier
     * eviction); the loss notice is produced inside the dispatch transaction.
     */
    public static final String PAYLOAD_EXPIRED = "payload-expired";

    private DlqReasons() {}
}
