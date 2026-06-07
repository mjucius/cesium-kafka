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
     * The payload was no longer fetchable at dispatch time (retention/compaction/size or tier
     * eviction); the loss notice is produced inside the dispatch transaction.
     */
    public static final String PAYLOAD_EXPIRED = "payload-expired";

    private DlqReasons() {}
}
