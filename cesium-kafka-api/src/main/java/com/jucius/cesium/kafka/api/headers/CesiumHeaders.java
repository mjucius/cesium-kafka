package com.jucius.cesium.kafka.api.headers;

/**
 * Header names and value grammar of the cesium protocol (design §2.3–2.4).
 *
 * <p><strong>Encoding.</strong> All cesium header values are canonical UTF-8 ASCII decimal
 * ({@link #ASCII_DECIMAL_VALUE_REGEX}) so every client ecosystem can produce them without helper
 * code. An 8-byte big-endian {@code long} decode for the control headers exists behind the
 * {@code headers.accept-binary-long-values} compatibility flag (default off); the modes are
 * mutually exclusive because a length-8 value is ambiguous between the two (design D1).
 *
 * <p><strong>Control headers</strong> ({@link #DELAY_MS}, {@link #DELIVER_AT}) are consumed by
 * cesium and are the only headers <em>stripped</em> on relay. If both are present,
 * {@link #DELIVER_AT} wins (conflict counted and logged); multiple values for one header resolve
 * to the last value, also counted as a conflict. Validation is regex plus range:
 * {@code cesium-delay-ms} in {@code [0, delay.max]}, {@code cesium-deliver-at} at most
 * {@code now + delay.max}. Past or zero values relay immediately and are <em>not</em> errors.
 * Malformed and over-max values are handled by the configured policies (default: DLQ for both;
 * design D3).
 *
 * <p><strong>Provenance headers</strong> are stamped by cesium on relayed records (default on,
 * {@code headers.stamp-provenance}); everything else about the record — key, value, all
 * non-control headers — is preserved byte-for-byte.
 *
 * <p><strong>DLQ headers</strong> describe why a record landed on the dead-letter topic;
 * header-error DLQ records additionally carry the original key/value/headers (including the
 * offending control headers) plus provenance.
 */
public final class CesiumHeaders {

    /**
     * Namespace prefix of every header cesium owns, and the relay strip set (VULN-004): the entire
     * {@code cesium-} namespace is stripped from a relayed record, so a producer cannot forge
     * provenance/CLAMP headers that ride through as authentic. cesium re-stamps its own authoritative
     * provenance/CLAMP after stripping. A DLQ record additionally preserves the offending control
     * headers ({@link #DELAY_MS}, {@link #DELIVER_AT}) for diagnosis. Producers should not set
     * {@code cesium-}-prefixed headers other than the two control headers.
     */
    public static final String CONTROL_HEADER_PREFIX = "cesium-";

    /**
     * Grammar of every cesium header value: ASCII decimal, 1–19 digits (fits an unsigned-decimal
     * rendering of a non-negative Java {@code long}). Values failing this regex are malformed and
     * handled by the {@code delay.on-malformed-header} policy.
     */
    public static final String ASCII_DECIMAL_VALUE_REGEX = "^[0-9]{1,19}$";

    // --- Control headers (consumed by cesium; stripped on relay) ---------------------------------

    /**
     * Control header: relay the record N milliseconds after the <em>source record timestamp</em>
     * (CreateTime — deterministic across EOS abort/retry cycles; falls back to the ingest wall
     * clock when the record has no timestamp). Value: ASCII decimal milliseconds, valid range
     * {@code [0, delay.max]}.
     */
    public static final String DELAY_MS = "cesium-delay-ms";

    /**
     * Control header: relay the record at an absolute instant. Value: ASCII decimal epoch
     * milliseconds UTC, at most {@code now + delay.max}. Takes precedence over {@link #DELAY_MS}
     * when both are present. A past instant relays immediately and is not an error.
     */
    public static final String DELIVER_AT = "cesium-deliver-at";

    // --- Provenance headers (stamped by cesium on relayed records) -------------------------------

    /** Provenance: instant the record was relayed, ASCII decimal epoch milliseconds UTC. */
    public static final String RELAYED_AT = "cesium-relayed-at";

    /** Provenance: name of the source topic the record was consumed from. UTF-8. */
    public static final String SOURCE_TOPIC = "cesium-source-topic";

    /** Provenance: source partition the record was consumed from, ASCII decimal. */
    public static final String SOURCE_PARTITION = "cesium-source-partition";

    /** Provenance: source offset of the record, ASCII decimal. */
    public static final String SOURCE_OFFSET = "cesium-source-offset";

    /**
     * Provenance: the source record's original timestamp, ASCII decimal epoch milliseconds UTC.
     * Relayed records default to a fresh DISPATCH timestamp (design §2.4); the original time is
     * recoverable from this header.
     */
    public static final String SOURCE_TIMESTAMP = "cesium-source-timestamp";

    /**
     * Provenance, delayed records only: the instant the record was scheduled for, ASCII decimal
     * epoch milliseconds UTC. Compare with {@link #RELAYED_AT} to observe scheduling precision.
     */
    public static final String SCHEDULED_FOR = "cesium-scheduled-for";

    /**
     * Provenance: stamped {@code true} when the requested delay exceeded {@code delay.max} and the
     * {@code delay.on-over-max: CLAMP} policy pinned the delivery time to {@code now + delay.max}
     * (design §2.3).
     */
    public static final String CLAMPED = "cesium-clamped";

    // --- DLQ headers (design §2.4 DLQ contract) ---------------------------------------------------

    /**
     * DLQ: machine-readable reason the record was dead-lettered. Values:
     * {@code malformed-header}, {@code over-max-delay} (header-error records, produced inside the
     * ingest transaction), {@code payload-expired} (loss notices, produced inside the dispatch
     * transaction).
     */
    public static final String ERROR_REASON = "cesium-error-reason";

    /** DLQ: human-readable detail accompanying {@link #ERROR_REASON} (e.g. the offending value). */
    public static final String ERROR_DETAIL = "cesium-error-detail";

    private CesiumHeaders() {}
}
