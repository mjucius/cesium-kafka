package com.jucius.cesium.kafka.core.headers;

/**
 * Outcome of decoding the cesium control headers of one source record (design §2.3).
 *
 * <p>The result is purely syntactic + range semantics: it says what the headers <em>requested</em>
 * and whether the request was valid, but takes no position on what to do about violations — that
 * is the policy engine's job ({@code com.jucius.cesium.kafka.core.policy.IngestPolicyEngine}), keeping
 * both halves independently table-testable.
 *
 * <p><strong>Conflicts are not errors.</strong> When both control headers are present
 * {@code cesium-deliver-at} wins, and when one header carries multiple values the last value wins
 * (design §2.3); either case sets {@link #conflicted()} so the ingest loop can increment
 * {@code cesium_header_errors_total{type="conflict"}} and WARN without changing the outcome.
 */
public sealed interface HeaderParseResult {

    /**
     * Whether precedence had to resolve ambiguous input: both control headers present, or multiple
     * values for one header. Observability-only — a conflicted result is still authoritative.
     */
    boolean conflicted();

    /** Neither control header present: the record relays immediately, untouched. */
    record NoDelay() implements HeaderParseResult {
        /** Shared instance for the hot path — most records carry no control headers. */
        public static final NoDelay INSTANCE = new NoDelay();

        @Override
        public boolean conflicted() {
            return false;
        }
    }

    /**
     * The requested dispatch instant is now or already past. Past or zero values relay immediately
     * and are <em>not</em> errors (design §2.3) — reason {@value #REASON_PAST_DUE} for metrics.
     */
    record RelayImmediately(boolean conflicted) implements HeaderParseResult {
        /** Metric/log reason for an on-time-or-late request relayed without scheduling. */
        public static final String REASON_PAST_DUE = "past_due";

        /** Why the record relays immediately despite carrying a control header. */
        public String reason() {
            return REASON_PAST_DUE;
        }
    }

    /**
     * A valid future dispatch instant within {@code delay.max}.
     *
     * @param dispatchAtMs requested delivery instant, epoch milliseconds UTC, strictly after the
     *     ingest clock and at most {@code now + delay.max}
     */
    record Schedule(long dispatchAtMs, boolean conflicted) implements HeaderParseResult {}

    /**
     * The winning control header's value failed the grammar: not ASCII decimal
     * {@code ^[0-9]{1,19}$} (or not exactly 8 bytes in binary mode, or a negative binary delay),
     * a null value, or a decimal string overflowing a signed 64-bit long. Handled by
     * {@code delay.on-malformed-header}.
     *
     * @param detail human-readable diagnostic naming the header and the offending value
     */
    record Malformed(String detail, boolean conflicted) implements HeaderParseResult {}

    /**
     * The request was well-formed but exceeded {@code delay.max}: {@code cesium-delay-ms} above
     * the maximum, or {@code cesium-deliver-at} after {@code now + delay.max}. Handled by
     * {@code delay.on-over-max}.
     *
     * @param requestedDispatchAtMs the instant the producer asked for, epoch milliseconds UTC;
     *     overflow-saturated, never wrapped
     * @param detail human-readable diagnostic naming the header, the value, and the limit
     */
    record OverMax(long requestedDispatchAtMs, String detail, boolean conflicted) implements HeaderParseResult {}
}
