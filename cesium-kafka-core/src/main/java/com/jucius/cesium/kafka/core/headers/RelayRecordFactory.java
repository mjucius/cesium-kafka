package com.jucius.cesium.kafka.core.headers;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.jspecify.annotations.Nullable;

/**
 * Builds destination and DLQ {@link ProducerRecord}s from consumed source records per the relay
 * fidelity, provenance, and DLQ contracts of design §2.4.
 *
 * <p><strong>Relay fidelity.</strong> Key, value, and all headers are preserved byte-for-byte in
 * original order, minus exactly the two control headers ({@code cesium-delay-ms},
 * {@code cesium-deliver-at}); other {@code cesium-}-prefixed headers a producer set are <em>not</em>
 * stripped (the prefix names a namespace, not the strip set).
 *
 * <p><strong>Provenance</strong> ({@code headers.stamp-provenance}, default on) appends
 * {@code cesium-relayed-at}, {@code cesium-source-topic/-partition/-offset/-timestamp} and, for
 * delayed records only, {@code cesium-scheduled-for}. {@code cesium-source-timestamp} is omitted
 * when the source record carries no timestamp (the grammar has no rendering for "absent").
 * {@code cesium-clamped: true} is part of the CLAMP policy contract (design §2.3), not provenance,
 * and is stamped regardless of the provenance flag.
 *
 * <p><strong>DLQ records</strong> are part of the versioned public DLQ contract and always carry
 * provenance, independent of {@code headers.stamp-provenance}. The payload-expired loss notice
 * hand-rolls its tiny JSON value — core takes no JSON dependency.
 *
 * <p>Stateless and thread-safe; the clock is an argument on every method.
 */
public final class RelayRecordFactory {

    private final String destinationTopic;

    @Nullable private final String dlqTopic;

    private final boolean stampProvenance;
    private final RelayTimestampPolicy timestampPolicy;
    private final RelayPartitioning partitioning;

    /**
     * @param destinationTopic the relay target topic
     * @param dlqTopic the dead-letter topic, or null when no policy routes to a DLQ (using a DLQ
     *     builder then throws {@link IllegalStateException}; startup validation prevents that
     *     configuration from ever booting)
     * @param stampProvenance {@code headers.stamp-provenance} (default on)
     * @param timestampPolicy {@code relay.timestamp} (default {@code DISPATCH})
     * @param partitioning {@code relay.partitioning} (default {@code BY_KEY})
     */
    public RelayRecordFactory(
            String destinationTopic,
            @Nullable String dlqTopic,
            boolean stampProvenance,
            RelayTimestampPolicy timestampPolicy,
            RelayPartitioning partitioning) {
        this.destinationTopic = Objects.requireNonNull(destinationTopic, "destinationTopic");
        this.dlqTopic = dlqTopic;
        this.stampProvenance = stampProvenance;
        this.timestampPolicy = Objects.requireNonNull(timestampPolicy, "timestampPolicy");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
    }

    /** Relay record for a message that was never scheduled (no delay, past-due, or policy relay). */
    public ProducerRecord<byte[], byte[]> relayImmediate(ConsumerRecord<byte[], byte[]> source, long nowMs) {
        return relay(source, nowMs, false, 0L, false);
    }

    /**
     * Relay record for a dispatched scheduled message.
     *
     * @param scheduledForMs the instant the entry was scheduled for ({@code cesium-scheduled-for})
     * @param clamped whether the schedule came from the {@code on-over-max: CLAMP} policy; stamps
     *     {@code cesium-clamped: true} (independent of the provenance flag)
     */
    public ProducerRecord<byte[], byte[]> relayScheduled(
            ConsumerRecord<byte[], byte[]> source, long nowMs, long scheduledForMs, boolean clamped) {
        return relay(source, nowMs, true, scheduledForMs, clamped);
    }

    private ProducerRecord<byte[], byte[]> relay(
            ConsumerRecord<byte[], byte[]> source,
            long nowMs,
            boolean scheduled,
            long scheduledForMs,
            boolean clamped) {
        Headers headers = new RecordHeaders();
        // Preserve all headers byte-for-byte in order, minus exactly the two control headers.
        for (Header header : source.headers()) {
            if (!isControlHeader(header.key())) {
                headers.add(header);
            }
        }
        // CLAMP contract stamp (§2.3) — applies even when provenance stamping is off.
        if (clamped) {
            headers.add(CesiumHeaders.CLAMPED, ascii("true"));
        }
        if (stampProvenance) {
            stampProvenance(headers, source, nowMs);
            if (scheduled) {
                headers.add(CesiumHeaders.SCHEDULED_FOR, ascii(scheduledForMs));
            }
        }
        Integer partition = partitioning == RelayPartitioning.SOURCE_PARTITION ? source.partition() : null;
        Long timestamp =
                switch (timestampPolicy) {
                    case DISPATCH -> nowMs;
                    case SOURCE -> hasTimestamp(source) ? source.timestamp() : null;
                };
        return new ProducerRecord<>(destinationTopic, partition, timestamp, source.key(), source.value(), headers);
    }

    /**
     * Header-error DLQ record (reasons {@link DlqReasons#MALFORMED_HEADER},
     * {@link DlqReasons#OVER_MAX_DELAY}): the original key/value and <em>all</em> headers —
     * including the offending control headers, which is the point — plus error reason/detail and
     * provenance. Produced by the caller inside the ingest transaction.
     */
    public ProducerRecord<byte[], byte[]> headerErrorDlqRecord(
            ConsumerRecord<byte[], byte[]> source, String reason, String detail, long nowMs) {
        String topic = requireDlqTopic();
        Headers headers = new RecordHeaders();
        for (Header header : source.headers()) {
            headers.add(header);
        }
        headers.add(CesiumHeaders.ERROR_REASON, ascii(reason));
        headers.add(CesiumHeaders.ERROR_DETAIL, detail.getBytes(StandardCharsets.UTF_8));
        // DLQ records always carry provenance — part of the versioned contract, not gated by config.
        stampProvenance(headers, source, nowMs);
        return new ProducerRecord<>(topic, null, nowMs, source.key(), source.value(), headers);
    }

    /**
     * Unrelayable-record DLQ record (reason {@link DlqReasons#UNRELAYABLE}): a record the
     * destination broker <em>permanently</em> rejected on produce (too large, invalid) under
     * {@code route.relay.on-unrelayable: DLQ}. Reuses the header-error DLQ shape — the original
     * key/value and <em>all</em> headers (the source record carries the {@code cesium-*} control
     * headers; they ride along harmlessly, unlike a relay they are not stripped) plus error
     * reason/detail and provenance. Produced by the caller inside the relaying transaction (the
     * ingest transaction for an immediate relay, the dispatch transaction for a delayed relay),
     * atomically with the source-offset advance / COMPLETE tombstone.
     *
     * <p>The {@code source} is the consumed record being relayed: the source-topic record for an
     * immediate relay, or the seek-fetched source payload for a delayed relay.
     *
     * @param detail the destination rejection reason (the producer exception's message)
     */
    public ProducerRecord<byte[], byte[]> unrelayableDlqRecord(
            ConsumerRecord<byte[], byte[]> source, String detail, long nowMs) {
        String topic = requireDlqTopic();
        Headers headers = new RecordHeaders();
        for (Header header : source.headers()) {
            headers.add(header);
        }
        headers.add(CesiumHeaders.ERROR_REASON, ascii(DlqReasons.UNRELAYABLE));
        headers.add(CesiumHeaders.ERROR_DETAIL, detail.getBytes(StandardCharsets.UTF_8));
        // DLQ records always carry provenance — part of the versioned contract, not gated by config.
        stampProvenance(headers, source, nowMs);
        return new ProducerRecord<>(topic, null, nowMs, source.key(), source.value(), headers);
    }

    /**
     * Payload-expired loss notice (design §2.4): the payload is gone and only the pointer remains,
     * so the record is {@code key=null} with a small JSON value:
     * <pre>{"v":1,"sourceTopic":…,"sourcePartition":…,"sourceOffset":…,"scheduledFor":…,
     * "detectedAt":…,"reason":"payload-expired"}</pre>
     * plus {@code cesium-error-reason: payload-expired}. Produced by the caller inside the dispatch
     * transaction, in the same transaction as the COMPLETE tombstone.
     */
    public ProducerRecord<byte[], byte[]> payloadExpiredDlqRecord(
            String sourceTopic, int sourcePartition, long sourceOffset, long scheduledForMs, long detectedAtMs) {
        String topic = requireDlqTopic();
        String json = "{\"v\":1,\"sourceTopic\":" + jsonString(sourceTopic)
                + ",\"sourcePartition\":" + sourcePartition
                + ",\"sourceOffset\":" + sourceOffset
                + ",\"scheduledFor\":" + scheduledForMs
                + ",\"detectedAt\":" + detectedAtMs
                + ",\"reason\":\"" + DlqReasons.PAYLOAD_EXPIRED + "\"}";
        Headers headers = new RecordHeaders();
        headers.add(CesiumHeaders.ERROR_REASON, ascii(DlqReasons.PAYLOAD_EXPIRED));
        return new ProducerRecord<>(topic, null, detectedAtMs, null, json.getBytes(StandardCharsets.UTF_8), headers);
    }

    /** Appends the §2.4 provenance headers (everything except {@code cesium-scheduled-for}). */
    private static void stampProvenance(Headers headers, ConsumerRecord<byte[], byte[]> source, long nowMs) {
        headers.add(CesiumHeaders.RELAYED_AT, ascii(nowMs));
        headers.add(CesiumHeaders.SOURCE_TOPIC, source.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(CesiumHeaders.SOURCE_PARTITION, ascii(source.partition()));
        headers.add(CesiumHeaders.SOURCE_OFFSET, ascii(source.offset()));
        if (hasTimestamp(source)) {
            headers.add(CesiumHeaders.SOURCE_TIMESTAMP, ascii(source.timestamp()));
        }
    }

    private static boolean isControlHeader(String key) {
        return CesiumHeaders.DELAY_MS.equals(key) || CesiumHeaders.DELIVER_AT.equals(key);
    }

    private static boolean hasTimestamp(ConsumerRecord<?, ?> source) {
        return source.timestampType() != TimestampType.NO_TIMESTAMP_TYPE && source.timestamp() >= 0;
    }

    private String requireDlqTopic() {
        if (dlqTopic == null) {
            throw new IllegalStateException(
                    "no DLQ topic configured; startup validation requires one whenever a policy routes to the DLQ");
        }
        return dlqTopic;
    }

    private static byte[] ascii(long value) {
        return ascii(Long.toString(value));
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Minimal RFC 8259 string rendering for the loss-notice JSON — quotes, backslashes, and
     * control characters escaped. Hand-rolled on purpose: core takes no JSON dependency for one
     * seven-field object.
     */
    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
