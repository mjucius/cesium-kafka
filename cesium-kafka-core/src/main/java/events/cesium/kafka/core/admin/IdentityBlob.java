package events.cesium.kafka.core.admin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.kafka.common.Uuid;

/**
 * The versioned identity blob committed inside {@code OffsetAndMetadata} metadata (design §3.1,
 * R-10/R-17): {@code {v: 1, clusterId, sourceTopicId}}. Kafka topic IDs survive nothing — a
 * recreated topic gets a new {@link Uuid} — so binding the committed offsets to the cluster ID and
 * source topic ID lets the engine detect recreation at startup and at every offset fetch and
 * fail-fast instead of delivering wrong payloads.
 *
 * <p><strong>Wire format</strong> (URL-safe unpadded Base64 of a compact binary layout, ~34 raw
 * bytes for standard KRaft cluster ids):
 *
 * <pre>
 *   byte 0      version (0x01)
 *   byte 1      flags (bit 0: cluster id stored as a 16-byte Uuid)
 *   bytes 2-17  source topic id (msb, lsb — big-endian)
 *   cluster id  16 Uuid bytes when bit 0 is set (KRaft cluster ids are canonical 22-char
 *               base64url Uuid strings); otherwise a 1-byte length followed by UTF-8 bytes
 * </pre>
 *
 * <p>Unknown versions are rejected (forward compatibility is explicit, never silent): a newer
 * cesium may extend the blob, and an older engine must refuse to half-read it.
 *
 * @param clusterId the Kafka cluster id from {@code Admin.describeCluster()}
 * @param sourceTopicId the source topic's {@link Uuid} from {@code Admin.describeTopics()}
 */
public record IdentityBlob(String clusterId, Uuid sourceTopicId) {

    /** The only wire version this engine reads and writes. */
    public static final byte VERSION = 1;

    private static final byte FLAG_CLUSTER_ID_IS_UUID = 0x01;
    private static final int UUID_BYTES = 16;
    private static final int HEADER_BYTES = 2;
    private static final int MAX_CLUSTER_ID_UTF8_BYTES = 255;

    /** Validates that components are present and the cluster id is encodable. */
    public IdentityBlob {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(sourceTopicId, "sourceTopicId");
        if (asUuid(clusterId) == null
                && clusterId.getBytes(StandardCharsets.UTF_8).length > MAX_CLUSTER_ID_UTF8_BYTES) {
            throw new IllegalArgumentException(
                    "clusterId exceeds " + MAX_CLUSTER_ID_UTF8_BYTES + " UTF-8 bytes: '" + clusterId + "'");
        }
    }

    /** Encodes this blob as URL-safe unpadded Base64 for the offset-metadata channel. */
    public String encode() {
        Uuid clusterUuid = asUuid(clusterId);
        ByteBuffer buffer;
        if (clusterUuid != null) {
            buffer = ByteBuffer.allocate(HEADER_BYTES + UUID_BYTES + UUID_BYTES);
            buffer.put(VERSION).put(FLAG_CLUSTER_ID_IS_UUID);
            putUuid(buffer, sourceTopicId);
            putUuid(buffer, clusterUuid);
        } else {
            byte[] clusterBytes = clusterId.getBytes(StandardCharsets.UTF_8);
            buffer = ByteBuffer.allocate(HEADER_BYTES + UUID_BYTES + 1 + clusterBytes.length);
            buffer.put(VERSION).put((byte) 0);
            putUuid(buffer, sourceTopicId);
            buffer.put((byte) clusterBytes.length).put(clusterBytes);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    /**
     * Decodes a blob previously produced by {@link #encode()}.
     *
     * @throws IllegalArgumentException on malformed Base64, a truncated layout, trailing bytes, or
     *     an unknown version — the caller treats any of these as an integrity fail-fast (§3.6)
     */
    public static IdentityBlob decode(String metadata) {
        byte[] raw;
        try {
            raw = Base64.getUrlDecoder().decode(metadata);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("identity blob is not valid Base64: " + e.getMessage(), e);
        }
        if (raw.length < HEADER_BYTES + UUID_BYTES) {
            throw new IllegalArgumentException("identity blob truncated: " + raw.length + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        byte version = buffer.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unknown identity blob version " + version + " (this engine reads v"
                    + VERSION + "); the offsets may have been written by a newer cesium");
        }
        byte flags = buffer.get();
        Uuid sourceTopicId = getUuid(buffer);
        String clusterId;
        if ((flags & FLAG_CLUSTER_ID_IS_UUID) != 0) {
            if (buffer.remaining() != UUID_BYTES) {
                throw new IllegalArgumentException("identity blob cluster-id section malformed: expected " + UUID_BYTES
                        + " Uuid bytes, got " + buffer.remaining());
            }
            clusterId = getUuid(buffer).toString();
        } else {
            if (buffer.remaining() < 1) {
                throw new IllegalArgumentException("identity blob truncated before the cluster-id length");
            }
            int length = buffer.get() & 0xFF;
            if (buffer.remaining() != length) {
                throw new IllegalArgumentException("identity blob cluster-id section malformed: declared " + length
                        + " bytes, found " + buffer.remaining());
            }
            byte[] clusterBytes = new byte[length];
            buffer.get(clusterBytes);
            clusterId = new String(clusterBytes, StandardCharsets.UTF_8);
        }
        return new IdentityBlob(clusterId, sourceTopicId);
    }

    /** True when {@code live} carries the same cluster and source-topic identity. */
    public boolean matches(IdentityBlob live) {
        return equals(live);
    }

    /**
     * A human-readable description of how {@code live} differs from this recorded identity,
     * naming the fail-fast rule and the runbook action (§3.1, R-9/R-10). Returns
     * {@code "identity matches"} when nothing differs.
     */
    public String describeMismatch(IdentityBlob live) {
        if (matches(live)) {
            return "identity matches";
        }
        StringBuilder sb = new StringBuilder();
        if (!clusterId.equals(live.clusterId)) {
            sb.append("cluster id changed: recorded '")
                    .append(clusterId)
                    .append("' but the connected cluster is '")
                    .append(live.clusterId)
                    .append("'");
        }
        if (!sourceTopicId.equals(live.sourceTopicId)) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append("source topic id changed: recorded ")
                    .append(sourceTopicId)
                    .append(" but the live topic is ")
                    .append(live.sourceTopicId)
                    .append(" — the source topic was recreated under the same name");
        }
        sb.append(" (§3.1 identity binding; R-10 runbook: fail fast — reset the tracker or accept loss explicitly;"
                + " never deliver payloads from a topic with a different identity)");
        return sb.toString();
    }

    private static void putUuid(ByteBuffer buffer, Uuid uuid) {
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
    }

    private static Uuid getUuid(ByteBuffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new Uuid(msb, lsb);
    }

    /**
     * Parses {@code clusterId} as a canonical Kafka {@link Uuid} string (KRaft cluster ids are
     * 22-char unpadded base64url), or returns null when the string-mode fallback must be used.
     * The round-trip check guarantees decode reproduces the exact original string.
     */
    private static @org.jspecify.annotations.Nullable Uuid asUuid(String clusterId) {
        try {
            Uuid parsed = Uuid.fromString(clusterId);
            return parsed.toString().equals(clusterId) ? parsed : null;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
