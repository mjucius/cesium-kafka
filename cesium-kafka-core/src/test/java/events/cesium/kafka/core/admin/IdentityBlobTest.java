package events.cesium.kafka.core.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** {@link IdentityBlob}: §3.1 round-trip, version gating, and the ~30-byte compactness claim. */
class IdentityBlobTest {

    private static final Uuid TOPIC_ID = Uuid.fromString("vXfwOqGAQpKLFu5kJSeSEg");
    private static final String KRAFT_CLUSTER_ID =
            Uuid.fromString("MkU3OEVBNTcwNTJENDM2Qg").toString();

    @Nested
    class RoundTrip {

        @Test
        void uuidStyleClusterId() {
            IdentityBlob blob = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID);
            assertEquals(blob, IdentityBlob.decode(blob.encode()));
        }

        @Test
        void arbitraryStringClusterId() {
            IdentityBlob blob = new IdentityBlob("my-onprem-cluster-1", TOPIC_ID);
            assertEquals(blob, IdentityBlob.decode(blob.encode()));
        }

        @Test
        void nonCanonicalBase64ClusterIdFallsBackToStringModeAndRoundTrips() {
            // 22 chars but not a canonical Uuid rendering (Uuid.toString would differ): must
            // round-trip byte-for-byte through string mode, not be "normalized".
            String nonCanonical = "AAAAAAAAAAAAAAAAAAAAA/";
            IdentityBlob blob = new IdentityBlob(nonCanonical, TOPIC_ID);
            assertEquals(nonCanonical, IdentityBlob.decode(blob.encode()).clusterId());
        }

        @Test
        void randomizedUuids() {
            for (int i = 0; i < 100; i++) {
                IdentityBlob blob = new IdentityBlob(Uuid.randomUuid().toString(), Uuid.randomUuid());
                assertEquals(blob, IdentityBlob.decode(blob.encode()));
            }
        }
    }

    @Nested
    class SizeBound {

        @Test
        void uuidModeIsCompact() {
            String encoded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID).encode();
            byte[] raw = Base64.getUrlDecoder().decode(encoded);
            // §3.1 "~30 bytes": version + flags + 16-byte topic id + 16-byte cluster uuid.
            assertEquals(34, raw.length);
            assertTrue(encoded.length() <= 48, "encoded length " + encoded.length() + " exceeds the budget");
        }

        @Test
        void oversizedClusterIdRejectedAtConstruction() {
            String oversized = "x".repeat(256);
            assertThrows(IllegalArgumentException.class, () -> new IdentityBlob(oversized, TOPIC_ID));
        }
    }

    @Nested
    class Rejection {

        @Test
        void unknownVersionRejected() {
            byte[] raw = Base64.getUrlDecoder().decode(new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID).encode());
            raw[0] = 2; // a future version
            String tampered = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
            IllegalArgumentException e =
                    assertThrows(IllegalArgumentException.class, () -> IdentityBlob.decode(tampered));
            assertTrue(e.getMessage().contains("version"), e.getMessage());
        }

        @Test
        void malformedBase64Rejected() {
            assertThrows(IllegalArgumentException.class, () -> IdentityBlob.decode("!!!not base64!!!"));
        }

        @Test
        void truncatedBlobRejected() {
            String encoded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID).encode();
            byte[] raw = Base64.getUrlDecoder().decode(encoded);
            byte[] truncated = new byte[10];
            System.arraycopy(raw, 0, truncated, 0, truncated.length);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> IdentityBlob.decode(
                            Base64.getUrlEncoder().withoutPadding().encodeToString(truncated)));
        }

        @Test
        void trailingGarbageRejected() {
            byte[] raw = Base64.getUrlDecoder().decode(new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID).encode());
            byte[] extended = new byte[raw.length + 3];
            System.arraycopy(raw, 0, extended, 0, raw.length);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> IdentityBlob.decode(
                            Base64.getUrlEncoder().withoutPadding().encodeToString(extended)));
        }
    }

    @Nested
    class Mismatch {

        @Test
        void matchingIdentity() {
            IdentityBlob recorded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID);
            IdentityBlob live = IdentityBlob.decode(recorded.encode());
            assertTrue(recorded.matches(live));
            assertEquals("identity matches", recorded.describeMismatch(live));
        }

        @Test
        void recreatedSourceTopicNamed() {
            IdentityBlob recorded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID);
            IdentityBlob live = new IdentityBlob(KRAFT_CLUSTER_ID, Uuid.fromString("zNxfPVBLRzWGDfFGAfMyLQ"));
            assertFalse(recorded.matches(live));
            String description = recorded.describeMismatch(live);
            assertTrue(description.contains("source topic id changed"), description);
            assertTrue(description.contains("recreated"), description);
            assertTrue(description.contains(TOPIC_ID.toString()), description);
            assertTrue(description.contains("R-10"), description);
        }

        @Test
        void differentClusterNamed() {
            IdentityBlob recorded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID);
            IdentityBlob live = new IdentityBlob("other-cluster", TOPIC_ID);
            String description = recorded.describeMismatch(live);
            assertTrue(description.contains("cluster id changed"), description);
            assertTrue(description.contains(KRAFT_CLUSTER_ID), description);
            assertTrue(description.contains("other-cluster"), description);
        }

        @Test
        void bothFieldsNamed() {
            IdentityBlob recorded = new IdentityBlob(KRAFT_CLUSTER_ID, TOPIC_ID);
            IdentityBlob live = new IdentityBlob("other-cluster", Uuid.randomUuid());
            String description = recorded.describeMismatch(live);
            assertTrue(description.contains("cluster id changed"), description);
            assertTrue(description.contains("source topic id changed"), description);
        }
    }
}
