package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

/**
 * Smoke test for the {@link ToxiproxySupport} capability: a record round-trips through the proxied
 * broker listener, a {@code cut} deterministically breaks the client&#8596;broker link, and a
 * {@code restore} brings it back — verified on the always-healthy direct path. Proves the M6
 * fault-injection substrate works end-to-end before scenario suites build on it.
 */
class ToxiproxyHarnessSmokeIT {

    @Test
    void proxyCutAndRestoreRoundTripsARecord() {
        ToxiproxySupport cluster = ToxiproxySupport.shared();

        // The proxied path is a distinct endpoint from the direct path — that separation is what
        // lets one member be partitioned while another stays reachable.
        assertNotEquals(
                cluster.directBootstrap(), cluster.proxiedBootstrap(), "proxied and direct bootstraps must differ");

        // produce(before) -> cut -> produce fails -> restore -> produce(after); exactly before+after
        // are committed and readable on the direct path.
        cluster.assertProxyRoundTrips();
    }
}
