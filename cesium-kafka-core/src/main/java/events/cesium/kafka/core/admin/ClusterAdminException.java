package events.cesium.kafka.core.admin;

/**
 * A {@link ClusterAdmin} operation failed (transport error, timeout, broker-side error). The
 * original broker exception, when one exists, is preserved in the cause chain so callers can
 * classify (e.g. {@link StartupValidator} downgrades ACL application to a warning when the chain
 * contains {@code SecurityDisabledException} — the cluster simply has no authorizer).
 */
public class ClusterAdminException extends RuntimeException {

    /** Creates an exception with a message only. */
    public ClusterAdminException(String message) {
        super(message);
    }

    /** Creates an exception preserving the broker-side cause. */
    public ClusterAdminException(String message, Throwable cause) {
        super(message, cause);
    }

    /** True when {@code type} appears anywhere in this exception's cause chain (bounded walk). */
    public boolean hasCause(Class<? extends Throwable> type) {
        Throwable current = this;
        for (int depth = 0; current != null && depth < 50; depth++) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
