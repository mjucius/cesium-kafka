package com.jucius.cesium.kafka.app.metrics;

import com.jucius.cesium.kafka.api.store.StoreCapabilities;
import com.jucius.cesium.kafka.app.json.Json;
import java.util.List;
import java.util.Optional;

/**
 * The immutable payload of the {@code /info} endpoint (design §9): version + git commit, the
 * applicationId, the loop roles this instance runs, the store type and (once the store has started)
 * its declared {@link StoreCapabilities}, and any named startup acknowledgments the operator opted
 * into (e.g. {@code size-based-retention}, §7.6).
 *
 * <p>Capabilities are {@link Optional} because the observability server starts before the engine
 * resolves the store; the app supplies a fuller {@code ServiceInfo} once the store is up. The
 * endpoint reads through a supplier, so it reflects the latest snapshot on every request.
 *
 * @param build version and git provenance
 * @param applicationId the route's application id (namespaces groups, txn ids, metrics, readiness)
 * @param roles the started loop roles, lower-cased (e.g. {@code ["ingest", "dispatch"]})
 * @param storeType the configured store type (e.g. {@code kafka-tracker})
 * @param capabilities the store's self-declared capabilities once started, else empty
 * @param acknowledgments named operator acknowledgments accepted at startup
 */
public record ServiceInfo(
        BuildInfo build,
        String applicationId,
        List<String> roles,
        String storeType,
        Optional<StoreCapabilities> capabilities,
        List<String> acknowledgments) {

    /** Defensively copies the collection components so the record is deeply immutable. */
    public ServiceInfo {
        roles = List.copyOf(roles);
        acknowledgments = List.copyOf(acknowledgments);
    }

    /**
     * Renders this info as a JSON object for the {@code /info} endpoint. {@code version} and {@code
     * gitCommit} are always present (the ops fields). The sensitive/operational detail —
     * {@code applicationId} (the fencing-id seed), {@code roles}, the store {@code capabilities}, and
     * {@code acknowledgments} — is emitted only when {@code detailed} is set, so the unauthenticated
     * endpoint discloses nothing reconnaissance-worthy by default (security M3).
     *
     * @param detailed whether to include the sensitive/operational fields (opt-in, off by default)
     */
    public String toJson(boolean detailed) {
        Json.Obj root = Json.object().str("version", build.version());
        build.gitCommit().ifPresent(commit -> root.str("gitCommit", commit));
        if (!detailed) {
            // Default unauthenticated payload: ops provenance plus the innocuous store type only.
            return root.raw("store", Json.object().str("type", storeType).end()).end();
        }
        Json.Arr rolesJson = Json.array();
        for (String role : roles) {
            rolesJson.str(role);
        }
        Json.Arr ackJson = Json.array();
        for (String acknowledgment : acknowledgments) {
            ackJson.str(acknowledgment);
        }
        Json.Obj store = Json.object().str("type", storeType);
        capabilities.ifPresent(caps -> store.raw(
                "capabilities",
                Json.object()
                        .str("affinity", caps.affinity().name())
                        .str("dispatchGuarantee", caps.dispatchGuarantee().name())
                        .bool("requiresTrackerTopic", caps.requiresTrackerTopic())
                        .bool("supportsCancellation", caps.supportsCancellation())
                        .end()));
        return root.str("applicationId", applicationId)
                .raw("roles", rolesJson.end())
                .raw("store", store.end())
                .raw("acknowledgments", ackJson.end())
                .end();
    }
}
