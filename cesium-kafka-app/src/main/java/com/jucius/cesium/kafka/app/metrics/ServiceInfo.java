package com.jucius.cesium.kafka.app.metrics;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * The immutable payload of the {@code /info} endpoint (design §9): version + git commit, the
 * applicationId, the loop roles this instance runs, and any named startup acknowledgments the
 * operator opted into (e.g. {@code size-based-retention}, §7.6).
 *
 * @param build version and git provenance
 * @param applicationId the route's application id (namespaces groups, txn ids, metrics, readiness)
 * @param roles the started loop roles, lower-cased (e.g. {@code ["ingest", "dispatch"]})
 * @param acknowledgments named operator acknowledgments accepted at startup
 */
public record ServiceInfo(BuildInfo build, String applicationId, List<String> roles, List<String> acknowledgments) {

    /** Defensively copies the collection components so the record is deeply immutable. */
    public ServiceInfo {
        roles = List.copyOf(roles);
        acknowledgments = List.copyOf(acknowledgments);
    }

    /**
     * Renders this info as a JSON object for the {@code /info} endpoint. {@code version} and {@code
     * gitCommit} are always present (the ops fields). The sensitive/operational detail —
     * {@code applicationId} (the fencing-id seed), {@code roles} and {@code acknowledgments} — is
     * emitted only when {@code detailed} is set, so the unauthenticated
     * endpoint discloses nothing reconnaissance-worthy by default (security M3).
     *
     * @param detailed whether to include the sensitive/operational fields (opt-in, off by default)
     */
    public String toJson(boolean detailed) {
        ObjectNode root = JsonNodeFactory.instance.objectNode().put("version", build.version());
        build.gitCommit().ifPresent(commit -> root.put("gitCommit", commit));
        if (!detailed) {
            return root.toString(); // default unauthenticated payload: ops provenance only
        }
        root.put("applicationId", applicationId);
        roles.forEach(root.putArray("roles")::add);
        acknowledgments.forEach(root.putArray("acknowledgments")::add);
        return root.toString();
    }
}
