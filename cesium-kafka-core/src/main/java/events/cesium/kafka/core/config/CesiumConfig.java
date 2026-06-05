package events.cesium.kafka.core.config;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * The frozen root configuration (design §8 sketch). One route per process — group ids,
 * transactional ids, metrics, readiness, and blast radius all key off one {@code applicationId}.
 *
 * <p>An instance of this record is <em>structurally</em> complete (every component non-null,
 * defaults materialized) but not necessarily <em>valid</em>: required fields may be empty and
 * ranges unchecked until {@link CesiumConfigValidator} has produced a clean
 * {@link ValidationReport}. Loaders must validate before handing the config to the engine.
 *
 * @param applicationId required; namespaces consumer groups, transactional ids, and default
 *     internal topic names
 * @param instanceId required stable deployment-slot id, or the explicit {@code random} opt-in
 *     (D10)
 * @param roles which loops this instance runs; default both (stored as an immutable
 *     {@link EnumSet})
 * @param kafka client passthrough + locked-key policy (§8)
 * @param route the served route
 * @param delay delay limits and violation policies (§2.3)
 * @param headers control-header protocol options (§2.3, §2.4)
 * @param store scheduler-store selection (D7)
 * @param ingest ingest-loop settings
 * @param dispatch dispatch-loop settings
 * @param observability metrics/health endpoint settings (§9)
 * @param startupChecks startup-check strictness (§7.6)
 */
public record CesiumConfig(
        String applicationId,
        InstanceId instanceId,
        Set<Role> roles,
        KafkaConfig kafka,
        RouteConfig route,
        DelayConfig delay,
        HeadersConfig headers,
        StoreConfig store,
        IngestConfig ingest,
        DispatchConfig dispatch,
        ObservabilityConfig observability,
        StartupChecks startupChecks) {

    /** Default roles: both loops (§8 defaults table). */
    public static final Set<Role> DEFAULT_ROLES = Collections.unmodifiableSet(EnumSet.allOf(Role.class));

    /** Materializes the §8 defaults for absent components. */
    public CesiumConfig {
        applicationId = Objects.requireNonNullElse(applicationId, "");
        instanceId = Objects.requireNonNullElse(instanceId, new InstanceId(""));
        roles = freezeRoles(roles);
        kafka = Objects.requireNonNullElse(kafka, KafkaConfig.defaults());
        route = Objects.requireNonNullElse(route, RouteConfig.defaults());
        delay = Objects.requireNonNullElse(delay, DelayConfig.defaults());
        headers = Objects.requireNonNullElse(headers, HeadersConfig.defaults());
        store = Objects.requireNonNullElse(store, StoreConfig.defaults());
        ingest = Objects.requireNonNullElse(ingest, IngestConfig.defaults());
        dispatch = Objects.requireNonNullElse(dispatch, DispatchConfig.defaults());
        observability = Objects.requireNonNullElse(observability, ObservabilityConfig.defaults());
        startupChecks = Objects.requireNonNullElse(startupChecks, StartupChecks.defaults());
    }

    /**
     * Absent roles default to both loops; an explicitly empty set is preserved so the validator
     * can report it (an empty {@link EnumSet#copyOf} is also illegal). Non-empty sets are frozen
     * into an unmodifiable {@link EnumSet}.
     */
    private static Set<Role> freezeRoles(Set<Role> roles) {
        if (roles == null) {
            return DEFAULT_ROLES;
        }
        if (roles.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(EnumSet.copyOf(roles));
    }
}
