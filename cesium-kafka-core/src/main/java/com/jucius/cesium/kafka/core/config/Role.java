package com.jucius.cesium.kafka.core.config;

/**
 * The loops an instance runs (design §6). {@code roles: [ingest, dispatch]} is the default; the
 * two halves scale on separate fleets by giving each fleet a single role.
 */
public enum Role {
    /** Run the ingest loop: source consumer (group A) + ingest transactional producer (§3.1). */
    INGEST,
    /** Run the dispatch loop: tracker consumer (group B) + dispatch producer + seek consumer (§3.2). */
    DISPATCH
}
