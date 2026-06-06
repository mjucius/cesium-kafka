# ADR-0004: One route per process

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §10, §1.1

## Context

A "route" is one `(source → destination)` relay with its own tracker topic, consumer groups,
transactional ids, and durable scheduler state. The runnable app is the supported v1 product
surface ([ADR-0017](0017-kafka-4-floor-and-repo-only-publishing.md)), and almost everything that
defines an instance's identity and blast radius keys off a single `applicationId`: group ids
(`cesium.<applicationId>.ingest` / `.dispatch`), transactional ids, default topic names, metrics
common tags, and readiness. Running several routes in one process would multiplex all of those and
couple their failure domains, while buying nothing — Kubernetes (and every other scheduler) scales
per-deployment regardless.

The `core` library, by contrast, is multi-engine by construction: an embedder can instantiate
several engines in one JVM. The constraint is a property of the *app*, not the engine.

## Decision

The **app runs exactly one route per process**. Scaling, isolation, and the operational blast
radius are therefore per-route. The configuration schema reflects this with a singular `route:`
block (rather than a `routes:` list), and the YAML→records pipeline (D11) binds it to one
`RouteConfig`.

Embedders who genuinely need multiple routes in one JVM use `cesium-kafka-core` directly and
construct one engine per route; the app does not expose that.

## Consequences

- One `applicationId` ⇒ one set of groups, transactional ids, metrics, and one readiness verdict:
  operationally legible, with a contained blast radius.
- Fleet scaling is per-route (separate deployments), which also keeps the `roles: [ingest,
  dispatch]` split-fleet model ([ADR-0001](0001-two-consumer-group-architecture.md)) simple.
- A future multi-route app is a **compatible** schema evolution: `route:` → `routes:` adds a list
  without breaking single-route configs, so the decision is not a one-way door.
- Library users keep full multi-engine flexibility; only the packaged app is constrained.
