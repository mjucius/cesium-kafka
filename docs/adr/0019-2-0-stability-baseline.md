# ADR-0019: 2.0 stability baseline

- **Status:** Accepted
- **Date:** 2026-10-07
- **Design reference:** [`../design.md`](../design.md) post-approval revision 4 (2.0.0); amends
  [ADR-0003](0003-sealed-two-archetype-store-spi.md) and
  [ADR-0017](0017-kafka-4-floor-and-repo-only-publishing.md)

## Context

The 1.2.0 cleanup was planned as a fully backward-compatible minor release: the published
`cesium-kafka-api` and `cesium-kafka-store-testkit` surfaces could only gain additions, and config
keys, metric names, the JSON log shape and wire formats were frozen. A docs-versus-code audit then
found bugs whose compatible fixes would keep workarounds in place: config keys that do nothing
(`dispatch.coalesce`), a published javadoc promise that was never implemented
(`store.type: class:<FQCN>`), a metric that counts only half its events, and a per-worker cap
documented as process-wide. cesium-kafka has no external users yet, so keeping 1.x compatibility
protects nobody.

## Decision

- The cleanup ships as **2.0.0** instead of 1.2.0. 2.0.0 may break the api, testkit, config keys,
  metrics and log shape where that gives the clean fix. Every break is listed in the CHANGELOG
  "Breaking changes" section.
- From 2.0.0, `cesium-kafka-api` and `cesium-kafka-store-testkit` are the semver-stable surface for
  store implementers, evolving additively within 2.x.
- `cesium-kafka-core` (the engine's programmatic API) stays internal, with no compatibility promise.
  The runnable app remains the supported product (ADR-0004, ADR-0017).
- Wire formats are unchanged: header protocol version 1, tracker value format `0x01`, and the
  committed-cursor offset metadata. A mixed 1.1.5/2.0.0 rolling restart reads the same tracker,
  offsets and worker-0 static member ids.

## Consequences

- Statements that said "semver-stable from 1.0" or "internal until 1.x" now say "from 2.0.0" and
  "internal".
- Config defaults chosen only for 1.x compatibility are open to change in 2.0; each change is
  recorded in the CHANGELOG.
- Breaking the api or testkit again requires 3.0.0.
