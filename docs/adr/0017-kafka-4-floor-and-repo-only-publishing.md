# ADR-0017: Kafka 4.0 floor + repo-only publishing + app as product surface

- **Status:** Accepted
- **Date:** 2026-06-06
- **Amended by:** [ADR-0019](0019-2-0-stability-baseline.md) (2.0 stability baseline)
- **Design reference:** [`../design.md`](../design.md) §13 (post-approval revision 2; supersedes the
  release-workflow wording of §13 and Open Questions #1, #2, #4)

> This ADR records post-approval project-owner decisions that supersede part of design §13. Where
> §13 still describes publishing to Maven Central and pushing an image to GHCR, this ADR wins:
> v1.0 publishing is repo-only.

## Context

Design §13's `release.yml` originally proposed publishing `api`/`core`/`store-kafka`/`testkit` to
Maven Central (signed, OIDC) and pushing the app image to GHCR, and the design's open questions left
the broker-version floor and the supported-surface commitment undecided. The owner resolved all
three for v1.0.

## Decision

- **Broker floor: Kafka 4.0+.** No 3.x lane. The KIP-890 transactions-v2 / KIP-848 posture assumes
  4.0+ brokers.
- **Publishing: repo-only for v1.0.** No Maven Central, no GHCR image push. The release workflow
  runs the full build/test and publishes a GitHub Release with the built distribution archives
  (`distTar` / `distZip`) only. The `Dockerfile` (an `eclipse-temurin:21-jre` runtime; the Gradle
  `application`-plugin start script needs a shell, so a shell-less distroless base is not used)
  remains for users to build the image locally. It is not pushed.
- **Product surface.** The runnable app is the supported v1 product. The engine's programmatic
  API is internal (amended by ADR-0019: no compatibility promise). The store SPI module (`cesium-kafka-api`) plus the
  `cesium-kafka-store-testkit` are the stable, semver-published surface for store implementers
  from 1.0 ([ADR-0003](0003-sealed-two-archetype-store-spi.md)).
- **License:** MIT; the repository is public.

## Consequences

- `release.yml` scope shrinks to build + full integration pass + GitHub Release of the distribution
  archives; no signing, Sonatype/Central, OIDC, or GHCR infrastructure ships in v1.
- The only semver-stability promise is over `cesium-kafka-api` + `cesium-kafka-store-testkit`; `core`
  may evolve freely (amended by ADR-0019; the semver promise starts at 2.0.0). Javadoc well-formedness for those two modules (broken `{@link}` references,
  malformed HTML, bad tags) is gated in `check` with `-Werror` (`-Xdoclint:all,-missing`). The
  doclint "missing-tag" group (formal `@param`/`@return` on self-describing accessors) is not
  enforced and is a deferred follow-up, so the gate cannot flake on a tag omission. The prose on
  every public type and method is complete.
- App-as-product reinforces one route per process
  ([ADR-0004](0004-one-route-per-process.md)) and the Docker/`docker-compose` quickstart as the
  primary entry point.
- Maven Central / GHCR publication remain available as a future, additive release-workflow change
  if the project later chooses to distribute libraries and images. That is not a v1 commitment.
