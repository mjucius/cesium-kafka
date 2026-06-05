# Contributing to cesium-kafka

Thanks for your interest! This project is in pre-release development; expect churn until v1.0.

## Building

- JDK 21+ (the build provisions the 21 toolchain automatically)
- `./gradlew build` — compile, unit tests, formatting and static-analysis checks
- `./gradlew :cesium-kafka-it:integrationTest` — integration tests (requires Docker)

## Test tiers

| Tier | Command | Speed |
|---|---|---|
| Unit + property tests | `./gradlew test` | milliseconds, no broker |
| Store SPI contract kit | runs with `test` in store modules | fast |
| Integration (Testcontainers, Kafka 4.x) | `./gradlew :cesium-kafka-it:integrationTest` | minutes, needs Docker |

## Code style

- Formatting is enforced by Spotless + palantir-java-format: `./gradlew spotlessApply`
- Error Prone + NullAway run on every compile; production packages are `@NullMarked`

## Design changes

Significant decisions are recorded as ADRs under `docs/adr/`. Start from
[docs/design.md](docs/design.md) before proposing architectural changes — invariants I1–I9 and the
failure matrix there are load-bearing; changes to them need a corresponding test and ADR.
