plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "cesium-kafka store SPI and public protocol constants (stable surface for store implementers)"

dependencies {
    // The SPI references Kafka header types and exposes a MeterRegistry via StoreContext.
    api(libs.kafka.clients)
    api(libs.micrometer.core)
}

// Published-surface Javadoc gate (design §12 / ADR-0017): cesium-kafka-api IS a product surface, so
// its Javadoc well-formedness is validated with -Werror as part of `check`. The doclint "missing"
// group (formal @param/@return on self-describing accessors) is intentionally NOT enforced — the
// prose on every public type/method is complete; gating on missing tags would be churn without
// correctness value and risks last-minute flakiness. What IS enforced: broken {@link} references,
// malformed HTML, and bad tags (reference/syntax/html groups) — exactly the failures that rot a
// published SPI's docs. See the v1.0 release notes for the deferred missing-tag follow-up.
tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        addBooleanOption("Xdoclint:all,-missing", true)
        addBooleanOption("Werror", true)
        encoding = "UTF-8"
    }
}
tasks.named("check") { dependsOn(tasks.javadoc) }
