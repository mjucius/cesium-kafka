plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "Executable specification for SchedulerStore implementations: abstract JUnit 5 contract classes and fixtures"

dependencies {
    api(project(":cesium-kafka-api"))
    api(libs.junit.jupiter)
    api(libs.jqwik)
}

// Published-surface Javadoc gate — see cesium-kafka-api/build.gradle.kts for the rationale. The
// store-testkit is the stable surface store implementers subclass, so its Javadoc well-formedness
// is validated with -Werror (reference/syntax/html), excluding the noisy "missing" group.
tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        addBooleanOption("Xdoclint:all,-missing", true)
        addBooleanOption("Werror", true)
        encoding = "UTF-8"
    }
}
tasks.named("check") { dependsOn(tasks.javadoc) }
