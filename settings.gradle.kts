pluginManagement {
    includeBuild("build-logic")
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    // Auto-provisions the JDK 21 toolchain when the host JVM differs (e.g. CI lanes on newer LTS).
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "cesium-kafka"

include(
    "cesium-kafka-api",
    "cesium-kafka-core",
    "cesium-kafka-store-kafka",
    "cesium-kafka-store-testkit",
    "cesium-kafka-app",
    "cesium-kafka-it",
)
// cesium-kafka-benchmarks (JMH + JOL) is added at milestone M8.
