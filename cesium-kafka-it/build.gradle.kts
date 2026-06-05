plugins {
    id("cesium.java-conventions")
}

description = "Integration tests against real Kafka 4.x (Testcontainers, KRaft); cesium instances run as separate JVMs for crash tests"

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter(libs.versions.junit.asProvider())
            dependencies {
                implementation(project(":cesium-kafka-app"))
                implementation(project(":cesium-kafka-core"))
                implementation(project(":cesium-kafka-store-kafka"))
                implementation(libs.kafka.clients)
                implementation(libs.micrometer.core)
                implementation(libs.testcontainers.kafka)
                implementation(libs.testcontainers.junit)
                implementation(libs.testcontainers.toxiproxy)
                implementation(libs.awaitility)
                implementation(libs.slf4j.api)
                runtimeOnly(libs.logback.classic)
            }
            targets.all {
                testTask.configure {
                    // Testcontainers 1.21.3 pins docker-java to Docker API v1.32 when no
                    // api.version is configured (DockerClientProviderStrategy), and Docker
                    // Engine 29+ removed API versions < 1.43 (HTTP 400 on /v1.32/*). Declaring
                    // a modern version skips the legacy pin; 1.44 = Engine 25+ (Jan 2024 floor).
                    // Overridable via -Dapi.version for older daemons.
                    systemProperty("api.version", System.getProperty("api.version") ?: "1.44")
                }
            }
        }
    }
}

// Integration tests are run explicitly (CI integration job / nightly matrix), not as part of `check`.
