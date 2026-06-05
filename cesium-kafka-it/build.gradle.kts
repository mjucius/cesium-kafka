plugins {
    id("cesium.java-conventions")
}

description = "Integration tests against real Kafka 4.x (Testcontainers, KRaft); cesium instances run as separate JVMs for crash tests"

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project(":cesium-kafka-app"))
                implementation(project(":cesium-kafka-core"))
                implementation(project(":cesium-kafka-store-kafka"))
                implementation(libs.kafka.clients)
                implementation(libs.testcontainers.kafka)
                implementation(libs.testcontainers.junit)
                implementation(libs.testcontainers.toxiproxy)
                implementation(libs.awaitility)
                implementation(libs.slf4j.api)
                runtimeOnly(libs.logback.classic)
            }
        }
    }
}

// Integration tests are run explicitly (CI integration job / nightly matrix), not as part of `check`.
