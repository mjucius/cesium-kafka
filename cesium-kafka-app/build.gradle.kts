plugins {
    id("cesium.java-conventions")
    application
}

description = "Runnable cesium-kafka service: configuration, lifecycle, health and metrics endpoints"

dependencies {
    implementation(project(":cesium-kafka-core"))
    implementation(project(":cesium-kafka-store-kafka"))
    implementation(libs.jackson.yaml)
    // Optional<T> record components (route.dlq, tracker.acl-principal) bind via the Jdk8Module.
    implementation(libs.jackson.jdk8)
    implementation(libs.micrometer.prometheus)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass = "events.cesium.kafka.app.CesiumApp"
    applicationName = "cesium-kafka"
}
