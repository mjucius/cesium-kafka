plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "cesium-kafka engine: ingest/dispatch loops, transaction management, policies, validation (internal until 1.x)"

dependencies {
    api(project(":cesium-kafka-api"))
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    implementation(libs.micrometer.core)
}
