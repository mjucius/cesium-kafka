plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "cesium-kafka engine: ingest/dispatch loops, transaction management, policies, validation (internal, no compatibility promise)"

dependencies {
    api(project(":cesium-kafka-api"))
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    implementation(libs.micrometer.core)

    testImplementation(libs.jqwik)
}
