plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "KafkaTrackerStore: the flagship tracker-topic-backed scheduler store (fastutil-backed in-memory index)"

dependencies {
    api(project(":cesium-kafka-api"))
    implementation(libs.fastutil.core)
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
}
