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
