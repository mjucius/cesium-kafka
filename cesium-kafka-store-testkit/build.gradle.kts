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
