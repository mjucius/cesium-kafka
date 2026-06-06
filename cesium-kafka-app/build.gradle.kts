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
    // Logback is a compile dependency (not runtimeOnly) because the app ships a custom one-line
    // JSON encoder (events.cesium.kafka.app.logging.CompactJsonEncoder, design §9) referenced from
    // the JSON logging profile; the encoder extends logback's EncoderBase.
    implementation(libs.logback.classic)
}

application {
    mainClass = "events.cesium.kafka.app.CesiumApp"
    applicationName = "cesium-kafka"
}

// Build-info resource for the /info endpoint (design §9): version (the Gradle project version) and
// the short git commit when available. Read at runtime by events.cesium.kafka.app.metrics.BuildInfo
// from the classpath; a clean fallback to the jar manifest / "unknown" keeps the app runnable when
// the resource or git is absent.
val generateBuildInfo by tasks.registering {
    val outputDir = layout.buildDirectory.dir("generated/build-info")
    val projectVersion = project.version.toString()
    val repoRoot = rootProject.layout.projectDirectory.asFile
    outputs.dir(outputDir)
    doLast {
        val gitCommit =
            try {
                val process = ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .directory(repoRoot)
                    .redirectErrorStream(true)
                    .start()
                val text = process.inputStream.bufferedReader().use { it.readText() }.trim()
                if (process.waitFor() == 0) text else ""
            } catch (ignored: Exception) {
                ""
            }
        val dir = outputDir.get().asFile
        dir.mkdirs()
        dir.resolve("cesium-build-info.properties")
            .writeText("version=$projectVersion\ngitCommit=$gitCommit\n")
    }
}

sourceSets {
    main {
        resources {
            srcDir(generateBuildInfo)
        }
    }
}
