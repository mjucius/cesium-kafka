import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
    java
    id("com.diffplug.spotless")
    id("net.ltgt.errorprone")
}

val libs = the<VersionCatalogsExtension>().named("libs")

group = "events.cesium"
version = providers.gradleProperty("cesiumVersion").getOrElse("0.1.0-SNAPSHOT")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    compileOnly(libs.findLibrary("jspecify").orElseThrow())
    testCompileOnly(libs.findLibrary("jspecify").orElseThrow())
    "errorprone"(libs.findLibrary("errorprone-core").orElseThrow())
    "errorprone"(libs.findLibrary("nullaway").orElseThrow())
    testImplementation(libs.findLibrary("junit-jupiter").orElseThrow())
    testRuntimeOnly(libs.findLibrary("junit-platform-launcher").orElseThrow())
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release = 21
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint:all,-processing,-serial"))
    options.errorprone {
        disableWarningsInGeneratedCode = true
        option("NullAway:AnnotatedPackages", "events.cesium")
        if (name.contains("Test", ignoreCase = true)) {
            // Tests may exercise nullness edge cases deliberately.
            disable("NullAway")
        } else {
            error("NullAway")
        }
    }
}

// CI lanes can run the test suites on a newer JVM than the compile toolchain
// (e.g. ./gradlew build -PtestToolchain=25).
val testToolchain = providers.gradleProperty("testToolchain")
// Nightly-lane tag gates (design §13 / §11.3-11 / D12). A test task can opt the heavy lanes back
// in via Gradle properties — nightly.yml sets them; the default PR lane leaves them unset:
//   -PincludeNightly  the heavy Toxiproxy/compaction/multi-instance scenarios (@Tag("nightly"))
//   -PincludeKip848   the KIP-848 consumer-protocol EOS variants (@Tag("kip848")), alongside the rest
//   -Pkip848Only      run ONLY the @Tag("kip848") variants (the non-blocking, continue-on-error job)
fun gradleFlag(name: String): Boolean =
    providers.gradleProperty(name).map { it.equals("true", ignoreCase = true) }.getOrElse(false)

tasks.withType<Test>().configureEach {
    // @Tag("soak") tests (design §11.2: large-entry-count memory ceilings, GC-delta
    // measurements) are nightly-lane signal, not PR-gating signal: they are excluded from every
    // default lane and run only through the dedicated `soakTest` task below. @Tag("nightly") (heavy
    // Toxiproxy/compaction/multi-instance ITs) and @Tag("kip848") (the KIP-848 consumer-protocol EOS
    // variants, non-blocking per D12) are likewise off the default lane unless explicitly opted in.
    val isSoakLane = name == "soakTest"
    val includeNightly = gradleFlag("includeNightly")
    val includeKip848 = gradleFlag("includeKip848")
    val kip848Only = gradleFlag("kip848Only")
    useJUnitPlatform {
        when {
            isSoakLane -> includeTags("soak")
            // The dedicated KIP-848 nightly job (continue-on-error): only the consumer-protocol
            // variants run, so a 848 incompatibility never masks a classic regression.
            kip848Only -> includeTags("kip848")
            else -> {
                excludeTags("soak")
                if (!includeNightly) {
                    excludeTags("nightly")
                }
                if (!includeKip848) {
                    excludeTags("kip848")
                }
            }
        }
    }
    if (testToolchain.isPresent) {
        javaLauncher = javaToolchains.launcherFor {
            languageVersion = JavaLanguageVersion.of(testToolchain.get().toInt())
        }
    }
    testLogging {
        events("failed", "skipped")
        showExceptions = true
        showCauses = true
    }
}

// The nightly soak lane (design §11.2): runs only the @Tag("soak") tests of the default test
// source set. `./gradlew soakTest`.
tasks.register<Test>("soakTest") {
    description = "Runs @Tag(\"soak\") tests (nightly lane, design §11.2)."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    shouldRunAfter(tasks.test)
}

spotless {
    java {
        palantirJavaFormat(
            libs.findVersion("palantir-java-format").orElseThrow().requiredVersion
        )
        target("src/**/*.java")
        removeUnusedImports()
        formatAnnotations()
    }
}
