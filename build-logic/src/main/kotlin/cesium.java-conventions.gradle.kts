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
tasks.withType<Test>().configureEach {
    useJUnitPlatform()
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
