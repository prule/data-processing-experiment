import org.jetbrains.dokka.gradle.DokkaTask
import java.net.URL

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)

    kotlin("plugin.serialization") version "1.9.20"
    id("org.jetbrains.dokka") version "1.9.10"

}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {

    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.7")
    implementation("io.github.xn32:json5k:0.3.0")


    // Use the Kotlin JUnit 5 integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")

    // Use the JUnit 5 integration.
    testImplementation(libs.junit.jupiter.engine)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation ("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation ("io.kotest:kotest-assertions-core:5.8.0")

}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.withType<DokkaTask>().configureEach {
    dokkaSourceSets {
        named("main") {
            // used as project name in the header
            // needs to match the module name in the first line of Module.md
            moduleName.set("Data Processing Experiment - Tables")

            // contains descriptions for the module and the packages
            includes.from("Module.md")

            // adds source links that lead to this repository, allowing readers
            // to easily find source code for inspected declarations
            sourceLink {
                localDirectory.set(file("src/main/kotlin"))
                remoteUrl.set(URL(
                    "https://github.com/prule/data-processing-experiment/tree/part-3/tables/src/main/kotlin"
                ))
                remoteLineSuffix.set("#L")
            }
        }
    }
}