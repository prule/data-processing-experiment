import org.jetbrains.dokka.gradle.DokkaTask
import java.net.URL

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)

    kotlin("plugin.serialization") version "${libs.plugins.jvm.get().version}"
    id("org.jetbrains.dokka") version "${libs.plugins.jvm.get().version}"

}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(libs.bundles.logging)
    implementation("io.github.xn32:json5k:0.3.0")

    testImplementation(libs.bundles.kotest)

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
                    "https://github.com/prule/data-processing-experiment/tree/part-8/tables/src/main/kotlin"
                ))
                remoteLineSuffix.set("#L")
            }
        }
    }
}