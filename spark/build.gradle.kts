import org.jetbrains.dokka.gradle.DokkaTask
import java.net.URL

plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)
    kotlin("plugin.serialization") version "${libs.plugins.jvm.get().version}"
    id("org.jetbrains.dokka") version libs.versions.dokka.get()
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {

    api(project(":tables"))

    api("org.scala-lang:scala-library:2.13.12")
    api("org.apache.spark:spark-sql_2.13:3.5.0")
    api("com.fasterxml.jackson.core:jackson-core:2.15.2")

    implementation("io.github.xn32:json5k:0.3.0")
    implementation(libs.bundles.logging)

    testImplementation(libs.bundles.kotest)
}

tasks.named<Test>("test") {
    useJUnitPlatform()

    val sparkJava17CompatibleJvmArgs = listOf(
        "--add-exports=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-exports=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-exports=java.base/java.io=ALL-UNNAMED",
        "--add-exports=java.base/java.net=ALL-UNNAMED",
        "--add-exports=java.base/java.nio=ALL-UNNAMED",
        "--add-exports=java.base/java.util=ALL-UNNAMED",
        "--add-exports=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-exports=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    )
    jvmArgs = sparkJava17CompatibleJvmArgs
}

tasks.withType<DokkaTask>().configureEach {
    dokkaSourceSets {
        named("main") {
            // used as project name in the header
            // needs to match the module name in the first line of Module.md
            moduleName.set("Data Processing Experiment - Spark")

            // contains descriptions for the module and the packages
            includes.from("Module.md")

            // adds source links that lead to this repository, allowing readers
            // to easily find source code for inspected declarations
            sourceLink {
                localDirectory.set(file("src/main/kotlin"))
                remoteUrl.set(URL(
                    "https://github.com/prule/data-processing-experiment/tree/part-8/spark/src/main/kotlin"
                ))
                remoteLineSuffix.set("#L")
            }
        }
    }
}

//configurations.all {
//    exclude(group = "org.apache.logging.log4j")
//}