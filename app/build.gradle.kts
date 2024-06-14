
plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)

    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {

    implementation(project(":spark"))

    implementation(libs.logback.classic)
    implementation(libs.bundles.logging)
    implementation("io.github.xn32:json5k:0.3.0")

    testImplementation(libs.bundles.kotest)
}

application {
    // Define the main class for the application.
    mainClass = "com.example.dataprocessingexperiment.app.AppKt"

    // spark Java17 Compatible JvmArgs
    applicationDefaultJvmArgs  = listOf(
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
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}