plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    api(project(":tables"))

    api("org.scala-lang:scala-library:2.13.12")
    api("org.apache.spark:spark-sql_2.13:3.5.0")
    api("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.2_2.13:1.2.4")
//    api("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.2_2.13:3.5.0")
    api("com.fasterxml.jackson.core:jackson-core:2.15.2")
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
