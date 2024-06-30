package com.example.dataprocessingexperiment.app

import mu.KotlinLogging
import org.apache.parquet.Log
import java.time.LocalDateTime

enum class LogEvent(val description: String) {
    LE001("I'm Givin' Her All She's Got"),
    LE002("Live Long and Prosper"),
    LE003("Make It So");

    override fun toString(): String {
        return "$name/$description: "
    }
}

class LoggerExample {
    private val logger = KotlinLogging.logger {}

    fun go() {
        // version 1
        logger.warn { "I'm Givin' Her All She's Got, Captain!" }

        // version 2
        // this is used by a Datadog monitor, please don't change
        // <link to monitor goes here>
        logger.warn { "I'm Givin' Her All She's Got, Captain!" }

        // version 3
        logger.warn { "${LogEvent.LE001} and any other information goes here" }

        LogMgr.log(LogEvent.LE001) { "and any other information goes here ${LocalDateTime.now()}" }
    }
}

object LogMgr {
    private val logger = KotlinLogging.logger {}

    fun log(event: LogEvent, msg: () -> kotlin.Any?) {
        logger.info { "$event - ${msg()}" }
    }
}


fun main() {
    println("Starting...")

    LoggerExample().go()

    println("Finished...")
}
