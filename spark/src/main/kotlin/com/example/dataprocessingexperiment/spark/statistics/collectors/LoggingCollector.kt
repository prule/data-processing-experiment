package com.example.dataprocessingexperiment.spark.statistics.collectors

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import mu.KotlinLogging

class LoggingCollector: Collector {
    private val logger = KotlinLogging.logger {}

    override fun add(key: String, column: String, discriminator: String, value: Any) {
        logger.debug { "key = $key, column = $column, discriminator = $discriminator, value = $value" }
    }

    override fun close() {

    }

    fun add(key: String, value: Any) {
        add(key, "", "", value)
    }
}