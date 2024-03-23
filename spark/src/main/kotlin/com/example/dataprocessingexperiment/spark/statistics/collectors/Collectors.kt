package com.example.dataprocessingexperiment.spark.statistics.collectors

class Collectors(private val collectors: List<Collector>): Collector {
    override fun add(key: String, column: String, discriminator: String, value: Any) {
        for (collector in collectors) {
            collector.add(key, column, discriminator, value)
        }
    }

    override fun close() {
        for (collector in collectors) {
            collector.close()
        }
    }
}