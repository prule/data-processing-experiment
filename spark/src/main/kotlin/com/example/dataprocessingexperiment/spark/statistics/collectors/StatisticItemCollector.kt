package com.example.dataprocessingexperiment.spark.statistics.collectors

/**
 * Collects the added results into a memory list which can be retrieved by calling the function `values()`
 */
class StatisticItemCollector : Collector {

    private val values = mutableListOf<StatisticItem>()
    override fun add(key: String, column: String, discriminator: String, value: Any) {
        values.add(
            StatisticItem(
                key,
                discriminator,
                value
            )
        )
    }

    override fun close() {

    }

    fun values(): List<StatisticItem> {
        return values.toList()
    }

    fun add(key: String, value: Any) {
        add(key, "", "", value)
    }
}

