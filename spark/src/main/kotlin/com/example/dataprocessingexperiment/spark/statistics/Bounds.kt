package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Calculates the MAX and MIN values for the given column.
 */
@Serializable
class Bounds(private val column: String) : Statistic, StatisticDefinition {

    override fun run(data: Dataset<Row>, collector: Collector) {
        Minimum(column).run(data, collector)
        Maximum(column).run(data, collector)
    }
}
