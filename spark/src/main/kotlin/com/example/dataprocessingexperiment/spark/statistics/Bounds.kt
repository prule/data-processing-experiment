package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Calculates the MAX and MIN values for the given column.
 */
class Bounds(private val col: String) : Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {
        Minimum(col).run(data, collector)
        Maximum(col).run(data, collector)
    }

    override fun of(col: String): Statistic {
        return Bounds(col)
    }
}
