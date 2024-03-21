package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Column count for the dataset.
 */
class ColCount(): Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {
        val value = data.columns().size
        collector.add("column count", "", "", value)
    }

    override fun of(col: String): Statistic {
        return ColCount()
    }
}
