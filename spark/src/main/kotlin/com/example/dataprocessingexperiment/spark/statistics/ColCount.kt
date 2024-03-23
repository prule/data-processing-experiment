package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Column count for the dataset.
 */
@Serializable
class ColCount(): Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {
        val value = data.columns().size
        collector.add("column count", "", "", value)
    }

}
