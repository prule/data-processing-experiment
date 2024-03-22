package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*

@Serializable
class Minimum(private val col: String) :
    Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {

        val value = data.agg(min(col)).head().get(0)
        collector.add("min", col, "", value)

    }
}
