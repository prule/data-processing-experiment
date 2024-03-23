package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

@Serializable
class NullCount(private val columns: List<String>) : Statistic {
    override fun run(data: Dataset<Row>, collector: Collector) {
        val cols = if (columns.isNotEmpty()) {
            data.columns().intersect(columns)
        } else {
            data.columns().toList()
        }
        cols.forEach {
            val count = data.where(functions.col(it).isNull).count()

            collector.add(
                "NullCount", it, "", count
            )
        }
    }
}