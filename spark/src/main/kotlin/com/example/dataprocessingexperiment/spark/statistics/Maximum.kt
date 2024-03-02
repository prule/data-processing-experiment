package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*

class Maximum(private val col: String) : Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {

        val value = data.agg(max(col)).head().get(0)
        collector.add("max", col, "", value)

    }

    override fun of(col: String): Statistic {
        return Maximum(col)
    }

}
