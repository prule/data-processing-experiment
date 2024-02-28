package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

interface Statistic {
    fun run(data: Dataset<Row>, collector: Collector)
    fun of(col: String): Statistic
}