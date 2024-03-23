package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

interface Statistic: StatisticDefinition {
    fun run(data: Dataset<Row>, collector: Collector)
}