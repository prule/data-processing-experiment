package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.statistics.StatisticRepository
import com.example.dataprocessingexperiment.spark.statistics.StatisticsRunner
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

class StatisticProcessor(val sparkSession: SparkSession) {

    fun process(statisticConfiguration: Statistics?, dataset: Dataset<Row>, path: String) {
        statisticConfiguration?.let {
            // transform from configuration to implementation
            val statistics = StatisticRepository().buildStatistics(statisticConfiguration)
            // instantiate a collector for gathering results
            val collector = SparkCollector(sparkSession, path)
            // process the statistics for the given dataset, and close the collector on completion
            // this will result in the statistics being written to CSV
            collector.use {
                StatisticsRunner().process(dataset, statistics, collector)
            }
        }
    }

}