package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import mu.KotlinLogging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

class StatisticsRunner{
    private val logger = KotlinLogging.logger {}

    fun process(data: Dataset<Row>, statistics: List<Statistic>, collector: Collector) {
        for (i in statistics) {
            logger.info { "Running statistic: ${i.javaClass.name}" }
            i.run(data, collector)
        }
    }

}
