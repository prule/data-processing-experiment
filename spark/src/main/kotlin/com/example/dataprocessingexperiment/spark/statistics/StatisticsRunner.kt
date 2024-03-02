package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

class StatisticsRunner{

    fun process(data: Dataset<Row>, statistics: List<Statistic>, collector: Collector) {
        for (i in statistics) {
            i.run(data, collector)
        }
    }

}
