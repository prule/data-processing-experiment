package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Row count for the dataset.
 *
 * Interesting because
 * - if we are receiving data at regular intervals we can set how its growing each time new data is delivered.
 * - can be used to compare between raw and valid datasets so we know the magnitude of invalid rows.
 */
class RowCount(): Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {
        val value = data.count()
        collector.add("row count", "", "", value)
    }

    override fun of(col: String): Statistic {
        return RowCount()
    }
}
