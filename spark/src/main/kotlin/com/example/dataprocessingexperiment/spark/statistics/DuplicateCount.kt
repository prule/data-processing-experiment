package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

/**
 * Counts the number of duplicate rows in the dataset
 */
class DuplicateCount() : Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {

        val columns = data.columns()

        val result = data.groupBy(*columns.map { functions.col(it) }.toTypedArray())
            .count()
            .where(functions.col("count").gt(1))
            .select(functions.sum("count"))

        result.show()

        collector.add(
            "duplicate row count", "", "", result.first().get(0) ?: 0
        )

    }

    override fun of(col: String): Statistic {
        return DuplicateCount()
    }
}
