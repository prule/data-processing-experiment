package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Calculates the count, mean, stddev, min and max for the specified columns (or all columns if none are specified).
 */
@Serializable
class Describe(private val columns: List<String>) : Statistic, StatisticDefinition {

    override fun run(data: Dataset<Row>, collector: Collector) {
        // if columns list is empty use all columns, otherwise use those specified
        val columnsToUse = if (this.columns.isEmpty()) {
            data.columns().toList()
        } else {
            columns
        }

        val describe = data.describe(*columnsToUse.map { it }.toTypedArray())

        // in a describe result the first column is the summary which details which measure the row is for.
        // the other columns have the value of that measure for each column.

//        +-------+-----------------+-----------------+
//        |summary|             val1|             val2|
//        +-------+-----------------+-----------------+
//        |  count|                4|                4|
//        |   mean|              3.5|              9.5|
//        | stddev|5.066228051190222|7.593857166596345|
//        |    min|               -1|                2|
//        |    max|               10|               20|
//        +-------+-----------------+-----------------+

        describe.columns().forEachIndexed { idx, col ->
            // skip the summary column
            if (idx > 0) {
                val rows = describe.select("summary", col).collectAsList()
                rows.forEach { row ->
                    val measure = row.getString(0)
                    val value = if (row.get(1) != null) {
                        row.get(1)
                    } else {
                        ""
                    }
                    collector.add("describe", col, measure, value)
                }
            }
        }
    }
}

