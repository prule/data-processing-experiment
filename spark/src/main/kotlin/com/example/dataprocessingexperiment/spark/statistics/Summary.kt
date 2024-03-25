package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

/**
 * Calculates the count, mean, stddev, min and max for the specified columns (or all columns if none are specified).
 * Available statistics:
 *  - count
 *  - mean
 *  - stddev
 *  - min
 *  - max
 *  - arbitrary approximate percentiles specified as a percentage (e.g. 75%)
 *  - count_distinct
 *  - approx_count_distinct
 */
@Serializable
class Summary(private val columns: List<String>, private val statistics: List<String>) : Statistic,
    StatisticDefinition {

    private val defaultStats = listOf(
        "count",
        "mean",
        "stddev",
        "min",
        "max",
        "25%", "50%", "75%"
    )

    private val otherStats = listOf(
        "count_distinct",
        "approx_count_distinct"
    )

    override fun run(data: Dataset<Row>, collector: Collector) {
        val columnsToSelect = calculateColumns(this.columns, data.columns().toList())
        val statisticsToSelect = calculateStatistics(this.statistics)

        val summary = data
            .select(*columnsToSelect.map { functions.col(it) }.toTypedArray())
            .summary(*statisticsToSelect.map { it }.toTypedArray())

        summary.show()
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

        summary.columns().forEachIndexed { idx, col ->
            // skip the summary column
            if (idx > 0) {
                val rows = summary.select("summary", col).collectAsList()
                rows.forEach { row ->
                    val measure = row.getString(0)
                    val value = if (row.get(1) != null) {
                        row.get(1)
                    } else {
                        ""
                    }
                    collector.add("Summary", col, measure, value)
                }
            }
        }
    }

    // if columns list is empty use all columns, otherwise use those specified
    private fun calculateColumns(
        columns: List<String>,
        allColumns: List<String>
    ): List<String> {
        val columnsToUse = if (columns.isEmpty()) {
            allColumns
        } else {
            columns
        }
        return columnsToUse
    }

    private fun calculateStatistics(providedStats: List<String>): List<String> {
        if (providedStats.isEmpty()) {
            return defaultStats
        } else {
            return providedStats
        }
    }
}

