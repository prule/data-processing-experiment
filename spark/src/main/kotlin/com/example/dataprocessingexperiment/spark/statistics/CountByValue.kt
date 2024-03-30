package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*

/**
 * Row count grouping by the specified column.
 *
 * This will result in multiple rows (one for each value in the specified column).
 * It should only be used on low cardinality columns (for example those that contain enumerated values like "status").
 * The key will be CountByValue, the discriminator is the value and the value is the row count.
 */

@Serializable
class CountByValue(private val column: String) : Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {

        val alias = "value(${column})"

        val count = data.groupBy(
            trim(col(column)).alias(alias)
        ).count().orderBy(alias)

        count.toLocalIterator().forEach { row ->
            collector.add(
                "CountByValue",
                column,
                if (row.get(0) != null) row.get(0).toString() else "",
                row.get(1)
            )
        }
    }
}