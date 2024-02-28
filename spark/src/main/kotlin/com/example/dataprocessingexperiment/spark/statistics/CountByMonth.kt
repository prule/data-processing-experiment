package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*

/**
 * Row count grouping by Year-Month.
 *
 * This statistic needs a DATE column from which it will format the date to `yyyy-MM` and then group by this.
 * This will result in multiple rows (one for each yyyy-MM combination).
 * The key will be CountByMonth, the discriminator is the yyyy-MM value (eg 2000-03) and the value is the row count.
 */
class CountByMonth(private val col: String) : Statistic {

    override fun run(data: Dataset<Row>, collector: Collector) {

        val alias = "month(${col})"
        val count = data.groupBy(date_format(col(col), "yyyy-MM").alias(alias)).count().orderBy(alias)

        count.toLocalIterator()
            .forEach { row -> collector.add("CountByMonth", col, row.get(0)?.toString() ?: "", row.get(1)) }
    }

    override fun of(col: String): Statistic {
        return CountByMonth(col)
    }

}
