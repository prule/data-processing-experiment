package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.ScalaToKotlin
import com.example.dataprocessingexperiment.spark.data.SparkDataTypes
import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import mu.KotlinLogging
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField

/**
 * This counts the empty values for each column (or just the columns you specify).
 * "Empty" means different things depending on the data type of the column.
 * For numbers, it can be NULL or NaN.
 * For strings, it could be NULL, or a blank string, or whitespace.
 *
 * Also calculates the empty percentage = (empty count / rows * cols) * 100
 *
 * @param columns If an empty list, all columns will be included - otherwise just the named columns will have this statistic applied.
 */
@Serializable
class EmptyCount(private val columns: List<String>?) : Statistic {
    private val logger = KotlinLogging.logger {}

    @Transient
    private val sparkDataTypes = SparkDataTypes()

    override fun run(data: Dataset<Row>, collector: Collector) {
        val cols = if (!columns.isNullOrEmpty()) {
            data.columns().intersect(columns)
        } else {
            data.columns().toList()
        }

        val fields: List<StructField> = ScalaToKotlin.toList(data.schema().toList())

        // columns we want to select with null count criteria
        val colsToSelect = fields.filter { cols.contains(it.name()) }

        val colsWithConstraints: List<Column> = colsToSelect
            .map {
                functions.count_if(
                    sparkDataTypes.type(it.dataType()).nullPredicate.invoke(
                        col(it.name()),
                        it.nullable()
                    )
                ).alias(it.name())
            }

        // result set contains null count for each column in a column of the same name
        val result = data.select(*colsWithConstraints.map { it }.toTypedArray())

        var totalNulls = 0L
        colsToSelect.forEach {
            val count = result.select(it.name()).first()[0] as Long
            totalNulls += count
            collector.add(
                "EmptyCount", it.name(), "", count
            )
        }

        collector.add("EmptyPercentage", "", "", totalNulls * 100 / (data.count() * data.columns().size))

    }
}