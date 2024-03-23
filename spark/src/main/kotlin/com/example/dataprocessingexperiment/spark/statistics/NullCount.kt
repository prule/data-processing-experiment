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
 */
@Serializable
class NullCount(private val columns: List<String>) : Statistic {
    private val logger = KotlinLogging.logger {}

    @Transient
    private val sparkDataTypes = SparkDataTypes()

    override fun run(data: Dataset<Row>, collector: Collector) {
        val cols = if (columns.isNotEmpty()) {
            data.columns().intersect(columns)
        } else {
            data.columns().toList()
        }

        val fields: List<StructField> = ScalaToKotlin.toList(data.schema().toList())

        // columns we want to select with null count criteria
        val colsToSelect: List<Column> = fields.filter { cols.contains(it.name()) }
            .filter { sparkDataTypes.type(it.dataType()) != null }
            .map {
                functions.count_if(
                    sparkDataTypes.type(it.dataType())?.nullPredicate?.invoke(
                        col(it.name()),
                        it.nullable()
                    )
                ).alias(it.name())
            }

        // result set contains null count for each column in a column of the same name
        val result = data.select(*colsToSelect.map { it }.toTypedArray())

        fields.forEach {
            collector.add(
                "NullCount", it.name(), "", result.select(it.name()).first()[0]
            )
        }

    }
}