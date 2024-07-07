package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.functions.regexp_replace

/**
 * Either include or exclude rows that contain values in a specified column.
 * It has parameters for the table, column, values, and whether to exclude or include the values.
 */
@Serializable
class AggregateSumProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val table: String,
    val sumColumn: String,
    val groupColumn: String,
    val resultTable: String
) : Processor {

    override fun process(context: SparkContext) {

        var dataSet = context.get(table)
        dataSet = dataSet.groupBy(groupColumn).agg(sum(sumColumn).alias(sumColumn))
        context.set(resultTable, dataSet)
    }

    override fun toString(): String {
        return "AggregateSumProcessor(id='$id', name='$name', description='$description', table='$table', sumColumn='$sumColumn', groupColumn='$groupColumn', resultTable='$resultTable')"
    }


}