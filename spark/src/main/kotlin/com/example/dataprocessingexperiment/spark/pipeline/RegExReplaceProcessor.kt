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
class RegExReplaceProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val table: String,
    val column: String,
    val regex: String,
    val replacement: String
) : Processor {

    override fun process(context: SparkContext) {

        var dataSet = context.get(table)
        dataSet = dataSet.withColumn(column, regexp_replace(col(column), regex, replacement))

        context.set(table, dataSet)
    }

    override fun toString(): String {
        return "RegExReplaceProcessor(id='$id', name='$name', description='$description', table='$table', column='$column', regex='$regex', replacement='$replacement')"
    }


}