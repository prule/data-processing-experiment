package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.spark.sql.functions.*

/**
 *
 */
@Serializable
class ValuesProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val table: String,
    val column: String,
    val values: List<String>,
    val exclude: Boolean
) : Processor {

    override fun process(context: SparkContext) {
        val isin = col(column).isin(*values.toTypedArray())

        var dataSet = context.get(table)
        dataSet = if (exclude) dataSet.where(not(isin)) else dataSet.where(isin)
        context.set(table, dataSet)
    }

    override fun toString(): String {
        return "RemoveValuesProcessor(id='$id', name='$name', description='$description', table='$table', column='$column', values=$values)"
    }


}