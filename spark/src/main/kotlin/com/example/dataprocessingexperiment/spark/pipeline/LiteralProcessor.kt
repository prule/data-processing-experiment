package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.spark.sql.functions

/**
 *
 */
@Serializable
class LiteralProcessor(
    override val id: String,
    override  val name: String,
    override  val description: String,
    val table: String,
    val columns: Map<String, String>
) : Processor {

    override fun process(context: SparkContext) {

        var dataSet = context.get(table)

        columns.map {
            dataSet = dataSet.withColumn(it.key, functions.lit(it.value))
        }

        context.set(table, dataSet)
    }

    override fun toString(): String {
        return "LiteralProcessor(id='$id', name='$name', description='$description', table='$table', columns=$columns)"
    }


}