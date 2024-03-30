package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable

/**
 * Writes the dataframe to the given path.
 */
@Serializable
class OutputProcessor(
    override  val id: String,
    override   val name: String,
    override   val description: String,
    val table: String,
    val path: String,
    val format: String,
    val mode: String,
    val options: Map<String, String>
) : Processor {
    override fun process(context: SparkContext) {

        val table = context.get(this.table)

        table.write()
            .options(this.options)
            .format(this.format)
            .mode(this.mode)
            .save(this.path)
    }

    override fun toString(): String {
        return "OutputProcessor(id='$id', name='$name', description='$description', table='$table', path='$path', format='$format', mode='$mode', options=$options)"
    }


}