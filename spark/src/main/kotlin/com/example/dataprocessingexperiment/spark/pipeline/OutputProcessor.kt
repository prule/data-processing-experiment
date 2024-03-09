package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.OutputTaskDefinition
import org.apache.spark.sql.functions

/**
 *
 */
class OutputProcessor : Processor {
    fun process(context: SparkContext, outputDefinition: OutputTaskDefinition) {

        val table = context.get(outputDefinition.table)

        table.write()
            .options(outputDefinition.options)
            .format(outputDefinition.format)
            .mode(outputDefinition.mode)
            .save(outputDefinition.path)
    }

    override fun process(context: SparkContext, task: AbstractTaskDefinition) {
        process(context, task as OutputTaskDefinition)
    }
}