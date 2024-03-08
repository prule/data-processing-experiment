package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTask
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTask
import org.apache.spark.sql.functions

/**
 *
 */
class LiteralProcessor : Processor {
    fun process(context: SparkContext, literalDefinition: LiteralTask) {

        var table = context.get(literalDefinition.table)

        literalDefinition.columns.map {
            table = table.withColumn(it.key, functions.lit(it.value))
        }

        context.set(literalDefinition.table, table)
    }

    override fun process(context: SparkContext, task: AbstractTask) {
        process(context, task as LiteralTask)
    }
}