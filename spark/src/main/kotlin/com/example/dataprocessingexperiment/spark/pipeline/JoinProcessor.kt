package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTask
import com.example.dataprocessingexperiment.tables.pipeline.JoinTask
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 *
 */
class JoinProcessor : Processor {

    fun process(context: SparkContext, joinDefinition: JoinTask) {

        // alias the tables so we avoid problems where columns are named the same in both datasets
        val table1 = context.get(joinDefinition.table1).alias(joinDefinition.table1)
        val table2 = context.get(joinDefinition.table2).alias(joinDefinition.table2)

        // build the "on" condition for the join
        var expr = functions.lit(true)
        joinDefinition.on.forEach { (key, value) ->
            expr = expr.and(
                functions.col("${joinDefinition.table1}.$key").equalTo(functions.col("${joinDefinition.table2}.$value"))
            )
        }

        val columns: MutableList<Column> = mutableListOf()

        // add all columns from table 1
        table1.columns().forEach {
            columns.add(functions.col("${joinDefinition.table1}.$it").alias(it))
        }

        // add the desired columns from table 2
        table2.columns().forEach {
            // only bring across the columns specified
            if (joinDefinition.columns.contains(it)) {
                // if the column name exists in both tables then prefix it with the table id
                if (table1.columns().contains(it)) {
                    columns.add(functions.col("${joinDefinition.table2}.$it").alias("${joinDefinition.table2}_$it"))
                } else {
                    // otherwise no need to prefix the column since there is no clash
                    columns.add(functions.col("${joinDefinition.table2}.$it").alias(it))
                }
            }
        }

        // join and select the columns
        val result = table1.join(table2, expr)
            .select(*columns.map { it }.toTypedArray())

        // add to context
        context.set(joinDefinition.destination, result)

    }

    override fun process(context: SparkContext, task: AbstractTask) {
        process(context, task as JoinTask)
    }
}