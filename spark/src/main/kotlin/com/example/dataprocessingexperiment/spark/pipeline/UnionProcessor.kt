package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.UnionTaskDefinition

/**
 * Uses the `union` property on the source to union several tables together into one `destination` table.
 */
class UnionProcessor : Processor {
    fun process(context: SparkContext, task: UnionTaskDefinition) {

        for (table in task.tables) {

            // if a union has been defined for this table
            if (task.destination.isNotBlank()) {
                // if we already have a dataframe to union to then perform the union
                if (context.contains(task.destination)) {
                    context.set(
                        task.destination,
                        context.get(task.destination).union(context.get(table))
                    )
                } else {
                    // otherwise this is the first one so just add the current dataframe
                    context.set(
                        task.destination,
                        context.get(table)
                    )
                }
            }
        }
    }

    override fun process(context: SparkContext, task: AbstractTaskDefinition) {
        process(context, task as UnionTaskDefinition)
    }
}