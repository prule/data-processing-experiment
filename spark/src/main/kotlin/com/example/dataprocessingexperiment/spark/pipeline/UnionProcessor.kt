package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable

/**
 * Uses the `union` property on the source to union several tables together into one `destination` table.
 */
@Serializable
class UnionProcessor(
    override   val id: String,
    override   val name: String,
    override   val description: String,
    val destination: String,
    val tables: List<String>
) : Processor {
    override fun process(context: SparkContext) {

        for (table in this.tables) {

            // if a union has been defined for this table
            if (this.destination.isNotBlank()) {
                // if we already have a dataframe to union to then perform the union
                if (context.contains(this.destination)) {
                    context.set(
                        this.destination,
                        context.get(this.destination).union(context.get(table))
                    )
                } else {
                    // otherwise this is the first one so just add the current dataframe
                    context.set(
                        this.destination,
                        context.get(table)
                    )
                }
            }
        }
    }

    override fun toString(): String {
        return "UnionProcessor(id='$id', name='$name', description='$description', destination='$destination', tables=$tables)"
    }

}