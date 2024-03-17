package com.example.dataprocessingexperiment.spark

/**
 * Iterates over all the table definitions and processes the `union` attribute.
 */
class UnionProcessor(private val context: SparkContext) {
    fun process() {
        // for each table
        context.tables.sources.forEach { fileSource ->

            val source = fileSource.id
            val destination = fileSource.union

            // if a union has been defined for this table
            if (!destination.isNullOrBlank()) {
                // if we already have a dataframe to union to then perform the union
                if (context.contains(destination)) {
                    context.add(
                        destination,
                        context.get(destination).union(context.get(source))
                    )
                } else {
                    // otherwise this is the first one so just add the current dataframe
                    context.add(
                        destination,
                        context.get(source)
                    )
                }
            }
        }

    }
}