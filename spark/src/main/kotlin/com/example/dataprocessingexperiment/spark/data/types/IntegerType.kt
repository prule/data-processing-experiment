package com.example.dataprocessingexperiment.spark.data.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 * Converts to an integer.
 *
 * The following column definition specifies an integer
 *
 * ```
 *       {
 *         name: "amount",
 *         description: "amount",
 *         type: "integer"
 *       }
 * ```
 *
 * @link https://spark.apache.org/docs/3.5.0/api/java/org/apache/spark/sql/types/IntegerType.html
 *
 * At the moment casting as integer will do a FLOOR. This could be extended to use the formats parameter to allow
 * FLOOR, CEIL, ROUND
 */
class IntegerType : Typer {
    override fun key(): String {
        return "integer"
    }

    override fun process(name: String, formats: List<String>?): Column {
        return functions.col(name).cast("integer").alias(name)
    }
}