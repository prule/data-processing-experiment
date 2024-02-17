package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
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