package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

class IntegerType : Typer {
    override fun key(): String {
        return "integer"
    }

    override fun process(name: String, type: String, formats: List<String>?): Column {
        return functions.col(name).cast("integer").alias(name)
    }
}