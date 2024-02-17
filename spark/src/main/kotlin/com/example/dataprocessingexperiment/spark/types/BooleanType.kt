package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

class BooleanType : Typer {
    override fun key(): String {
        return "boolean"
    }

    override fun process(name: String, formats: List<String>?): Column {
        return functions.col(name).cast("boolean").alias(name)
    }
}