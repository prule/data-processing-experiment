package com.example.dataprocessingexperiment.spark.data.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

class StringType : Typer {
    override fun key(): String {
        return "string"
    }

    override fun process(name: String, formats: List<String>?): Column {
        return functions.col(name).alias(name)
    }
}