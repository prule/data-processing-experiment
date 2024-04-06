package com.example.dataprocessingexperiment.spark.data.types

import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

@Serializable
class StringType : Typer {
    override fun key(): String {
        return "string"
    }

    override fun process(name: String): Column {
        return functions.col(name).alias(name)
    }
}