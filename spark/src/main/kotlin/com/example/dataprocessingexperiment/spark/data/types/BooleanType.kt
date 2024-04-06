package com.example.dataprocessingexperiment.spark.data.types

import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

@Serializable
class BooleanType : Typer {
    override fun key(): String {
        return "boolean"
    }

    override fun process(name: String): Column {
        return functions.col(name).cast("boolean").alias(name)
    }
}